/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package datastore

import (
	"bytes"
	"encoding/binary"
	"golang.org/x/exp/mmap"
	"io"
	// "log"
	"os"
	"sync"
	"sync/atomic"

	"github.com/lulf/slim/pkg/api"
)

type mappedFile struct {
	path   string
	lock   *sync.RWMutex
	handle *os.File
	reader *mmap.ReaderAt

	size         int64
	startOffset  int64
	fileLocation int64
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

const METADATA_SZ int64 = int64(16)

func OpenMapped(path string) (*mappedFile, error) {
	exists := true
	finfo, err := os.Stat(path)
	if os.IsNotExist(err) {
		exists = false
	}
	handle, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	reader, err := mmap.Open(path)
	if err != nil {
		return nil, err
	}

	startOffset := int64(0)
	fileLocation := int64(16)

	if !exists {
		finfo, err = os.Stat(path)
		if err != nil {
			return nil, err
		}
	}
	if finfo.Size() > 0 {
		hdr := make([]byte, 16)
		_, err := reader.ReadAt(hdr, 0)
		if err != nil {
			return nil, err
		}
		startOffset = int64(binary.LittleEndian.Uint64(hdr[0:8]))
		fileLocation = int64(binary.LittleEndian.Uint64(hdr[8:16]))
	}

	file := &mappedFile{
		path:         path,
		lock:         &sync.RWMutex{},
		handle:       handle,
		reader:       reader,
		startOffset:  startOffset,
		fileLocation: fileLocation,
		size:         finfo.Size(),
	}
	err = file.ensureAvailable(16)
	if err != nil {
		return nil, err
	}
	if !exists {
		err = file.updateMetadata(startOffset, fileLocation)
		if err != nil {
			return nil, err
		}
	}
	return file, nil
}

func (f *mappedFile) Close() error {
	f.reader.Close()
	f.handle.Close()
	return nil
}

func (f *mappedFile) ensureAvailable(sz int64) error {
	//log.Println("ensureAvailable", sz, f.available, f.size)
	if f.size-f.fileLocation < sz {
		f.lock.Lock()
		defer f.lock.Unlock()
		newSize := f.size + (10 * 1024 * 1024)
		err := f.handle.Truncate(newSize)
		if err != nil {
			return err
		}
		f.size = newSize

		// Reopen mmaped file
		err = f.reader.Close()
		if err != nil {
			return err
		}
		reader, err := mmap.Open(f.path)
		if err != nil {
			return err
		}
		f.reader = reader
	}
	//log.Println("after ensureAvailable", sz, f.available, f.size)
	return nil
}

func (f *mappedFile) AppendMessage(message *api.Message) (int64, error) {
	nbytes := int64(len(message.Payload) + 16)
	err := f.ensureAvailable(nbytes)
	if err != nil {
		return -1, err
	}
	szbuf := new(bytes.Buffer)
	binary.Write(szbuf, binary.LittleEndian, message.Offset)
	binary.Write(szbuf, binary.LittleEndian, int64(len(message.Payload)))
	// log.Println("Writing", f.path, message.Offset, len(message.Payload), message.Payload, f.fileLocation)

	dataOffset := f.fileLocation
	_, err = f.handle.WriteAt(szbuf.Bytes(), f.fileLocation)
	if err != nil {
		return -1, err
	}
	_, err = f.handle.WriteAt(message.Payload, f.fileLocation+16)
	if err != nil {
		return -1, err
	}
	return dataOffset, f.updateMetadata(f.startOffset, atomic.AddInt64(&f.fileLocation, nbytes))
}

func (f *mappedFile) Sync() error {
	return f.handle.Sync()
}

func (f *mappedFile) updateMetadata(startOffset int64, fileLocation int64) error {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, startOffset)
	binary.Write(buf, binary.LittleEndian, fileLocation)
	// log.Println("Update metadata", f.path, startOffset, fileLocation)
	_, err := f.handle.WriteAt(buf.Bytes(), 0)
	return err
}

func (f *mappedFile) AppendIndex(offset int64, fileOffset int64) error {
	nbytes := int64(16)
	err := f.ensureAvailable(nbytes)
	if err != nil {
		return err
	}
	indexBuf := new(bytes.Buffer)
	binary.Write(indexBuf, binary.LittleEndian, offset)
	binary.Write(indexBuf, binary.LittleEndian, fileOffset)
	// log.Println("Indexing", f.path, offset, fileOffset, f.fileLocation)
	_, err = f.handle.WriteAt(indexBuf.Bytes(), f.fileLocation)
	if err != nil {
		return err
	}
	return f.updateMetadata(f.startOffset, atomic.AddInt64(&f.fileLocation, nbytes))
}

func (f *mappedFile) ReadMessageAt(fileLocation int64) (*api.Message, error) {
	f.lock.RLock()
	defer f.lock.RUnlock()

	hdr := make([]byte, 16)
	_, err := f.reader.ReadAt(hdr, fileLocation)
	if err != nil {
		return nil, err
	}
	offset := int64(binary.LittleEndian.Uint64(hdr[0:8]))
	sz := int64(binary.LittleEndian.Uint64(hdr[8:16]))
	// log.Println("ReadMessageAt", f.path, offset, sz, fileLocation)

	d := make([]byte, sz)
	_, err = f.reader.ReadAt(d, fileLocation+16)
	if err != nil {
		return nil, err
	}
	data := api.NewMessage(offset, d)
	return data, nil
}

func (f *mappedFile) ReadFileOffset(offset int64) (int64, error) {
	f.lock.RLock()
	defer f.lock.RUnlock()

	loc := METADATA_SZ + f.startOffset + (offset * 16)
	// log.Println("Read File Offset", f.path, loc, f.fileLocation)
	if loc > atomic.LoadInt64(&f.fileLocation)-16 {
		return -1, io.EOF
	}
	hdr := make([]byte, 16)
	_, err := f.reader.ReadAt(hdr, loc)
	if err != nil {
		return -1, err
	}
	// log.Println("Record", int64(binary.LittleEndian.Uint64(hdr[0:8])), int64(binary.LittleEndian.Uint64(hdr[8:16])))
	return int64(binary.LittleEndian.Uint64(hdr[8:16])), nil
}

func (f *mappedFile) ReadLastOffset() (int64, error) {
	f.lock.RLock()
	defer f.lock.RUnlock()

	loc := f.fileLocation - 16
	// log.Println("Read File Offset", f.path, loc, f.fileLocation)
	if loc < 16 {
		return -1, nil
	}
	hdr := make([]byte, 16)
	_, err := f.reader.ReadAt(hdr, loc)
	if err != nil {
		return -1, err
	}
	// log.Println("Record", int64(binary.LittleEndian.Uint64(hdr[0:8])), int64(binary.LittleEndian.Uint64(hdr[8:16])))
	return int64(binary.LittleEndian.Uint64(hdr[0:8])), nil
}
