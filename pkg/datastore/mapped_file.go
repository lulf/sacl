/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package datastore

import (
	"bytes"
	"encoding/binary"
	"golang.org/x/exp/mmap"
	//	"log"
	"os"
	"sync"

	"github.com/lulf/slim/pkg/api"
)

type mappedFile struct {
	path   string
	lock   *sync.RWMutex
	handle *os.File
	reader *mmap.ReaderAt

	size      int64
	available int64
}

func OpenMapped(path string) (*mappedFile, error) {
	handle, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	reader, err := mmap.Open(path)
	if err != nil {
		return nil, err
	}

	finfo, err := handle.Stat()
	if err != nil {
		return nil, err
	}

	return &mappedFile{
		path:      path,
		lock:      &sync.RWMutex{},
		handle:    handle,
		reader:    reader,
		size:      finfo.Size(),
		available: finfo.Size(),
	}, nil
}

func (f *mappedFile) Close() error {
	f.reader.Close()
	f.handle.Close()
	return nil
}

func (f *mappedFile) ensureAvailable(sz int64) error {
	//log.Println("ensureAvailable", sz, f.available, f.size)
	if f.available < sz {
		f.lock.Lock()
		defer f.lock.Unlock()
		newSize := f.size + (10 * 1024 * 1024)
		err := f.handle.Truncate(newSize)
		if err != nil {
			return err
		}
		f.size = newSize
		f.available += 10 * 1024 * 1024

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

func (f *mappedFile) AppendMessage(message *api.Message) error {
	nbytes := int64(len(message.Payload) + 16)
	err := f.ensureAvailable(nbytes)
	if err != nil {
		return err
	}
	szbuf := new(bytes.Buffer)
	binary.Write(szbuf, binary.LittleEndian, message.Offset)
	binary.Write(szbuf, binary.LittleEndian, int64(len(message.Payload)))
	//log.Println("Writing", f.path, szbuf.Bytes())
	_, err = f.handle.Write(szbuf.Bytes())
	if err != nil {
		return err
	}
	_, err = f.handle.Write(message.Payload)
	if err != nil {
		return err
	}
	f.available -= nbytes
	return nil
}

func (f *mappedFile) Sync() error {
	return f.handle.Sync()
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
	//log.Println("Indexing", f.path, indexBuf.Bytes())
	_, err = f.handle.Write(indexBuf.Bytes())
	if err != nil {
		return err
	}
	f.available -= nbytes
	return nil
}

func (f *mappedFile) ReadMessageAt(fileOffset int64) (*api.Message, error) {
	f.lock.RLock()
	defer f.lock.RUnlock()

	hdr := make([]byte, 16)
	_, err := f.reader.ReadAt(hdr, fileOffset)
	if err != nil {
		return nil, err
	}
	offset := int64(binary.LittleEndian.Uint64(hdr[0:8]))
	sz := int64(binary.LittleEndian.Uint64(hdr[8:16]))
	//log.Println("ReadMessageAt", f.path, hdr, offset, sz, fileOffset)

	d := make([]byte, sz)
	_, err = f.reader.ReadAt(d, fileOffset+16)
	if err != nil {
		return nil, err
	}
	data := api.NewMessage(offset, d)
	return data, nil
}

func (f *mappedFile) ReadFileOffset(offset int64) (int64, error) {
	f.lock.RLock()
	defer f.lock.RUnlock()

	o := offset * 16
	hdr := make([]byte, 16)
	_, err := f.reader.ReadAt(hdr, o)
	if err != nil {
		return -1, err
	}
	//log.Println("Read File Offset", f.path, hdr)
	fileOffset := int64(binary.LittleEndian.Uint64(hdr[8:16]))
	if fileOffset < o {
		return -1, nil
	}
	return fileOffset, nil

}
