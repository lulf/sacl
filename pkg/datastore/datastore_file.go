/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package datastore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"golang.org/x/exp/mmap"
	"log"
	"os"
	"sync"
	"time"

	"database/sql"

	_ "github.com/mattn/go-sqlite3"

	"github.com/lulf/slim/pkg/api"
)

type fileDatastore struct {
	dataDir string
	topicDb *sql.DB

	topics map[string]*topicData

	maxBufferSize int64
	maxLogSize    int64
	maxLogAge     int64
}

type topicData struct {
	lock  *sync.Mutex
	index *index

	reader    *mmap.ReaderAt
	dataFile  *os.File
	indexFile *os.File
}

type index struct {
	nextLoc int64
	records []indexRecord
}

type indexRecord struct {
	data     *api.Message
	id       int64
	location int64
	size     int64
}

/*
type indexHeader struct {
	size      int64
	location  int64
	dataFile  string
	nextIndex string
}
*/

// Flush Algorithm
// For each topic:
// 1. Atomically compare flush vs write index
// 2. If available, fetch next message
// 3. Append message to current position known in header
// 4. Increment header location and size
// 5. Append index entry with index, location and size
// 6. Write index header
// 7. Flush data & index (in order)
// 8. Update flushIdx
func (ds fileDatastore) Flush() error {
	for topic, data := range ds.topics {
		data.lock.Lock()
		defer data.lock.Unlock()

		log.Println("Flushing topic", topic)
		index := data.index
		for i, _ := range index.records {
			record := &index.records[i]
			if record.data != nil {
				_, err := data.dataFile.Write(record.data.Payload)
				if err != nil {
					return err
				}
				buf := new(bytes.Buffer)
				binary.Write(buf, binary.LittleEndian, record.id)
				binary.Write(buf, binary.LittleEndian, record.location)
				binary.Write(buf, binary.LittleEndian, record.size)
				binary.Write(buf, binary.LittleEndian, int64(0))

				_, err = data.indexFile.Write(buf.Bytes())
				if err != nil {
					return err
				}
				record.data = nil
			}
		}
		err := data.dataFile.Sync()
		if err != nil {
			return err
		}
		err = data.indexFile.Sync()
		if err != nil {
			return err
		}
		// Reopen mmaped file
		err = data.reader.Close()
		if err != nil {
			return err
		}
		reader, err := mmap.Open(dataFileName(topic))
		if err != nil {
			return err
		}
		data.reader = reader

	}
	return nil
}

func (ds fileDatastore) Close() {
	for _, data := range ds.topics {
		data.dataFile.Close()
		data.indexFile.Close()
	}
}

func indexFileName(topic string) string {
	return fmt.Sprintf("%s/index.bin", dataDirName(topic))
}

func dataFileName(topic string) string {
	return fmt.Sprintf("%s/data.bin", dataDirName(topic))
}

func dataDirName(topic string) string {
	return fmt.Sprintf("data/%s/0", topic)
}

func NewFileDatastore(dataDir string, maxLogAge int64, maxLogSize int64) (*fileDatastore, error) {

	topicDbFile := dataDir + string(os.PathSeparator) + "store.db"

	db, err := sql.Open("sqlite3", topicDbFile)
	if err != nil {
		log.Print("Opening Database:", err)
		return nil, err
	}

	return &fileDatastore{
		dataDir:    dataDir,
		topicDb:    db,
		maxLogSize: maxLogSize,
		maxLogAge:  maxLogAge,
		topics:     make(map[string]*topicData),
	}, nil
}

func (ds *fileDatastore) Initialize() error {
	tableCreate := "create table if not exists topics (name text not null primary key, data_dir text, partitions integer);"
	_, err := ds.topicDb.Exec(tableCreate)
	if err != nil {
		log.Print("Creating topics table:", err)
		return err
	}

	topics, err := ds.ListTopics()
	if err != nil {
		log.Print("Listing topics:", err)
		return err
	}
	for _, topic := range topics {
		indexFh, err := os.OpenFile(indexFileName(topic), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}

		dataFh, err := os.OpenFile(dataFileName(topic), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}

		reader, err := mmap.Open(dataFileName(topic))
		if err != nil {
			return err
		}

		idx := &index{
			nextLoc: 0,                      // TODO: Read from index file header
			records: make([]indexRecord, 0), // Read from index file
		}

		ds.topics[topic] = &topicData{
			lock:      &sync.Mutex{},
			indexFile: indexFh,
			dataFile:  dataFh,
			reader:    reader,
			index:     idx,
		}
	}
	return nil
}

func (ds *fileDatastore) CreateTopic(topic string) error {
	tx, err := ds.topicDb.Begin()
	if err != nil {
		log.Print("Starting transaction:", err)
		return err
	}

	createTopic, err := tx.Prepare("INSERT INTO topics (name, data_dir, partitions) values(?, ?, 0);")
	if err != nil {
		log.Print("Preparing create topic:", err)
		return err
	}
	defer createTopic.Close()

	_, err = createTopic.Exec(topic, topic)
	if err != nil {
		log.Print("Create topic:", topic, err)
		return err
	}

	err = os.MkdirAll(dataDirName(topic), os.ModePerm)
	if err != nil {
		return err
	}

	indexFile, err := os.OpenFile(indexFileName(topic), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	dataFile, err := os.OpenFile(dataFileName(topic), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	reader, err := mmap.Open(dataFileName(topic))
	if err != nil {
		return err
	}
	ds.topics[topic] = &topicData{
		lock: &sync.Mutex{},
		index: &index{
			nextLoc: 0,
			records: make([]indexRecord, 0),
		},
		reader:    reader,
		indexFile: indexFile,
		dataFile:  dataFile,
	}

	return tx.Commit()
}

// Write algorithm
// 1. Locate topic
// 2. Compare flushIdx with writeIdx
// 3. If room, put message in buffer and increment idx
func (ds *fileDatastore) InsertMessage(topic string, message *api.Message) error {
	store := ds.topics[topic]

	index := store.index
	// TODO: Use circular fast buffers with atomic operations
	store.lock.Lock()
	defer store.lock.Unlock()
	record := indexRecord{
		data:     message,
		id:       message.Id,
		size:     int64(len(message.Payload)),
		location: index.nextLoc,
	}
	index.records = append(index.records, record)
	index.nextLoc += record.size
	return nil
}

func (ds *fileDatastore) GarbageCollect(topic string) error {
	return nil
}

func (ds *fileDatastore) ListMessages(topic string, limit int64, offset int64, insertionTime int64) ([]*api.Message, error) {
	return nil, nil
}

// Read algorithm
// 1. Locate topic
// 2. Lookup id in index using binary search
// 3. If in cache,
func (ds *fileDatastore) StreamMessages(topic string, offset int64, callback StreamingFunc) error {
	store := ds.topics[topic]
	index := store.index

	store.lock.Lock()
	defer store.lock.Unlock()
	if len(index.records) == 0 {
		return nil
	}

	// TODO: Use binary search to find record
	for _, record := range index.records {
		if record.id >= offset {
			data := record.data
			if data == nil {
				// Do memory mapped lookup
				d := make([]byte, record.size)
				_, err := store.reader.ReadAt(d, record.location)
				if err != nil {
					return err
				}
				data = api.NewMessage(record.id, d)
			}
			err := callback(data)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (ds *fileDatastore) NumMessages(topic string) (int64, error) {
	store := ds.topics[topic]
	index := store.index

	store.lock.Lock()
	defer store.lock.Unlock()
	return int64(len(index.records)), nil
}

func (ds *fileDatastore) LastMessageId(topic string) (int64, error) {
	store := ds.topics[topic]
	index := store.index

	store.lock.Lock()
	defer store.lock.Unlock()
	if len(index.records) == 0 {
		return -1, nil
	} else {
		return index.records[0].id, nil
	}
}

func (ds *fileDatastore) ListTopics() ([]string, error) {
	stmt, err := ds.topicDb.Prepare("SELECT name FROM topics")
	if err != nil {
		log.Print("Preparing query:", err)
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		log.Print("Executing query:", err)
		return nil, err
	}

	var topics []string = make([]string, 0)
	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		if err != nil {
			log.Print("Scan row:", err)
			return nil, err
		}

		topics = append(topics, name)
	}

	return topics, nil
}

func Flusher(flushInterval time.Duration, ds Datastore) {
	for {
		time.Sleep(flushInterval * time.Second)
		log.Println("Flushing datastore")
		err := ds.Flush()
		if err != nil {
			log.Println("Error flush datastore:", err)
		}
	}
}
