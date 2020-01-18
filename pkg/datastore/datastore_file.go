/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package datastore

import (
	"encoding/binary"
	"log"
	"os"
	"sync"

	"database/sql"

	_ "github.com/mattn/go-sqlite3"

	"github.com/lulf/slim/pkg/api"
)

type fileDatastore struct {
	dataDir     string
	topicDbFile string
	topicDb     *sql.DB
	topicLock   *sync.Mutex
	topics      map[string]*topicIndex

	maxBufferSize int64
	maxLogSize    int64
	maxLogAge     int64
}

type topicIndex struct {
	indexFile string
	dataFile  string
	index     index
	writeIdx  int64
	flushIdx  int64

	data []*api.Message
}

type index struct {
	header indexHeader
}

type indexHeader struct {
	size      int64
	location  int64
	dataFile  string
	nextIndex string
}

type indexRecord struct {
	index    int64
	location int64
	size     int64
}

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
	// Update filehandles
	dataFileHandles := make(map[string]*os.File, 0)
	indexFileHandles := make(map[string]*os.File, 0)
	indexes := make(map[string]*topicIndex, 0)

	{
		fs.topicLock.Lock()
		defer fs.topicLock.Unlock()
		for topic, index := range ds.topics {
			if _, ok := dataFileHandles[topic]; !ok {
				dh, err := os.Open(index.dataFile)
				if err != nil {
					return err
				}
				dataFileHandles[topic] = dh
			}

			if _, ok := indexFileHandles[topic]; !ok {
				dh, err := os.Open(index.indexFile)
				if err != nil {
					return err
				}
				indexFileHandles[topic] = dh
			}

			indexes[topic] = index
		}
	}

	for topic, index := range indexes {
		log.Println("Flushing topic", topic)

	}
}

func (ds fileDatastore) Close() {
}

func NewFileDatastore(dataDir string, maxLogAge int64, maxLogSize int64) (*fileDatastore, error) {

	topicDbFile := dataDir + string(os.PathSeparator) + "store.db"
	db, err := sql.Open("sqlite3", topicDbFile)
	if err != nil {
		log.Print("Opening Database:", err)
		return nil, err
	}

	return &fileDatastore{
		dataDir:     dataDir,
		topicDb:     db,
		topicLock:   &sync.Mutex{},
		topicDbFile: topicDbFile,
		maxLogSize:  maxLogSize,
		maxLogAge:   maxLogAge,
		topics:      make(map[string]*topicIndex),
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
		// TODO: Read from disk
		ds.topics[topic] = &topicIndex{
			indexFile: "data" + string(os.PathSeparator) + "0" + string(os.PathSeparator) + "index.dat",
			dataFile:  "data" + string(os.PathSeparator) + "0" + string(os.PathSeparator) + "data.bin",
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

	ds.topicLock.Lock()
	ds.topics[topic] = &topicIndex{
		indexFile: "data" + string(os.PathSeparator) + "0" + string(os.PathSeparator) + "index0.dat",
		dataFile:  "data" + string(os.PathSeparator) + "0" + string(os.PathSeparator) + "data0.bin",
		index: &index{
			header: &indexHeader{
				size:      0,
				location:  0,
				dataFile:  "data0.bin",
				nextIndex: "",
			},
			records: make([]indexRecord, 0),
		},
		data: make([]*api.Message, 0),
	}
	ds.topicLock.Unlock()

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
	index.Lock()
	defer index.Unlock()
	store.data = append(store.data, message)
	record := indexRecord{
		index:    message.Id,
		location: index.header.location,
		size:     len(message.Payload),
	}
	index.records = append(index.records, record)
	index.header.location += record.size
}

func (ds *fileDatastore) GarbageCollect(topic string) error {
	return nil
}

func (ds *fileDatastore) ListMessages(topic string, limit int64, offset int64, insertionTime int64) ([]*api.Message, error) {
	return nil, nil
}

// Read algorithm
// 1. Locate topic
// 2. Lookup offset in cache
// 3. If in cache,
func (ds *fileDatastore) StreamMessages(topic string, offset int64, callback StreamingFunc) error {
	store := ds.topics[topic]
	index := store.index

	index.Lock()
	defer index.Unlock()
	for _, record := range index.records {

	}
	store.data = append(store.data, message)
	record := indexRecord{
		index:    message.Id,
		location: index.header.location,
		size:     len(message.Payload),
	}
	index.records = append(index.records, record)
	index.header.location += record.size
}

func (ds *fileDatastore) NumMessages(topic string) (int64, error) {
	return 0, nil
}

func (ds *fileDatastore) LastMessageId(topic string) (int64, error) {
	return 0, nil
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
