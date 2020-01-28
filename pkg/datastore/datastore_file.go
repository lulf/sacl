/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package datastore

import (
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"database/sql"

	_ "github.com/mattn/go-sqlite3"

	"github.com/lulf/slim/pkg/api"
)

type fileDatastore struct {
	dataDir string
	topicDb *sql.DB

	topics map[string]*topicData

	maxSegmentSize int64
	maxBufferSize  int64
	maxLogSize     int64
	maxLogAge      int64
}

type topicData struct {
	dataFile  *mappedFile
	indexFile *mappedFile
}

func (ds fileDatastore) Flush() error {
	for _, data := range ds.topics {
		data.dataFile.Sync()
		data.indexFile.Sync()
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
		dataDir:        dataDir,
		topicDb:        db,
		maxSegmentSize: 10 * 1024 * 1024,
		maxLogSize:     maxLogSize,
		maxLogAge:      maxLogAge,
		topics:         make(map[string]*topicData),
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
		indexFile, err := OpenMapped(indexFileName(topic))
		if err != nil {
			return err
		}

		dataFile, err := OpenMapped(dataFileName(topic))
		if err != nil {
			return err
		}

		ds.topics[topic] = &topicData{
			indexFile: indexFile,
			dataFile:  dataFile,
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

	indexFile, err := OpenMapped(indexFileName(topic))
	if err != nil {
		return err
	}

	dataFile, err := OpenMapped(dataFileName(topic))
	if err != nil {
		return err
	}
	ds.topics[topic] = &topicData{
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

	// log.Println("Appending message", message, store.nextFileOffset)
	dataOffset, err := store.dataFile.AppendMessage(message)
	if err != nil {
		return err
	}

	err = store.indexFile.AppendIndex(message.Offset, dataOffset)
	if err != nil {
		return err
	}
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

	if offset < 0 {
		offset = 0
	}
	for {
		// log.Println("Streaming message", offset)
		fileOffset, err := store.indexFile.ReadFileOffset(offset)
		if err != nil {
			// log.Println("ERROR", err)
			if err == io.EOF {
				// log.Println("Stoooping stream")
				return nil
			}
			return err
		}
		// log.Println("Located file offset", fileOffset)
		message, err := store.dataFile.ReadMessageAt(fileOffset)
		if err != nil {
			return err
		}
		//log.Println("Located message", message)

		err = callback(message)
		if err != nil {
			return err
		}
		offset += 1
	}
	return nil
}

func (ds *fileDatastore) NumMessages(topic string) (int64, error) {
	return 0, nil
}

func (ds *fileDatastore) LastOffset(topic string) (int64, error) {
	data := ds.topics[topic]
	return data.indexFile.ReadLastOffset()
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
