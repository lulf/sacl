/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package datastore

import (
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

	maxLogSize int64
	maxLogAge  int64
}

type topicIndex struct {
	indexFile string
	dataFile  string
	index     index
	data      []*api.Message
}

type index struct {
	header indexHeader
	index  []indexRecord
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
		ds.topics[topic] = &topicIndex{}
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
	ds.topics[topic] = &topicIndex{}
	ds.topicLock.Unlock()

	return tx.Commit()
}

func (ds *fileDatastore) InsertMessage(topic string, message *api.Message) error {
	return nil
}

func (ds *fileDatastore) GarbageCollect(topic string) error {
	return nil
}

func (ds *fileDatastore) ListMessages(topic string, limit int64, offset int64, insertionTime int64) ([]*api.Message, error) {
	return nil, nil
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
