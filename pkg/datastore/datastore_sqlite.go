/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package datastore

import (
	"database/sql"
	"fmt"
	"github.com/lulf/slim/pkg/api"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"time"
)

func (ds SqlDatastore) Close() {
	ds.handle.Close()
}

func NewSqliteDatastore(fileName string, maxLogAge int64, maxLogSize int64) (*SqlDatastore, error) {
	db, err := sql.Open("sqlite3", fileName)
	if err != nil {
		log.Print("Opening Database:", err)
		return nil, err
	}

	return &SqlDatastore{
		handle:     db,
		maxLogSize: maxLogSize,
		maxLogAge:  maxLogAge,
	}, nil
}

func (ds SqlDatastore) Initialize() error {
	tableCreate := "create table if not exists topics (name text not null primary key, table_name text);"
	_, err := ds.handle.Exec(tableCreate)
	if err != nil {
		log.Print("Creating topics table:", err)
		return err
	}
	return nil
}

func (ds SqlDatastore) CreateTopic(topic string) error {
	// Create initial database table
	tx, err := ds.handle.Begin()
	if err != nil {
		log.Print("Starting transaction:", err)
		return err
	}

	createTopic, err := tx.Prepare("INSERT INTO topics (name, table_name) values(?, ?);")
	if err != nil {
		log.Print("Preparing create topic:", err)
		return err
	}
	defer createTopic.Close()

	topicTableName := getTopicTableName(topic)
	createTable, err := tx.Prepare(fmt.Sprintf("create table if not exists %s (id integer not null primary key, insertion_time integer, payload text);", topicTableName))
	if err != nil {
		log.Print("Preparing create table:", err)
		return err
	}
	defer createTable.Close()

	_, err = createTopic.Exec(topic, topicTableName)
	if err != nil {
		log.Print("Create topic:", topic, err)
		return err
	}

	_, err = createTable.Exec()
	if err != nil {
		log.Print("Creating topic table:", topicTableName, err)
		return err
	}
	return tx.Commit()
}

func (ds SqlDatastore) InsertMessage(topic string, message *api.Message) error {
	tx, err := ds.handle.Begin()
	if err != nil {
		log.Print("Starting transaction:", err)
		return err
	}

	insertionTime := time.Now().UTC().Unix()

	insertStmt, err := tx.Prepare(fmt.Sprintf("INSERT INTO %s (id, insertion_time, payload) values(?, ?, ?)", getTopicTableName(topic)))
	if err != nil {
		log.Print("Preparing insert statement:", err)
		return err
	}
	defer insertStmt.Close()

	_, err = insertStmt.Exec(message.Id, insertionTime, message.Payload)
	if err != nil {
		log.Print("Inserting entry:", err)
		return err
	}

	return tx.Commit()
}

func (ds SqlDatastore) GarbageCollect(topic string) error {
	tx, err := ds.handle.Begin()
	if err != nil {
		log.Print("Starting transaction:", err)
		return err
	}

	if ds.maxLogSize > 0 {
		/*
			removeStmt, err := tx.Prepare(fmt.Sprintf("DELETE FROM %s WHERE id NOT IN (SELECT id FROM %s ORDER BY id DESC LIMIT ?)")
			if err != nil {
				log.Print("Preparing remove statement:", err)
				return err
			}
			defer removeStmt.Close()*/

		/*
			_, err = removeStmt.Exec()
			if err != nil {
				log.Print("Removing oldest entry:", err)
				return err
			}
		*/
	}

	var removeByAge *sql.Stmt
	if ds.maxLogAge > 0 {
		now := time.Now().UTC().Unix()
		oldest := now - ds.maxLogAge
		removeByAge, err = tx.Prepare(fmt.Sprintf("DELETE FROM %s WHERE insertion_time < ?", getTopicTableName(topic)))
		if err != nil {
			log.Print("Preparing remove statement:", err)
			return err
		}
		defer removeByAge.Close()
		_, err = removeByAge.Exec(oldest)
		if err != nil {
			log.Print("Removing oldest entry:", err)
			return err
		}
	}
	return tx.Commit()
}

func (ds SqlDatastore) ListMessages(topic string, limit int64, offset int64, insertionTime int64) ([]*api.Message, error) {
	stmt, err := ds.handle.Prepare(fmt.Sprintf("SELECT id, payload FROM %s WHERE id > ? AND insertion_time > ? ORDER BY id ASC LIMIT ?", getTopicTableName(topic)))
	if err != nil {
		log.Print("Preparing query:", err)
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(offset, insertionTime, limit)
	if err != nil {
		log.Print("Executing query:", err)
		return nil, err
	}

	var messages []*api.Message
	for rows.Next() {
		var id int64
		var payload []byte

		err = rows.Scan(&id, &payload)
		if err != nil {
			log.Print("Scan row:", err)
			return nil, err
		}

		messages = append(messages, api.NewMessage(id, payload))
	}

	return messages, nil
}

func (ds SqlDatastore) NumMessages(topic string) (int64, error) {
	var count int64
	row := ds.handle.QueryRow(fmt.Sprintf("SELECT COUNT(id) FROM %s", getTopicTableName(topic)))
	err := row.Scan(&count)
	return count, err
}

func getTopicTableName(topic string) string {
	return fmt.Sprintf("topic_%s", topic)
}

func (ds SqlDatastore) LastMessageId(topic string) (int64, error) {
	var count sql.NullInt64
	row := ds.handle.QueryRow(fmt.Sprintf("SELECT MAX(id) FROM %s", getTopicTableName(topic)))
	err := row.Scan(&count)
	return count.Int64, err
}

func (ds SqlDatastore) ListTopics() ([]string, error) {
	stmt, err := ds.handle.Prepare("SELECT name FROM topics")
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
