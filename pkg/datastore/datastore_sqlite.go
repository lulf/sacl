/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package datastore

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"log"
)

func (ds SqlDatastore) Close() {
	ds.handle.Close()
}

func NewSqliteDatastore(fileName string, maxSize int) (*SqlDatastore, error) {

	db, err := sql.Open("sqlite3", fileName)
	if err != nil {
		log.Print("Opening Database:", err)
		return nil, err
	}

	return &SqlDatastore{
		handle:  db,
		maxSize: maxSize,
	}, nil
}

func (ds SqlDatastore) Initialize() error {
	// Create initial database table
	tableCreate := `
	create table if not exists events (id integer not null primary key, insertion_time integer, creation_time integer, device_id integer, payload text);
        `

	_, err := ds.handle.Exec(tableCreate)
	if err != nil {
		log.Print("Creating Database Tables:", err)
		return err
	}
	return nil
}

func (ds SqlDatastore) InsertNewEntry(insertTime int64, creationTime int64, deviceId string, payload string) error {
	tx, err := ds.handle.Begin()
	if err != nil {
		log.Print("Starting transaction:", err)
		return err
	}

	insertStmt, err := tx.Prepare("INSERT INTO events(insertion_time, creation_time, device_id, payload) values(?, ?, ?, ?)")
	if err != nil {
		log.Print("Preparing insert statement:", err)
		return err
	}
	defer insertStmt.Close()

	removeStmt, err := tx.Prepare("DELETE FROM events WHERE device_id=? AND id NOT IN (SELECT id FROM events WHERE device_id=? ORDER BY insertion_time DESC LIMIT ?)")
	if err != nil {
		log.Print("Preparing remove statement:", err)
		return err
	}
	defer removeStmt.Close()

	_, err = insertStmt.Exec(insertTime, creationTime, deviceId, payload)
	if err != nil {
		log.Print("Inserting entry:", err)
		return err
	}

	_, err = removeStmt.Exec(deviceId, deviceId, ds.maxSize)
	if err != nil {
		log.Print("Removing oldest entry:", err)
		return err
	}

	return tx.Commit()
}
