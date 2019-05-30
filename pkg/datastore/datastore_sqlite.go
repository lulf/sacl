/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package datastore

import (
	"database/sql"
	"github.com/lulf/teig-event-store/pkg/api"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"time"
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

func (ds SqlDatastore) InsertEvent(event *api.Event) error {
	tx, err := ds.handle.Begin()
	if err != nil {
		log.Print("Starting transaction:", err)
		return err
	}

	insertionTime := time.Now().UTC().Unix()

	insertStmt, err := tx.Prepare("INSERT INTO events(id, insertion_time, creation_time, device_id, payload) values(?, ?, ?, ?, ?)")
	if err != nil {
		log.Print("Preparing insert statement:", err)
		return err
	}
	defer insertStmt.Close()

	removeStmt, err := tx.Prepare("DELETE FROM events WHERE device_id=? AND id NOT IN (SELECT id FROM events WHERE device_id=? ORDER BY id DESC LIMIT ?)")
	if err != nil {
		log.Print("Preparing remove statement:", err)
		return err
	}
	defer removeStmt.Close()

	_, err = insertStmt.Exec(event.Id, insertionTime, event.CreationTime, event.DeviceId, event.Payload)
	if err != nil {
		log.Print("Inserting entry:", err)
		return err
	}

	_, err = removeStmt.Exec(event.DeviceId, event.DeviceId, ds.maxSize)
	if err != nil {
		log.Print("Removing oldest entry:", err)
		return err
	}

	return tx.Commit()
}

func (ds SqlDatastore) ListEvents(limit int64, offset int64) ([]*api.Event, error) {
	stmt, err := ds.handle.Prepare("SELECT id, creation_time, device_id, payload FROM events WHERE id > ? ORDER BY id ASC LIMIT ?")
	if err != nil {
		log.Print("Preparing query:", err)
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(offset, limit)
	if err != nil {
		log.Print("Executing query:", err)
		return nil, err
	}

	var events []*api.Event
	for rows.Next() {
		var id int64
		var creationTime int64
		var deviceId string
		var payload string

		err = rows.Scan(&id, &creationTime, &deviceId, &payload)
		if err != nil {
			log.Print("Scan row:", err)
			return nil, err
		}

		events = append(events, api.NewEvent(id, creationTime, deviceId, payload))
	}

	return events, nil
}

func (ds SqlDatastore) NumEvents() (int64, error) {
	var count int64
	row := ds.handle.QueryRow("SELECT COUNT(id) FROM events")
	err := row.Scan(&count)
	return count, err
}

func (ds SqlDatastore) LastEventId() (int64, error) {
	var count sql.NullInt64
	row := ds.handle.QueryRow("SELECT MAX(id) FROM events")
	err := row.Scan(&count)
	return count.Int64, err
}
