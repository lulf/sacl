/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package datastore

import (
	"context"
	"database/sql"
	sqlite3 "github.com/mattn/go-sqlite3"
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

	conn, err := db.Conn(context.TODO())
	if err != nil {
		log.Print("Opening connection:", err)
		return nil, err
	}

	watches := make([]*SqlWatch, 0)

	return &SqlDatastore{
		handle:  db,
		conn:    conn,
		watches: watches,
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

func (ds SqlDatastore) InsertEvent(event *Event) error {
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

	_, err = insertStmt.Exec(event.insertTime, event.creationTime, event.deviceId, event.payload)
	if err != nil {
		log.Print("Inserting entry:", err)
		return err
	}

	_, err = removeStmt.Exec(event.deviceId, event.deviceId, ds.maxSize)
	if err != nil {
		log.Print("Removing oldest entry:", err)
		return err
	}

	return tx.Commit()
}

func (ds SqlDatastore) ListEvents(limit int, offset int) ([]*Event, error) {
	stmt, err := ds.handle.Prepare("SELECT insertion_time, creation_time, device_id, payload FROM events ORDER BY insertion_time DESC LIMIT ? OFFSET ?")
	if err != nil {
		log.Print("Preparing query:", err)
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(limit, offset)
	if err != nil {
		log.Print("Executing query:", err)
		return nil, err
	}

	var events []*Event
	for rows.Next() {
		var id uint64
		var insertionTime int64
		var creationTime int64
		var deviceId string
		var payload string

		err = rows.Scan(&id, &insertionTime, &creationTime, &deviceId, &payload)
		if err != nil {
			log.Print("Scan row:", err)
			return nil, err
		}

		events = append(events, NewEventWithId(id, insertionTime, creationTime, deviceId, payload))
	}

	return events, nil
}

func (ds SqlDatastore) NumEvents() (int, error) {
	var count int
	row := ds.handle.QueryRow("SELECT COUNT(id) FROM events")
	err := row.Scan(&count)
	return count, err
}

func (ds SqlDatastore) WatchEvents(limit int, offset int, watcher Watcher) (Watch, error) {
	watch := &SqlWatch{
		watcher:    watcher,
		lastSeenId: 0,
	}
	if len(ds.watches) == 0 {
		s3conn := ds.conn.(*sqlite3.SQLiteConn)
		s3conn.RegisterUpdateHook(func(op int, db string, table string, rowid int64) {
			switch op {
			case sqlite3.SQLITE_INSERT:
				if len(ds.watches) > 0 {
					stmt, err := ds.handle.Prepare("SELECT id, insertion_time, creation_time, device_id, payload FROM ? WHERE rowid = ?")
					if err != nil {
						log.Print("Query row:", err)
						return
					}

					rows, err := stmt.Query(table, rowid)

					var events []*Event
					for rows.Next() {
						var id uint64
						var insertionTime int64
						var creationTime int64
						var deviceId string
						var payload string

						err = rows.Scan(&id, &insertionTime, &creationTime, &deviceId, &payload)
						if err != nil {
							log.Print("Scan row:", err)
							return
						}

						events = append(events, NewEventWithId(id, insertionTime, creationTime, deviceId, payload))
					}

					for _, event := range events {

						for _, watch := range ds.watches {
							watch.watcher(event)
						}
					}
				}
			}
		})
	}
	ds.watches = append(ds.watches, watch)
	return nil, nil
}
