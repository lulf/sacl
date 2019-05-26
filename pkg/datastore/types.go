/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package datastore

import (
	"database/sql"
)

type Event struct {
	id           uint64
	insertTime   int64
	creationTime int64
	deviceId     string
	payload      string
}

type Watcher func(event *Event) error

type Watch interface {
	Close()
}

type Datastore interface {
	Initialize() error
	InsertEvent(event *Event) error
	// List events starting from a given offset.  Offset = 0 starts at the oldest entry.
	ListEvents(limit int, offset int) ([]*Event, error)
	//  Watch events starting from a given offset. Offset = 0 starts at the oldest entry. Offset = -1 starts watching new entries
	WatchEvents(limit int, offset int, watcher Watcher) (Watch, error)
	// Read the number of events stored
	NumEvents() (int, error)
	Close()
}

type SqlDatastore struct {
	handle  *sql.DB
	conn    *sql.Conn
	maxSize int
	watches []*SqlWatch
}

type SqlWatch struct {
	watcher    Watcher
	lastSeenId int
}
