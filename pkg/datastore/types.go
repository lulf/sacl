/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package datastore

import (
	"database/sql"
)

type Event struct {
	InsertTime   int64  `json:"insertTime,omitempty"`
	CreationTime int64  `json:"creationTime"`
	DeviceId     string `json:"deviceId"`
	Payload      string `json:"payload"`
}

type Datastore interface {
	Initialize() error
	InsertEvent(event *Event) error
	// List events starting from a given offset.  Offset = 0 starts at the oldest entry.
	ListEvents(limit int, offset int) ([]*Event, error)
	// Read the number of events stored
	NumEvents() (int, error)
	Close()
}

type SqlDatastore struct {
	handle  *sql.DB
	maxSize int
}
