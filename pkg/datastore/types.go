/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package datastore

import (
	"github.com/lulf/slim/pkg/api"
)

type Datastore interface {
	Initialize() error
	CreateTopic(topic string) error
	InsertMessage(topic string, message *api.Message) error
	// List messages starting from a given offset.  Offset = 0 starts at the oldest entry.
	ListMessages(topic string, limit int64, offset int64, insertTime int64) ([]*api.Message, error)
	// Read the number of events stored
	NumMessages(topic string) (int64, error)
	LastMessageId(topic string) (int64, error)
	GarbageCollect(topic string) error
	ListTopics() ([]string, error)
	Close()
}
