/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package datastore

import (
	"github.com/lulf/slim/pkg/api"
)

type StreamingFunc func(*api.Message) error

type Datastore interface {
	Initialize() error
	CreateTopic(topic string) error
	InsertMessage(topic string, message *api.Message) error
	StreamMessages(topic string, offset int64, callback StreamingFunc) error
	// Read the number of events stored
	NumMessages(topic string) (int64, error)
	LastOffset(topic string) (int64, error)
	Flush() error
	GarbageCollect(topic string) error
	ListTopics() ([]string, error)
	Close()
}
