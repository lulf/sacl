/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package commitlog

import (
	"github.com/lulf/slim/pkg/api"
	"github.com/lulf/slim/pkg/datastore"
	"sync"
)

type Subscriber struct {
	id     string
	lock   *sync.Mutex
	cond   *sync.Cond
	offset int64
	since  int64
	topic  *Topic
}

type CommitLog struct {
	ds       datastore.Datastore
	topicMap map[string]*Topic
	lock     *sync.Mutex
}

type Topic struct {
	name          string
	ds            datastore.Datastore
	idCounter     int64
	lastCommitted int64
	incoming      chan *Entry
	subs          map[string]*Subscriber
	subLock       *sync.Mutex
}

type CommitListener func(bool)

type Entry struct {
	message  *api.Message
	listener CommitListener
}

func NewEntry(message *api.Message, listener CommitListener) *Entry {
	return &Entry{
		message:  message,
		listener: listener,
	}
}
