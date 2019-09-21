/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package eventlog

import (
	"github.com/lulf/sacl/pkg/api"
	"github.com/lulf/sacl/pkg/datastore"
	"log"
	"sync"
	"sync/atomic"
)

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

func NewEventLog(ds datastore.Datastore) (*EventLog, error) {
	lastId, err := ds.LastEventId()
	if err != nil {
		return nil, err
	}
	subLock := &sync.Mutex{}
	return &EventLog{
		ds:             ds,
		lastCommitted:  lastId,
		idCounter:      lastId,
		incomingEvents: make(chan *api.Event, 100),
		subs:           make(map[string]*Subscriber),
		subLock:        subLock,
	}, nil
}

func (el *EventLog) AddEvent(event *api.Event) {
	el.incomingEvents <- event
}

func (el *EventLog) Run() {
	for {
		e := <-el.incomingEvents
		e.Id = atomic.AddInt64(&el.idCounter, 1)
		err := el.ds.InsertEvent(e)
		if err != nil {
			log.Print("Inserting event:", err)
			continue
		}
		atomic.StoreInt64(&el.lastCommitted, e.Id)
		for _, sub := range el.subs {
			sub.cond.Signal()
		}
	}
}

func (el *EventLog) NewSubscriber(id string, offset int64) *Subscriber {
	lock := &sync.Mutex{}
	cond := sync.NewCond(lock)
	if offset == -1 {
		offset = atomic.LoadInt64(&el.lastCommitted) - 10
	}
	sub := &Subscriber{
		id:     id,
		lock:   lock,
		cond:   cond,
		offset: offset,
		el:     el,
	}

	el.subLock.Lock()
	el.subs[sub.id] = sub
	el.subLock.Unlock()
	return sub
}

func (s *Subscriber) Poll() ([]*api.Event, error) {
	el := s.el
	var lastCommitted int64
	s.lock.Lock()
	for {
		lastCommitted = atomic.LoadInt64(&el.lastCommitted)
		if lastCommitted == s.offset {
			s.cond.Wait()
		} else {
			break
		}
	}
	s.lock.Unlock()
	return el.ds.ListEvents(-1, s.offset)
}

func (s *Subscriber) Commit(offset int64) {
	s.offset = offset
}

func (s *Subscriber) Close() {
	s.el.subLock.Lock()
	delete(s.el.subs, s.id)
	s.el.subLock.Unlock()
}
