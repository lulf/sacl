/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package eventlog

import (
	"github.com/lulf/teig-event-store/pkg/datastore"
	"log"
	"sync"
	"sync/atomic"
	"time"
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
	return &EventLog{
		ds:             ds,
		lastCommitted:  lastId,
		idCounter:      lastId,
		incomingEvents: make(chan *datastore.Event, 100),
		incomingSubs:   make(chan *Subscriber, 100),
		subs:           make([]*Subscriber, 0),
	}, nil
}

func (el *EventLog) AddSubscriber(sub *Subscriber) {
	el.incomingSubs <- sub
}

func (el *EventLog) AddEvent(event *datastore.Event) {
	el.incomingEvents <- event
}

func (el *EventLog) Run() {
	for {
		select {
		case e := <-el.incomingEvents:
			e.InsertTime = time.Now().UTC().Unix()
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
		case sub := <-el.incomingSubs:
			el.subs = append(el.subs, sub)
		}
	}
}

func (el *EventLog) NewSubscriber(id string, offset int64) *Subscriber {
	lock := &sync.Mutex{}
	cond := sync.NewCond(lock)
	if offset == -1 {
		offset = atomic.LoadInt64(&el.lastCommitted) - 10
	}
	return &Subscriber{
		id:     id,
		lock:   lock,
		cond:   cond,
		offset: offset,
		el:     el,
	}
}

func (s *Subscriber) Poll() ([]*datastore.Event, error) {
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
