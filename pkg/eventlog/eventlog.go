/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package eventlog

import (
	"encoding/json"
	"github.com/lulf/teig-event-store/pkg/datastore"
	"log"
	"qpid.apache.org/amqp"
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
	var eventIdCounter uint64
	eventIdCounter, err := ds.LastEventId()
	if err != nil {
		return nil, err
	}
	return &EventLog{
		lock:           &sync.Mutex{},
		ds:             ds,
		idCounter:      eventIdCounter,
		incomingEvents: make(chan *datastore.Event),
		incomingSubs:   make(chan *Subscriber),
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
			e.Id = atomic.AddUint64(&el.idCounter, 1)
			e.InsertTime = time.Now().UTC().Unix()

			err := el.ds.InsertEvent(e)
			if err != nil {
				log.Print("Inserting event:", err)
				continue
			}

			m := amqp.NewMessage()
			data, err := json.Marshal(e)
			m.Marshal(data)
			for _, sub := range el.subs {
				sub.outgoing <- &m
			}

		case sub := <-el.incomingSubs:
			el.subs = append(el.subs, sub)
			if sub.replay > 0 {
				count, err := el.ds.NumEvents()
				if err != nil {
					log.Print("Reading num events:", err)
				} else {
					events, err := el.ds.ListEvents(sub.replay, max(0, count-sub.replay))
					if err != nil {
						log.Print("Listing events:", err)
					} else {
						for _, event := range events {
							m := amqp.NewMessage()
							data, err := json.Marshal(event)
							if err != nil {
								log.Print("Encoding json:", err)
							} else {
								m.Marshal(data)
								sub.outgoing <- &m
							}
						}
					}
				}
			}
		}

	}
}

func NewSubscriber(id string, replay int, channel chan *amqp.Message) *Subscriber {
	return &Subscriber{
		id:       id,
		replay:   replay,
		outgoing: channel,
	}
}
