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
	"time"
)

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

func NewEventLog(ds datastore.Datastore) *EventLog {
	return &EventLog{
		ds:             ds,
		incomingEvents: make(chan *datastore.Event, 10),
		incomingSubs:   make(chan *Subscriber, 10),
		subs:           make([]*Subscriber, 0),
	}
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
			log.Print("New event to persist:", e)
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
				log.Print("Forwarding event to sub")
				sub.outgoing <- &m
			}

		case sub := <-el.incomingSubs:
			log.Print("New subscription!")
			el.subs = append(el.subs, sub)
			if sub.replay > 0 {
				count, err := el.ds.NumEvents()
				if err != nil {
					log.Print("Reading num events:", err)
				} else {
					offset := max(0, count-sub.replay)
					log.Print("Replaying with offset:", offset)
					events, err := el.ds.ListEvents(sub.replay, offset)
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
								log.Print("Encoding and sending")
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
