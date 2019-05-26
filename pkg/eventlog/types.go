/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package eventlog

import (
	"github.com/lulf/teig-event-store/pkg/datastore"
	"qpid.apache.org/amqp"
	"sync"
)

type Subscriber struct {
	id       string
	replay   int
	outgoing chan *amqp.Message
}

type EventLog struct {
	lock           *sync.Mutex
	ds             datastore.Datastore
	idCounter      uint64
	incomingEvents chan *datastore.Event
	incomingSubs   chan *Subscriber
	subs           []*Subscriber
}
