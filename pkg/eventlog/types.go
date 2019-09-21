/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package eventlog

import (
	"github.com/lulf/sacl/pkg/api"
	"github.com/lulf/sacl/pkg/datastore"
	"sync"
)

type Subscriber struct {
	id     string
	lock   *sync.Mutex
	cond   *sync.Cond
	offset int64
	el     *EventLog
}

type EventLog struct {
	ds             datastore.Datastore
	idCounter      int64
	lastCommitted  int64
	incomingEvents chan *api.Event
	subs           map[string]*Subscriber
	subLock        *sync.Mutex
}
