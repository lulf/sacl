/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package commitlog

import (
	"log"
	"sync"
	"sync/atomic"
)

func (topic *Topic) AddEntry(entry *Entry) {
	topic.incoming <- entry
}

func (topic *Topic) run() {
	for {
		e := <-topic.incoming
		m := e.message
		m.Id = atomic.AddInt64(&topic.idCounter, 1)
		err := topic.ds.InsertMessage(topic.name, m)
		if err != nil {
			log.Print("Inserting event:", err)
			e.listener(false)
			continue
		}
		atomic.StoreInt64(&topic.lastCommitted, m.Id)
		e.listener(true)
		for _, sub := range topic.subs {
			sub.cond.Signal()
		}
	}
}

func (topic *Topic) NewSubscriber(id string, offset int64, since int64) *Subscriber {
	lock := &sync.Mutex{}
	cond := sync.NewCond(lock)
	if offset == -1 {
		offset = atomic.LoadInt64(&topic.lastCommitted)
	}
	sub := &Subscriber{
		id:     id,
		topic:  topic,
		lock:   lock,
		cond:   cond,
		offset: offset,
		since:  since,
	}

	topic.subLock.Lock()
	topic.subs[sub.id] = sub
	topic.subLock.Unlock()
	return sub
}
