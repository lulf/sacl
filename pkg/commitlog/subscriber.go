/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package commitlog

import (
	"github.com/lulf/slim/pkg/api"
	"sync/atomic"
)

type StreamFn = func(message *api.Message) error

func (s *Subscriber) Stream(callback StreamFn) error {
	topic := s.topic
	var lastCommitted int64
	s.lock.Lock()
	for {
		lastCommitted = atomic.LoadInt64(&topic.lastCommitted)
		if lastCommitted == s.offset-1 {
			s.cond.Wait()
		} else {
			break
		}
	}
	s.lock.Unlock()
	return topic.ds.StreamMessages(topic.name, s.offset, callback)
}

func (s *Subscriber) Commit(offset int64) {
	s.offset = offset + 1
}

func (s *Subscriber) Close() {
	s.topic.subLock.Lock()
	delete(s.topic.subs, s.id)
	s.topic.subLock.Unlock()
}
