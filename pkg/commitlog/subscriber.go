/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package commitlog

import (
	"github.com/lulf/sacl/pkg/api"
	"sync/atomic"
)

func (s *Subscriber) Poll() ([]*api.Message, error) {
	topic := s.topic
	var lastCommitted int64
	s.lock.Lock()
	for {
		lastCommitted = atomic.LoadInt64(&topic.lastCommitted)
		if lastCommitted == s.offset {
			s.cond.Wait()
		} else {
			break
		}
	}
	s.lock.Unlock()
	return topic.ds.ListMessages(topic.name, -1, s.offset)
}

func (s *Subscriber) Commit(offset int64) {
	s.offset = offset
}

func (s *Subscriber) Close() {
	s.topic.subLock.Lock()
	delete(s.topic.subs, s.id)
	s.topic.subLock.Unlock()
}
