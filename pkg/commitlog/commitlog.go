/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package commitlog

import (
	"github.com/lulf/slim/pkg/datastore"
	"log"
	"sync"
)

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

func NewCommitLog(ds datastore.Datastore) (*CommitLog, error) {
	topicNames, err := ds.ListTopics()
	if err != nil {
		return nil, err
	}

	var topicMap map[string]*Topic = make(map[string]*Topic)
	for _, topicName := range topicNames {
		lastOffset, err := ds.LastOffset(topicName)
		if err != nil {
			return nil, err
		}
		topic := createTopic(topicName, lastOffset, ds)
		topicMap[topicName] = topic
		go topic.run()
	}
	lock := &sync.Mutex{}
	return &CommitLog{
		lock:     lock,
		ds:       ds,
		topicMap: topicMap,
	}, nil
}

func (cl *CommitLog) GetOrNewTopic(topicName string) (*Topic, error) {
	cl.lock.Lock()
	defer cl.lock.Unlock()
	topic, ok := cl.topicMap[topicName]
	if ok {
		return topic, nil
	}
	err := cl.ds.CreateTopic(topicName)
	if err != nil {
		log.Print("Creating topic:", err)
		return nil, err
	}
	topic = createTopic(topicName, -1, cl.ds)
	cl.topicMap[topicName] = topic
	go topic.run()
	return topic, nil
}

func createTopic(topicName string, lastOffset int64, ds datastore.Datastore) *Topic {
	subLock := &sync.Mutex{}
	return &Topic{
		name:          topicName,
		lastCommitted: lastOffset,
		offsetCounter: lastOffset,
		subs:          make(map[string]*Subscriber),
		incoming:      make(chan *Entry, 100),
		subLock:       subLock,
		ds:            ds,
	}
}
