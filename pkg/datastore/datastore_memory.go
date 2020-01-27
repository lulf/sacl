/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package datastore

import (
	"sync"

	"github.com/lulf/slim/pkg/api"
)

type MemoryDatastore struct {
	mapLock   *sync.Mutex
	topicLock map[string]*sync.Mutex
	topicMap  map[string][]*api.Message
}

func NewMemoryDatastore() (*MemoryDatastore, error) {
	return &MemoryDatastore{
		mapLock:   &sync.Mutex{},
		topicMap:  make(map[string][]*api.Message, 0),
		topicLock: make(map[string]*sync.Mutex, 0),
	}, nil
}

func min(a int64, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func (m *MemoryDatastore) Initialize() error {
	return nil
}

func (m *MemoryDatastore) CreateTopic(topic string) error {
	m.mapLock.Lock()
	defer m.mapLock.Unlock()

	if _, ok := m.topicMap[topic]; !ok {
		m.topicMap[topic] = make([]*api.Message, 0)
		m.topicLock[topic] = &sync.Mutex{}
	}
	return nil
}

func (m *MemoryDatastore) Flush() error {
	return nil
}

func (m *MemoryDatastore) InsertMessage(topic string, message *api.Message) error {
	m.topicLock[topic].Lock()
	m.topicMap[topic] = append(m.topicMap[topic], message)
	m.topicLock[topic].Unlock()
	return nil
}

func (m *MemoryDatastore) StreamMessages(topic string, offset int64, callback StreamingFunc) error {
	m.topicLock[topic].Lock()
	messages := m.topicMap[topic]
	if offset > 0 {
		offset = min(offset, int64(len(messages)))
		messages = messages[offset:]
	}
	for _, message := range messages {
		err := callback(message)
		if err != nil {
			return err
		}
	}

	defer m.topicLock[topic].Unlock()
	return nil
}

func (m *MemoryDatastore) NumMessages(topic string) (int64, error) {
	m.topicLock[topic].Lock()
	defer m.topicLock[topic].Unlock()
	return int64(len(m.topicMap[topic])), nil
}

func (m *MemoryDatastore) LastOffset(topic string) (int64, error) {
	m.topicLock[topic].Lock()
	defer m.topicLock[topic].Unlock()
	if len(m.topicMap[topic]) > 0 {
		return m.topicMap[topic][len(m.topicMap[topic])-1].Offset, nil
	}
	return 0, nil
}

func (m *MemoryDatastore) GarbageCollect(topic string) error {
	return nil
}

func (m *MemoryDatastore) ListTopics() ([]string, error) {
	m.mapLock.Lock()
	defer m.mapLock.Unlock()
	keys := make([]string, 0, len(m.topicMap))
	for k := range m.topicMap {
		keys = append(keys, k)
	}
	return keys, nil
}

func (m *MemoryDatastore) Close() {}
