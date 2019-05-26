/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package datastore

func NewEvent(insertTime int64, creationTime int64, deviceId string, payload string) *Event {
	return &Event{
		id:           0,
		insertTime:   insertTime,
		creationTime: creationTime,
		deviceId:     deviceId,
		payload:      payload,
	}
}

func NewEventWithId(id uint64, insertTime int64, creationTime int64, deviceId string, payload string) *Event {
	return &Event{
		id:           id,
		insertTime:   insertTime,
		creationTime: creationTime,
		deviceId:     deviceId,
		payload:      payload,
	}
}
