/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package datastore

func NewEvent(id uint64, insertTime int64, creationTime int64, deviceId string, payload string) *Event {
	return &Event{
		Id:           id,
		InsertTime:   insertTime,
		CreationTime: creationTime,
		DeviceId:     deviceId,
		Payload:      payload,
	}
}
