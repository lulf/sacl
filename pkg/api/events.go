/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package api

type Event struct {
	Id           int64  `json:"-"`
	CreationTime int64  `json:"creationTime"`
	DeviceId     string `json:"deviceId"`
	Payload      string `json:"payload"`
}

func NewEvent(id int64, creationTime int64, deviceId string, payload string) *Event {
	return &Event{
		Id:           id,
		CreationTime: creationTime,
		DeviceId:     deviceId,
		Payload:      payload,
	}
}
