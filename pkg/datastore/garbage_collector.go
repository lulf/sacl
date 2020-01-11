/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package datastore

import (
	"log"
	"time"
)

func GarbageCollector(gcInterval time.Duration, ds Datastore) {
	for {
		topics, err := ds.ListTopics()
		if err != nil {
			log.Print("Error listing topics:", err)
		} else {
			for _, topic := range topics {
				err = ds.GarbageCollect(topic)
				if err != nil {
					log.Print("Error garbage collecting topic:", topic, err)
				}
			}
		}
		time.Sleep(gcInterval * time.Second)
	}
}
