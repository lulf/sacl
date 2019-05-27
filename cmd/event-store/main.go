/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/lulf/teig-event-store/pkg/datastore"
	"github.com/lulf/teig-event-store/pkg/eventlog"
	"log"
	"net"
	"os"
	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
)

func main() {

	var dbfile string
	var maxlogsize int
	var defaultReplayCount int
	var listenAddr string
	var listenPort int

	flag.StringVar(&dbfile, "d", "store.db", "Path to database file (default: store.db)")
	flag.IntVar(&maxlogsize, "m", 100, "Max number of entries in log (default: 100)")
	flag.IntVar(&defaultReplayCount, "c", 10, "Default number of events to replay for new subscribers (default: 10)")
	flag.StringVar(&listenAddr, "l", "127.0.0.1", "Address of AMQP event source (default: 127.0.0.1)")
	flag.IntVar(&listenPort, "p", 5672, "Port to listen on (default: 5672)")

	flag.Usage = func() {
		fmt.Printf("Usage of %s:\n", os.Args[0])
		fmt.Printf("    [-l 0.0.0.0] [-p 5672] [-c 10] [-m 100] [-d store.db]\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	ds, err := datastore.NewSqliteDatastore(dbfile, maxlogsize)
	if err != nil {
		log.Fatal("Opening Datastore:", err)
	}
	defer ds.Close()

	err = ds.Initialize()
	if err != nil {
		log.Fatal("Initializing Datastore:", err)
	}

	el, err := eventlog.NewEventLog(ds)
	if err != nil {
		log.Fatal("Creating eventlog:", err)
	}
	go el.Run()

	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", listenAddr, listenPort))
	if err != nil {
		log.Fatal("Listening:", err)
	}
	defer listener.Close()
	fmt.Printf("Listening on %v\n", listener.Addr())

	container := electron.NewContainer("event-store")
	for {
		conn, err := container.Accept(listener)
		if err != nil {
			log.Print("Accept error:", err)
			continue
		}
		go func(conn electron.Connection) {
			for in := range conn.Incoming() {
				switch in := in.(type) {
				case *electron.IncomingSender:
					snd := in.Accept().(electron.Sender)
					// TODO: Read offset from properties
					sub := el.NewSubscriber(snd.LinkName(), -1)
					go func(snd electron.Sender, sub *eventlog.Subscriber) {
						for {
							events, err := sub.Poll()
							if err != nil {
								log.Print("Error polling events for sub", err)
								// TODO: Close sub
								continue
							}
							for _, event := range events {
								m := amqp.NewMessage()
								data, err := json.Marshal(event)
								if err != nil {
									log.Print("Serializing event:", event)
									continue
								}
								m.Marshal(data)
								snd.SendSync(m)
								sub.Commit(event.Id)
							}
						}
					}(snd, sub)
					el.AddSubscriber(sub)

				case *electron.IncomingReceiver:
					in.SetPrefetch(true)
					in.SetCapacity(10) // TODO: Adjust based on backlog
					rcv := in.Accept().(electron.Receiver)
					go func(rcv electron.Receiver) {
						for {
							if rm, err := rcv.Receive(); err == nil {
								m := rm.Message
								var event datastore.Event
								body := m.Body()
								var bodyBytes []byte
								switch t := body.(type) {
								case amqp.Binary:
									bodyBytes = []byte(body.(amqp.Binary).String())
								default:
									log.Print("Unsupported type:", t)
								}
								err := json.Unmarshal(bodyBytes, &event)
								if err != nil {
									rm.Reject()
								} else {
									el.AddEvent(&event)
									rm.Accept()
								}
							}
						}

					}(rcv)
				default:
					in.Accept()
				}
			}
		}(conn)
	}

}
