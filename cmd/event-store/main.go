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
	"log"
	"net"
	"os"
	"qpid.apache.org/electron"
)

func main() {

	var dbfile string
	var maxlogsize int
	var listenAddr string
	var listenPort int

	flag.StringVar(&dbfile, "d", "store.db", "Path to database file (default: store.db)")
	flag.IntVar(&maxlogsize, "m", 100, "Max number of entries in log (default: 100)")
	flag.StringVar(&listenAddr, "l", "127.0.0.1", "Address of AMQP event source (default: 127.0.0.1)")
	flag.IntVar(&listenPort, "p", 5672, "Port to listen on (default: 5672)")

	flag.Usage = func() {
		fmt.Printf("Usage of %s:\n", os.Args[0])
		fmt.Printf("    [-l 0.0.0.0] [-p 5672] [-m 100] [-d store.db]\n")
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
					go func(snd electron.Sender) {

					}(snd)

				case *electron.IncomingReceiver:
					in.SetPrefetch(true)
					in.SetCapacity(10) // TODO: Make configurable
					rcv := in.Accept().(electron.Receiver)
					go func(rcv electron.Receiver) {
						for {
							if rm, err := rcv.Receive(); err == nil {
								m := rm.Message
								var event datastore.Event
								err := json.Unmarshal(m.Body().([]byte), &event)
								if err != nil {
									rm.Reject()
								} else {
									err = ds.InsertEvent(&event)
									if err != nil {
										rm.Reject()
									} else {
										rm.Accept()
									}
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
