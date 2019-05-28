/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package main

import (
	"flag"
	"fmt"
	"github.com/lulf/teig-event-store/pkg/datastore"
	"github.com/lulf/teig-event-store/pkg/eventlog"
	"github.com/lulf/teig-event-store/pkg/eventserver"
	"log"
	"net"
	"os"
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

	es := eventserver.NewEventServer("event-store", el)

	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", listenAddr, listenPort))
	if err != nil {
		log.Fatal("Listening:", err)
	}
	defer listener.Close()
	fmt.Printf("Listening on %v\n", listener.Addr())

	es.Run(listener)
}
