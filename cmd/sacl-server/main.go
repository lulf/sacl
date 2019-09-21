/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package main

import (
	"flag"
	"fmt"
	"github.com/lulf/sacl/pkg/commitlog"
	"github.com/lulf/sacl/pkg/datastore"
	"github.com/lulf/sacl/pkg/server"
	"log"
	"net"
	"os"
)

func main() {
	var dbfile string
	var maxlogage int64
	var maxlogsize int64
	var listenAddr string
	var listenPort int

	flag.StringVar(&dbfile, "d", "store.db", "Path to database file (default: store.db)")
	flag.Int64Var(&maxlogsize, "m", -1, "Max number of bytes in log (default: unlimited)")
	flag.Int64Var(&maxlogage, "a", -1, "Max age in seconds of log entries (default: unlimited)")
	flag.StringVar(&listenAddr, "l", "127.0.0.1", "Interface address to listen on (default: 127.0.0.1)")
	flag.IntVar(&listenPort, "p", 5672, "Port to listen on (default: 5672)")

	flag.Usage = func() {
		fmt.Printf("Usage of %s:\n", os.Args[0])
		fmt.Printf("    [-l 0.0.0.0] [-p 5672] [-c 10] [-m 100] [-d store.db]\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	ds, err := datastore.NewSqliteDatastore(dbfile, maxlogage, maxlogsize)
	if err != nil {
		log.Fatal("Opening Datastore:", err)
	}
	defer ds.Close()

	cl, err := commitlog.NewCommitLog(ds)
	if err != nil {
		log.Fatal("Creating commit log:", err)
	}

	es := server.NewServer("sacl-server", cl)

	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", listenAddr, listenPort))
	if err != nil {
		log.Fatal("Listening:", err)
	}
	defer listener.Close()
	fmt.Printf("Listening on %v\n", listener.Addr())

	es.Run(listener)
}
