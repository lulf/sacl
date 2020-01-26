/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"runtime/pprof"
	"time"

	"github.com/lulf/slim/pkg/commitlog"
	"github.com/lulf/slim/pkg/datastore"
	"github.com/lulf/slim/pkg/server"
)

func main() {
	var dataDir string
	var maxlogage int64
	var maxlogsize int64
	var listenAddr string
	var listenPort int
	var gcInterval int
	var dataStoreType string
	var flushInterval int

	flag.StringVar(&dataDir, "d", "data", "Path to data directory (default: data)")
	flag.Int64Var(&maxlogsize, "m", -1, "Max number of bytes in log (default: unlimited)")
	flag.Int64Var(&maxlogage, "a", -1, "Max age in seconds of log entries (default: unlimited)")
	flag.IntVar(&gcInterval, "g", 0, "Garbage collect interval (default: 0 (never))")
	flag.StringVar(&listenAddr, "l", "127.0.0.1", "Interface address to listen on (default: 127.0.0.1)")
	flag.IntVar(&listenPort, "p", 5672, "Port to listen on (default: 5672)")
	flag.StringVar(&dataStoreType, "t", "file", "Data store type to use (memory, file or sqlite. Default: file)")
	flag.IntVar(&flushInterval, "f", 10, "Flush interval (Only for file data store type. Default: 10 seconds)")

	flag.Usage = func() {
		fmt.Printf("Usage of %s:\n", os.Args[0])
		fmt.Printf("    [-l 0.0.0.0] [-p 5672] [-c 10] [-m 100] [-g 120] [-d /var/run/slim/data]\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	var err error

	err = os.MkdirAll(dataDir, os.ModePerm)
	if err != nil {
		log.Fatal("Error creating datadir:", err)
	}

	var ds datastore.Datastore
	if dataStoreType == "memory" {
		ds, err = datastore.NewMemoryDatastore()
	} else if dataStoreType == "sqlite" {
		ds, err = datastore.NewSqliteDatastore(dataDir, maxlogage, maxlogsize)
	} else if dataStoreType == "file" {
		ds, err = datastore.NewFileDatastore(dataDir, maxlogage, maxlogsize)
	} else {
		log.Fatal("Invalid data store type specified:", dataStoreType)
	}
	if err != nil {
		log.Fatal("Opening Datastore:", err)
	}
	defer ds.Close()

	err = ds.Initialize()
	if err != nil {
		log.Fatal("Initializing Datastore:", err)
	}

	if gcInterval > 0 {
		go datastore.GarbageCollector(time.Duration(gcInterval), ds)
	}

	if dataStoreType == "file" {
		go datastore.Flusher(time.Duration(flushInterval), ds)
	}

	cl, err := commitlog.NewCommitLog(ds)
	if err != nil {
		log.Fatal("Creating commit log:", err)
	}

	es := server.NewServer("slim-server", cl)

	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", listenAddr, listenPort))
	if err != nil {
		log.Fatal("Listening:", err)
	}
	defer listener.Close()
	fmt.Printf("Listening on %v\n", listener.Addr())

	go func() {
		time.Sleep(50 * time.Second)
		out, err := os.Create("slim-server.mprof")
		if err != nil {
			log.Fatal("Creating profile:", err)
		}

		pprof.WriteHeapProfile(out)
		out.Close()
	}()
	es.Run(listener)
}
