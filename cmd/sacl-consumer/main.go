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
	"qpid.apache.org/electron"
)

type Payload struct {
	Counter int64 `json:"counter"`
}

func main() {
	var offset int64
	var connectHost string
	var topic string
	var port int

	flag.Int64Var(&offset, "o", 5, "Offset to start consuming from")
	flag.StringVar(&connectHost, "c", "127.0.0.1", "Host to connect to")
	flag.StringVar(&topic, "t", "mytopic", "Topic to consume from")
	flag.IntVar(&port, "p", 5672, "Port to connect to")

	flag.Usage = func() {
		fmt.Printf("Usage of %s:\n", os.Args[0])
		fmt.Printf("    [-o 5] [-c 127.0.0.1] [-p 5672] [-t mytopic]\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	tcpConn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", connectHost, port))
	if err != nil {
		log.Fatal("Dialing:", err)
	}

	opts := []electron.ConnectionOption{
		electron.ContainerId("sacl-consumer"),
	}
	amqpConn, err := electron.NewConnection(tcpConn, opts...)
	if err != nil {
		log.Fatal("NewConnection:", err)
	}

	sopts := []electron.LinkOption{electron.Source(topic)}
	r, err := amqpConn.Receiver(sopts...)
	if err != nil {
		log.Fatal("Receiver:", r)
	}

	for {
		if rm, err := r.Receive(); err == nil {
			rm.Accept()
			m := rm.Message
			body := m.Body()
			fmt.Println(body)
		} else if err == electron.Closed {
			return
		} else {
			log.Fatalf("receive error %v", err)
		}
	}
}
