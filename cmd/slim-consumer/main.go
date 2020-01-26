/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package main

import (
	"flag"
	"fmt"
	"github.com/apache/qpid-proton/go/pkg/amqp"
	"github.com/apache/qpid-proton/go/pkg/electron"
	"log"
	"net"
	"os"
)

type Payload struct {
	Counter int64 `json:"counter"`
}

func main() {
	var offset int64
	var connectHost string
	var topic string
	var port int
	var numMessages int

	flag.Int64Var(&offset, "o", 5, "Offset to start consuming from")
	flag.StringVar(&connectHost, "c", "127.0.0.1", "Host to connect to")
	flag.StringVar(&topic, "t", "mytopic", "Topic to consume from")
	flag.IntVar(&numMessages, "m", -1, "Number of messages to receive")
	flag.IntVar(&port, "p", 5672, "Port to connect to")

	flag.Usage = func() {
		fmt.Printf("Usage of %s:\n", os.Args[0])
		fmt.Printf("    [-o 5] [-c 127.0.0.1] [-p 5672] [-t mytopic] [-m -1]\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	tcpConn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", connectHost, port))
	if err != nil {
		log.Fatal("Dialing:", err)
	}

	opts := []electron.ConnectionOption{
		electron.ContainerId("slim-consumer"),
	}
	amqpConn, err := electron.NewConnection(tcpConn, opts...)
	if err != nil {
		log.Fatal("NewConnection:", err)
	}

	props := map[amqp.Symbol]interface{}{"offset": offset}
	sopts := []electron.LinkOption{electron.Source(topic), electron.Filter(props)}
	r, err := amqpConn.Receiver(sopts...)
	if err != nil {
		log.Fatal("Receiver:", r)
	}

	numReceived := 0
	for {
		if numMessages >= 0 && numReceived >= numMessages {
			amqpConn.Close(nil)
			break
		}
		if rm, err := r.Receive(); err == nil {
			rm.Accept()
			m := rm.Message
			body := m.Body()
			fmt.Println(body)
			numReceived += 1
		} else if err == electron.Closed {
			return
		} else {
			log.Fatalf("receive error %v", err)
		}
	}
}
