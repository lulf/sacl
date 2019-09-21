/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"pack.ag/amqp"
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

	amqpConn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%d", connectHost, port))
	if err != nil {
		log.Fatal("Dialing:", err)
	}
	defer amqpConn.Close()

	session, err := amqpConn.NewSession()
	if err != nil {
		log.Fatal("Session:", err)
	}

	r, err := session.NewReceiver(
		amqp.LinkSourceAddress(topic),
		amqp.LinkCredit(10),
		amqp.LinkPropertyInt64("offset", offset),
	)
	if err != nil {
		log.Fatal("Receiver:", r)
	}

	for {
		if msg, err := r.Receive(context.TODO()); err == nil {
			body := msg.Value
			fmt.Println(body)
			msg.Accept()
		} else {
			log.Fatalf("receive error %v", err)
		}
	}
}
