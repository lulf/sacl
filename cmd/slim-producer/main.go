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
	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
)

type Payload struct {
	Counter int64 `json:"counter"`
}

func main() {
	var numMessages int
	var connectHost string
	var topic string
	var port int

	flag.IntVar(&numMessages, "m", 5, "Number of messages to send")
	flag.StringVar(&connectHost, "c", "127.0.0.1", "Host to connect to")
	flag.StringVar(&topic, "t", "mytopic", "Topic to send to")
	flag.IntVar(&port, "p", 5672, "Port to connect to")

	flag.Usage = func() {
		fmt.Printf("Usage of %s:\n", os.Args[0])
		fmt.Printf("    [-m 5] [-c 127.0.0.1] [-p 5672] [-t mytopic]\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	tcpConn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", connectHost, port))
	if err != nil {
		log.Fatal("Dialing:", err)
	}

	opts := []electron.ConnectionOption{
		electron.ContainerId("slim-producer"),
	}
	amqpConn, err := electron.NewConnection(tcpConn, opts...)
	if err != nil {
		log.Fatal("NewConnection:", err)
	}

	sopts := []electron.LinkOption{electron.Target(topic)}
	sender, err := amqpConn.Sender(sopts...)
	if err != nil {
		log.Fatal("Sender:", sender)
	}

	for id := 1; id <= numMessages; id++ {
		m := amqp.NewMessage()
		body := fmt.Sprintf("counter: %d", id)
		m.Marshal(body)
		outcome := sender.SendSync(m)
		fmt.Println(body)
		if outcome.Status == electron.Unsent || outcome.Status == electron.Unacknowledged {
			log.Print("Error sending:", outcome.Status)
		}
	}
}
