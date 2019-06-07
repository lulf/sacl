/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/lulf/teig-event-store/pkg/api"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
	"time"
)

func main() {
	var numdevices int
	flag.IntVar(&numdevices, "n", 5, "Number of devices to simulate")

	flag.Usage = func() {
		fmt.Printf("Usage of %s:\n", os.Args[0])
		fmt.Printf("    [-n 5]\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	tcpConn, err := net.Dial("tcp", "127.0.0.1:5672")
	if err != nil {
		log.Fatal("Dialing:", err)
	}

	opts := []electron.ConnectionOption{
		electron.ContainerId("event-genereator"),
	}
	amqpConn, err := electron.NewConnection(tcpConn, opts...)
	if err != nil {
		log.Fatal("NewConnection:", err)
	}

	sopts := []electron.LinkOption{electron.Target("events")}
	for id := 1; id <= numdevices; id++ {
		device := fmt.Sprintf("Dings %d", id)
		s, err := amqpConn.Sender(sopts...)
		if err != nil {
			log.Fatal("Sender:", s)
		}
		wait := 2 + rand.Intn(5)
		go runSender(device, s, wait)
	}

	for {
		time.Sleep(time.Duration(6000))
	}
}

func runSender(device string, sender electron.Sender, wait int) {
	log.Print(fmt.Sprintf("Running %s with interval %d", device, wait))
	step := 0.1
	startValue := rand.Float64() * math.Pi
	value := startValue
	maxValue := 2.0 * math.Pi
	for {
		time.Sleep(time.Duration(wait) * time.Second)
		now := time.Now().UTC().Unix()
		if value > maxValue {
			value = 0.0
		}

		payload := fmt.Sprintf("%f", 15.0+(10.0*math.Sin(value)))
		value += step
		event := api.NewEvent(0, now, device, payload)
		m := amqp.NewMessage()
		data, err := json.Marshal(event)
		if err != nil {
			log.Print("Serializing event:", device, err)
			continue
		}
		log.Print(fmt.Sprintf("Woke up %s to send data: %s", device, data))
		m.Marshal(data)
		outcome := sender.SendSync(m)
		if outcome.Status == electron.Unsent || outcome.Status == electron.Unacknowledged {
			log.Print("Error sending:", outcome.Status)
			continue
		}
	}
}
