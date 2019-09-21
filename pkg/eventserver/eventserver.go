/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package eventserver

import (
	"encoding/json"
	"github.com/lulf/sacl/pkg/api"
	"github.com/lulf/sacl/pkg/eventlog"
	"log"
	"net"
	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
)

func NewEventServer(id string, el *eventlog.EventLog) *EventServer {
	container := electron.NewContainer(id)
	return &EventServer{
		container: container,
		el:        el,
	}
}

func (es *EventServer) Run(listener net.Listener) {
	for {
		conn, err := es.container.Accept(listener)
		if err != nil {
			log.Print("Accept error:", err)
			continue
		}
		go es.connection(conn)
	}
}

func (es *EventServer) connection(conn electron.Connection) {
	done := conn.Done()
	subs := make([]*eventlog.Subscriber, 0)
	for {
		select {
		case <-done:
			log.Print("Closing connection: ", conn.String())
			for _, sub := range subs {
				sub.Close()
			}
			conn.Close(nil)
			return
		case in := <-conn.Incoming():
			switch in := in.(type) {
			case *electron.IncomingSender:
				snd := in.Accept().(electron.Sender)
				// TODO: Read offset from properties
				sub := es.el.NewSubscriber(conn.Container().Id()+"-"+snd.LinkName(), -1)
				subs = append(subs, sub)
				go es.sender(snd, sub)

			case *electron.IncomingReceiver:
				in.SetPrefetch(true)
				in.SetCapacity(10) // TODO: Adjust based on backlog
				rcv := in.Accept().(electron.Receiver)
				go es.receiver(rcv)
			default:
				in.Accept()
			}
		}
	}
}

func (es *EventServer) sender(snd electron.Sender, sub *eventlog.Subscriber) {
	done := snd.Done()
	for {
		select {
		case <-done:
			log.Print("Closing link: ", snd.String())
			snd.Close(nil)
			sub.Close()
			return
		default:
			events, err := sub.Poll()
			if err != nil {
				log.Print("Error polling events for sub", err)
				snd.Close(nil)
				sub.Close()
				return
			}
			for _, event := range events {
				m := amqp.NewMessage()
				data, err := json.Marshal(event)
				if err != nil {
					log.Print("Serializing event:", event)
					continue
				}
				m.Marshal(data)
				outcome := snd.SendSync(m)
				if outcome.Status == electron.Unsent || outcome.Status == electron.Unacknowledged {
					log.Print("Error sending message:", outcome.Error)
					continue
				}
				sub.Commit(event.Id)
			}
		}
	}
}

func (es *EventServer) receiver(rcv electron.Receiver) {
	done := rcv.Done()
	for {
		select {
		case <-done:
			log.Print("Closing link: ", rcv.String())
			rcv.Close(nil)
			return
		default:
			rm, err := rcv.Receive()
			if err == nil {
				m := rm.Message
				var event api.Event
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
					es.el.AddEvent(&event)
					rm.Accept()
				}
			}
		}
	}
}
