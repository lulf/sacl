/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package server

import (
	"github.com/apache/qpid-proton/go/pkg/amqp"
	"github.com/apache/qpid-proton/go/pkg/electron"
	"github.com/lulf/slim/pkg/api"
	"github.com/lulf/slim/pkg/commitlog"
	"log"
	"net"
)

func NewServer(id string, cl *commitlog.CommitLog) *Server {
	container := electron.NewContainer(id)
	return &Server{
		container: container,
		cl:        cl,
		codec: &amqp.MessageCodec{
			Buffer: make([]byte, 1024),
		},
	}
}

func (s *Server) Run(listener net.Listener) {
	for {
		conn, err := s.container.Accept(listener)
		if err != nil {
			log.Print("Accept error:", err)
			continue
		}
		go s.connection(conn)
	}
}

func (s *Server) connection(conn electron.Connection) {
	done := conn.Done()
	subs := make([]*commitlog.Subscriber, 0)
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
				log.Println("Got new sender", snd)
				topicName := snd.Source()
				topic, err := s.cl.GetOrNewTopic(topicName)
				if err != nil {
					log.Print("Closing link: ", snd.String())
					snd.Close(nil)
					continue
				}
				filter := snd.Filter()
				offsetProp, ok := filter["offset"]
				var offset int64
				if ok {
					switch offsetProp.(type) {
					case int64:
						offset = offsetProp.(int64)
					case int32:
						offset = int64(offsetProp.(int32))
					case uint32:
						offset = int64(offsetProp.(uint32))
					case uint64:
						offset = int64(offsetProp.(uint64))
					case int:
						offset = int64(offsetProp.(int))
					default:
						log.Print("Invalid offset type: ", offsetProp)
						snd.Close(nil)
						continue
					}
				} else {
					offset = -1
				}
				sub := topic.NewSubscriber(conn.Container().Id()+"-"+snd.LinkName(), offset)
				subs = append(subs, sub)
				go s.sender(snd, sub)

			case *electron.IncomingReceiver:
				in.SetPrefetch(true)
				in.SetCapacity(10) // TODO: Adjust based on backlog
				rcv := in.Accept().(electron.Receiver)

				topicName := rcv.Target()
				topic, err := s.cl.GetOrNewTopic(topicName)
				if err != nil {
					log.Print("Closing link: ", rcv.String())
					rcv.Close(nil)
					continue
				}
				go s.receiver(topic, rcv)
			default:
				in.Accept()
			}
		}
	}
}

func (s *Server) sender(snd electron.Sender, sub *commitlog.Subscriber) {
	done := snd.Done()
	for {
		select {
		case <-done:
			log.Print("Closing link: ", snd.String())
			snd.Close(nil)
			sub.Close()
			return
		default:
			messages, err := sub.Poll()
			if err != nil {
				log.Print("Error polling events for sub", err)
				snd.Close(nil)
				sub.Close()
				return
			}
			for _, msg := range messages {
				m, err := amqp.DecodeMessage(msg.Payload)
				if err != nil {
					log.Print("Decoding message:", m)
					continue
				}
				outcome := snd.SendSync(m)
				if outcome.Status == electron.Unsent || outcome.Status == electron.Unacknowledged {
					log.Print("Error sending message:", outcome.Error)
					continue
				}
				sub.Commit(msg.Id)
			}
		}
	}
}

func (s *Server) receiver(topic *commitlog.Topic, rcv electron.Receiver) {
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
				data, err := s.codec.Encode(m, make([]byte, 0))
				if err != nil {
					rm.Reject()
				} else {
					message := api.NewMessage(0, data)
					topic.AddEntry(commitlog.NewEntry(message,
						func(ok bool) {
							if ok {
								rm.Accept()
							} else {
								rm.Reject()
							}
						},
					))
				}
			}
		}
	}
}
