# Simple AMQP Commit Log (SACL)

Sacl is an AMQP-based ordered commit log (like Kafka) with focus on a simple interface, keeping a small footprint, and providing decent performance.

Sacl does not offer features such as TLS, replication or authentication.

Sacl can be used in combination with other AMQP components such as the [Apache Qpid Dispatch Router](https://qpid.apache.org/components/dispatch-router/index.html) to provide TLS, authentication and load balancing across multiple instances (at the expense of ordering).

The event log can be limited by time or not at all (by size will be added).

Producers send AMQP messages to a topic. The messages are stored immutable in the commit log in the order produced.

Consumers consume events by attaching to a topic starting from the last entry. (Ability to specify offset will be added)


## Building

SACL uses the Apache Qpid Proton Go bindings, which is a wrapper around a C library. To compile SACL, you must install the [Apache Qpid Proton](https://qpid.apache.org/proton/index.html) library.

```
make
```

## Usage

```
sacl-server -d log.db -l 127.0.0.1 -p 5672 &

sacl-producer -m 10 -h 127.0.0.1 -p 5672
sacl-consumer -o 5 -h 127.0.0.1 -p 5672
```

## TODO

* Support consumer offset as link property
* Limit log by size
