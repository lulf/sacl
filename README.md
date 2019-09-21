# Simple AMQP Commit Log (SACL)

Sacl is an AMQP-based ordered commit log (like Kafka) with focus on a simple interface, keeping a small footprint, and providing decent performance.

Sacl does not offer advanced features such replication for high availability or authentication.

Sacl can be used in combination with other AMQP components such as the [Apache Qpid Dispatch Router](https://qpid.apache.org/components/dispatch-router/index.html) to provide authentication and load balancing across multiple instances (at the expense of ordering).

The event log can be limited by size or time or not at all.

Producers send events as AMQP messages. The messages are stored immutable in the commit log in the order produced.

Consumers consume events by specifying an offset (as an AMQP link property) from where to start consuming. Otherwise, consumers start from the last log entry. Consumers are responsible for tracking their own offset.
