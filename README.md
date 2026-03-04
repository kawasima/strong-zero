# StrongZero

![StrongZero](https://github.com/kawasima/strong-zero/blob/master/doc/img/logo.png?raw=true)

A reliable data synchronization library for microservices, built on the **Transactional Outbox** pattern with [ZeroMQ](https://zeromq.org/) messaging.

- **Zero data loss** -- messages are persisted in your database transaction before delivery
- **At-least-once delivery** -- consumers track their position and can resume after failures
- **Ordered** -- messages are delivered in the order they were produced (Flake ID-based)
- **Easy setup** -- just add two tables and wire up a few components

## Why StrongZero

Most transactional outbox implementations require an external message broker (Kafka, RabbitMQ, etc.) or a CDC infrastructure (connector clusters, schema registries, etc.). This adds significant operational overhead -- especially when your system only needs reliable data synchronization between a handful of services.

StrongZero takes a different approach: messaging is handled by [ZeroMQ](https://zeromq.org/), which runs as an embedded library inside your application process. There is **no external broker to deploy or manage**. You just add two database tables and a Maven dependency, and you're ready to go.

This makes StrongZero a good fit when:

- You need transactional messaging guarantees but **don't want to operate a broker cluster**
- Your architecture is small-to-medium scale and a full CDC pipeline would be overkill
- You want a **self-contained library** with minimal infrastructure dependencies

## How It Works

```text
 Producer Application              StrongZero                      Consumer Application
 ─────────────────────       ──────────────────────────       ──────────────────────────

  BEGIN transaction           StrongZeroProducer (ROUTER)
  ├─ INSERT business data                │
  ├─ sender.send(type, obj)       ┌──────┴──────┐
  │   └─ INSERT INTO             Pump       Pump
  │      produced_zero         (DEALER)    (DEALER)
  COMMIT                          │              │
                                  │  SELECT FROM │
                                  │  produced_zero
                                  │  WHERE id > ?│
                                  └──────┬───────┘
                                         │
                              ZeroMQ  ◄──┘
                             (ROUTER/DEALER)
                                  │
                                  ▼
                             StrongZeroConsumer (REQ)
                                  │
                                  ├─ handler.consume(id, msg)
                                  └─ UPDATE consumed_zero
                                     SET last_id = ?
```

1. **Producer** writes the domain entity and an outbox row (`produced_zero`) in the **same database transaction**.
2. **Pump workers** poll the outbox table and forward messages to consumers via ZeroMQ.
3. **Consumer** dispatches messages to registered handlers by type, then records the last consumed ID so it can resume from that point.

## Getting Started

### Maven

```xml
<dependency>
    <groupId>net.unit8.zero</groupId>
    <artifactId>strong-zero</artifactId>
    <version>0.1.0</version>
</dependency>
```

### Database Tables

Create these tables in your producer and consumer schemas.

**Producer schema -- `produced_zero`**

| Column  | Type         | Constraint  |
|---------|--------------|-------------|
| id      | VARCHAR(32)  | PRIMARY KEY |
| type    | VARCHAR(32)  | NOT NULL    |
| message | BLOB         | NOT NULL    |

**Consumer schema -- `consumed_zero`**

| Column      | Type         | Constraint  |
|-------------|--------------|-------------|
| producer_id | VARCHAR(16)  | PRIMARY KEY |
| last_id     | VARCHAR(32)  | NOT NULL    |

### Producer Setup

```java
// 1. Start the routing broker
String frontendAddress     = "tcp://127.0.0.1:5959";
String backendAddress      = "ipc://backend";
String notificationAddress = "ipc://notification";

StrongZeroProducer producer = new StrongZeroProducer(frontendAddress, backendAddress);
producer.start();

// 2. Start pump workers (one or more)
ExecutorService workers = Executors.newFixedThreadPool(2);
workers.submit(new StrongZeroPump(backendAddress, notificationAddress, dataSource));
workers.submit(new StrongZeroPump(backendAddress, notificationAddress, dataSource));

// 3. Create a sender
ObjectMapper mapper = new ObjectMapper(new MessagePackFactory());
StrongZeroSender sender = new StrongZeroSender(notificationAddress, dataSource, mapper);

// 4. Send messages inside your transaction
transactionManager.required(() -> {
    userDao.insert(user);
    sender.send("USER", user);
});
```

### Consumer Setup

```java
String producerAddress = "tcp://127.0.0.1:5959";
ObjectMapper mapper    = new ObjectMapper(new MessagePackFactory());

StrongZeroConsumer consumer =
    new StrongZeroConsumer("producer1", producerAddress, dataSource);
consumer.setObjectMapper(mapper);

// Register a handler for each message type
consumer.registerHandler("USER", (id, msg) -> {
    User user = mapper.readValue(msg, User.class);
    transactionManager.required(() -> {
        if (memberDao.update(user) == 0) {
            memberDao.insert(user);
        }
    });
});

// start() blocks and consumes indefinitely
consumer.start();
```

## Configuration

### StrongZeroSender

| Option                 | Default | Description                                            |
|------------------------|---------|--------------------------------------------------------|
| `thresholdUntilNotify` | 3       | Number of inserts before a ZeroMQ notification is sent |

### StrongZeroPump

| Option      | Default | Description                            |
|-------------|---------|----------------------------------------|
| `batchSize` | 100     | Maximum rows fetched per database query|

### StrongZeroConsumer

| Option            | Default             | Description                                                               |
|-------------------|---------------------|---------------------------------------------------------------------------|
| `autoAcknowledge` | true                | Automatically update `last_id` after each handler completes without error |
| `objectMapper`    | MessagePack         | ObjectMapper for deserializing message payloads                           |
| `meterRegistry`   | SimpleMeterRegistry | Micrometer registry for consumer metrics                                  |

#### Manual Acknowledgement

Set `autoAcknowledge` to `false` when you need to update `last_id` within the same transaction as your business logic:

```java
consumer.setAutoAcknowledge(false);
consumer.registerHandler("ORDER", (id, msg) -> {
    Order order = mapper.readValue(msg, Order.class);
    transactionManager.required(() -> {
        orderDao.insert(order);
        consumer.consumed(id);  // ack inside the same transaction
    });
});
```

## Architecture

### ZeroMQ Socket Topology

```text
Consumer (REQ)
     │
     │ tcp://
     ▼
Producer (ROUTER frontend)  ◄───►  Producer (ROUTER backend)
                                        │
                              ┌─────────┼─────────┐
                              ▼         ▼         ▼
                          Pump (DEALER) ...   Pump (DEALER)
                              │                   │
                              │   ipc:// or       │
                              │   inproc://        │
                              ▼                   ▼
                          Sender (PUB) ─── notification ──► Pump (SUB)
```

- **ROUTER/DEALER** -- reliable request-response between the producer broker and pump workers.
- **PUB/SUB** -- lightweight update notifications from the sender to pump workers. Pumps also poll the database on a 10-second timeout as a fallback.
- **REQ/ROUTER** -- consumer requests messages from the producer, which routes them to an available pump worker.

### Flake ID

Messages are ordered by a 128-bit **Flake ID** (32 hex characters):

| Bytes | Content                           |
|-------|-----------------------------------|
| 0-7   | Timestamp in milliseconds (UTC)   |
| 8-13  | MAC address                       |
| 14-15 | Sequence number (per millisecond) |

Flake IDs are time-ordered, unique across hosts, and thread-safe (up to 65,536 IDs per millisecond per instance).

### Metrics

StrongZero exposes [Micrometer](https://micrometer.io/) metrics:

| Metric                          | Type    | Description                     |
|---------------------------------|---------|---------------------------------|
| `strong_zero.consumer.messages` | Counter | Total messages consumed         |
| `strong_zero.consumer.time`     | Timer   | Time spent in message handlers  |
| `strong_zero.pump.count`        | Gauge   | Available pump workers in queue |

## Requirements

- Java 21+
- A relational database accessible via JDBC

## Dependencies

| Library                        | Purpose                   |
|--------------------------------|---------------------------|
| JeroMQ 0.6                     | ZeroMQ messaging          |
| jackson-dataformat-msgpack 0.9 | MessagePack serialization |
| Failsafe 3.3                   | Retry policies            |
| Micrometer 1.16                | Observability metrics     |

## Examples

Full working examples are available under `src/dev/java/`:

- `example.producer.ExampleProducer` -- HTTP endpoint that inserts a user and publishes via StrongZero
- `example.consumer.ExampleConsumer` -- Consumes user events and replicates them as members

Run with the `dev` Maven profile (active by default).

## License

[Eclipse Public License 2.0](https://www.eclipse.org/legal/epl-2.0/)
