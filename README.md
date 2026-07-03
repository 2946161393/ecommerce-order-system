# E-commerce Order System

A Spring Boot backend that demonstrates four things end to end:

1. **Stateless JWT authentication** (Spring Security 6 + BCrypt)
2. **Reliable event publishing** via the **transactional outbox pattern** (MySQL + Kafka)
3. **Idempotent async consumption** writing an order status log to **Cassandra**
4. **A choreography Saga with compensation**: orders are only confirmed after
   inventory is reserved; insufficient stock automatically cancels the order

## Architecture

```
Client ──register/login──> Auth (BCrypt + JWT, MySQL users)

Client ──POST /api/orders (Bearer JWT)──> OrderService
            @Transactional {
                INSERT orders (status=PENDING)  ┐ one DB transaction:
                INSERT outbox (ORDER_CREATED)   ┘ both commit or roll back
            }
                        │
        OutboxPoller ──> Kafka topic: order-events
                        │
        ┌───────────────┴──────────────────┐
        ▼                                  ▼
InventoryConsumer                   OrderEventConsumer
  try reserve stock                   (idempotent via event_id)
  outcome -> outbox                   writes Cassandra status log
        │
        ▼ Kafka topic: inventory-events
OrderSagaConsumer
  INVENTORY_RESERVED -> order PENDING -> CONFIRMED
  INVENTORY_FAILED   -> order PENDING -> CANCELLED   <-- Saga COMPENSATION
```

Order state machine: `PENDING -> CONFIRMED` (stock reserved) or
`PENDING -> CANCELLED` (compensated / user-cancelled). Orders are never
physically deleted; every transition is appended to the Cassandra log.

## Tech stack

- Java 17, Spring Boot 3.3
- Spring Security 6, JWT (jjwt)
- Spring Data JPA + MySQL 8
- Spring for Apache Kafka, Confluent cp-kafka 7.7 (KRaft mode, no Zookeeper)
- Spring Data Cassandra, Cassandra 4.1
- Docker Compose for all infrastructure

## Prerequisites (Windows 10)

1. **Docker Desktop** with the WSL2 backend. (Windows 10 2004+ supports WSL2.)
   After install, run `wsl --update` once in an admin PowerShell if prompted.
2. **JDK 17** (e.g. Temurin 17). Check with `java -version`.
3. **Maven** (or use the bundled wrapper if you add one). Check with `mvn -version`.
4. `curl` is built into Windows 10 (1803+).

## Run it

All commands are run from the project root in a normal terminal (cmd or
PowerShell). Use forward slashes in paths.

### 1. Start infrastructure

```
docker compose up -d
```

Wait until all three containers are healthy (about a minute for Cassandra):

```
docker compose ps
```

### 2. Create the Cassandra schema (one time)

MySQL tables are created automatically by Hibernate on first boot. Cassandra is
not, so create its keyspace and tables once the container is healthy:

```
docker exec -i eos-cassandra cqlsh < db/cassandra-init.cql
```

(If `<` redirection misbehaves in PowerShell, use:
`type db\cassandra-init.cql | docker exec -i eos-cassandra cqlsh`)

### 3. Run the app

```
mvn spring-boot:run
```

The app starts on http://localhost:8080.

### 4. Try the flow

Run `demo.bat`, or do it by hand:

```
REM register
curl -X POST http://localhost:8080/api/auth/register -H "Content-Type: application/json" -d "{\"username\":\"alice\",\"password\":\"secret123\"}"

REM login -> returns a token
curl -X POST http://localhost:8080/api/auth/login -H "Content-Type: application/json" -d "{\"username\":\"alice\",\"password\":\"secret123\"}"

REM set the token, then create an order
set TOKEN=paste-token-here
curl -X POST http://localhost:8080/api/orders -H "Authorization: Bearer %TOKEN%" -H "Content-Type: application/json" -d "{\"productName\":\"Keyboard\",\"amount\":49.99}"

REM check the consumer wrote the status log to Cassandra
docker exec -it eos-cassandra cqlsh -e "SELECT * FROM orderks.order_status_log;"
```

You should see a row in `order_status_log` a couple of seconds after creating an
order, written by the Kafka consumer.

## Interview talking points

These three are the reason the project exists. Be ready to explain each.

### 1. Why not just send to Kafka inside `@Transactional`?

Because the DB commit and the Kafka send are two separate systems with no shared
transaction. If the process dies between them you get an order with no event, or
an event with no order. This is the dual-write problem.

The outbox pattern fixes it: the event is written to an `outbox` table in the
**same** DB transaction as the order, so they commit or roll back together. A
separate poller publishes PENDING rows to Kafka afterward and marks them SENT.
Publishing is now retryable and never produces a phantom event. The delivery
guarantee is **at-least-once**.

### 2. Why must the consumer be idempotent?

Because the producer side is at-least-once. The poller can publish a row, then
crash before marking it SENT, and republish it next tick. So the same event can
arrive twice. The consumer checks `processed_events` by `event_id` before
writing; a duplicate is skipped. It also uses **manual ack**, acknowledging the
offset only after the Cassandra write succeeds, so a failed write is redelivered
rather than lost.

### 3. Why Cassandra for the status log, and why this key design?

The status log is write-heavy and queried by user over time, which fits
Cassandra's log-structured, partition-oriented model. Primary key:
`((user_id, bucket), event_time DESC, event_id)`.

- `user_id` partitions data so one user's events live together.
- `bucket` (= `user_id` + year-month) caps partition growth so a very active
  user does not create one unboundedly large/hot partition.
- `event_time DESC` returns a user's most recent events first.
- `event_id` is a stable clustering tiebreaker.

### 4. How does the Saga work, and why not 2PC?

Creating an order spans two services (order + inventory) with no shared
transaction. 2PC would give strong consistency but requires holding locks
across services and a coordinator that is a single point of failure.

Instead this is a choreography Saga: each step commits locally and publishes
an event (via its own outbox). The order is created as PENDING; the inventory
service consumes ORDER_CREATED and tries to reserve stock; the order service
consumes the outcome. On INVENTORY_FAILED the order is compensated: a
business-level reverse action (PENDING -> CANCELLED), not a database rollback,
because those transactions already committed. The cost is a short window where
the order is PENDING; for an order flow that is acceptable.

Idempotency in the Saga uses the state machine itself: the completion handler
only acts on PENDING orders, so duplicate outcome events are no-ops. Contrast
with the Cassandra consumer, which dedupes by event_id. Two different
idempotency techniques, each fitting its consumer.

### Bonus: JWT trade-off

Stateless JWT means any instance with the same secret can validate a token, so
auth scales horizontally with no shared session store. The cost is that a token
cannot be revoked before it expires without extra machinery (a server-side
blacklist, or short expiry plus refresh tokens). This project uses a 1-hour
expiry and does not implement refresh, which is a deliberate scope decision.

## What was intentionally left out

Front end, refresh tokens, Redis, a real migration tool (Flyway), and container
orchestration. Each is a known extension, omitted to keep the scope to the three
concepts above.
