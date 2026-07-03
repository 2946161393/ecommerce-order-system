-- Run this AFTER the cassandra container is healthy:
--   docker exec -i eos-cassandra cqlsh < db/cassandra-init.cql
-- It is idempotent (IF NOT EXISTS), safe to run more than once.

CREATE KEYSPACE IF NOT EXISTS orderks
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE orderks;

-- Order status flow log.
-- Partition key (user_id, bucket): bucket = user_id + "-" + YYYYMM, to avoid
--   a single very active user becoming a hot partition.
-- Clustering: event_time DESC so a user's recent events come first,
--   event_id as a tiebreaker for stable ordering / dedup.
CREATE TABLE IF NOT EXISTS order_status_log (
    user_id     text,
    bucket      text,
    event_time  timestamp,
    event_id    uuid,
    order_id    bigint,
    status      text,
    PRIMARY KEY ((user_id, bucket), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC, event_id ASC);

-- Idempotency guard: which event_ids have already been consumed.
-- Consumer checks this before writing the status log, so duplicate
-- Kafka deliveries (at-least-once) do not produce duplicate rows.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id    uuid,
    processed_at timestamp,
    PRIMARY KEY (event_id)
);
