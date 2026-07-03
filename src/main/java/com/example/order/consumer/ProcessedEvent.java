package com.example.order.consumer;

import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

import java.time.Instant;
import java.util.UUID;

/**
 * Records which event ids have already been consumed.
 *
 * Before writing a status log row, the consumer checks whether this event_id
 * is already present. If it is, the message is a duplicate (at-least-once
 * redelivery) and is skipped. This is what makes the consumer idempotent.
 *
 * Column names are given explicitly so the snake_case table columns
 * (event_id, processed_at) match; otherwise Spring Data derives camelCase
 * names lowercased (eventid, processedat) which do not exist in the table.
 */
@Table("processed_events")
public class ProcessedEvent {

    @PrimaryKey
    @Column("event_id")
    private UUID eventId;

    @Column("processed_at")
    private Instant processedAt;

    public ProcessedEvent() {
    }

    public ProcessedEvent(UUID eventId, Instant processedAt) {
        this.eventId = eventId;
        this.processedAt = processedAt;
    }

    public UUID getEventId() { return eventId; }
    public Instant getProcessedAt() { return processedAt; }
}
