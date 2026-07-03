package com.example.order.outbox;

import jakarta.persistence.*;
import java.time.Instant;

/**
 * The outbox table. The whole point of the project.
 *
 * A row is written to this table IN THE SAME DB TRANSACTION as the business
 * write (the order). Because both writes commit or roll back together, we never
 * end up with an order but no event, or an event but no order.
 *
 * A separate poller later reads PENDING rows and publishes them to Kafka,
 * then marks them SENT. That decouples "the event is durably recorded" (done
 * inside the transaction) from "the event reached Kafka" (done afterward,
 * retryable). This gives at-least-once delivery.
 */
@Entity
@Table(name = "outbox")
public class OutboxEvent {

    public enum Status { PENDING, SENT }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** A stable unique id for this event, used by consumers for idempotency. */
    @Column(name = "event_id", nullable = false, unique = true, length = 36)
    private String eventId;

    /** The id of the aggregate this event is about (the order id). */
    @Column(name = "aggregate_id", nullable = false)
    private String aggregateId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    /**
     * Which Kafka topic this event should be published to. Added for the Saga:
     * order-service events go to order-events, inventory-service events go to
     * inventory-events. The poller reads this per-row instead of using one
     * hard-coded topic, so the same outbox mechanism serves every step of the
     * Saga.
     */
    @Column(name = "destination_topic", nullable = false)
    private String destinationTopic;

    /** JSON payload, published as the Kafka message value. */
    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private Status status = Status.PENDING;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    protected OutboxEvent() {
    }

    public OutboxEvent(String eventId, String aggregateId, String eventType,
                       String destinationTopic, String payload) {
        this.eventId = eventId;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.destinationTopic = destinationTopic;
        this.payload = payload;
    }

    public void markSent() {
        this.status = Status.SENT;
    }

    public Long getId() { return id; }
    public String getEventId() { return eventId; }
    public String getAggregateId() { return aggregateId; }
    public String getEventType() { return eventType; }
    public String getDestinationTopic() { return destinationTopic; }
    public String getPayload() { return payload; }
    public Status getStatus() { return status; }
    public Instant getCreatedAt() { return createdAt; }
}
