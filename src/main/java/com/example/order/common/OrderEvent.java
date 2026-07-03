package com.example.order.common;

import java.math.BigDecimal;

/**
 * The event payload serialized into the outbox and published to Kafka.
 *
 * eventId is the idempotency key: the consumer uses it to detect and skip
 * duplicate deliveries (at-least-once means the same event can arrive twice).
 */
public record OrderEvent(
        String eventId,
        Long orderId,
        String userId,
        String productName,
        BigDecimal amount,
        String status,
        long occurredAtEpochMs) {
}
