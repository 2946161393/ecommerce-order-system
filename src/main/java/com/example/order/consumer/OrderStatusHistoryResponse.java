package com.example.order.consumer;

import java.time.Instant;
import java.util.UUID;

/**
 * One entry in an order's status history, returned by the history query.
 *
 * This is the read side of the Cassandra status log: the consumer writes
 * OrderStatusLog rows, and this DTO is what we expose when reading them back.
 */
public record OrderStatusHistoryResponse(
        Long orderId,
        String status,
        Instant eventTime,
        UUID eventId) {

    public static OrderStatusHistoryResponse from(OrderStatusLog log) {
        return new OrderStatusHistoryResponse(
                log.getOrderId(),
                log.getStatus(),
                log.getEventTime(),
                log.getEventId());
    }
}
