package com.example.order.common;

/**
 * Event published by the inventory service on the inventory-events topic,
 * consumed by the order service to complete or compensate the Saga.
 *
 * eventType is INVENTORY_RESERVED (stock decremented, order can be confirmed)
 * or INVENTORY_FAILED (insufficient stock, order must be compensated).
 */
public record InventoryEvent(
        String eventId,
        Long orderId,
        String productName,
        int quantity,
        String eventType,
        String reason,
        long occurredAtEpochMs) {
}
