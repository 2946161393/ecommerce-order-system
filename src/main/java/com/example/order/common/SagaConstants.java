package com.example.order.common;

/**
 * Central definitions for the order/inventory Saga: topic names and event
 * types. Keeping them in one place avoids typos across producers and consumers
 * (a mistyped event type silently breaks the Saga).
 */
public final class SagaConstants {

    private SagaConstants() {
    }

    // Topics
    public static final String ORDER_EVENTS_TOPIC = "order-events";
    public static final String INVENTORY_EVENTS_TOPIC = "inventory-events";

    // Event types
    public static final String ORDER_CREATED = "ORDER_CREATED";
    public static final String ORDER_CANCELLED = "ORDER_CANCELLED";
    public static final String INVENTORY_RESERVED = "INVENTORY_RESERVED";
    public static final String INVENTORY_FAILED = "INVENTORY_FAILED";

    // Order statuses (the Saga state machine)
    public static final String STATUS_PENDING = "PENDING";
    public static final String STATUS_CONFIRMED = "CONFIRMED";
    public static final String STATUS_CANCELLED = "CANCELLED";
}
