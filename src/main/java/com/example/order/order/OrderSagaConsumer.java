package com.example.order.order;

import com.example.order.common.InventoryEvent;
import com.example.order.common.SagaConstants;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Order side of the Saga reply channel.
 *
 * Consumes inventory outcomes:
 *   INVENTORY_RESERVED -> confirm the order (PENDING -> CONFIRMED)
 *   INVENTORY_FAILED   -> compensate (PENDING -> CANCELLED)
 *
 * Idempotency here is by state: completeSaga only acts on PENDING orders, so a
 * duplicate outcome event finds the order already CONFIRMED/CANCELLED and is a
 * no-op. This is "natural idempotency" via the state machine, a different
 * technique from the event-id dedup used by the Cassandra consumer, and worth
 * contrasting in an interview.
 */
@Component
public class OrderSagaConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderSagaConsumer.class);

    private final ObjectMapper objectMapper;
    private final OrderService orderService;

    public OrderSagaConsumer(ObjectMapper objectMapper, OrderService orderService) {
        this.objectMapper = objectMapper;
        this.orderService = orderService;
    }

    @KafkaListener(
            topics = SagaConstants.INVENTORY_EVENTS_TOPIC,
            groupId = "order-saga")
    public void consume(String message, Acknowledgment ack) {
        try {
            InventoryEvent event = objectMapper.readValue(message, InventoryEvent.class);

            boolean reserved = SagaConstants.INVENTORY_RESERVED.equals(event.eventType());
            orderService.completeSaga(event.orderId(), reserved);

            log.info("Saga {} for order {} ({})",
                    reserved ? "CONFIRMED" : "COMPENSATED (cancelled)",
                    event.orderId(),
                    event.reason() == null ? "stock reserved" : event.reason());

            ack.acknowledge();

        } catch (Exception e) {
            log.error("Saga consumer failed, message will be redelivered: {}",
                    e.getMessage(), e);
        }
    }
}
