package com.example.order.inventory;

import com.example.order.common.OrderEvent;
import com.example.order.common.SagaConstants;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Inventory side of the Saga: consumes ORDER_CREATED from order-events and
 * attempts to reserve stock. The outcome event goes out via the outbox inside
 * tryReserve's transaction.
 *
 * Idempotency: at-least-once delivery means the same ORDER_CREATED can arrive
 * twice. Reserving twice would double-decrement stock. We keep an in-memory
 * set of processed event ids (sufficient for a single-instance demo; a
 * production system would persist this like the Cassandra processed_events
 * table on the order side, and we call that out as a known simplification).
 *
 * Concurrency: stock rows carry @Version, so two simultaneous reservations of
 * the same product serialize via optimistic locking; the loser is retried by
 * not acking, and the redelivery re-runs the reservation against fresh state.
 */
@Component
public class InventoryConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryConsumer.class);

    private final ObjectMapper objectMapper;
    private final InventoryService inventoryService;
    private final Set<String> processedEventIds =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

    public InventoryConsumer(ObjectMapper objectMapper,
                             InventoryService inventoryService) {
        this.objectMapper = objectMapper;
        this.inventoryService = inventoryService;
    }

    @KafkaListener(
            topics = SagaConstants.ORDER_EVENTS_TOPIC,
            groupId = "inventory-service")
    public void consume(String message, Acknowledgment ack) {
        try {
            // Only ORDER_CREATED starts a reservation; ORDER_CANCELLED and other
            // event types on this topic are not for us.
            JsonNode root = objectMapper.readTree(message);
            OrderEvent event = objectMapper.treeToValue(root, OrderEvent.class);

            if (!SagaConstants.STATUS_PENDING.equals(event.status())) {
                ack.acknowledge();
                return;
            }

            if (processedEventIds.contains(event.eventId())) {
                log.info("Inventory: duplicate event {} ignored", event.eventId());
                ack.acknowledge();
                return;
            }

            inventoryService.tryReserve(event.orderId(), event.productName(), 1);

            processedEventIds.add(event.eventId());
            ack.acknowledge();

        } catch (OptimisticLockingFailureException e) {
            // concurrent stock update: do not ack, redelivery retries cleanly
            log.warn("Inventory: optimistic lock conflict, will retry: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Inventory: failed to process message, will be redelivered: {}",
                    e.getMessage(), e);
        }
    }
}
