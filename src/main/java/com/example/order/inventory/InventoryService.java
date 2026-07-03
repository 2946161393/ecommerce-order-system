package com.example.order.inventory;

import com.example.order.common.InventoryEvent;
import com.example.order.common.SagaConstants;
import com.example.order.outbox.OutboxEvent;
import com.example.order.outbox.OutboxRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Inventory side of the Saga.
 *
 * tryReserve is called when an ORDER_CREATED event arrives. It attempts to
 * decrement stock and, IN THE SAME TRANSACTION, writes the outcome event
 * (INVENTORY_RESERVED or INVENTORY_FAILED) to the outbox. The same outbox
 * mechanism that protects order creation protects this step too: stock change
 * and outcome event commit or roll back together, then the poller publishes
 * the event to the inventory-events topic.
 */
@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    private final InventoryRepository inventoryRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public InventoryService(InventoryRepository inventoryRepository,
                            OutboxRepository outboxRepository,
                            ObjectMapper objectMapper) {
        this.inventoryRepository = inventoryRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * Attempt to reserve stock for an order and record the outcome event.
     * Returns true if reserved, false if insufficient (or unknown product).
     */
    @Transactional
    public boolean tryReserve(Long orderId, String productName, int quantity) {
        boolean reserved = inventoryRepository.findById(productName)
                .map(inv -> {
                    boolean ok = inv.tryReserve(quantity);
                    if (ok) {
                        inventoryRepository.save(inv);
                    }
                    return ok;
                })
                .orElse(false); // unknown product = cannot reserve

        String eventType = reserved
                ? SagaConstants.INVENTORY_RESERVED
                : SagaConstants.INVENTORY_FAILED;
        String reason = reserved ? null : "Insufficient stock for " + productName;

        writeOutcomeEvent(orderId, productName, quantity, eventType, reason);

        log.info("Inventory {} for order {} ({} x{})",
                reserved ? "RESERVED" : "FAILED", orderId, productName, quantity);
        return reserved;
    }

    /** Upsert stock for a product (used by the admin endpoint for demos). */
    @Transactional
    public InventoryEntity setStock(String productName, int quantity) {
        InventoryEntity inv = inventoryRepository.findById(productName)
                .map(existing -> {
                    existing.setAvailableQuantity(quantity);
                    return existing;
                })
                .orElseGet(() -> new InventoryEntity(productName, quantity));
        return inventoryRepository.save(inv);
    }

    public Iterable<InventoryEntity> listStock() {
        return inventoryRepository.findAll();
    }

    private void writeOutcomeEvent(Long orderId, String productName, int quantity,
                                   String eventType, String reason) {
        String eventId = UUID.randomUUID().toString();
        InventoryEvent event = new InventoryEvent(
                eventId, orderId, productName, quantity, eventType, reason,
                System.currentTimeMillis());

        outboxRepository.save(new OutboxEvent(
                eventId,
                String.valueOf(orderId),
                eventType,
                SagaConstants.INVENTORY_EVENTS_TOPIC,
                toJson(event)));
    }

    private String toJson(InventoryEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize InventoryEvent", e);
        }
    }
}
