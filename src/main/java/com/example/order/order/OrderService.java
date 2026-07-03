package com.example.order.order;

import com.example.order.common.OrderEvent;
import com.example.order.common.SagaConstants;
import com.example.order.outbox.OutboxEvent;
import com.example.order.outbox.OutboxRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public OrderService(OrderRepository orderRepository,
                        OutboxRepository outboxRepository,
                        ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * Create an order.
     *
     * Both writes (orders + outbox) happen in ONE transaction. We deliberately
     * do NOT call Kafka here. Publishing to Kafka inside a DB transaction is the
     * classic dual-write bug: the DB commit and the Kafka send are two separate
     * systems with no shared transaction, so a crash between them leaves them
     * inconsistent (order with no event, or event with no order).
     *
     * Instead we record the event durably in the same DB transaction, and let
     * the OutboxPoller publish it afterward. If the transaction rolls back, the
     * outbox row rolls back with it, so no phantom event is ever published.
     */
    @Transactional
    public OrderEntity createOrder(String userId, String productName, BigDecimal amount) {
        // 1. write the business row. Saga: order starts PENDING and is only
        //    CONFIRMED after the inventory service reserves stock.
        OrderEntity order = orderRepository.save(
                new OrderEntity(userId, productName, amount, SagaConstants.STATUS_PENDING));

        // 2. write the event to the outbox in the SAME transaction
        writeOutboxEvent(order, SagaConstants.ORDER_CREATED);

        return order;
    }

    /**
     * Saga completion: called when the inventory service reports the outcome.
     *
     * INVENTORY_RESERVED  -> PENDING order becomes CONFIRMED (happy path done).
     * INVENTORY_FAILED    -> PENDING order becomes CANCELLED. This is the
     *                        COMPENSATION step: the local "create order" commit
     *                        is undone by a business-level reverse action, not
     *                        a database rollback (those transactions already
     *                        committed). That is the essence of a Saga.
     *
     * Both transitions emit an event so the Cassandra status log records the
     * full lifecycle (PENDING -> CONFIRMED, or PENDING -> CANCELLED).
     * Idempotent by state check: if the order already left PENDING, do nothing.
     */
    @Transactional
    public void completeSaga(Long orderId, boolean reserved) {
        OrderEntity order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return; // unknown order; nothing to do
        }
        if (!SagaConstants.STATUS_PENDING.equals(order.getStatus())) {
            return; // already completed or cancelled; duplicate outcome event
        }

        if (reserved) {
            order.changeStatus(SagaConstants.STATUS_CONFIRMED);
            orderRepository.save(order);
            writeOutboxEvent(order, "ORDER_CONFIRMED");
        } else {
            order.changeStatus(SagaConstants.STATUS_CANCELLED);
            orderRepository.save(order);
            writeOutboxEvent(order, SagaConstants.ORDER_CANCELLED);
        }
    }

    /**
     * Cancel an order (user-initiated).
     *
     * This is a soft delete / status transition, not a physical delete. The
     * order row stays in MySQL with status CANCELLED, and a ORDER_CANCELLED
     * event goes to the outbox in the SAME transaction, exactly like create.
     * The consumer then appends a CANCELLED row to the Cassandra status log,
     * so the full lifecycle is preserved as an audit trail.
     */
    @Transactional
    public OrderEntity cancelOrder(String userId, Long orderId) {
        OrderEntity order = orderRepository.findById(orderId)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "Order not found"));

        // a user may only cancel their own order
        if (!order.getUserId().equals(userId)) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Not your order");
        }
        if (SagaConstants.STATUS_CANCELLED.equals(order.getStatus())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Order already cancelled");
        }

        order.changeStatus(SagaConstants.STATUS_CANCELLED);
        try {
            // JPA adds "WHERE version = ?" here. If another transaction already
            // updated this order since we read it, this save matches 0 rows and
            // throws, which we surface as 409 so the client can retry.
            orderRepository.saveAndFlush(order);
        } catch (OptimisticLockingFailureException e) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Order was modified concurrently, please retry");
        }

        writeOutboxEvent(order, SagaConstants.ORDER_CANCELLED);

        return order;
    }

    @Transactional(readOnly = true)
    public List<OrderEntity> listOrders(String userId) {
        return orderRepository.findByUserIdOrderByCreatedAtDesc(userId);
    }

    /** Build an OrderEvent for the order's current state and write it to the outbox. */
    private void writeOutboxEvent(OrderEntity order, String eventType) {
        String eventId = UUID.randomUUID().toString();
        OrderEvent event = new OrderEvent(
                eventId,
                order.getId(),
                order.getUserId(),
                order.getProductName(),
                order.getAmount(),
                order.getStatus(),
                System.currentTimeMillis());

        outboxRepository.save(new OutboxEvent(
                eventId,
                String.valueOf(order.getId()),
                eventType,
                SagaConstants.ORDER_EVENTS_TOPIC,
                toJson(event)));
    }

    private String toJson(OrderEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            // serialization of a small record should never fail; fail loudly if it does
            throw new IllegalStateException("Failed to serialize OrderEvent", e);
        }
    }
}
