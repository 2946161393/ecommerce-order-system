package com.example.order.order;

import com.example.order.common.SagaConstants;
import com.example.order.outbox.OutboxEvent;
import com.example.order.outbox.OutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for OrderService. The repositories are mocked, so these run fast
 * with no database. They pin down the Saga state machine and the optimistic
 * lock behavior, the two pieces most likely to break on refactor.
 */
@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock OrderRepository orderRepository;
    @Mock OutboxRepository outboxRepository;

    // a real ObjectMapper is fine; it has no external dependency
    ObjectMapper objectMapper = new ObjectMapper();

    OrderService orderService;

    @BeforeEach
    void setUp() {
        orderService = new OrderService(orderRepository, outboxRepository, objectMapper);
    }

    @Test
    void createOrder_startsPending_andWritesOutbox() {
        when(orderRepository.save(any(OrderEntity.class)))
                .thenAnswer(inv -> inv.getArgument(0));

        OrderEntity order = orderService.createOrder("alice", "Keyboard", new BigDecimal("49.99"));

        // order begins its life PENDING (Saga confirms it later)
        assertThat(order.getStatus()).isEqualTo(SagaConstants.STATUS_PENDING);

        // exactly one outbox event was written, of type ORDER_CREATED, to order-events
        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxRepository).save(captor.capture());
        assertThat(captor.getValue().getEventType()).isEqualTo(SagaConstants.ORDER_CREATED);
        assertThat(captor.getValue().getDestinationTopic())
                .isEqualTo(SagaConstants.ORDER_EVENTS_TOPIC);
    }

    @Test
    void completeSaga_reservedTrue_confirmsPendingOrder() {
        OrderEntity pending = new OrderEntity("alice", "Keyboard",
                new BigDecimal("49.99"), SagaConstants.STATUS_PENDING);
        when(orderRepository.findById(1L)).thenReturn(Optional.of(pending));
        when(orderRepository.save(any(OrderEntity.class)))
                .thenAnswer(inv -> inv.getArgument(0));

        orderService.completeSaga(1L, true);

        assertThat(pending.getStatus()).isEqualTo(SagaConstants.STATUS_CONFIRMED);
    }

    @Test
    void completeSaga_reservedFalse_compensatesToCancelled() {
        OrderEntity pending = new OrderEntity("alice", "Laptop",
                new BigDecimal("999"), SagaConstants.STATUS_PENDING);
        when(orderRepository.findById(2L)).thenReturn(Optional.of(pending));
        when(orderRepository.save(any(OrderEntity.class)))
                .thenAnswer(inv -> inv.getArgument(0));

        orderService.completeSaga(2L, false);

        // compensation: insufficient stock cancels the order
        assertThat(pending.getStatus()).isEqualTo(SagaConstants.STATUS_CANCELLED);
    }

    @Test
    void completeSaga_isIdempotent_whenOrderAlreadyLeftPending() {
        OrderEntity confirmed = new OrderEntity("alice", "Keyboard",
                new BigDecimal("49.99"), SagaConstants.STATUS_CONFIRMED);
        when(orderRepository.findById(3L)).thenReturn(Optional.of(confirmed));

        // a duplicate INVENTORY_FAILED arrives after the order is already CONFIRMED
        orderService.completeSaga(3L, false);

        // state unchanged, and no new outbox event written
        assertThat(confirmed.getStatus()).isEqualTo(SagaConstants.STATUS_CONFIRMED);
        verify(orderRepository, never()).save(any());
        verify(outboxRepository, never()).save(any());
    }

    @Test
    void cancelOrder_onOptimisticLockConflict_throws409() {
        OrderEntity order = new OrderEntity("alice", "Keyboard",
                new BigDecimal("49.99"), SagaConstants.STATUS_CONFIRMED);
        when(orderRepository.findById(4L)).thenReturn(Optional.of(order));
        when(orderRepository.saveAndFlush(any(OrderEntity.class)))
                .thenThrow(new OptimisticLockingFailureException("version conflict"));

        assertThatThrownBy(() -> orderService.cancelOrder("alice", 4L))
                .isInstanceOf(ResponseStatusException.class)
                .hasMessageContaining("409");
    }

    @Test
    void cancelOrder_byNonOwner_throws403() {
        OrderEntity order = new OrderEntity("alice", "Keyboard",
                new BigDecimal("49.99"), SagaConstants.STATUS_CONFIRMED);
        when(orderRepository.findById(5L)).thenReturn(Optional.of(order));

        assertThatThrownBy(() -> orderService.cancelOrder("mallory", 5L))
                .isInstanceOf(ResponseStatusException.class)
                .hasMessageContaining("403");
    }
}