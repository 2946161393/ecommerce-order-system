package com.example.order.inventory;

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

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for InventoryService: the reserve outcome must match the emitted
 * event type, and stock must only be decremented when the reservation succeeds.
 */
@ExtendWith(MockitoExtension.class)
class InventoryServiceTest {

    @Mock InventoryRepository inventoryRepository;
    @Mock OutboxRepository outboxRepository;

    ObjectMapper objectMapper = new ObjectMapper();

    InventoryService inventoryService;

    @BeforeEach
    void setUp() {
        inventoryService = new InventoryService(
                inventoryRepository, outboxRepository, objectMapper);
    }

    @Test
    void tryReserve_enoughStock_reservesAndEmitsReserved() {
        InventoryEntity inv = new InventoryEntity("Keyboard", 2);
        when(inventoryRepository.findById("Keyboard")).thenReturn(Optional.of(inv));
        when(inventoryRepository.save(any(InventoryEntity.class)))
                .thenAnswer(i -> i.getArgument(0));

        boolean reserved = inventoryService.tryReserve(1L, "Keyboard", 1);

        assertThat(reserved).isTrue();
        assertThat(inv.getAvailableQuantity()).isEqualTo(1); // 2 - 1

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxRepository).save(captor.capture());
        assertThat(captor.getValue().getEventType()).isEqualTo(SagaConstants.INVENTORY_RESERVED);
        assertThat(captor.getValue().getDestinationTopic())
                .isEqualTo(SagaConstants.INVENTORY_EVENTS_TOPIC);
    }

    @Test
    void tryReserve_insufficientStock_failsAndEmitsFailed() {
        InventoryEntity inv = new InventoryEntity("Keyboard", 0);
        when(inventoryRepository.findById("Keyboard")).thenReturn(Optional.of(inv));

        boolean reserved = inventoryService.tryReserve(1L, "Keyboard", 1);

        assertThat(reserved).isFalse();
        assertThat(inv.getAvailableQuantity()).isEqualTo(0); // unchanged
        verify(inventoryRepository, never()).save(any());    // no decrement saved

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxRepository).save(captor.capture());
        assertThat(captor.getValue().getEventType()).isEqualTo(SagaConstants.INVENTORY_FAILED);
    }

    @Test
    void tryReserve_unknownProduct_failsAndEmitsFailed() {
        when(inventoryRepository.findById("Ghost")).thenReturn(Optional.empty());

        boolean reserved = inventoryService.tryReserve(1L, "Ghost", 1);

        assertThat(reserved).isFalse();
        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxRepository).save(captor.capture());
        assertThat(captor.getValue().getEventType()).isEqualTo(SagaConstants.INVENTORY_FAILED);
    }
}