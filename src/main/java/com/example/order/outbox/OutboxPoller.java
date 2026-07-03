package com.example.order.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Publishes outbox rows to Kafka.
 *
 * Runs on a fixed schedule. For each PENDING row it sends the payload to the
 * order-events topic (keyed by aggregate id so all events for one order land
 * in the same partition and stay ordered), then marks the row SENT.
 *
 * Failure handling: if the Kafka send fails, the row stays PENDING and is
 * retried on the next tick. If the send succeeds but the app crashes before
 * marking SENT, the row is republished next time. That double-send is exactly
 * why the consumer must be idempotent. This is at-least-once delivery.
 */
@Component
public class OutboxPoller {

    private static final Logger log = LoggerFactory.getLogger(OutboxPoller.class);

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final int batchSize;

    public OutboxPoller(OutboxRepository outboxRepository,
                        KafkaTemplate<String, String> kafkaTemplate,
                        @Value("${app.outbox.batch-size}") int batchSize) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms}")
    @Transactional
    public void publishPending() {
        List<OutboxEvent> pending = outboxRepository.findByStatus(
                OutboxEvent.Status.PENDING, PageRequest.of(0, batchSize));

        if (pending.isEmpty()) {
            return;
        }

        for (OutboxEvent event : pending) {
            try {
                // each event carries its own destination topic (order-events or
                // inventory-events), keyed by aggregate id for per-order ordering.
                kafkaTemplate.send(event.getDestinationTopic(),
                        event.getAggregateId(), event.getPayload()).get();
                event.markSent();
                log.info("Published {} event {} to {} (aggregate {})",
                        event.getEventType(), event.getEventId(),
                        event.getDestinationTopic(), event.getAggregateId());
            } catch (Exception e) {
                // leave this row PENDING; it will be retried next tick.
                log.warn("Failed to publish outbox event {}, will retry: {}",
                        event.getEventId(), e.getMessage());
            }
        }
        // marked-SENT rows are flushed when the transaction commits
    }
}
