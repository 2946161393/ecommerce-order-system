package com.example.order.consumer;

import com.example.order.common.OrderEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/**
 * Consumes order events and writes a status-log row to Cassandra.
 *
 * Idempotency: the producer side is at-least-once (the outbox poller can
 * republish a row after a crash), so the same event can arrive more than once.
 * Before writing, we check processed_events for this event_id. If it is already
 * there, we skip the write and just ack. This makes reprocessing a no-op.
 *
 * Manual ack: we only acknowledge the offset AFTER the Cassandra writes
 * succeed. If the write throws, we do not ack, so the message is redelivered
 * and retried, rather than silently lost.
 */
@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);
    private static final DateTimeFormatter MONTH =
            DateTimeFormatter.ofPattern("yyyyMM").withZone(ZoneOffset.UTC);

    private final ObjectMapper objectMapper;
    private final OrderStatusLogRepository statusLogRepository;
    private final ProcessedEventRepository processedEventRepository;

    public OrderEventConsumer(ObjectMapper objectMapper,
                              OrderStatusLogRepository statusLogRepository,
                              ProcessedEventRepository processedEventRepository) {
        this.objectMapper = objectMapper;
        this.statusLogRepository = statusLogRepository;
        this.processedEventRepository = processedEventRepository;
    }

    @KafkaListener(
            topics = com.example.order.common.SagaConstants.ORDER_EVENTS_TOPIC,
            groupId = "${spring.kafka.consumer.group-id}")
    public void consume(String message, Acknowledgment ack) {
        try {
            OrderEvent event = objectMapper.readValue(message, OrderEvent.class);
            UUID eventId = UUID.fromString(event.eventId());

            // idempotency guard
            if (processedEventRepository.existsById(eventId)) {
                log.info("Duplicate event {} ignored", eventId);
                ack.acknowledge();
                return;
            }

            Instant eventTime = Instant.ofEpochMilli(event.occurredAtEpochMs());
            String bucket = event.userId() + "-" + MONTH.format(eventTime);

            statusLogRepository.save(new OrderStatusLog(
                    event.userId(), bucket, eventTime, eventId,
                    event.orderId(), event.status()));

            processedEventRepository.save(new ProcessedEvent(eventId, Instant.now()));

            log.info("Wrote status log for order {} (event {})", event.orderId(), eventId);
            ack.acknowledge();

        } catch (Exception e) {
            // do not ack: the message will be redelivered and retried
            log.error("Failed to process message, will be redelivered: {}", e.getMessage(), e);
        }
    }
}
