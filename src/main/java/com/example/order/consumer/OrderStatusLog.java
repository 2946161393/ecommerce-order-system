package com.example.order.consumer;

import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

import java.time.Instant;
import java.util.UUID;

/**
 * Cassandra row for the order status flow log.
 *
 * Partition key (user_id, bucket): bucket = userId + "-" + YYYYMM keeps any one
 * user's partition from growing without bound (avoids a hot/huge partition).
 * Clustering by event_time DESC means a user's most recent status events come
 * back first; event_id is a stable tiebreaker.
 */
@Table("order_status_log")
public class OrderStatusLog {

    @PrimaryKeyColumn(name = "user_id", type = PrimaryKeyType.PARTITIONED, ordinal = 0)
    private String userId;

    @PrimaryKeyColumn(name = "bucket", type = PrimaryKeyType.PARTITIONED, ordinal = 1)
    private String bucket;

    @PrimaryKeyColumn(name = "event_time", type = PrimaryKeyType.CLUSTERED,
            ordinal = 2, ordering = Ordering.DESCENDING)
    private Instant eventTime;

    @PrimaryKeyColumn(name = "event_id", type = PrimaryKeyType.CLUSTERED, ordinal = 3)
    private UUID eventId;

    @Column("order_id")
    private Long orderId;

    @Column("status")
    private String status;

    public OrderStatusLog() {
    }

    public OrderStatusLog(String userId, String bucket, Instant eventTime, UUID eventId,
                          Long orderId, String status) {
        this.userId = userId;
        this.bucket = bucket;
        this.eventTime = eventTime;
        this.eventId = eventId;
        this.orderId = orderId;
        this.status = status;
    }

    public String getUserId() { return userId; }
    public String getBucket() { return bucket; }
    public Instant getEventTime() { return eventTime; }
    public UUID getEventId() { return eventId; }
    public Long getOrderId() { return orderId; }
    public String getStatus() { return status; }
}
