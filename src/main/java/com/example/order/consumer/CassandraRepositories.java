package com.example.order.consumer;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.MapIdCassandraRepository;

import java.util.List;
import java.util.UUID;

interface OrderStatusLogRepository extends MapIdCassandraRepository<OrderStatusLog> {

    /**
     * Query a user's status events for one bucket (= userId + "-" + YYYYMM).
     *
     * This hits the FULL partition key (user_id, bucket), so it is a single
     * efficient partition read, no ALLOW FILTERING needed. Results come back in
     * event_time DESC order automatically because that is the table's declared
     * clustering order.
     */
    List<OrderStatusLog> findByUserIdAndBucket(String userId, String bucket);
}

interface ProcessedEventRepository extends CassandraRepository<ProcessedEvent, UUID> {
}
