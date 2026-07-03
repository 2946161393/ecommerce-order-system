package com.example.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point.
 *
 * @EnableScheduling turns on the @Scheduled OutboxPoller, which is the
 * piece that makes the outbox pattern work: it periodically scans the
 * outbox table and publishes PENDING rows to Kafka.
 */
@SpringBootApplication
@EnableScheduling
public class OrderApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderApplication.class, args);
    }
}
