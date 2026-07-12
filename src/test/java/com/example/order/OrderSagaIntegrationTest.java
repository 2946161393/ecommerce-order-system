package com.example.order;

import com.example.order.common.SagaConstants;
import com.example.order.inventory.InventoryService;
import com.example.order.order.OrderEntity;
import com.example.order.order.OrderRepository;
import com.example.order.order.OrderService;
import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end Saga test against REAL infrastructure.
 *
 * Testcontainers starts genuine MySQL, Kafka, and Cassandra in Docker for the
 * duration of the test class, the full Spring context boots against them, and
 * we drive the system through its actual service layer:
 *
 *   happy path:   set stock -> create order -> outbox poller publishes ->
 *                 inventory consumer reserves -> saga consumer CONFIRMs
 *   compensation: create order for an unknown product -> INVENTORY_FAILED ->
 *                 saga consumer CANCELs (the compensation step)
 *
 * Because the flow is asynchronous (outbox polling + Kafka), assertions use
 * Awaitility: poll the order status until it reaches the expected state or a
 * timeout expires. Never Thread.sleep in async tests.
 */
@Testcontainers
@SpringBootTest
@EnabledIfEnvironmentVariable(named = "RUN_INTEGRATION_TESTS", matches = "true")
class OrderSagaIntegrationTest {

    @Container
    @ServiceConnection
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0");

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.4.0")).withKraft();

    @Container
    static CassandraContainer<?> cassandra =
            new CassandraContainer<>("cassandra:4.1");

    /**
     * Cassandra has no @ServiceConnection auto-config for the schema, so wire
     * the connection properties manually and create the keyspace/tables before
     * Spring boots (spring-data needs the keyspace to exist).
     */
    @DynamicPropertySource
    static void cassandraProps(DynamicPropertyRegistry registry) {
        registry.add("spring.cassandra.contact-points",
                () -> cassandra.getHost() + ":" + cassandra.getMappedPort(9042));
        registry.add("spring.cassandra.local-datacenter", () -> "datacenter1");
        registry.add("spring.cassandra.keyspace-name", () -> "orderks");
    }

    @BeforeAll
    static void createCassandraSchema() {
        try (CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(
                        cassandra.getHost(), cassandra.getMappedPort(9042)))
                .withLocalDatacenter("datacenter1")
                .build()) {
            session.execute("CREATE KEYSPACE IF NOT EXISTS orderks WITH replication = "
                    + "{'class': 'SimpleStrategy', 'replication_factor': 1}");
            session.execute("CREATE TABLE IF NOT EXISTS orderks.order_status_log ("
                    + "user_id text, bucket text, event_time timestamp, event_id uuid, "
                    + "order_id bigint, status text, "
                    + "PRIMARY KEY ((user_id, bucket), event_time, event_id)) "
                    + "WITH CLUSTERING ORDER BY (event_time DESC, event_id ASC)");
            session.execute("CREATE TABLE IF NOT EXISTS orderks.processed_events ("
                    + "event_id uuid PRIMARY KEY, processed_at timestamp)");
        }
    }

    @Autowired OrderService orderService;
    @Autowired OrderRepository orderRepository;
    @Autowired InventoryService inventoryService;

    @Test
    void happyPath_orderIsConfirmed_whenStockIsAvailable() {
        inventoryService.setStock("Keyboard", 2);

        OrderEntity order = orderService.createOrder(
                "alice", "Keyboard", new BigDecimal("49.99"));
        assertThat(order.getStatus()).isEqualTo(SagaConstants.STATUS_PENDING);

        // outbox poll (2s) + kafka round trip: poll until CONFIRMED
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    OrderEntity reloaded = orderRepository.findById(order.getId()).orElseThrow();
                    assertThat(reloaded.getStatus()).isEqualTo(SagaConstants.STATUS_CONFIRMED);
                });
    }

    @Test
    void compensation_orderIsCancelled_whenStockIsInsufficient() {
        // "Vaporware" has no stock record: the inventory service will emit
        // INVENTORY_FAILED and the saga must compensate.
        OrderEntity order = orderService.createOrder(
                "alice", "Vaporware", new BigDecimal("999"));
        assertThat(order.getStatus()).isEqualTo(SagaConstants.STATUS_PENDING);

        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    OrderEntity reloaded = orderRepository.findById(order.getId()).orElseThrow();
                    assertThat(reloaded.getStatus()).isEqualTo(SagaConstants.STATUS_CANCELLED);
                });
    }
}
