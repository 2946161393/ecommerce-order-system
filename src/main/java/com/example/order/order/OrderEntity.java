package com.example.order.order;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "orders")
public class OrderEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "user_id", nullable = false)
    private String userId;

    @Column(name = "product_name", nullable = false)
    private String productName;

    @Column(nullable = false)
    private BigDecimal amount;

    @Column(nullable = false)
    private String status;

    /**
     * Optimistic lock version. JPA auto-increments this on every update and
     * adds "WHERE version = ?" to the UPDATE. If two transactions read the same
     * row and both try to update it, the second one matches 0 rows and JPA
     * throws OptimisticLockingFailureException. This prevents lost updates on
     * concurrent modification (e.g. two requests cancelling the same order)
     * without holding a database lock.
     */
    @Version
    @Column(nullable = false)
    private Long version;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    protected OrderEntity() {
    }

    public OrderEntity(String userId, String productName, BigDecimal amount, String status) {
        this.userId = userId;
        this.productName = productName;
        this.amount = amount;
        this.status = status;
    }

    /** Transition the order to a new status (e.g. CREATED -> CANCELLED). */
    public void changeStatus(String newStatus) {
        this.status = newStatus;
    }

    public Long getId() { return id; }
    public String getUserId() { return userId; }
    public String getProductName() { return productName; }
    public BigDecimal getAmount() { return amount; }
    public String getStatus() { return status; }
    public Instant getCreatedAt() { return createdAt; }
}
