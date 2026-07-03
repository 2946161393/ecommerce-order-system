package com.example.order.inventory;

import jakarta.persistence.*;

/**
 * Stock level for a product.
 *
 * available quantity carries a @Version too: concurrent reservations against
 * the same product must not oversell, so the version check serializes
 * conflicting decrements (the loser retries).
 */
@Entity
@Table(name = "inventory")
public class InventoryEntity {

    @Id
    @Column(name = "product_name")
    private String productName;

    @Column(name = "available_quantity", nullable = false)
    private int availableQuantity;

    @Version
    @Column(nullable = false)
    private Long version;

    protected InventoryEntity() {
    }

    public InventoryEntity(String productName, int availableQuantity) {
        this.productName = productName;
        this.availableQuantity = availableQuantity;
    }

    /** Returns true and decrements if enough stock; false if insufficient. */
    public boolean tryReserve(int quantity) {
        if (availableQuantity >= quantity) {
            availableQuantity -= quantity;
            return true;
        }
        return false;
    }

    public void setAvailableQuantity(int availableQuantity) {
        this.availableQuantity = availableQuantity;
    }

    public String getProductName() { return productName; }
    public int getAvailableQuantity() { return availableQuantity; }
    public Long getVersion() { return version; }
}
