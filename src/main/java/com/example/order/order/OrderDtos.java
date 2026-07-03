package com.example.order.order;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.time.Instant;

public class OrderDtos {

    public record CreateOrderRequest(
            @NotBlank String productName,
            @NotNull @DecimalMin(value = "0.01") BigDecimal amount) {
    }

    public record OrderResponse(
            Long id,
            String userId,
            String productName,
            BigDecimal amount,
            String status,
            Instant createdAt) {

        public static OrderResponse from(OrderEntity o) {
            return new OrderResponse(
                    o.getId(), o.getUserId(), o.getProductName(),
                    o.getAmount(), o.getStatus(), o.getCreatedAt());
        }
    }
}
