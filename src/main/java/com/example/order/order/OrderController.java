package com.example.order.order;

import com.example.order.consumer.OrderHistoryService;
import com.example.order.consumer.OrderStatusHistoryResponse;
import com.example.order.order.OrderDtos.*;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService orderService;
    private final OrderHistoryService orderHistoryService;

    public OrderController(OrderService orderService,
                           OrderHistoryService orderHistoryService) {
        this.orderService = orderService;
        this.orderHistoryService = orderHistoryService;
    }

    @PostMapping
    public ResponseEntity<OrderResponse> create(
            @AuthenticationPrincipal UserDetails principal,
            @Valid @RequestBody CreateOrderRequest req) {

        // userId comes from the authenticated JWT, never from the request body
        String userId = principal.getUsername();
        OrderEntity order = orderService.createOrder(userId, req.productName(), req.amount());
        return ResponseEntity.status(HttpStatus.CREATED).body(OrderResponse.from(order));
    }

    @GetMapping
    public ResponseEntity<List<OrderResponse>> list(
            @AuthenticationPrincipal UserDetails principal) {
        List<OrderResponse> orders = orderService.listOrders(principal.getUsername())
                .stream().map(OrderResponse::from).toList();
        return ResponseEntity.ok(orders);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<OrderResponse> cancel(
            @AuthenticationPrincipal UserDetails principal,
            @PathVariable Long id) {
        // soft delete: transitions the order to CANCELLED and emits an event,
        // it does not physically remove the row
        OrderEntity order = orderService.cancelOrder(principal.getUsername(), id);
        return ResponseEntity.ok(OrderResponse.from(order));
    }

    /**
     * Read an order's status history from Cassandra for the given month.
     *
     * month is yyyyMM (e.g. 202606); omit it to default to the current month.
     * The lookup uses the full Cassandra partition key (user + month bucket),
     * so it is an efficient single-partition read returned newest-first.
     */
    @GetMapping("/history")
    public ResponseEntity<List<OrderStatusHistoryResponse>> history(
            @AuthenticationPrincipal UserDetails principal,
            @RequestParam(required = false) String month) {
        List<OrderStatusHistoryResponse> history =
                orderHistoryService.getHistory(principal.getUsername(), month);
        return ResponseEntity.ok(history);
    }
}
