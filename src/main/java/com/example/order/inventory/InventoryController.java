package com.example.order.inventory;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Admin-ish endpoints to set and inspect stock, used to stage Saga demos
 * (e.g. set stock to 0 to force the compensation path).
 */
@RestController
@RequestMapping("/api/inventory")
public class InventoryController {

    private final InventoryService inventoryService;

    public InventoryController(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    public record SetStockRequest(
            @NotBlank String productName,
            @Min(0) int quantity) {
    }

    public record StockResponse(String productName, int availableQuantity) {
        static StockResponse from(InventoryEntity e) {
            return new StockResponse(e.getProductName(), e.getAvailableQuantity());
        }
    }

    @PutMapping
    public ResponseEntity<StockResponse> setStock(@Valid @RequestBody SetStockRequest req) {
        InventoryEntity saved = inventoryService.setStock(req.productName(), req.quantity());
        return ResponseEntity.ok(StockResponse.from(saved));
    }

    @GetMapping
    public ResponseEntity<List<StockResponse>> list() {
        List<StockResponse> out = new ArrayList<>();
        inventoryService.listStock().forEach(e -> out.add(StockResponse.from(e)));
        return ResponseEntity.ok(out);
    }
}
