package org.example.web;

import org.example.model.Purchase;
import org.example.service.PurchaseProducerService;
import org.example.service.StoreQueryService;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST API for producing purchase events and reading per-user stats from Redis.
 *
 * <pre>
 *   GET  /api/stats            – all per-user aggregates (from Redis, shared across instances)
 *   GET  /api/stats/{userId}   – single user's stats
 *   POST /api/purchases        – produce a specific purchase (request body = Purchase JSON)
 *   POST /api/purchases/random – produce one random purchase
 * </pre>
 */
@RestController
@RequestMapping("/api")
public class StatsController {

    private final StoreQueryService storeQueryService;
    private final PurchaseProducerService producerService;

    public StatsController(StoreQueryService storeQueryService,
                           PurchaseProducerService producerService) {
        this.storeQueryService = storeQueryService;
        this.producerService = producerService;
    }

    @GetMapping("/stats")
    public ResponseEntity<?> getAllStats() {
        try {
            return ResponseEntity.ok(storeQueryService.getAll());
        } catch (DataAccessException e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Redis unavailable: " + e.getMessage());
        }
    }

    @GetMapping("/stats/{userId}")
    public ResponseEntity<?> getStatsForUser(@PathVariable String userId) {
        try {
            return storeQueryService.getForUser(userId)
                    .<ResponseEntity<?>>map(ResponseEntity::ok)
                    .orElse(ResponseEntity.notFound().build());
        } catch (DataAccessException e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Redis unavailable: " + e.getMessage());
        }
    }

    @PostMapping("/purchases")
    public ResponseEntity<Purchase> produce(@RequestBody Purchase purchase) {
        producerService.send(purchase);
        return ResponseEntity.accepted().body(purchase);
    }

    @PostMapping("/purchases/random")
    public ResponseEntity<Purchase> produceRandom() {
        Purchase p = producerService.sendRandom();
        return ResponseEntity.accepted().body(p);
    }
}
