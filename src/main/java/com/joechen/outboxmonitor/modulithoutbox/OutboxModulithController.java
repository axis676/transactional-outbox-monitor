package com.joechen.outboxmonitor.modulithoutbox;

import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/modulith-outbox")
public class OutboxModulithController {

    private final OrderCommandService orderCommandService;
    private final OutboxQueryService outboxQueryService;

    public OutboxModulithController(OrderCommandService orderCommandService,
                                    OutboxQueryService outboxQueryService) {
        this.orderCommandService = orderCommandService;
        this.outboxQueryService = outboxQueryService;
    }

    @PostMapping("/orders")
    public ResponseEntity<Map<String, Object>> createOrder(@Valid @RequestBody OrderRequest request) {
        OrderCreatedEvent event = orderCommandService.createOrder(request);
        return ResponseEntity.ok(Map.of(
                "status", "order_created",
                "orderId", event.orderId(),
                "eventId", event.trace().eventId(),
                "correlationId", event.trace().correlationId(),
                "traceparent", event.trace().traceparent()
        ));
    }

    @GetMapping("/outbox")
    public ResponseEntity<Map<String, Object>> outbox(@RequestParam(defaultValue = "20") int limit) {
        return ResponseEntity.ok(Map.of(
                "metrics", outboxQueryService.metrics(),
                "items", outboxQueryService.latestOutbox(Math.max(1, Math.min(limit, 200)))
        ));
    }
}
