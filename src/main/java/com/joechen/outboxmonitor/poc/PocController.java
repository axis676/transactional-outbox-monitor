package com.joechen.outboxmonitor.poc;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/poc")
public class PocController {

    private final PocOutboxService pocOutboxService;
    private final PocOutboxStore outboxStore;
    private final PocMessageBroker broker;

    public PocController(PocOutboxService pocOutboxService,
                         PocOutboxStore outboxStore,
                         PocMessageBroker broker) {
        this.pocOutboxService = pocOutboxService;
        this.outboxStore = outboxStore;
        this.broker = broker;
    }

    @PostMapping("/produce")
    public ResponseEntity<Map<String, Object>> produce(@RequestBody ProduceRequest req) {
        OutboxEvent event = pocOutboxService.writeBusinessEvent(
                req.eventType(),
                req.payload(),
                req.correlationId(),
                req.causationId()
        );

        return ResponseEntity.ok(Map.of(
                "status", "written_to_outbox",
                "eventType", event.eventType(),
                "payload", event.payload(),
                "trace", event.trace(),
                "outboxSize", outboxStore.size(),
                "brokerQueueSize", broker.size()
        ));
    }

    @PostMapping("/publish")
    public ResponseEntity<Map<String, Object>> publish() {
        List<PocMessage> published = pocOutboxService.flushOutboxToBroker();
        return ResponseEntity.ok(Map.of(
                "status", "published",
                "count", published.size(),
                "messages", published,
                "outboxSize", outboxStore.size(),
                "brokerQueueSize", broker.size()
        ));
    }

    @PostMapping("/consume")
    public ResponseEntity<PocOutboxService.ConsumeResult> consume(
            @RequestParam(defaultValue = "consumer-a") String consumer) {
        return ResponseEntity.ok(pocOutboxService.consumeOnce(consumer));
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        return ResponseEntity.ok(Map.of(
                "outboxSize", outboxStore.size(),
                "brokerQueueSize", broker.size()
        ));
    }

    public record ProduceRequest(
            String eventType,
            String payload,
            String correlationId,
            String causationId
    ) {
    }
}
