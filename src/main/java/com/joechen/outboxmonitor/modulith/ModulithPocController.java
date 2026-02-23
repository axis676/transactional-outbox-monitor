package com.joechen.outboxmonitor.modulith;

import com.joechen.outboxmonitor.poc.OutboxEvent;
import com.joechen.outboxmonitor.poc.PocMessageBroker;
import com.joechen.outboxmonitor.poc.PocOutboxService;
import com.joechen.outboxmonitor.poc.PocOutboxStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/modulith")
public class ModulithPocController {

    private final ModulithOutboxCommandService commandService;
    private final PocOutboxService pocOutboxService;
    private final PocOutboxStore outboxStore;
    private final PocMessageBroker broker;

    public ModulithPocController(ModulithOutboxCommandService commandService,
                                 PocOutboxService pocOutboxService,
                                 PocOutboxStore outboxStore,
                                 PocMessageBroker broker) {
        this.commandService = commandService;
        this.pocOutboxService = pocOutboxService;
        this.outboxStore = outboxStore;
        this.broker = broker;
    }

    @PostMapping("/produce")
    public ResponseEntity<Map<String, Object>> produce(@RequestBody ProduceRequest req) {
        OutboxEvent event = commandService.writeAndPublish(
                req.eventType(),
                req.payload(),
                req.correlationId(),
                req.causationId()
        );

        return ResponseEntity.ok(Map.of(
                "status", "written_to_outbox_and_published_via_modulith_event",
                "eventType", event.eventType(),
                "payload", event.payload(),
                "trace", event.trace(),
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
