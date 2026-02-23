package com.joechen.outboxmonitor.consumer;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/consumer")
public class ConsumerDebugController {

    private final ConsumedEventStore consumedEventStore;
    private final ConsumerMetrics consumerMetrics;

    public ConsumerDebugController(ConsumedEventStore consumedEventStore,
                                   ConsumerMetrics consumerMetrics) {
        this.consumedEventStore = consumedEventStore;
        this.consumerMetrics = consumerMetrics;
    }

    @GetMapping("/events")
    public ResponseEntity<Map<String, Object>> events(@RequestParam(defaultValue = "20") int limit) {
        int n = Math.max(1, Math.min(limit, 200));
        return ResponseEntity.ok(Map.of(
                "count", consumedEventStore.latest(n).size(),
                "metrics", Map.of(
                        "processed", consumerMetrics.processed(),
                        "dedupHit", consumerMetrics.dedupHit()
                ),
                "items", consumedEventStore.latest(n)
        ));
    }
}
