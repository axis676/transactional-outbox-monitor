package com.joechen.outboxmonitor.consumer;

import com.joechen.outboxmonitor.security.AdminTokenGuard;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/consumer")
public class ConsumerDebugController {

    private final ConsumedEventStore consumedEventStore;
    private final ConsumerMetrics consumerMetrics;
    private final DltEventStore dltEventStore;
    private final DltReplayService dltReplayService;
    private final AdminTokenGuard adminTokenGuard;

    public ConsumerDebugController(ConsumedEventStore consumedEventStore,
                                   ConsumerMetrics consumerMetrics,
                                   DltEventStore dltEventStore,
                                   DltReplayService dltReplayService,
                                   AdminTokenGuard adminTokenGuard) {
        this.consumedEventStore = consumedEventStore;
        this.consumerMetrics = consumerMetrics;
        this.dltEventStore = dltEventStore;
        this.dltReplayService = dltReplayService;
        this.adminTokenGuard = adminTokenGuard;
    }

    @GetMapping("/events")
    public ResponseEntity<Map<String, Object>> events(@RequestParam(defaultValue = "20") int limit) {
        int n = Math.max(1, Math.min(limit, 200));
        return ResponseEntity.ok(Map.of(
                "count", consumedEventStore.latest(n).size(),
                "metrics", Map.of(
                        "processed", consumerMetrics.processed(),
                        "dedupHit", consumerMetrics.dedupHit(),
                        "dlt", consumerMetrics.dlt()
                ),
                "items", consumedEventStore.latest(n)
        ));
    }

    @GetMapping("/dlt")
    public ResponseEntity<Map<String, Object>> dlt(@RequestParam(defaultValue = "20") int limit) {
        int n = Math.max(1, Math.min(limit, 200));
        return ResponseEntity.ok(Map.of(
                "count", dltEventStore.latest(n).size(),
                "items", dltEventStore.latest(n)
        ));
    }

    @PostMapping("/dlt/{dltId}/replay")
    public ResponseEntity<Map<String, Object>> replay(@PathVariable String dltId,
                                                       @RequestHeader(name = "X-Admin-Token", required = false) String adminToken) {
        if (!adminTokenGuard.isAuthorized(adminToken)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of(
                    "status", "unauthorized",
                    "message", "invalid or missing X-Admin-Token"
            ));
        }

        try {
            return ResponseEntity.ok(dltReplayService.replayById(dltId));
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.badRequest().body(Map.of(
                    "status", "not_found",
                    "message", ex.getMessage()
            ));
        }
    }
}
