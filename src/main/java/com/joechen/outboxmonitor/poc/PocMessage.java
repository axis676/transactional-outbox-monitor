package com.joechen.outboxmonitor.poc;

import java.time.Instant;
import java.util.Map;

public record PocMessage(
        String eventType,
        String payload,
        Map<String, String> headers,
        Instant publishedAt
) {
}
