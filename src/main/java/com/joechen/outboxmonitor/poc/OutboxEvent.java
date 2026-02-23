package com.joechen.outboxmonitor.poc;

import com.joechen.outboxmonitor.tracing.TraceEnvelope;

import java.time.Instant;

public record OutboxEvent(
        String eventType,
        String payload,
        TraceEnvelope trace,
        Instant createdAt
) {
}
