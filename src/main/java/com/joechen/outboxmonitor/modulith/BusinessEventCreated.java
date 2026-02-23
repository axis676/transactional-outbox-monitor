package com.joechen.outboxmonitor.modulith;

import com.joechen.outboxmonitor.tracing.TraceEnvelope;

import java.time.Instant;

public record BusinessEventCreated(
        String eventType,
        String payload,
        TraceEnvelope trace,
        Instant createdAt
) {
}
