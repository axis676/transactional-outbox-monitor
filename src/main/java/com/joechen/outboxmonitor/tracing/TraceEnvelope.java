package com.joechen.outboxmonitor.tracing;

import java.util.Objects;

/**
 * Data that should travel with an outbox event so async consumers can correlate traces.
 */
public record TraceEnvelope(
        String traceparent,
        String tracestate,
        String correlationId,
        String eventId,
        String causationId
) {
    public TraceEnvelope {
        Objects.requireNonNull(correlationId, "correlationId is required");
        Objects.requireNonNull(eventId, "eventId is required");
    }
}
