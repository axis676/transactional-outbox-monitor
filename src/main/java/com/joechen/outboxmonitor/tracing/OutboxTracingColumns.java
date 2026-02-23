package com.joechen.outboxmonitor.tracing;

/**
 * Suggested outbox table columns for cross-service tracing and event correlation.
 */
public record OutboxTracingColumns(
        String traceparent,
        String tracestate,
        String correlationId,
        String eventId,
        String causationId
) {
    public static OutboxTracingColumns fromEnvelope(TraceEnvelope envelope) {
        return new OutboxTracingColumns(
                envelope.traceparent(),
                envelope.tracestate(),
                envelope.correlationId(),
                envelope.eventId(),
                envelope.causationId()
        );
    }
}
