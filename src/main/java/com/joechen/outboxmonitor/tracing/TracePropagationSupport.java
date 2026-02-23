package com.joechen.outboxmonitor.tracing;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Utilities for outbox trace-context capture and async consumer span-link creation.
 */
@Component
public class TracePropagationSupport {

    private static final String TRACEPARENT = "traceparent";
    private static final String TRACESTATE = "tracestate";

    private static final TextMapSetter<Map<String, String>> MAP_SETTER = Map::put;

    private static final TextMapGetter<Map<String, String>> MAP_GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet();
        }

        @Override
        public String get(Map<String, String> carrier, String key) {
            if (carrier == null) {
                return null;
            }
            return carrier.get(key);
        }
    };

    private final Tracer tracer = GlobalOpenTelemetry.getTracer("outbox-tracing");

    /**
     * Capture current trace context + business correlation identifiers for writing into outbox.
     */
    public TraceEnvelope captureForOutbox(String correlationId, String eventId, String causationId) {
        Map<String, String> carrier = new HashMap<>();
        W3CTraceContextPropagator.getInstance().inject(Context.current(), carrier, MAP_SETTER);

        return new TraceEnvelope(
                carrier.get(TRACEPARENT),
                carrier.get(TRACESTATE),
                correlationId,
                eventId,
                causationId
        );
    }

    /**
     * Build outgoing headers for MQ/Kafka from a persisted envelope.
     */
    public Map<String, String> toMessageHeaders(TraceEnvelope envelope) {
        Map<String, String> headers = new HashMap<>();
        headers.put("correlation_id", envelope.correlationId());
        headers.put("event_id", envelope.eventId());

        if (envelope.traceparent() != null) {
            headers.put(TRACEPARENT, envelope.traceparent());
        }
        if (envelope.tracestate() != null) {
            headers.put(TRACESTATE, envelope.tracestate());
        }
        if (envelope.causationId() != null && !envelope.causationId().isBlank()) {
            headers.put("causation_id", envelope.causationId());
        }

        return headers;
    }

    /**
     * Start a CONSUMER span and link it to producer context extracted from message headers.
     *
     * Async boundaries are better modeled with span links than parent-child continuation.
     */
    public Span startLinkedConsumerSpan(String spanName, Map<String, String> messageHeaders) {
        Context extracted = W3CTraceContextPropagator.getInstance()
                .extract(Context.current(), messageHeaders, MAP_GETTER);

        SpanContext producerSpanContext = Span.fromContext(extracted).getSpanContext();
        SpanBuilder builder = tracer.spanBuilder(spanName).setSpanKind(SpanKind.CONSUMER);

        if (producerSpanContext.isValid()) {
            builder.addLink(producerSpanContext);
        }

        Span span = builder.startSpan();
        span.setAttribute("messaging.correlation_id", messageHeaders.getOrDefault("correlation_id", ""));
        span.setAttribute("messaging.event_id", messageHeaders.getOrDefault("event_id", ""));
        span.setAttribute("messaging.causation_id", messageHeaders.getOrDefault("causation_id", ""));
        return span;
    }
}
