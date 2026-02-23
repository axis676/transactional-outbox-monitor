package com.joechen.outboxmonitor.poc;

import com.joechen.outboxmonitor.tracing.TraceEnvelope;
import com.joechen.outboxmonitor.tracing.TracePropagationSupport;
import io.opentelemetry.api.trace.Span;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class PocOutboxService {

    private final TracePropagationSupport tracePropagationSupport;
    private final PocOutboxStore outboxStore;
    private final PocMessageBroker broker;

    public PocOutboxService(TracePropagationSupport tracePropagationSupport,
                            PocOutboxStore outboxStore,
                            PocMessageBroker broker) {
        this.tracePropagationSupport = tracePropagationSupport;
        this.outboxStore = outboxStore;
        this.broker = broker;
    }

    public OutboxEvent writeBusinessEvent(String eventType, String payload, String correlationId, String causationId) {
        String effectiveCorrelationId = (correlationId == null || correlationId.isBlank())
                ? UUID.randomUUID().toString()
                : correlationId;

        String eventId = UUID.randomUUID().toString();
        TraceEnvelope envelope = tracePropagationSupport.captureForOutbox(effectiveCorrelationId, eventId, causationId);

        OutboxEvent event = new OutboxEvent(eventType, payload, envelope, Instant.now());
        outboxStore.add(event);
        return event;
    }

    public List<PocMessage> flushOutboxToBroker() {
        List<OutboxEvent> events = outboxStore.drainAll();
        List<PocMessage> published = new ArrayList<>();

        for (OutboxEvent event : events) {
            Map<String, String> headers = tracePropagationSupport.toMessageHeaders(event.trace());
            PocMessage message = new PocMessage(event.eventType(), event.payload(), headers, Instant.now());
            broker.publish(message);
            published.add(message);
        }

        return published;
    }

    public ConsumeResult consumeOnce(String consumerName) {
        PocMessage msg = broker.poll();
        if (msg == null) {
            return ConsumeResult.empty(outboxStore.size(), broker.size());
        }

        Span span = tracePropagationSupport.startLinkedConsumerSpan(
                "poc.consume." + consumerName,
                msg.headers()
        );

        try {
            span.setAttribute("poc.consumer", consumerName);
            span.setAttribute("poc.event_type", msg.eventType());

            return ConsumeResult.consumed(
                    consumerName,
                    msg.eventType(),
                    msg.payload(),
                    msg.headers(),
                    outboxStore.size(),
                    broker.size()
            );
        } finally {
            span.end();
        }
    }

    public record ConsumeResult(
            boolean consumed,
            String consumer,
            String eventType,
            String payload,
            Map<String, String> headers,
            int outboxSize,
            int brokerQueueSize,
            String message
    ) {
        static ConsumeResult consumed(String consumer,
                                      String eventType,
                                      String payload,
                                      Map<String, String> headers,
                                      int outboxSize,
                                      int brokerQueueSize) {
            return new ConsumeResult(true, consumer, eventType, payload, headers, outboxSize, brokerQueueSize, "ok");
        }

        static ConsumeResult empty(int outboxSize, int brokerQueueSize) {
            return new ConsumeResult(false, null, null, null, Map.of(), outboxSize, brokerQueueSize, "broker queue empty");
        }
    }
}
