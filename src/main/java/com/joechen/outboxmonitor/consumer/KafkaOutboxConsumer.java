package com.joechen.outboxmonitor.consumer;

import com.joechen.outboxmonitor.tracing.TracePropagationSupport;
import io.opentelemetry.api.trace.Span;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Component
public class KafkaOutboxConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaOutboxConsumer.class);

    private final TracePropagationSupport tracePropagationSupport;
    private final ConsumedEventStore consumedEventStore;
    private final ConsumerIdempotencyService idempotencyService;
    private final ConsumerMetrics consumerMetrics;
    private final String failOnPayloadContains;

    public KafkaOutboxConsumer(TracePropagationSupport tracePropagationSupport,
                               ConsumedEventStore consumedEventStore,
                               ConsumerIdempotencyService idempotencyService,
                               ConsumerMetrics consumerMetrics,
                               @Value("${app.consumer.fail-on-payload-contains:}") String failOnPayloadContains) {
        this.tracePropagationSupport = tracePropagationSupport;
        this.consumedEventStore = consumedEventStore;
        this.idempotencyService = idempotencyService;
        this.consumerMetrics = consumerMetrics;
        this.failOnPayloadContains = failOnPayloadContains;
    }

    @KafkaListener(topics = "outbox.event.OrderCreated", groupId = "outbox-monitor-consumer")
    public void onOrderCreated(ConsumerRecord<String, String> record) {
        Map<String, String> headers = toMap(record);
        Span span = tracePropagationSupport.startLinkedConsumerSpan("kafka.consume.order-created", headers);

        String correlationId = headers.getOrDefault("correlation_id", "");
        String eventId = headers.getOrDefault("id", headers.getOrDefault("event_id", ""));
        MDC.put("correlation_id", correlationId);
        try {
            span.setAttribute("messaging.system", "kafka");
            span.setAttribute("messaging.destination", record.topic());
            span.setAttribute("messaging.kafka.partition", record.partition());
            span.setAttribute("messaging.event_id", eventId);

            if (!failOnPayloadContains.isBlank() && record.value() != null
                    && record.value().contains(failOnPayloadContains)) {
                throw new IllegalStateException("simulated consumer failure for retry/DLT testing");
            }

            if (!eventId.isBlank()) {
                boolean firstSeen = idempotencyService.markIfFirstSeen(eventId, "outbox-monitor-consumer");
                if (!firstSeen) {
                    consumerMetrics.incDedupHit();
                    log.info("Skipped duplicate event topic={} key={} event_id={} correlation_id={}",
                            record.topic(), record.key(), eventId, correlationId);
                    return;
                }
            } else {
                log.warn("event_id header missing, skip idempotency check topic={} key={}",
                        record.topic(), record.key());
            }

            consumerMetrics.incProcessed();
            log.info("Consumed outbox event topic={} key={} event_id={} correlation_id={}",
                    record.topic(), record.key(), eventId, correlationId);

            consumedEventStore.add(record.topic(), record.key(), record.value(), headers);
        } finally {
            MDC.remove("correlation_id");
            span.end();
        }
    }

    private Map<String, String> toMap(ConsumerRecord<String, String> record) {
        Map<String, String> map = new HashMap<>();
        for (Header header : record.headers()) {
            map.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
        }
        return map;
    }
}
