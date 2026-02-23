package com.joechen.outboxmonitor.consumer;

import com.joechen.outboxmonitor.tracing.TracePropagationSupport;
import io.opentelemetry.api.trace.Span;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
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

    public KafkaOutboxConsumer(TracePropagationSupport tracePropagationSupport,
                               ConsumedEventStore consumedEventStore) {
        this.tracePropagationSupport = tracePropagationSupport;
        this.consumedEventStore = consumedEventStore;
    }

    @KafkaListener(topics = "outbox.event.OrderCreated", groupId = "outbox-monitor-consumer")
    public void onOrderCreated(ConsumerRecord<String, String> record) {
        Map<String, String> headers = toMap(record);
        Span span = tracePropagationSupport.startLinkedConsumerSpan("kafka.consume.order-created", headers);

        String correlationId = headers.getOrDefault("correlation_id", "");
        MDC.put("correlation_id", correlationId);
        try {
            span.setAttribute("messaging.system", "kafka");
            span.setAttribute("messaging.destination", record.topic());
            span.setAttribute("messaging.kafka.partition", record.partition());

            log.info("Consumed outbox event topic={} key={} correlation_id={}",
                    record.topic(), record.key(), correlationId);

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
