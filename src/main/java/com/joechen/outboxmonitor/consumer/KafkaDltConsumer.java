package com.joechen.outboxmonitor.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Component
public class KafkaDltConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaDltConsumer.class);

    private final ConsumerMetrics consumerMetrics;

    public KafkaDltConsumer(ConsumerMetrics consumerMetrics) {
        this.consumerMetrics = consumerMetrics;
    }

    @KafkaListener(topics = "outbox.event.OrderCreated.DLT", groupId = "outbox-monitor-dlt-consumer")
    public void onDlt(ConsumerRecord<String, String> record) {
        Map<String, String> headers = new HashMap<>();
        for (Header header : record.headers()) {
            headers.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
        }

        consumerMetrics.incDlt();
        log.error("DLT received topic={} key={} headers={} payload={}",
                record.topic(), record.key(), headers, record.value());
    }
}
