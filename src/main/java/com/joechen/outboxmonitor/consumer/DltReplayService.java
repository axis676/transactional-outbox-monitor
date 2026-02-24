package com.joechen.outboxmonitor.consumer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Service
public class DltReplayService {

    private final DltEventStore dltEventStore;
    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final ReplayAuditService replayAuditService;

    public DltReplayService(DltEventStore dltEventStore,
                            KafkaTemplate<Object, Object> kafkaTemplate,
                            ReplayAuditService replayAuditService) {
        this.dltEventStore = dltEventStore;
        this.kafkaTemplate = kafkaTemplate;
        this.replayAuditService = replayAuditService;
    }

    public Map<String, Object> replayById(String dltId, String actor, String reason) {
        Map<String, Object> event = dltEventStore.findById(dltId);
        if (event == null) {
            throw new IllegalArgumentException("DLT event not found: " + dltId);
        }

        String dltTopic = (String) event.get("topic");
        String mainTopic = dltTopic.endsWith(".DLT")
                ? dltTopic.substring(0, dltTopic.length() - 4)
                : dltTopic;

        String key = (String) event.get("key");
        String payload = (String) event.get("payload");

        @SuppressWarnings("unchecked")
        Map<String, String> headers = (Map<String, String>) event.get("headers");

        ProducerRecord<Object, Object> record = new ProducerRecord<>(mainTopic, key, payload);
        record.headers().add(new RecordHeader("replayed_from_dlt", "true".getBytes(StandardCharsets.UTF_8)));
        headers.forEach((k, v) -> {
            if (v != null) {
                record.headers().add(new RecordHeader(k, v.getBytes(StandardCharsets.UTF_8)));
            }
        });

        kafkaTemplate.send(record);

        replayAuditService.record(dltId, dltTopic, mainTopic, key, actor, reason);

        return Map.of(
                "status", "replayed",
                "dltId", dltId,
                "fromTopic", dltTopic,
                "toTopic", mainTopic,
                "key", key,
                "actor", actor,
                "reason", reason
        );
    }
}
