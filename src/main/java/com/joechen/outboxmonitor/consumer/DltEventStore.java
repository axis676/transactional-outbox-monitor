package com.joechen.outboxmonitor.consumer;

import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
public class DltEventStore {

    private final List<Map<String, Object>> items = Collections.synchronizedList(new ArrayList<>());

    public String add(String topic, String key, String payload, Map<String, String> headers) {
        String dltId = UUID.randomUUID().toString();
        items.add(Map.of(
                "dltId", dltId,
                "topic", topic,
                "key", key == null ? "" : key,
                "payload", payload == null ? "" : payload,
                "headers", headers,
                "receivedAt", Instant.now().toString()
        ));
        return dltId;
    }

    public List<Map<String, Object>> latest(int limit) {
        synchronized (items) {
            int start = Math.max(items.size() - limit, 0);
            return new ArrayList<>(items.subList(start, items.size()));
        }
    }

    public Map<String, Object> findById(String dltId) {
        synchronized (items) {
            return items.stream()
                    .filter(it -> dltId.equals(it.get("dltId")))
                    .findFirst()
                    .orElse(null);
        }
    }
}
