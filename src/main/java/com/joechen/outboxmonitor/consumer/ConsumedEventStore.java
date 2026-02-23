package com.joechen.outboxmonitor.consumer;

import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Component
public class ConsumedEventStore {

    private final List<Map<String, Object>> items = Collections.synchronizedList(new ArrayList<>());

    public void add(String topic, String key, String payload, Map<String, String> headers) {
        items.add(Map.of(
                "topic", topic,
                "key", key,
                "payload", payload,
                "headers", headers,
                "consumedAt", Instant.now().toString()
        ));
    }

    public List<Map<String, Object>> latest(int limit) {
        synchronized (items) {
            int start = Math.max(items.size() - limit, 0);
            return new ArrayList<>(items.subList(start, items.size()));
        }
    }
}
