package com.joechen.outboxmonitor.poc;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Component
public class PocOutboxStore {

    private final List<OutboxEvent> events = Collections.synchronizedList(new ArrayList<>());

    public void add(OutboxEvent event) {
        events.add(event);
    }

    public List<OutboxEvent> drainAll() {
        synchronized (events) {
            List<OutboxEvent> copy = new ArrayList<>(events);
            events.clear();
            return copy;
        }
    }

    public int size() {
        return events.size();
    }
}
