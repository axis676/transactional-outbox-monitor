package com.joechen.outboxmonitor.consumer;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Component
public class ConsumerMetrics {

    private final AtomicLong processed = new AtomicLong();
    private final AtomicLong dedupHit = new AtomicLong();

    public void incProcessed() {
        processed.incrementAndGet();
    }

    public void incDedupHit() {
        dedupHit.incrementAndGet();
    }

    public long processed() {
        return processed.get();
    }

    public long dedupHit() {
        return dedupHit.get();
    }
}
