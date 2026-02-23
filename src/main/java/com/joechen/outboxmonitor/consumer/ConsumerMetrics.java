package com.joechen.outboxmonitor.consumer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Component
public class ConsumerMetrics {

    private final Counter processedCounter;
    private final Counter dedupHitCounter;
    private final Counter dltCounter;

    private final AtomicLong processed = new AtomicLong();
    private final AtomicLong dedupHit = new AtomicLong();
    private final AtomicLong dlt = new AtomicLong();

    public ConsumerMetrics(MeterRegistry meterRegistry) {
        this.processedCounter = Counter.builder("consumer_processed_total")
                .description("Total successfully processed events")
                .register(meterRegistry);
        this.dedupHitCounter = Counter.builder("consumer_dedup_hit_total")
                .description("Total duplicate events skipped by idempotency")
                .register(meterRegistry);
        this.dltCounter = Counter.builder("consumer_dlt_total")
                .description("Total events routed to DLT")
                .register(meterRegistry);
    }

    public void incProcessed() {
        processed.incrementAndGet();
        processedCounter.increment();
    }

    public void incDedupHit() {
        dedupHit.incrementAndGet();
        dedupHitCounter.increment();
    }

    public long processed() {
        return processed.get();
    }

    public long dedupHit() {
        return dedupHit.get();
    }

    public void incDlt() {
        dlt.incrementAndGet();
        dltCounter.increment();
    }

    public long dlt() {
        return dlt.get();
    }
}
