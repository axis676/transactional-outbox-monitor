package com.joechen.outboxmonitor.modulithoutbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class OutboxMetrics {

    private final Counter writeSuccess;
    private final Counter writeFailure;

    public OutboxMetrics(MeterRegistry meterRegistry) {
        this.writeSuccess = Counter.builder("outbox_write_success_total")
                .description("Total successful writes to outbox table")
                .register(meterRegistry);
        this.writeFailure = Counter.builder("outbox_write_failure_total")
                .description("Total failed writes to outbox table")
                .register(meterRegistry);
    }

    public void incWriteSuccess() {
        writeSuccess.increment();
    }

    public void incWriteFailure() {
        writeFailure.increment();
    }
}
