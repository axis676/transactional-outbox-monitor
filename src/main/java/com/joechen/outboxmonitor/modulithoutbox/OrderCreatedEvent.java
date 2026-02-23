package com.joechen.outboxmonitor.modulithoutbox;

import com.joechen.outboxmonitor.tracing.TraceEnvelope;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderCreatedEvent(
        String orderId,
        String customerId,
        BigDecimal amount,
        Instant occurredAt,
        TraceEnvelope trace
) {
}
