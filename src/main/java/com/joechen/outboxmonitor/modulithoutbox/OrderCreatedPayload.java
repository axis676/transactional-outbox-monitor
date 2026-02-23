package com.joechen.outboxmonitor.modulithoutbox;

import java.math.BigDecimal;

public record OrderCreatedPayload(
        String orderId,
        String customerId,
        BigDecimal amount
) {
}
