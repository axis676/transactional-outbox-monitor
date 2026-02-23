package com.joechen.outboxmonitor.observability;

import org.slf4j.MDC;

public final class CorrelationIdContext {

    private CorrelationIdContext() {
    }

    public static String current() {
        return MDC.get(CorrelationIdFilter.MDC_KEY);
    }
}
