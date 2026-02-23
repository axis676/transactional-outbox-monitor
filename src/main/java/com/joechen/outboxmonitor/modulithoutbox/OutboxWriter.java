package com.joechen.outboxmonitor.modulithoutbox;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
public class OutboxWriter {

    private final NamedParameterJdbcTemplate jdbc;
    private final boolean failFirstAttempt;

    private volatile boolean failedOnce = false;

    public OutboxWriter(NamedParameterJdbcTemplate jdbc,
                        @Value("${app.outbox.simulate-fail-first-attempt:false}") boolean failFirstAttempt) {
        this.jdbc = jdbc;
        this.failFirstAttempt = failFirstAttempt;
    }

    @ApplicationModuleListener
    @Retryable(maxAttemptsExpression = "${app.outbox.retry.max-attempts:5}",
            backoff = @Backoff(delayExpression = "${app.outbox.retry.backoff-ms:500}"))
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void on(OrderCreatedEvent event) {
        if (failFirstAttempt && !failedOnce) {
            failedOnce = true;
            throw new IllegalStateException("simulated transient failure before writing outbox");
        }

        jdbc.update("""
                insert into outbox_event(
                    event_id, aggregate_type, aggregate_id, event_type, payload,
                    traceparent, tracestate, correlation_id, causation_id, occurred_at
                ) values (
                    :eventId, :aggregateType, :aggregateId, :eventType, cast(:payload as jsonb),
                    :traceparent, :tracestate, :correlationId, :causationId, :occurredAt
                )
                on conflict (event_id) do nothing
                """, new MapSqlParameterSource()
                .addValue("eventId", event.trace().eventId())
                .addValue("aggregateType", "Order")
                .addValue("aggregateId", event.orderId())
                .addValue("eventType", "OrderCreated")
                .addValue("payload", "{\"orderId\":\"" + event.orderId() + "\",\"customerId\":\"" + event.customerId() + "\",\"amount\":" + event.amount() + "}")
                .addValue("traceparent", event.trace().traceparent())
                .addValue("tracestate", event.trace().tracestate())
                .addValue("correlationId", event.trace().correlationId())
                .addValue("causationId", event.trace().causationId())
                .addValue("occurredAt", event.occurredAt()));
    }
}
