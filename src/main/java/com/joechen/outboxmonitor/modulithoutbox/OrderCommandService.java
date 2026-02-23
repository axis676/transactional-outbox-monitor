package com.joechen.outboxmonitor.modulithoutbox;

import com.joechen.outboxmonitor.observability.CorrelationIdContext;
import com.joechen.outboxmonitor.tracing.TraceEnvelope;
import com.joechen.outboxmonitor.tracing.TracePropagationSupport;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
public class OrderCommandService {

    private final NamedParameterJdbcTemplate jdbc;
    private final ApplicationEventPublisher eventPublisher;
    private final TracePropagationSupport tracePropagationSupport;

    public OrderCommandService(NamedParameterJdbcTemplate jdbc,
                               ApplicationEventPublisher eventPublisher,
                               TracePropagationSupport tracePropagationSupport) {
        this.jdbc = jdbc;
        this.eventPublisher = eventPublisher;
        this.tracePropagationSupport = tracePropagationSupport;
    }

    @Transactional
    public OrderCreatedEvent createOrder(OrderRequest request) {
        jdbc.update("""
                insert into orders(order_id, customer_id, amount, created_at)
                values (:orderId, :customerId, :amount, :createdAt)
                """, new MapSqlParameterSource()
                .addValue("orderId", request.orderId())
                .addValue("customerId", request.customerId())
                .addValue("amount", request.amount())
                .addValue("createdAt", Instant.now()));

        String correlationId = request.correlationId();
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = CorrelationIdContext.current();
        }
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = request.orderId();
        }

        TraceEnvelope trace = tracePropagationSupport.captureForOutbox(
                correlationId,
                UUID.randomUUID().toString(),
                request.causationId()
        );

        OrderCreatedEvent event = new OrderCreatedEvent(
                request.orderId(),
                request.customerId(),
                request.amount(),
                Instant.now(),
                trace
        );

        eventPublisher.publishEvent(event);
        return event;
    }
}
