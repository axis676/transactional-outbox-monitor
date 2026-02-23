package com.joechen.outboxmonitor.modulith;

import com.joechen.outboxmonitor.poc.OutboxEvent;
import com.joechen.outboxmonitor.poc.PocOutboxStore;
import com.joechen.outboxmonitor.tracing.TraceEnvelope;
import com.joechen.outboxmonitor.tracing.TracePropagationSupport;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
public class ModulithOutboxCommandService {

    private final TracePropagationSupport tracePropagationSupport;
    private final PocOutboxStore outboxStore;
    private final ApplicationEventPublisher eventPublisher;

    public ModulithOutboxCommandService(TracePropagationSupport tracePropagationSupport,
                                        PocOutboxStore outboxStore,
                                        ApplicationEventPublisher eventPublisher) {
        this.tracePropagationSupport = tracePropagationSupport;
        this.outboxStore = outboxStore;
        this.eventPublisher = eventPublisher;
    }

    @Transactional
    public OutboxEvent writeAndPublish(String eventType, String payload, String correlationId, String causationId) {
        String effectiveCorrelationId = (correlationId == null || correlationId.isBlank())
                ? UUID.randomUUID().toString()
                : correlationId;

        String eventId = UUID.randomUUID().toString();
        TraceEnvelope envelope = tracePropagationSupport.captureForOutbox(effectiveCorrelationId, eventId, causationId);

        OutboxEvent outboxEvent = new OutboxEvent(eventType, payload, envelope, Instant.now());
        outboxStore.add(outboxEvent);

        eventPublisher.publishEvent(new BusinessEventCreated(
                outboxEvent.eventType(),
                outboxEvent.payload(),
                outboxEvent.trace(),
                outboxEvent.createdAt()
        ));

        return outboxEvent;
    }
}
