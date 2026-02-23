package com.joechen.outboxmonitor.modulith;

import com.joechen.outboxmonitor.poc.PocMessage;
import com.joechen.outboxmonitor.poc.PocMessageBroker;
import com.joechen.outboxmonitor.tracing.TracePropagationSupport;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;

@Component
public class ModulithOutboxPublicationListener {

    private final TracePropagationSupport tracePropagationSupport;
    private final PocMessageBroker broker;

    public ModulithOutboxPublicationListener(TracePropagationSupport tracePropagationSupport,
                                             PocMessageBroker broker) {
        this.tracePropagationSupport = tracePropagationSupport;
        this.broker = broker;
    }

    /**
     * Spring Modulith module-event listener.
     * In real projects this is where you publish to Kafka/RabbitMQ.
     */
    @ApplicationModuleListener
    public void on(BusinessEventCreated event) {
        Map<String, String> headers = tracePropagationSupport.toMessageHeaders(event.trace());
        broker.publish(new PocMessage(event.eventType(), event.payload(), headers, Instant.now()));
    }
}
