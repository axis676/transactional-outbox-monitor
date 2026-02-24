package com.joechen.outboxmonitor.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"outbox.event.OrderCreated", "outbox.event.OrderCreated.DLT"})
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.listener.auto-startup=false"
})
class DltReplayIntegrationTest {

    @Autowired
    DltEventStore dltEventStore;

    @Autowired
    DltReplayService dltReplayService;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<String, String> consumer;

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void replayShouldPublishBackToMainTopicWithHeaders() {
        String dltId = dltEventStore.add(
                "outbox.event.OrderCreated.DLT",
                "order-1",
                "{\"orderId\":\"order-1\"}",
                Map.of("correlation_id", "ORD-REPLAY-1", "event_id", UUID.randomUUID().toString())
        );

        Map<String, Object> result = dltReplayService.replayById(dltId, "test-user", "integration-test replay");
        assertThat(result.get("status")).isEqualTo("replayed");
        assertThat(result.get("toTopic")).isEqualTo("outbox.event.OrderCreated");

        Map<String, Object> props = KafkaTestUtils.consumerProps("test-replay-group", "false", embeddedKafkaBroker);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, "outbox.event.OrderCreated");

        ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(
                consumer,
                "outbox.event.OrderCreated",
                Duration.ofSeconds(10)
        );

        assertThat(record).isNotNull();
        assertThat(record.key()).isEqualTo("order-1");
        assertThat(record.value()).contains("order-1");
        assertThat(headerAsString(record, "replayed_from_dlt")).isEqualTo("true");
        assertThat(headerAsString(record, "correlation_id")).isEqualTo("ORD-REPLAY-1");
    }

    private String headerAsString(ConsumerRecord<String, String> record, String key) {
        if (record.headers().lastHeader(key) == null) {
            return null;
        }
        return new String(record.headers().lastHeader(key).value(), StandardCharsets.UTF_8);
    }
}
