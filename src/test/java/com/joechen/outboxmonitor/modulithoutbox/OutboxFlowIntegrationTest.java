package com.joechen.outboxmonitor.modulithoutbox;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Testcontainers
@SpringBootTest
@AutoConfigureMockMvc
class OutboxFlowIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("app")
            .withUsername("app")
            .withPassword("app");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.sql.init.mode", () -> "always");
        registry.add("spring.kafka.listener.auto-startup", () -> "false");
        registry.add("app.admin.token", () -> "test-token");
    }

    @Autowired
    MockMvc mockMvc;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Test
    void createOrderShouldWriteOutboxEventWithCorrelationId() throws Exception {
        mockMvc.perform(post("/api/modulith-outbox/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-Correlation-Id", "ORD-IT-1001")
                        .content("""
                                {
                                  "orderId": "IT-1001",
                                  "customerId": "C-IT",
                                  "amount": 88.8,
                                  "causationId": ""
                                }
                                """))
                .andExpect(status().isOk());

        Integer outboxCount = jdbcTemplate.queryForObject("select count(*) from outbox_event", Integer.class);
        String correlationId = jdbcTemplate.queryForObject(
                "select correlation_id from outbox_event where aggregate_id = ?",
                String.class,
                "IT-1001");

        assertThat(outboxCount).isNotNull();
        assertThat(outboxCount).isGreaterThan(0);
        assertThat(correlationId).isEqualTo("ORD-IT-1001");
    }
}
