package com.joechen.outboxmonitor.consumer;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class ConsumerIdempotencyService {

    private final NamedParameterJdbcTemplate jdbc;

    public ConsumerIdempotencyService(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public boolean markIfFirstSeen(String eventId, String consumerName) {
        int affected = jdbc.update("""
                insert into consumed_event(event_id, consumer_name)
                values (:eventId, :consumerName)
                on conflict (event_id, consumer_name) do nothing
                """, new MapSqlParameterSource()
                .addValue("eventId", eventId)
                .addValue("consumerName", consumerName));

        return affected == 1;
    }
}
