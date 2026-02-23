package com.joechen.outboxmonitor.modulithoutbox;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class OutboxQueryService {

    private final NamedParameterJdbcTemplate jdbc;

    public OutboxQueryService(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<Map<String, Object>> latestOutbox(int limit) {
        return jdbc.queryForList("""
                select id, event_id, aggregate_type, aggregate_id, event_type, payload,
                       correlation_id, causation_id, traceparent, occurred_at, created_at
                from outbox_event
                order by id desc
                limit :limit
                """, Map.of("limit", limit));
    }

    public Map<String, Object> metrics() {
        return jdbc.queryForMap("""
                select
                    count(*) as total,
                    max(created_at) as latest_created_at,
                    min(created_at) as oldest_created_at
                from outbox_event
                """, Map.of());
    }
}
