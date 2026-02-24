package com.joechen.outboxmonitor.consumer;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class ReplayAuditService {

    private final NamedParameterJdbcTemplate jdbc;

    public ReplayAuditService(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void record(String dltId, String fromTopic, String toTopic, String key, String actor, String reason) {
        jdbc.update("""
                insert into dlt_replay_audit(dlt_id, from_topic, to_topic, message_key, actor, reason)
                values (:dltId, :fromTopic, :toTopic, :key, :actor, :reason)
                """, new MapSqlParameterSource()
                .addValue("dltId", dltId)
                .addValue("fromTopic", fromTopic)
                .addValue("toTopic", toTopic)
                .addValue("key", key)
                .addValue("actor", actor)
                .addValue("reason", reason));
    }

    public List<Map<String, Object>> latest(int limit) {
        return jdbc.queryForList("""
                select id, dlt_id, from_topic, to_topic, message_key, actor, reason, replayed_at
                from dlt_replay_audit
                order by id desc
                limit :limit
                """, Map.of("limit", limit));
    }
}
