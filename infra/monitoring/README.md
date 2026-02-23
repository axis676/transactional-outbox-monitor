# Monitoring Notes

This folder contains Prometheus alerting examples for `transactional-outbox-monitor`.

## Files
- `prometheus-alert-rules.yml`: sample alert rules based on exposed metrics.

## Suggested wiring
1. Add this file into your Prometheus `rule_files` list.
2. Ensure Prometheus scrapes `http://<service-host>:8080/actuator/prometheus`.
3. Route alerts via Alertmanager (Slack/Telegram/PagerDuty).

## Included alerts
- `ConsumerDltIncreased`: DLT grew in 5m.
- `OutboxWriteFailuresDetected`: outbox write failures in 5m.
- `ConsumerDedupSpike`: duplicate skips unexpectedly high.

Tune thresholds by traffic profile before production.
