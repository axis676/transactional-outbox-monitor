# Observability (Tracing)

This folder provides a local tracing backend for development.

## Start Jaeger (with OTLP enabled)
```bash
cd infra/observability
docker compose up -d
```

- Jaeger UI: http://localhost:16686
- OTLP gRPC endpoint: `http://localhost:4317`
- OTLP HTTP endpoint: `http://localhost:4318`

## Send traces from app (recommended: OpenTelemetry Java Agent)

1. Download Java agent:
```bash
curl -L -o opentelemetry-javaagent.jar \
  https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/latest/download/opentelemetry-javaagent.jar
```

2. Run Spring Boot with OTLP exporter:
```bash
export OTEL_SERVICE_NAME=transactional-outbox-monitor
export OTEL_TRACES_EXPORTER=otlp
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export OTEL_EXPORTER_OTLP_PROTOCOL=grpc

mvn spring-boot:run \
  -Dspring-boot.run.jvmArguments="-javaagent:$(pwd)/opentelemetry-javaagent.jar"
```

3. Generate traffic and view traces in Jaeger UI.

## Notes
- This project already carries `traceparent` + `correlation_id` in outbox flow.
- For async boundaries, consumer spans should be linked using span links (already implemented).
