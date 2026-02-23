# transactional-outbox-monitor

POC 專案：示範 **Spring Modulith + Transactional Outbox + Debezium Relay + Correlation/Tracing** 的完整鏈路。

目標是把「交易一致性」、「非同步 relay」、「可追蹤性」這三件事放在同一個最小可跑範例中，方便後續擴展到正式微服務。

---

## 1. 這個專案在解什麼問題

在微服務中，常見需求是：
1. 同交易寫入業務資料（例如訂單）
2. 可靠發佈事件給其他服務（避免「DB 成功但訊息沒送出」）
3. 跨服務仍能追蹤同一筆業務事件（correlation id / trace context）

本專案採用：
- **Spring Modulith**：在應用內發佈模組事件
- **Outbox table**：事件先落 DB，確保與業務資料同交易
- **Debezium CDC**：監聽 outbox table，relay 到 Kafka
- **OTel trace + correlation id**：讓 producer/consumer 在 async 邊界可關聯

---

## 2. 架構總覽

```mermaid
flowchart LR
    C["Client / API Caller"] -->|"HTTP + X-Correlation-Id"| F["CorrelationIdFilter"]
    F -->|"MDC correlation_id"| OCS["OrderCommandService<br/>Transactional"]

    OCS -->|"Insert"| ORD[("orders")]
    OCS -->|"Publish"| EVT["OrderCreatedEvent"]

    EVT --> OW["OutboxWriter<br/>ApplicationModuleListener<br/>Retryable + REQUIRES_NEW"]
    OW -->|"Insert event_id, correlation_id, traceparent"| OB[("outbox_event")]

    OB --> DBZ["Debezium Postgres Connector"]
    DBZ --> SMT["Outbox Event Router SMT"]
    SMT --> K[("Kafka topic: outbox.event.OrderCreated")]

    K --> KC["KafkaOutboxConsumer"]
    KC -->|"headers -> MDC + span link"| LOG["Application Logs"]
    KC --> CES[("ConsumedEventStore (Debug)")]
```

補充：
- 同步邊界：`Client -> API -> DB`（同交易）
- 非同步邊界：`outbox_event -> Debezium -> Kafka -> Consumer`
- Trace 策略：consumer 新開 span，透過 `span link` 關聯 producer context

---

## 3. 目前功能

### 3.1 訂單建立 + outbox 寫入
- API: `POST /api/modulith-outbox/orders`
- 同交易寫入 `orders`
- 發佈 `OrderCreatedEvent`
- Listener 寫入 `outbox_event`

### 3.2 outbox 寫入重試
- `OutboxWriter` 使用 `@Retryable`
- 設定：
  - `app.outbox.retry.max-attempts`
  - `app.outbox.retry.backoff-ms`
- 可用 `app.outbox.simulate-fail-first-attempt=true` 模擬第一次失敗，驗證重試

### 3.3 Debezium relay
- infra 位於 `infra/debezium/`
- 包含 Postgres / Kafka / Kafka Connect / Kafdrop
- Connector config 已設定 Outbox Event Router

### 3.4 Correlation ID + Tracing
- `CorrelationIdFilter`：
  - 讀取 `X-Correlation-Id`
  - 若無則自動產 UUID
  - 寫入 MDC (`correlation_id`) 並回寫 response header
- Outbox 會保存：
  - `traceparent`, `tracestate`
  - `correlation_id`, `causation_id`, `event_id`
- Kafka consumer 端：
  - 從 headers 拿 correlation id
  - 寫入 MDC
  - 建立 linked consumer span

### 3.5 Consumer 防重複（Idempotency）
- consumer 先將 `event_id` 寫入 `consumed_event`
- SQL: `insert ... on conflict do nothing`
- 若已存在代表重覆事件，直接 skip，不重做副作用
- `/api/consumer/events` 可看到 `processed` 與 `dedupHit` 計數

### 3.6 Consumer Retry + DLT
- Kafka listener 使用 `DefaultErrorHandler`
- retry 策略：exponential backoff（500ms 起跳，倍增到 5s 上限）
- 超過重試後自動送往 `<原topic>.DLT`
  - 目前示範 topic: `outbox.event.OrderCreated.DLT`
- `KafkaDltConsumer` 會消費 DLT 並記錄錯誤
- `/api/consumer/events` 會多顯示 `dlt` 計數

### 3.7 Prometheus Metrics
可在 `GET /actuator/prometheus` 查看以下關鍵計數：
- `outbox_write_success_total`
- `outbox_write_failure_total`
- `consumer_processed_total`
- `consumer_dedup_hit_total`
- `consumer_dlt_total`

---

## 4. 專案結構（重點）

```text
src/main/java/com/joechen/outboxmonitor
├─ modulithoutbox
│  ├─ OrderCommandService.java         # 業務交易 + 事件發佈
│  ├─ OrderCreatedEvent.java           # 模組事件
│  ├─ OrderRequest.java                # API request model
│  ├─ OutboxWriter.java                # listener 寫 outbox + retry
│  ├─ OutboxQueryService.java          # 查詢 outbox
│  └─ OutboxModulithController.java    # /api/modulith-outbox/*
├─ observability
│  ├─ CorrelationIdFilter.java         # HTTP correlation id 進出 + MDC
│  └─ CorrelationIdContext.java        # service 取用 correlation id
├─ consumer
│  ├─ KafkaOutboxConsumer.java         # 消費 outbox topic + trace link
│  ├─ ConsumedEventStore.java          # debug in-memory store
│  └─ ConsumerDebugController.java     # /api/consumer/events
└─ tracing
   └─ TracePropagationSupport.java     # trace context capture / span link

src/main/resources
├─ application.yml
└─ schema.sql                          # orders + outbox_event

infra/debezium
├─ docker-compose.yml
├─ outbox-connector.json
└─ register-connector.sh
```

---

## 5. 資料表設計

### `orders`
- `order_id` (PK)
- `customer_id`
- `amount`
- `created_at`

### `outbox_event`
- `id` (PK, bigserial)
- `event_id` (unique)
- `aggregate_type`, `aggregate_id`
- `event_type`
- `payload` (jsonb)
- `traceparent`, `tracestate`
- `correlation_id`, `causation_id`
- `occurred_at`, `created_at`

### `consumed_event`（consumer idempotency）
- `event_id`
- `consumer_name`
- `consumed_at`
- PK: `(event_id, consumer_name)`

---

## 6. 本地啟動與驗證流程

### Step 1: 啟動 Debezium 基礎設施
```bash
cd infra/debezium
docker compose up -d
./register-connector.sh
```

### Step 2: 啟動應用
```bash
mvn spring-boot:run
```

### Step 3: 建立訂單（帶 correlation id）
```bash
curl -X POST http://localhost:8080/api/modulith-outbox/orders \
  -H 'Content-Type: application/json' \
  -H 'X-Correlation-Id: ORD-O-1001' \
  -d '{
    "orderId": "O-1001",
    "customerId": "C-001",
    "amount": 123.45,
    "causationId": ""
  }'
```

### Step 4: 查 outbox 是否落表
```bash
curl 'http://localhost:8080/api/modulith-outbox/outbox?limit=20'
```

### Step 5: 查 consumer 是否收到了 relay 事件
```bash
curl 'http://localhost:8080/api/consumer/events?limit=20'
```

### Step 6: 驗證 retry / DLT（可選）
將以下設定打開後重啟 app：
```yaml
app:
  consumer:
    fail-on-payload-contains: "O-FAIL"
```

再送一筆包含 `O-FAIL` 的 payload，事件會在重試後進入 DLT。

### Step 7: Kafka UI
- Kafdrop: `http://localhost:9000`
- Topics:
  - `outbox.event.OrderCreated`
  - `outbox.event.OrderCreated.DLT`

---

## 7. 設定重點（application.yml）

- DB: `spring.datasource.*`
- Kafka consumer: `spring.kafka.*`
- Modulith events: `spring.modulith.events.republish-outstanding-events-on-restart=true`
- Outbox retry:
  - `app.outbox.retry.max-attempts`
  - `app.outbox.retry.backoff-ms`
- 測試失敗注入:
  - `app.outbox.simulate-fail-first-attempt`
- Log pattern 已帶 correlation:
  - `logging.pattern.level: "%5p [corr:%X{correlation_id:-}]"`

---

## 8. 目前是 POC，哪些是刻意簡化

1. 消費後資料先放 in-memory (`ConsumedEventStore`)，非正式持久化
2. payload 序列化目前手動組字串，正式版建議改 Jackson
3. 尚未做 DLT/重試主題與完整 consumer idempotency
4. 未加入安全設定（SASL/ACL/TLS）

---

## 9. 監控與告警

### 9.1 Prometheus scrape
請確認 Prometheus 已抓取：
- `http://<service-host>:8080/actuator/prometheus`

### 9.2 Alert rules（範本）
本專案已提供範本：
- `infra/monitoring/prometheus-alert-rules.yml`

內含示例告警：
- `ConsumerDltIncreased`
- `OutboxWriteFailuresDetected`
- `ConsumerDedupSpike`

> 這些 threshold 是 POC 預設，請依實際流量調整。

### 9.3 Runbook（簡版）
- `ConsumerDltIncreased`：
  1. 看 `outbox.event.OrderCreated.DLT` payload + headers
  2. 看 app log（含 `correlation_id`）定位失敗點
  3. 修復後 replay（後續可做管理 API）
- `OutboxWriteFailuresDetected`：
  1. 檢查 DB 連線與 schema
  2. 檢查 `OutboxWriter` retry 日誌
  3. 確認 failure 是否持續增加
- `ConsumerDedupSpike`：
  1. 檢查是否重播 / 重複投遞
  2. 檢查 producer 重試是否異常升高
  3. 確認 replay 流程有無重複觸發

## 10. 下一步建議（正式化路線）

1. 將 consumer 改為獨立服務（不同 repo/deploy）
2. 引入 DLQ replay 管理 API
3. 將 payload schema 化（Avro/JSON Schema）
4. 加上整合測試（Testcontainers: Postgres + Kafka + Connect）
5. 對接 OTEL exporter（Tempo/Jaeger）做端到端追蹤

---

## 10. 關鍵設計原則（本專案採用）

- 非同步邊界不強求同一 trace id
- 使用：**新 consumer trace + span link + correlation id**
- `event_id` 用於唯一性與去重
- `correlation_id` 用於跨服務關聯查詢

這是目前團隊最容易落地、也最穩定的做法。