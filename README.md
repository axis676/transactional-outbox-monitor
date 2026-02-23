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
    C[Client / API Caller] -->|HTTP + X-Correlation-Id| F[CorrelationIdFilter]
    F -->|MDC.correlation_id| OCS[OrderCommandService\n@Transactional]

    OCS -->|Insert| ORD[(orders)]
    OCS -->|Publish| EVT[OrderCreatedEvent]

    EVT --> OW[OutboxWriter\n@ApplicationModuleListener\n@Retryable + REQUIRES_NEW]
    OW -->|Insert event_id/correlation_id/traceparent...| OB[(outbox_event)]

    OB --> DBZ[Debezium Postgres Connector]
    DBZ --> SMT[Outbox Event Router SMT]
    SMT --> K[(Kafka\noutbox.event.OrderCreated)]

    K --> KC[KafkaOutboxConsumer]
    KC -->|headers -> MDC + span link| LOG[Application Logs]
    KC --> CES[(ConsumedEventStore\nDebug)]
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

### Step 6: Kafka UI
- Kafdrop: `http://localhost:9000`
- Topic: `outbox.event.OrderCreated`

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

## 9. 下一步建議（正式化路線）

1. 將 consumer 改為獨立服務（不同 repo/deploy）
2. 加入 consumer idempotency table（以 `event_id` 去重）
3. 引入 DLQ/DLT 與告警
4. 將 payload schema 化（Avro/JSON Schema）
5. 加上整合測試（Testcontainers: Postgres + Kafka + Connect）
6. 對接 OTEL exporter（Tempo/Jaeger）做端到端追蹤

---

## 10. 關鍵設計原則（本專案採用）

- 非同步邊界不強求同一 trace id
- 使用：**新 consumer trace + span link + correlation id**
- `event_id` 用於唯一性與去重
- `correlation_id` 用於跨服務關聯查詢

這是目前團隊最容易落地、也最穩定的做法。