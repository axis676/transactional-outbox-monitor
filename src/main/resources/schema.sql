create table if not exists orders (
    order_id varchar(64) primary key,
    customer_id varchar(64) not null,
    amount numeric(18,2) not null,
    created_at timestamptz not null default now()
);

create table if not exists outbox_event (
    id bigserial primary key,
    event_id varchar(100) not null,
    aggregate_type varchar(100) not null,
    aggregate_id varchar(100) not null,
    event_type varchar(100) not null,
    payload jsonb not null,
    traceparent varchar(128),
    tracestate text,
    correlation_id varchar(100) not null,
    causation_id varchar(100),
    occurred_at timestamptz not null,
    created_at timestamptz not null default now()
);

create unique index if not exists uk_outbox_event_event_id on outbox_event(event_id);
create index if not exists idx_outbox_event_created_at on outbox_event(created_at);
create index if not exists idx_outbox_event_correlation_id on outbox_event(correlation_id);

create table if not exists consumed_event (
    event_id varchar(100) not null,
    consumer_name varchar(100) not null,
    consumed_at timestamptz not null default now(),
    primary key (event_id, consumer_name)
);

create table if not exists dlt_replay_audit (
    id bigserial primary key,
    dlt_id varchar(100) not null,
    from_topic varchar(255) not null,
    to_topic varchar(255) not null,
    message_key varchar(255),
    actor varchar(255) not null,
    reason text not null,
    replayed_at timestamptz not null default now()
);
