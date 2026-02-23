#!/usr/bin/env bash
set -euo pipefail

CONNECT_URL=${CONNECT_URL:-http://localhost:8083}

curl -sS -X PUT "$CONNECT_URL/connectors/outbox-connector/config" \
  -H 'Content-Type: application/json' \
  --data @"$(dirname "$0")/outbox-connector.json"

echo

echo "Connector upserted: outbox-connector"
