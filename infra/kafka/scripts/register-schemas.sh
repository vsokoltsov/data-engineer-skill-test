#!/bin/sh
set -eu

echo "Waiting for Schema Registry..."
until curl -sSf http://schema-registry:8081/subjects > /dev/null; do
  sleep 1
done

SUBJECT="transactions-value"
SCHEMA_FILE="/schemas/transactions_v1.avsc"

# Превращаем файл схемы в одну строку и экранируем кавычки
SCHEMA_ESCAPED=$(tr -d '\n' < "$SCHEMA_FILE" | sed 's/"/\\"/g')

echo "Registering schema for $SUBJECT..."
curl -sSf -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "{\"schema\":\"$SCHEMA_ESCAPED\"}" \
  "http://schema-registry:8081/subjects/$SUBJECT/versions" > /dev/null

echo "Schema registered."