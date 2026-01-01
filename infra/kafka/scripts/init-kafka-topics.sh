#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="kafka:9092"

echo "Waiting for Kafka..."
cub kafka-ready -b "$BOOTSTRAP" 1 40

echo "Creating topics..."

# internal topic for schema registry
kafka-topics --bootstrap-server "$BOOTSTRAP" --create \
  --topic _schemas \
  --partitions 1 --replication-factor 1 \
  --config cleanup.policy=compact \
  --if-not-exists

# your business topic
kafka-topics --bootstrap-server "$BOOTSTRAP" --create \
  --topic transactions \
  --partitions 1 --replication-factor 1 \
  --if-not-exists

echo "Done."