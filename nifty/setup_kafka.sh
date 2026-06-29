#!/bin/bash
# setup_kafka.sh
# Run this file once before starting producer and consumer
# Command: bash setup_kafka.sh
# This creates the two Kafka topics we need

echo "Creating Kafka topics..."

# Create the main topic: stock-ticks
# --partitions 50 means we create 50 lanes inside this topic
# One lane per Nifty 50 stock
# Each stock always goes to its own lane because we use symbol as the message key
# This means 50 consumers can each read one lane in parallel
# --replication-factor 3 means each lane is copied to 3 different Kafka servers
# So if one server dies, data is safe on the other 2 servers
# --config min.insync.replicas=2 means before saying success
# at least 2 of the 3 copies must confirm they saved the message
# --config retention.ms=86400000 means messages are kept for 24 hours
# After 24 hours old messages are automatically deleted to free disk space
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic stock-ticks \
  --partitions 50 \
  --replication-factor 3 \
  --if-not-exists \
  --config min.insync.replicas=2 \
  --config retention.ms=86400000

echo "Created stock-ticks topic with 50 partitions"

# Create the DLQ topic: stock-dlq
# Only 1 partition because failed messages are very few compared to normal messages
# retention.ms=604800000 means 7 days
# We keep failed messages for 7 days so engineers have time to investigate
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic stock-dlq \
  --partitions 1 \
  --replication-factor 3 \
  --if-not-exists \
  --config retention.ms=604800000

echo "Created stock-dlq topic with 1 partition"

echo ""
echo "Verify topics were created by running:"
echo "kafka-topics.sh --list --bootstrap-server localhost:9092"