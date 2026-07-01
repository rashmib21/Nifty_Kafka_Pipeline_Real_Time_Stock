#!/bin/bash
# kafdrop_setup.sh
# Kafdrop is a website that runs on your computer
# It lets you see everything happening in Kafka through a browser
# You can see all topics, how many messages are in each partition,
# how far behind the consumer is, and read actual messages from DLQ
#
# Run this command once after Kafka is running
# Command: bash kafdrop_setup.sh
# Then open your Brave browser and go to: http://localhost:9000

echo "Starting Kafdrop on http://localhost:9000 ..."

# This command downloads and runs Kafdrop using Docker
# -d means run in background so terminal stays free
# --name kafdrop gives the container a name so we can stop it later
# --network host means Kafdrop can reach our Kafka on localhost:9092
# KAFKA_BROKERCONNECT tells Kafdrop where our Kafka is running
# JVM_OPTS limits memory to 64MB because Kafdrop does not need much
# -p 9000:9000 means open port 9000 so we can access it in browser
docker run -d \
  --name kafdrop \
  --network host \
  -e KAFKA_BROKERCONNECT=localhost:9092 \
  -e JVM_OPTS="-Xms32M -Xmx64M" \
  -p 9000:9000 \
  obsidiandynamics/kafdrop:latest

echo ""
echo "Wait about 10 seconds then open Brave browser and go to:"
echo "http://localhost:9000"
echo ""
echo "What you will see in Kafdrop:"
echo "  - stock-ticks topic with 50 partitions"
echo "  - How many messages have been produced in each partition"
echo "  - stock-consumers group and how far behind it is"
echo "  - stock-dlq topic and any failed messages"
echo ""
echo "To stop Kafdrop: docker stop kafdrop"
echo "To start again: docker start kafdrop"
echo "To delete: sdocker rm kafdrop"