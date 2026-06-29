import json
import time
from kafka import KafkaConsumer
import mysql.connector
 
from config import (
    KAFKA_BROKER, KAFKA_DLQ,
    DB_HOST, DB_USER, DB_PASSWORD, DB_NAME
)

def deserializer(v):
    return json.loads(v).decode('utf-8') 

dlq_consumer = KafkaConsumer(
    KAFKA_DLQ,
    bootstrap_servers = [KAFKA_BROKER],
    group_id = 'dlq-monitor-group',
    auto_offset_reset = 'earliest',
    enable_auto_commit = True,
    value_deserializer = deserializer
)
