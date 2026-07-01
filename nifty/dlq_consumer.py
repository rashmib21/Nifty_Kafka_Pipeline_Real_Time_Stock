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
    value_deserializer = deserializer,
    session_timeout_ms     = 30000, #30 sec, dont assume consumer dead
    heartbeat_interval_ms  = 10000, #send heartbeat in every 10 sec
    max_poll_interval_ms   = 300000 #processing for 5 minutes
)

# Connect to MySQL to save failure records
mysql_connection = mysql.connector.connect(
    host     = DB_HOST,
    user     = DB_USER,
    password = DB_PASSWORD,
    database = DB_NAME
)
mysql_cursor = mysql_connection.cursor()

print("DLQ Monitor started. Watching topic: " + KAFKA_DLQ)

# Read every failed message from DLQ topic one by one
for kafka_message in dlq_consumer:
 
    data = kafka_message.value
 
    # Extract the three fields we saved in send_to_dlq() in consumer_db.py
    original_message = json.dumps(data.get('original_message', {}))
    error_reason     = data.get('error_reason', 'unknown reason')
    failed_at        = data.get('failed_at', int(time.time()))
 
    # Always print to terminal first
    # Even if database save fails below, we can see the failure in terminal
    print("\n--- FAILED MESSAGE ---")
    print("Reason   : " + error_reason)
    print("Failed at: " + str(failed_at))
    print("Message  : " + original_message[:200])
 
    # Try to save this failure record to MySQL
    try:
        sql = """
            INSERT INTO dlq_messages
            (raw_payload, error_reason, failed_at, retried)
            VALUES (%s, %s, FROM_UNIXTIME(%s), 0)
        """
 
        # FROM_UNIXTIME converts the number 1718001234
        # into a readable date like '2024-06-10 10:33:54'
 
        mysql_cursor.execute(sql, (original_message, error_reason, failed_at))
        mysql_connection.commit()
 
        print("Saved to dlq_messages table")
 
    except Exception as error:
        # If saving to database also fails, at least we printed to terminal above
        print("Could not save to database: " + str(error))
 
