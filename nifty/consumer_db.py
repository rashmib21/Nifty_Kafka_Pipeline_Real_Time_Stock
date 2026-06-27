import json
import time
import pytz
from datetime import datetime
from kafka import KafkaConsumer
from kafka import KafkaProducer
import mysql.connector
from config import (
	KAFKA_BROKER, KAFKA_TOPIC, KAFKA_DLQ, KAFKA_GROUP_ID,
    DB_HOST, DB_USER, DB_PASSWORD, DB_NAME,
    MARKET_OPEN_HOUR, MARKET_OPEN_MINUTE,
    MARKET_CLOSE_HOUR, MARKET_CLOSE_MINUTE)

IST=pytz.timezone('Asia/Kolkata')

#this set stores recently seen tick keys like "RELIANCE_1718001234", we use this to check if we already saved this exact tick before 
seen_tick_keys=set()

def is_market_open():
    current_time=datetime.now(IST)
    current_total=current_time.hour*60+current_time.minute
    open_total=MARKET_OPEN[0]*60+MARKET_OPEN[1]
    close_total=MARKET_CLOSE[0]*60+MARKET_CLOSE[1]
    if current_time.weekday()>=5:
        return False
    if current_total >= open_total and current_total <= close_total:
        return True
    else:
        return False

#Create Kafka Consumer
#group_id='stock-consumers' means this consumer is part of the DB saving group
#enable_auto_commit=False means we manually tell Kafka when we are done
kafka_consumer=KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    group_id=KAFKA_GROUP_DB,
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    isolation_level='read_committed',
    value_deserializer=deserializer
    )                

#Open MySQL connection and reuse it for all rows
mysql_connection=mysql.connector.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME
    ) 