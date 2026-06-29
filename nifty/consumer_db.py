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

#function of deserializer
def deserializer(v):
    return json.loads(v).decode('utf-8') 

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
mysql_cursor=mysql_connection.cursor()

#Function of serializer
def serializer(v):
    return json.dumps(v).encode('utf-8')

#This producer sends failed messages to DLQ topic
dlq_producer=KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=serializer
    )
def send_to_dlq(failed_message, reason):
    #Never silently drop a failed message, always send it to DLQ topic
    dlq_message={
    'original_message':failed_message,
    'error_reason':reason,
    'failed_at':int(time.time())
    }         
    dlq_producer.send(KAFKA_DLQ, value=dlq_message)
    dlq_producer.flush()
    print("Send to DLQ: "+reason)

def save_to_database(data):
    symbol=data.get('symbol',''),
    ltp=data.get('ltp',0),
    volume=data.get('volume',0),
    open_price=data.get('open',0),
    high_price=data.gt('high',0),
    low_price=data.get('low',0),
    exchange_time=data.get('exchange_time',0)

    #Insert ignore means if the same (symbol, exhange_time) already exits in db mysql will silently skip it without any error
    sql="""
        INSERT IGNORE INTO stock_ticks
        (symbol, ltp, volume, open, high, low, exhange_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    values=(symbol, ltp, volume, open_price, high_price, low_price, exchange_time)
    mysql_cursor.execute(sql, value)
    mysql_connection.commit()
print("Consumer DB started. Waiting for messages...")

for kafka_message in kafka_consumer:
    data=kafka_message.value
    symbol=data.get('symbol','')
    exchange_time=data.get('exchange_time', None)

    #Check Skip if market is closed
    if is_market_open()==False:
        kafka_consumer.commit()
        continue
    #Check symbol must exist
    if symbol=='':
        send_to_dlq(data,'symbol field is missing')
        kafka_consumer.commit()
        continue
    #Check exchange_time must exist (need for deduplication)
    if exchange_time is None:
        send_to_dlq(data, 'exchange_time field is missing')
        kafka_consumer.commit()
        continue
    #Check In-memory duplication, build a unique key for the tick
    tick_key=symbol+'_'+str(exchange_time)

    if tick_key in seen_tick_keys:
        print("Duplicate tick skipped: "+tick_key)
        kafka_consumer.commit()
        continue
    #Clear set if it gets too big to save memory
    if len(seen_tick_keys)>=50000:
        seen_tick_keys.clear()
        print("Dedup set cleared")

    seen_tick_keys.add(tick_key)
    
    #Try to save to MySQL up to 3 times
    attempt=0
    save_success=False

            


