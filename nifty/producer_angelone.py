#Connect to Angel One live market data and send every price tick to Kafka
#Flow: Angel One Websocket--> on_tick() function --> Kafka topic 'stock-ticks'

import pyotp
import json
import time
import pytz
from datetime import datetime
from SmartApi import SmartConnect
from SmartApi.SmartWebSocket import SmartWebSocket
from kafka import KafkaProducer
from config import (
    API_KEY, CLIENT_ID, PASSWORD, TOTP_SECRET,
    KAFKA_BROKER, KAFKA_TOPIC,
    MARKET_OPEN_HOUR, MARKET_OPEN_MINUTE,
    MARKET_CLOSE_HOUR, MARKET_CLOSE_MINUTE
)

from fetch_nifty50 import (
	fetch_all_nifty50,
	get_token_list,
	get_token_symbol_map
	)

#Create IST timezone object, we need this is check if Indian market is currently open
IST=pytz.timezone('Asia/Kolkata')

def is_market_open():
	current_time=datetime.now(IST)
	#check the weekdays weekday() gives 0 for monday and 6 for sunday, if weekday is 5 or 6 that means market is closed
	if current_time.weekday()==5:
		print("Today is Saturday-market closed")
		return False

	if current_time.weekday()==6:
		print("Today is Sunday-market closed")
		return False
	#Get the hour and minute right now
	current_hour=current_time.hours
	current_minute=current_time.minute

	#Convert current time and market times to total minutes for easy comparison, ex- 9:15 AM = 9*60 + 15 = 555 minutes
	current_total=current_hour*60+current_minute
	open_total=MARKET_OPEN_HOUR*60+MARKET_OPEN_MINUTE
	close_total+MARKET_CLOSE_HOUR*60+MARKET_CLOSE_MINUTE

	#Market is open if current time is between open and close
	if current_total>=open_total and current_total <=close_total:
		return True
	else:
		return False


#Step 1: Fetch all Nifty 50 stocks and their token number from Angel One
print("Fetching Nifty 50 stocks from NSE and Angel One...")
instruments=fetch_all_nifty50()
ws_token_list=get_token_list(instruments)		
token_symbol_map=get_token_symbol_map(instruments)
print("Ready with "+str(len(ws_token_list))+" stocks")

def serializer(v):
	return json.dumps(v.encode('utf-8'))

#Step 2: Create Kafka Producer, this object sends the message to Kafka, acks=all means Kafka will wait for all copies (replicas) to save the message
# enable_idempotence=True means even if we send the same message twice by mistake
# Kafka will only save it once. This prevents duplicates at producer level.
# retries=5 means if sending fails, try again 5 times before giving up

kafka_producer = KafkaProducer(
	bootstrap_servers=[KAFKA_BROKER],
	acks='all',
	enable_idempotence=True,
	retires=5,
	retry_backoff_ms=500,
	max_in_flight_requests_per_connection=1,
	value_serializer=serializer )



