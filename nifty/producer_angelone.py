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