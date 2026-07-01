#Connect to Angel One live market data and send every price tick to Kafka
#Flow: Angel One Websocket--> on_tick() function --> Kafka topic 'stock-ticks'

import pyotp
import json
import time
import pytz
from datetime import datetime
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from kafka import KafkaProducer
from config import (
	API_KEY, CLIENT_ID, PASSWORD, TOTP_SECRET,
	KAFKA_BROKER, KAFKA_TOPIC,
	MARKET_OPEN,
	MARKET_CLOSE
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
	current_hour=current_time.hour
	current_minute=current_time.minute

	#Convert current time and market times to total minutes for easy comparison, ex- 9:15 AM = 9*60 + 15 = 555 minutes
	current_total=current_hour*60+current_minute
	open_total  = MARKET_OPEN[0] * 60 + MARKET_OPEN[1]   
	close_total = MARKET_CLOSE[0] * 60 + MARKET_CLOSE[1]

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
	return json.dumps(v).encode('utf-8')

#Step 2: Create Kafka Producer, this object sends the message to Kafka, acks=all means Kafka will wait for all copies (replicas) to save the message
# enable_idempotence=True means even if we send the same message twice by mistake
# Kafka will only save it once. This prevents duplicates at producer level.
# retries=5 means if sending fails, try again 5 times before giving up

kafka_producer = KafkaProducer(
	bootstrap_servers=[KAFKA_BROKER],
	acks='all',
	enable_idempotence=True,
	retries=5,
	retry_backoff_ms=500,
	max_in_flight_requests_per_connection=1, #The producer sends one message request at a time to a broker and waits for its acknowledgment before sending the next one.
	value_serializer=serializer,
	transactional_id='nifty-producer' )


#Initialize transactions, this allow us to send multiple messages as one group, either all messages in the group save, or none save
kafka_producer.init_transactions()

def on_tick(ws, tick_data):
	#Angel One call this function automatically, when every time the price is changes, tick_data is the list of price updates, one for each stock changed
	print("RAW TICK DATA:", tick_data)
	print("TYPE:", type(tick_data))

	#check market is open or not before doing anything
	if is_market_open()==False:
		print("Market is closed - ignoring tick")
		return 

	#Start a transaction, all msg we send below are part of this one transaction
	kafka_producer.begin_transaction()

	try: 
		#Loop through each stock tick in this in this update
		token  = str(tick_data.get('token', ''))
		symbol = token_symbol_map.get(token, token)

		price_in_paisa  = tick_data.get('last_traded_price', 0)
		price_in_rupees = price_in_paisa / 100

		volume        = tick_data.get('volume_trade_for_the_day', 0)
		exchange_time = tick_data.get('exchange_timestamp', int(time.time()))

		open_price = tick_data.get('open_price_of_the_day', 0) / 100
		high_price = tick_data.get('high_price_of_the_day', 0) / 100
		low_price  = tick_data.get('low_price_of_the_day', 0) / 100

		message = {
			"symbol":        symbol,
			"token":         token,
			'ltp':           round(price_in_rupees, 2),
			'volume':        volume,
			'open':          round(open_price, 2),
			'high':          round(high_price, 2),
			'low':           round(low_price, 2),
			'exchange_time': exchange_time,
			'produced_at':   int(time.time())
		}

		kafka_producer.send(
			KAFKA_TOPIC,
			key   = symbol.encode('utf-8'),
			value = message
		)

		print("Sent: " + symbol + " @ Rs. " + str(round(price_in_rupees, 2)))

		kafka_producer.commit_transaction()

	except Exception as error:
		print("Error sending to Kafka: " + str(error))
		kafka_producer.abort_transaction()

	except Exception as error:
		print("Error sending to Kafka: "+str(error))
		kafka_producer.abort_transaction()

#Step 3: Login to Angel One
print("Logging into Angel One...")

#Generate 6 digit otp using our TOTP secret
otp=pyotp.TOTP(TOTP_SECRET).now()

#Create Angel One connection object
angel_obj=SmartConnect(api_key=API_KEY)

#Login with CLIENT_ID , password, otp, this returns session data including a JWT token
session=angel_obj.generateSession(CLIENT_ID, PASSWORD, otp)

#extract the jwt token from session, we need this to open WebSocket
jwt_token=session['data']['jwtToken']
feed_token=angel_obj.getfeedToken()

print("Login Successfully!")

#Step 4: Open WebSocket connection
#Websocket stays open and angel one pushes price updates automatically
smart_socket = SmartWebSocketV2(
	API_KEY,      
	CLIENT_ID,    
	jwt_token,    
	feed_token    
)


def on_open(ws):
	print("WebSocket connected!")
	print("Subscribing to " + str(len(ws_token_list)) + " stocks...")
	subscribe_list = [{"exchangeType": 1, "tokens": ws_token_list}]
	smart_socket.subscribe("nifty50_session", 2, subscribe_list)  

def on_error(ws, error):
	print("WebSocket error: " + str(error))

# This runs when connection closes
def on_close(ws):
	print("WebSocket closed")

smart_socket.on_open  = on_open
smart_socket.on_data  = on_tick    # on_data not on_tick for V2
smart_socket.on_error = on_error
smart_socket.on_close = on_close

print("Starting WebSocket for " + str(len(ws_token_list)) + " stocks...")
print("Waiting for market ticks...")

smart_socket.connect()
