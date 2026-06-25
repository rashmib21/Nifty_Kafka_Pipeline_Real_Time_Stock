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