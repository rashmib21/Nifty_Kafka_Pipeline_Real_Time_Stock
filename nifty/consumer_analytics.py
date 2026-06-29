import json
import time
import pytz
from datetime import datetime
from kafka import KafkaConsumer
import mysql.connector

from config import (
    KAFKA_BROKER, KAFKA_TOPIC,
    DB_HOST, DB_USER, DB_PASSWORD, DB_NAME,
    MARKET_OPEN, MARKET_CLOSE
)

IST = pytz.timezone('Asia/Kolkata')

#Dictionary to keep running totals for each stock in memory,We need these to calculate VWAP correctly
#running_totals = {
#     'RELIANCE': {
#         'total_price_x_volume': 5000000,
#         'total_volume': 1500,
#         'tick_count': 200,
#         'day_high': 2900.0,
#         'day_low': 2800.0
#     }
# }

running_totals = {}


