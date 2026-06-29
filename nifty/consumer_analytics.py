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

def is_market_open():
    current_time  = datetime.now(IST)
    current_total = current_time.hour * 60 + current_time.minute
    open_total    = MARKET_OPEN[0] * 60 + MARKET_OPEN[1]
    close_total   = MARKET_CLOSE[0] * 60 + MARKET_CLOSE[1]
    if current_time.weekday() >= 5:
        return False
    if current_total >= open_total and current_total <= close_total:
        return True
    else:
        return False


kafka_consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers  = [KAFKA_BROKER],
    group_id           = 'analytics-consumer-group',
    auto_offset_reset  = 'earliest',
    enable_auto_commit = False,
    isolation_level    = 'read_committed',
    value_deserializer = lambda v: json.loads(v.decode('utf-8'))
)
 
mysql_connection = mysql.connector.connect(
    host     = DB_HOST,
    user     = DB_USER,
    password = DB_PASSWORD,
    database = DB_NAME
)
mysql_cursor = mysql_connection.cursor()
 

def update_analytics(data):
    symbol     = data.get('symbol', '')
    ltp        = data.get('ltp', 0)
    volume     = data.get('volume', 0)
    open_price = data.get('open', 0)
 
    if symbol == '' or ltp == 0:
        return
 
    # If this is first tick for this stock today, set up initial values
    if symbol not in running_totals:
        running_totals[symbol] = {
            'total_price_x_volume' : 0,
            'total_volume'         : 0,
            'tick_count'           : 0,
            'day_high'             : ltp,
            'day_low'              : ltp
        }
 
    stock = running_totals[symbol]
 
    # Update tick count
    stock['tick_count'] = stock['tick_count'] + 1
 
    # Update day high
    if ltp > stock['day_high']:
        stock['day_high'] = ltp
 
    # Update day low
    if ltp < stock['day_low']:
        stock['day_low'] = ltp
 
    # Add to running totals for VWAP
    # VWAP = sum of (each price x its volume) divided by total volume
    # We keep adding to these totals every tick
    stock['total_price_x_volume'] = stock['total_price_x_volume'] + (ltp * volume)
    stock['total_volume']         = stock['total_volume'] + volume
 
    # Calculate VWAP now
    if stock['total_volume'] > 0:
        vwap = stock['total_price_x_volume'] / stock['total_volume']
        vwap = round(vwap, 2)
    else:
        vwap = ltp
 
    # Calculate price change percent from open
    if open_price > 0:
        price_change_percent = ((ltp - open_price) / open_price) * 100
        price_change_percent = round(price_change_percent, 2)
    else:
        price_change_percent = 0
 
    # Save to MySQL
    # ON DUPLICATE KEY UPDATE means:
    # If row for this symbol already exists -> update it
    # If row does not exist yet -> insert a new one
    # This keeps exactly one row per stock with latest values
    sql = """
        INSERT INTO stock_analytics
        (symbol, ltp, vwap, price_change_percent, day_high, day_low, tick_count, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE
            ltp                  = VALUES(ltp),
            vwap                 = VALUES(vwap),
            price_change_percent = VALUES(price_change_percent),
            day_high             = VALUES(day_high),
            day_low              = VALUES(day_low),
            tick_count           = VALUES(tick_count),
            updated_at           = NOW()
    """
 
    values = (
        symbol,
        ltp,
        vwap,
        price_change_percent,
        stock['day_high'],
        stock['day_low'],
        stock['tick_count']
    )
 
    mysql_cursor.execute(sql, values)
    mysql_connection.commit()
 
    print("Analytics: " + symbol +
          " LTP=" + str(ltp) +
          " VWAP=" + str(vwap) +
          " Change=" + str(price_change_percent) + "%")
 
         

