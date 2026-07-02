#Alerts are triggered when:
#   Alert 1: Price UP more than 2% from today's open price
#   Alert 2: Price DOWN more than 2% from today's open price
#   Alert 3: Stock hit a new day HIGH
#   Alert 4: Stock hit a new day LOW
#   Alert 5: Volume spike - volume suddenly 3x more than recent average
#
# Every alert is saved to stock_alerts table in MySQL
# We also print every alert to terminal so you can see it live



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
 
# Store the previous state of each stock
# We need this to compare current tick with what we saw before
# Example structure:
# previous_state = {
#     'RELIANCE': {
#         'last_known_high': 2900.0,
#         'last_known_low': 2800.0,
#         'recent_volumes': [100000, 120000, 98000, ...]
#     }
# }
previous_state = {}
 
# Alert settings
# Change these numbers to make alerts more or less sensitive
PRICE_ALERT_PERCENT     = 2.0   # alert if price moves 2% from open
VOLUME_SPIKE_MULTIPLIER = 3.0   # alert if volume is 3 times the recent average
 
 
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
 
 
def deserializer(v):
    return json.loads(v.decode('utf-8')) 
    
# Separate consumer group so alerts consumer reads all messages independently
kafka_consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers = [KAFKA_BROKER],
    group_id = 'alerts-consumer-group',
    auto_offset_reset = 'earliest',
    enable_auto_commit = False,
    isolation_level = 'read_committed',
    value_deserializer = deserializer,
    session_timeout_ms     = 30000, #30 sec, dont assume consumer dead
    heartbeat_interval_ms  = 10000, #send heartbeat in every 10 sec
    max_poll_interval_ms   = 300000 #processing for 5 minutes
)
 
mysql_connection = mysql.connector.connect(
    host = DB_HOST,
    user = DB_USER,
    password = DB_PASSWORD,
    database = DB_NAME
)
mysql_cursor = mysql_connection.cursor()
 
 
def save_alert_to_db(symbol, alert_type, alert_message, ltp):
    # Save one alert record to MySQL
    sql = """
        INSERT INTO stock_alerts
        (symbol, alert_type, alert_message, ltp_at_alert, alerted_at)
        VALUES (%s, %s, %s, %s, NOW())
    """
    mysql_cursor.execute(sql, (symbol, alert_type, alert_message, ltp))
    mysql_connection.commit()
    # Print alert to terminal with !! so it stands out
    print("!! ALERT !! " + alert_type + " | " + symbol + " | " + alert_message)
 
 
def check_all_alerts(data):
    symbol = data.get('symbol', '')
    ltp = data.get('ltp', 0)
    volume = data.get('volume', 0)
    open_price = data.get('open', 0)
    high_price = data.get('high', 0)
    low_price = data.get('low', 0)
 
    # Skip if data is not valid
    if symbol == '' or ltp == 0:
        return
 
    # Set up first-time state for this stock
    if symbol not in previous_state:
        previous_state[symbol] = {
            'last_known_high' : high_price,
            'last_known_low'  : low_price,
            'recent_volumes'  : []
        }
 
    state = previous_state[symbol]
 
    #  Alert 1: Price UP more than 2% from open 
    if open_price > 0:
        change_percent = ((ltp - open_price) / open_price) * 100
 
        if change_percent >= PRICE_ALERT_PERCENT:
            change_str  = str(round(change_percent, 2))
            message     = symbol + " is UP " + change_str + "% from open price"
            save_alert_to_db(symbol, 'PRICE_UP', message, ltp)
 
        #  Alert 2: Price DOWN more than 2% from open 
        if change_percent <= -PRICE_ALERT_PERCENT:
            change_str = str(round(abs(change_percent), 2))
            message    = symbol + " is DOWN " + change_str + "% from open price"
            save_alert_to_db(symbol, 'PRICE_DOWN', message, ltp)
 
    #  Alert 3: New day HIGH 
    if ltp > state['last_known_high']:
        message = symbol + " hit new day HIGH at Rs." + str(ltp)
        save_alert_to_db(symbol, 'NEW_DAY_HIGH', message, ltp)
        state['last_known_high'] = ltp   # update so we don't alert again for same level
 
    # Alert 4: New day LOW 
    if ltp < state['last_known_low']:
        message = symbol + " hit new day LOW at Rs." + str(ltp)
        save_alert_to_db(symbol, 'NEW_DAY_LOW', message, ltp)
        state['last_known_low'] = ltp   # update so we don't alert again for same level
 
    # Alert 5: Volume spike 
    # Add current volume to our recent list
    state['recent_volumes'].append(volume)
 
    # Keep only the last 20 volume readings
    # We use these 20 to calculate what is "normal" volume
    if len(state['recent_volumes']) > 20:
        state['recent_volumes'].pop(0)   # remove oldest
 
    # Only check for spike if we have enough history (at least 5 readings)
    if len(state['recent_volumes']) >= 5:
        # Calculate average of recent volumes
        total_volume = 0
        for v in state['recent_volumes']:
            total_volume = total_volume + v
        average_volume = total_volume / len(state['recent_volumes'])
 
        # If current volume is 3x more than average, that is a spike
        if average_volume > 0:
            if volume >= average_volume * VOLUME_SPIKE_MULTIPLIER:
                avg_str = str(round(average_volume))
                message = symbol + " volume SPIKE! Now: " + str(volume) + " vs recent avg: " + avg_str
                save_alert_to_db(symbol, 'VOLUME_SPIKE', message, ltp)
 
 
print("Alerts Consumer started. Watching for alerts...")
 
for kafka_message in kafka_consumer:
 
    data = kafka_message.value
 
    if is_market_open() == False:
        kafka_consumer.commit()
        continue
 
    try:
        check_all_alerts(data)
        kafka_consumer.commit()
    except Exception as error:
        print("Alert check failed: " + str(error))
        kafka_consumer.commit()
 






