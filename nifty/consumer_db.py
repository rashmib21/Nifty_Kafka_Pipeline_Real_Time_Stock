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