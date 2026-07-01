from config import KAFKA_BROKER, KAFKA_TOPIC
import json

def deserializer(v):
    return json.loads(v.decode('utf-8'))
