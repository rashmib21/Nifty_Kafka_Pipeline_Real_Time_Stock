from config import KAFKA_BROKER, KAFKA_TOPIC
import json

def deserializer(v):
    return json.loads(v.decode('utf-8'))

KAFKA_CONSUMER_CONFIG = {
    'bootstrap_servers'   : [KAFKA_BROKER],
    'auto_offset_reset'   : 'earliest',
    'enable_auto_commit'  : False,
    'isolation_level'     : 'read_committed',
    'value_deserializer'  : deserializer,
    'session_timeout_ms'  : 30000,     
    'heartbeat_interval_ms': 10000,    
    'max_poll_interval_ms' : 300000    
}