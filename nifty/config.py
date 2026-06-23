import os
from dotenv import load_dotenv

load_dotenv()

#Angel One credentials, read from .env
API_KEY=os.getenv('API_KEY','')
CLIENT_ID=os.getenv('CLIENT_ID','')
PASSWORD=os.getenv('PASSWORD','')
TOTP_SECRET=os.getenv('TOTP_SECRET','')

#kafka settings
KAFKA_BROKER='localhost:9092'
KAKFA_TOPIC='stock-ticks'
KAFKA_DLQ='stock-dlq'
KAFKA_GROUP_ID='stock-consumers'

#MySQL setting
DB_HOST='localhost'
DB_USER='root'
DB_PASSWORD=os.getenv('DB_PASSWORD','')
DB_NAME='nifty_db'

EXCHANGE='NSE'

#NSE market hours in IST
MARKET_OPEN=(9,15) #opening time of market
MARKET_CLOSE=(15,30) #closing time of market

#Producer tuning
PRODUCER_RETRIES=5 #No. of retries
RETRY_BACKOFF_MS=500 #the waiting time before retrying after a failed request, for producer and consumer 
BATCH_SIZE_BYTES=32768 #32 kb per batch
LINGER_MS=20 #wait for 20 ms to fill the batch