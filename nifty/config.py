import os
from dotenv import load_dotenv

load_dotenv()

#Angel One credentials, read from .env
API_KEY=os.getenv('API_KEY','')
CLIENT_ID=os.getenv('CLIENT_ID','')
PASSWORD=os.getenv('PASSWORD','')
TOTP_SECRET=os.getenv('TOTP_SECRET','')

