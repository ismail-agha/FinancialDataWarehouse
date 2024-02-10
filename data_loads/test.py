from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

import requests
from datetime import datetime

import os
import sys

# Add parent directory to the Python path
# current_dir = os.path.dirname(os.path.abspath(__file__))
# parent_dir = os.path.dirname(current_dir)
# sys.path.append(parent_dir)

from configs.config_urls import upstox_historical

try:
    engine = create_engine('postgresql://finapp_user:12345@localhost:5432/FinanceDB')
    print('Connection to the database established successfully.')
except SQLAlchemyError as e:
    print('Failed to connect to the database:', str(e))

def make_api_call(url):
    headers = {'Accept': 'application/json'}
    response = requests.get(url, headers=headers)
    return response.json()


interval = 'day'
from_date = '2024-02-08'
to_date = datetime.now().strftime("%Y-%m-%d")
data = make_api_call(upstox_historical.format('NSE', 'INE587G01015', interval, to_date, from_date))
print(data)