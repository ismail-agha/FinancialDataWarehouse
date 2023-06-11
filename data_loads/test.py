import pandas as pd
import requests
import io
from datetime import datetime
from configs.config_urls import endpoint_equity_historical_csv, headers

from_date = '30/05/2023'
to_date = datetime.now().strftime("%d/%m/%Y")
result =['500002','500003','500008','500009']

for row in range(1,600):
    security_code = '500002'

    # Make a GET request to the API
    response = requests.get(endpoint_equity_historical_csv.format(security_code, from_date, to_date), headers=headers)

    if response.status_code == 200:
        # API-JSON
        print(f"Response({row}): {response.content.decode('utf-8', errors='ignore')}")