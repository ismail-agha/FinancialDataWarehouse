import os
import sys

# Add parent directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

import requests
from configs.config_urls import url_upstox_token


headers = {
    'accept': 'application/json',
    'Content-Type': 'application/x-www-form-urlencoded',
}

data = {
    'code': 'OSiFeB',
    'client_id': '9cf11b90-77b8-432d-a41c-0e5e425dc285',
    'client_secret': 'pzws0rr44o',
    'redirect_uri': 'http://127.0.0.1',
    'grant_type': 'authorization_code',
}

response = requests.post(url_upstox_token, headers=headers, data=data)
data = response.json()
print(response.status_code)
print(response.json())

#print(f"access_token = {data['access_token']}")

#return data['access_token']

# Open the file in write mode ('w')
with open(file = '../token/token.txt', mode='w') as file:
    # Write the content to the file
    file.write(data['access_token'])