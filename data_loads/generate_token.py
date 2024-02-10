import requests
from configs.config_urls import url_upstox_token


headers = {
    'accept': 'application/json',
    'Content-Type': 'application/x-www-form-urlencoded',
}

data = {
    'code': 'mIlDwA',
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