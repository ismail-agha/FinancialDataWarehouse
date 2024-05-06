# import os
# import sys
# import subprocess, requests, logging
# from io import StringIO
#
# # Add parent directory to the Python path
# current_dir = os.path.dirname(os.path.abspath(__file__))
# parent_dir = os.path.dirname(current_dir)
# sys.path.append(parent_dir)
#
# from configs.config_urls import curl_cmd_bse_equity, nse_equity_request_params
# import pandas as pd
# from db.database_and_models import TABLE_MODEL_EQUITY_LIST, session
# import json
# from sqlalchemy import text
# import concurrent.futures
# from datetime import datetime
#
#
# # try:
# #     # Execute the curl command and capture the output
# #     print(f'Subprocess Starts for BSE')
# #     output = subprocess.check_output(curl_cmd_bse_equity)
# #     print(f'Subprocess Ends for BSE')
# #     # Convert the JSON string to a Python dictionary
# #     data = json.loads(output)
# #
# #     print(f'BSE Data = {data}')
# #
# #
# #
# # except subprocess.CalledProcessError as e:
# #     print(f"Error executing curl command for BSE: {e}")
#
# # NSE 1
#
# # print(f'Requests Starts for NSE')
# # #response = requests.get(**nse_equity_request_params)
# # response = requests.get(nse_equity_request_params["url"], headers=nse_equity_request_params["headers"])
# # print(f'Requests Ends for NSE = {response}')
# # # Check if the request was successful (status code 200)
# # if response.status_code == 200:
# #     # Read the response content (CSV data) into a DataFrame
# #     df = pd.read_csv(StringIO(response.text))
# #
# #     # Now you can work with the 'df' DataFrame as needed
# #     print(df.head())  # Print the first few rows of the DataFrame
# # else:
# #     print("Failed to download the file.")
#
#
# # NSE 2
#
# # requests - Works
#
# import requests
#
# url = "https://www.nseindia.com/api/marketStatus"
#
# headers = {
#     "accept": "*/*",
#     "accept-language": "en-US,en;q=0.9,la;q=0.8",
#     "priority": "u=1, i",
#     "referer": "https://www.nseindia.com/get-quotes/equity?symbol=SHREECEM",
#     "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
#     "sec-ch-ua-mobile": "?0",
#     "sec-ch-ua-platform": '"Windows"',
#     "sec-fetch-dest": "empty",
#     "sec-fetch-mode": "cors",
#     "sec-fetch-site": "same-origin",
#     "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
# }
#
# # print(f'Requests Starts for NSE')
# # response = requests.get(url, headers=headers) #requests.get(url, headers=headers)
# # print(f'Requests Ends for NSE = {response}')
# #
# # if response.status_code == 200:
# #     print(f'nse 2 - {response.json()}')
# # else:
# #     print("Failed to retrieve data. Status code:", response.status_code)
#
#
# import requests
#
# # us 44.226.167.102:3128
# # in 13.234.24.116:1080
#
# proxy = "49.249.155.3:80"
# print(f'\nRequests Starts for NSE Using Proxy')
# response = requests.get(url, headers=headers, proxies={'http': proxy, 'https': proxy})
# print(f'Requests Ends for NSE using Proxy')
#
# if response.status_code == 200:
#     print(f'NSE Proxy - {response.json()}')
# else:
#     print("Failed to retrieve data. Status code:", response.status_code)


import boto3

# Initialize the S3 client
s3_client = boto3.client('s3')

# Define the bucket name and file key
bucket_name = 's3.financial.data.warehouse'
file_key = 'Inbound/EQUITY_L.csv'

try:
    # Get the object from S3
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)

    # Read the file contents
    file_content = response['Body'].read().decode('utf-8')

    # Print or process the file content
    print(file_content)
except Exception as e:
    print(f"Error reading file from S3: {e}")
