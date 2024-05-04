import os
import sys
import subprocess, requests, logging
from io import StringIO

# Add parent directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from configs.config_urls import curl_cmd_bse_equity, nse_equity_request_params
import pandas as pd
from db.database_and_models import TABLE_MODEL_EQUITY_LIST, session
import json
from sqlalchemy import text
import concurrent.futures
from datetime import datetime


# try:
#     # Execute the curl command and capture the output
#     print(f'Subprocess Starts for BSE')
#     output = subprocess.check_output(curl_cmd_bse_equity)
#     print(f'Subprocess Ends for BSE')
#     # Convert the JSON string to a Python dictionary
#     data = json.loads(output)
#
#     print(f'BSE Data = {data}')
#
#
#
# except subprocess.CalledProcessError as e:
#     print(f"Error executing curl command for BSE: {e}")

# NSE 1

print(f'Requests Starts for NSE')
#response = requests.get(**nse_equity_request_params)
response = requests.get(nse_equity_request_params["url"], headers=nse_equity_request_params["headers"])
print(f'Requests Ends for NSE = {response}')
# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Read the response content (CSV data) into a DataFrame
    df = pd.read_csv(StringIO(response.text))

    # Now you can work with the 'df' DataFrame as needed
    print(df.head())  # Print the first few rows of the DataFrame
else:
    print("Failed to download the file.")


# NSE 2

# import subprocess
# import io
#
# # Define the curl command as a list of arguments
# curl_cmd = [
#     "curl",
#     nse_equity_request_params["url"],
#     "-H", f"User-Agent: {nse_equity_request_params['headers']['User-Agent']}",
#     "-H", f"Upgrade-Insecure-Requests: {nse_equity_request_params['headers']['Upgrade-Insecure-Requests']}",
#     "-H", f"sec-ch-ua: {nse_equity_request_params['headers']['sec-ch-ua']}",
#     "-H", f"sec-ch-ua-mobile: {nse_equity_request_params['headers']['sec-ch-ua-mobile']}",
#     "-H", f"sec-ch-ua-platform: {nse_equity_request_params['headers']['sec-ch-ua-platform']}"
# ]
#
# # Execute the curl command using subprocess and capture its output
# output = subprocess.check_output(curl_cmd)
#
# # Convert the output bytes to a string and then to a DataFrame
# df = pd.read_csv(io.StringIO(output.decode()))
#
# # Now you can work with the 'df' DataFrame as needed
# print(df.head())  # Print the first few rows of the DataFrame
