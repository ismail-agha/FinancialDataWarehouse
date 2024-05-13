import os
import sys

# Add parent directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
token_file_path = os.path.join(parent_dir, f"token/token.txt")

import json
import requests
from configs.config_urls import url_upstox_token, headers_upstox_token
from configs.config import s3_client, bucket_name, get_timestamp
from datetime import datetime
from helper.custom_logging_script import setup_logger, custom_logging

# ---------
# Custom Logger
# ---------
# Define the script name here
script_name = os.path.basename(__file__).split('.')[0]

# Initialize the logger with the script name
process_start_time = None
if len(sys.argv) > 1:
    process_start_time = sys.argv[1]
logger = setup_logger(script_name, process_start_time)

date_yyyymmdd = datetime.now().strftime("%Y%m%d")

file_key = f'Inbound/upstox_code_{date_yyyymmdd}.txt'

upstox_code = ''
try:
    # Get the object from S3
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)

    # Read the file contents
    upstox_code = response['Body'].read().decode('utf-8')

    # Print or process the file content
    print(upstox_code)
    custom_logging(logger, 'INFO', f'Completed reading data from file {file_key}. The code is {upstox_code}.')

except Exception as e:
    print(f"Error reading file from S3: {e}")
    custom_logging(logger, 'ERROR', f'Error while reading data from file {file_key}. Error = {e}.')
    exit(1)

data = {
    'code': upstox_code,
    'client_id': '9cf11b90-77b8-432d-a41c-0e5e425dc285',
    'client_secret': 'pzws0rr44o',
    'redirect_uri': 'http://127.0.0.1',
    'grant_type': 'authorization_code',
}

try:
    # Make the HTTP POST request to retrieve the access token
    response = requests.post(url_upstox_token, headers=headers_upstox_token, data=data)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()

        # Print the status code and response JSON for debugging
        print(f"Status Code: {response.status_code}")
        print("Response JSON:", data)

        # Extract the access token from the response
        access_token = data.get('access_token')

        # Check if access token is present in the response
        if access_token:
            print(f"Access Token: {access_token}")

            custom_logging(logger, 'INFO', f'Completed generating an Access Token.')

            # Write the access token to a file
            with open(file=token_file_path, mode='w') as file:
                file.write(access_token)

            custom_logging(logger, 'INFO', f'Completed creating a Token file.')
        else:
            print("Access token not found in the response.")
            custom_logging(logger, 'ERROR', f'Access token not found in the response.')
            exit(1)
    else:
        # Print error message if request was unsuccessful
        print(f"Error while generating token. Error = {response.status_code} - {response.reason} - {json.loads(response.text)['errors'][0]['message']}.")
        custom_logging(logger, 'ERROR', f"Error while generating token. Error = {response.status_code} - {response.reason} - {json.loads(response.text)['errors'][0]['message']}.")
        exit(1)

    custom_logging(logger, 'INFO', f'Completed creating token file.')
except Exception as e:
    # Handle any exceptions that occur during the execution of the code
    print(f"An error occurred: {e}")
    custom_logging(logger, 'ERROR', f'Error while generating token. Error = {e}.')
    exit(1)

