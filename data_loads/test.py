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


try:
    # Execute the curl command and capture the output
    print(f'Subprocess Starts for BSE')
    output = subprocess.check_output(curl_cmd_bse_equity)
    print(f'Subprocess Ends for BSE')
    # Convert the JSON string to a Python dictionary
    data = json.loads(output)

    print(f'BSE Data = {data}')



except subprocess.CalledProcessError as e:
    print(f"Error executing curl command for BSE: {e}")