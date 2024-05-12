import os
import sys
import requests

# Add parent directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from configs.config_urls import url_upstox_market_holidays
import json
from datetime import datetime

date_yyyy_mm_dd = datetime.now().strftime("%Y-%m-%d")
print(f'date_yyyy_mm_dd = {date_yyyy_mm_dd}')

def is_holiday():

    try:
        payload = {}
        headers = {
            'Accept': 'application/json'
        }

        response = requests.request("GET", url_upstox_market_holidays, headers=headers, data=payload)

        #print(response.text)

        # Save the JSON response to a file with pretty formatting
        with open('../files/market_holidays.json', "w") as json_file:
            json.dump(json.loads(response.text), json_file, indent=4)

        # Parse the JSON response
        data = json.loads(response.text)

        # List to store dates
        dates_with_only_nse_closed = []
        dates_with_only_bse_closed = []
        dates_with_nse_bse_closed = {}

        # Iterate over each item in the data list
        for item in data['data']:
            if 'NSE' in item['closed_exchanges'] and 'BSE' not in item['closed_exchanges']:
                dates_with_only_nse_closed.append(item['date'])
            elif 'NSE' not in item['closed_exchanges'] and 'BSE' in item['closed_exchanges']:
                dates_with_only_bse_closed.append(item['date'])
            elif 'NSE' in item['closed_exchanges'] and 'BSE' in item['closed_exchanges']:
                dates_with_nse_bse_closed[item['date']] = item['description']

        # Print the dates
        print("Dates with only NSE closed:", dates_with_only_nse_closed)
        print("Dates with only BSE closed:", dates_with_only_bse_closed)
        print("Dates with both NSE & BSE closed:", dates_with_nse_bse_closed)

        if date_yyyy_mm_dd in dates_with_nse_bse_closed:
            return {'is_holiday': True, 'info': f'{date_yyyy_mm_dd} : {dates_with_nse_bse_closed[date_yyyy_mm_dd]}'}

    except Exception as e:
        print(f'Error {e}')

    return {'is_holiday': False, 'info': f''}


if __name__ == "__main__":
    print(is_holiday())