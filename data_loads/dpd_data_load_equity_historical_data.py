import os
import sys

# Add parent directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

import pandas as pd
import requests
import time
from datetime import datetime
import logging
import concurrent.futures

from configs.config_urls import upstox_historical

from db.database_and_models import engine, TABLE_MODEL_EQUITY_HISTORICAL_DATA_DPD, session, SQLAlchemyError, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

#Logs
# Create a timestamp string
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Configure the logging settings
log_filename = f"../logs/dpd_data_load_equity_historical_data_{timestamp}.log"

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=log_filename,
    filemode='w'
)

# Create a logger object
logger = logging.getLogger(__name__)

log_msg_succ = ''
log_msg_error = ''

def main():

    result_df = pd.read_sql_query(read_sql_file('../sql/dpd_sql_identify_equity_for_historical_load.sql'), engine)
    #print(result_df)

    interval = 'day'
    from_date = '2000-01-01'
    to_date = datetime.now().strftime("%Y-%m-%d")

    total_rows = len(result_df)
    logger.info(f'Main - Total Rows (ISIN_NUMBERS) = {total_rows}.\n')
    logger.info(f'Main - Interval={interval} | From_Date={from_date} | To_Date={to_date}.\n')

    for index, row in result_df.iterrows():
        logger.info(f'Completion Stats = {index+1}/{total_rows}.\n')
        print(f'\nCompletion Stats = {index+1}/{total_rows}.')

        if row['exg'] == 'both': #INE587G01015
            urls = [upstox_historical.format('BSE', row['isin_number'], interval, to_date, from_date),
                    upstox_historical.format('NSE', row['isin_number'], interval, to_date, from_date)]

            # Create a ThreadPoolExecutor with a max_workers parameter
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(urls)) as executor:
                # Submit the API calls as concurrent tasks
                future_to_url = {executor.submit(make_api_call, url): url for url in urls}

                # Iterate over the completed tasks
                for future in concurrent.futures.as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        # Get the result of the completed task
                        data = future.result()
                        data = data['data']['candles']
                        insert_data(data, row['isin_number'], row['security_name'], 'BSE' if "BSE" in url else 'NSE')

                    except Exception as exc:
                        print(f'API call to {url} failed: {exc}')

        elif row['exg'] == 'BSE': #INF200K01VT2
            try:
                data = make_api_call(upstox_historical.format('BSE', row['isin_number'], interval, to_date, from_date))
                insert_data(data['data']['candles'], row['isin_number'], row['security_name'], row['exg'])
            except Exception as exc:
                print(f'ERROR: BSE({row["isin_number"]}) make_api_call() insert_data(): {exc}')
                logger.error(f'ERROR: BSE({row["isin_number"]}) make_api_call() insert_data(): {exc}')

        elif row['exg'] == 'NSE': #INE00CE01017
            try:
                data = make_api_call(upstox_historical.format('NSE', row['isin_number'], interval, to_date, from_date))
                insert_data(data['data']['candles'], row['isin_number'], row['security_name'], row['exg'])
            except Exception as exc:
                print(f'ERROR: NSE({row["isin_number"]}) make_api_call() insert_data(): {exc}')
                logger.error(f'ERROR: NSE({row["isin_number"]}) make_api_call() insert_data(): {exc}')

        time.sleep(2)

def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        sql_query = file.read()
    return sql_query

def make_api_call(url):
    try:
        headers = {'Accept': 'application/json'}
        response = requests.get(url, headers=headers)
        return response.json()
    except requests.exceptions.RequestException as e:
        # Handle any network-related errors
        print(f"Network error occurred: {e}")
        logger.error(f'make_api_call() - Network error occurred: {e}')
        return None
    except ValueError as e:
        # Handle JSON decoding error
        print(f"Error decoding JSON response: {e}")
        logger.error(f'make_api_call() - Error decoding JSON response: {e}')
        return None
    except Exception as e:
        print(f'make_api_call() - Exception : {e}')
        logger.error(f'make_api_call() - Exception : {e}')


def insert_data(data, isin_number, security_name, exchange):
    #print(data)
    df = pd.DataFrame(data)
    df.columns = ['trade_date', 'open', 'high', 'low', 'close', 'volume', 'open_interest']
    df['exchange'] = exchange
    df['isin_number'] = isin_number
    df['security_name'] = security_name
    df['audit_creation_date'] = pd.Timestamp.today()
    #print('ABCD = ' + df.to_string())

    records = df.to_dict(orient='records')

    # Create the insert statement
    insert_stmt = pg_insert(TABLE_MODEL_EQUITY_HISTORICAL_DATA_DPD).values(records)
    insert_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['exchange', 'isin_number', 'trade_date'])

    try:
        session.execute(insert_stmt)
        session.commit()
    except SQLAlchemyError as e:
        print(f'insert_data() - 1. Except: SecurityCode={isin_number} Errored-Out: {e}\n\n')
        logger.error(f'insert_data() - Except(1): SecurityCode={isin_number} Errored-Out: {e}\n')
        error = str(e)

    except Exception as e:
        print(f"insert_data() - 2. Except: SecurityCode={isin_number} Errored-Out:", e)
        logger.error(f'insert_data() - Except(2): SecurityCode={isin_number} Errored-Out: {e}')

    finally:
        print(f'Finally Completed: SecurityCode={isin_number} Loaded Successfuly.\n\n')
        logger.info(f'insert_data() - Finally Completed: SecurityCode={isin_number} Loaded Successfuly.\n')
        session.close()


if __name__ == "__main__":
    print(f'Start time: {pd.Timestamp.today()}')
    logger.info(f'Start time: {pd.Timestamp.today()}\n')

    main()

    print(f'End time: {pd.Timestamp.today()}')
    logger.info(f'End time: {pd.Timestamp.today()}\n')
