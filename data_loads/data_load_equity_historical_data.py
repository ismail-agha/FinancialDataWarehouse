import pandas as pd
import requests
import time
from datetime import datetime
import logging
import concurrent.futures

import os
import sys
# Add parent directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from configs.config_urls import upstox_historical

from db.database_and_models import engine, TABLE_MODEL_EQUITY_HISTORICAL_DATA, session, SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert as pg_insert

#Logs
# Create a timestamp string
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# # Configure the logging settings
# log_filename = f"../logs/data_load_equity_historical_data_{timestamp}.log"
#
# logging.basicConfig(
#     level=logging.DEBUG,
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     filename=log_filename,
#     filemode='w'
# )
#
# # Create a logger object
# logger = logging.getLogger(__name__)
#
# log_msg_succ = ''
# log_msg_error = ''

def main():
    sql_get_isin = "select isin_number," \
                   " case when security_name='NaN' then issuer_name else security_name end as security_name, \
                        case \
                        when bse=true and nse=true then 'both'\
                        when bse=true and nse=false then 'BSE'\
                        when bse=false and nse=true then 'NSE'\
                        end as exg\
                        from sm.equity_list\
                        where status='Active' " \
                   "--and isin_number in ('INE587G01015', 'INF200K01VT2', 'INE00CE01017') " \
                   ";"

    result_df = pd.read_sql_query(sql_get_isin, engine)
    #print(result_df)

    interval = 'day'
    from_date = '2000-01-01'
    to_date = datetime.now().strftime("%Y-%m-%d")

    for index, row in result_df.iterrows():
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
            data = make_api_call(upstox_historical.format('BSE', row['isin_number'], interval, to_date, from_date))
            insert_data(data['data']['candles'], row['isin_number'], row['security_name'], row['exg'])
            pass
        elif row['exg'] == 'NSE': #INE00CE01017
            data = make_api_call(upstox_historical.format('NSE', row['isin_number'], interval, to_date, from_date))
            insert_data(data['data']['candles'], row['isin_number'], row['security_name'], row['exg'])
            pass

        time.sleep(2)


def make_api_call(url):
    headers = {'Accept': 'application/json'}
    response = requests.get(url, headers=headers)
    return response.json()

def insert_data(data, isin_number, security_name, exchange):
    print(data)
    df = pd.DataFrame(data)
    df.columns = ['trade_date', 'open', 'high', 'low', 'close', 'volume', 'open_interest']
    df['exchange'] = exchange
    df['isin_number'] = isin_number
    df['security_name'] = security_name
    df['audit_creation_date'] = pd.Timestamp.today()
    #print('ABCD = ' + df.to_string())

    records = df.to_dict(orient='records')

    # Create the insert statement
    insert_stmt = pg_insert(TABLE_MODEL_EQUITY_HISTORICAL_DATA).values(records)
    insert_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['exchange', 'isin_number', 'trade_date'])

    try:
        session.execute(insert_stmt)
        session.commit()
    except SQLAlchemyError as e:
        print(f'Except: SecurityCode={isin_number} Errored-Out: {e}\n\n')
        error = str(e)

    except Exception as e:
        print("Error inserting data into the database:", e)

    finally:
        print(f'Finally: SecurityCode={isin_number} Loaded Successfuly.\n\n')
        session.close()


if __name__ == "__main__":
    print(f'Start time: {pd.Timestamp.today()}')
    main()
    print(f'End time: {pd.Timestamp.today()}')
