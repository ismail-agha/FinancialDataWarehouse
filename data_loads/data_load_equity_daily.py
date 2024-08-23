import os
import sys

# Add parent directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

import pandas as pd
import requests, json
from datetime import datetime
import concurrent.futures

from configs.config_urls import upstox_eq_full_market_quote, upstox_headers_market_quote
from db.database_and_models import engine, TABLE_MODEL_EQUITY_HISTORICAL_DATA, session, SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import text
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

# Create a timestamp string
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
date_yyyy_mm_dd = datetime.now().strftime("%Y-%m-%d")

def get_token():
    try:
        with open(os.path.join(parent_dir, f"token/token.txt"), 'r') as file:
            token = file.read()
        custom_logging(logger, 'INFO', f'Completed get_token().')
        return token
    except Exception as e:
        custom_logging(logger, 'ERROR', f'Error in get_token(). Error = {e}.')
        raise



def read_sql_file(file_path):
    try:
        with open(file_path, 'r') as file:
            sql_query = file.read()
        custom_logging(logger, 'INFO', f'Completed read_sql_file({file_path}).')
        return sql_query
    except Exception as e:
        custom_logging(logger, 'ERROR', f'Error in read_sql_file({file_path}). Error = {e}.')
        raise


def generate_isin_str(sql_file):
    try:
        file_path = os.path.join(parent_dir, f"sql/{sql_file}") #'../sql/' + sql_file
        result_df = pd.read_sql_query(read_sql_file(file_path), engine)
        custom_logging(logger, 'INFO', f'Completed generate_isin_str({sql_file}).')
        return result_df
    except Exception as e:
        custom_logging(logger, 'ERROR', f'Error in generate_isin_str({sql_file})). Error = {e}.')
        raise


def api_get_data(upstox_token, isin_df):
    urls = []
    upstox_headers_market_quote['Authorization'] = upstox_headers_market_quote['Authorization'].format(upstox_token)
    for index, row in isin_df.iterrows():
        urls.append(upstox_eq_full_market_quote.format(row["isin_numbers"]))

    # print(urls)
    df = pd.DataFrame()

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(urls)) as executor:
        # Submit the API calls as concurrent tasks
        future_to_url = {executor.submit(make_api_call, url, upstox_headers_market_quote): url for url in urls}

        # Iterate over the completed tasks
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                # Get the result of the completed task
                data = future.result()

                if data['status'] == 'success':
                    for i in data['data']:
                        # print(i)
                        # print(parsed_json['data'][i]['instrument_token'])
                        try:
                            trade_date = datetime.fromtimestamp(int(data['data'][i]['last_trade_time']) / 1000).date()
                            formatted_trade_date = trade_date.strftime('%Y-%m-%d')

                            new_row = {'exchange': data['data'][i]['instrument_token'][:3],
                                       'isin_number': data['data'][i]['instrument_token'][7:],

                                       'open': data['data'][i]['ohlc']['open'],
                                       'high': data['data'][i]['ohlc']['high'],
                                       'low': data['data'][i]['ohlc']['low'],
                                       'close': data['data'][i]['ohlc']['close'],

                                       'average_price': data['data'][i]['average_price'],
                                       'volume': data['data'][i]['volume'],
                                       'net_change': data['data'][i]['net_change'],
                                       'total_buy_quantity': data['data'][i]['total_buy_quantity'],
                                       'total_sell_quantity': data['data'][i]['total_sell_quantity'],
                                       'lower_circuit_limit': data['data'][i]['lower_circuit_limit'],
                                       'upper_circuit_limit': data['data'][i]['upper_circuit_limit'],
                                       'trade_date': formatted_trade_date,
                                       }

                            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                        except Exception as e:
                            print(f"Error API call (api_get_data) failed for data = {data['data'][i]}")
                            custom_logging(logger, 'ERROR', f'API call (api_get_data) failed for data = {e}.')


                    #print(df.to_string())

                    insert_data(df)
                    #custom_logging(logger, 'INFO', f'Completed api_get_data().')

            except Exception as exc:
                print(f'API call (api_get_data) to {url} failed: {exc}')
                custom_logging(logger, 'ERROR', f'Error in api_get_data(). Error = {e}.')
                raise

    db_final_activities()

    session.close()

def make_api_call(url, headers):
    #print(f'URL = {url} | headers = {headers}')
    try:
        #logger.info(f'URL = {url} \n')
        custom_logging(logger, 'INFO', f'\nRunning make_api_call() for URL = {url}.')
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            custom_logging(logger, 'ERROR',
                           f"Error in make_api_call(). Error = {response.status_code} - {response.reason} - {json.loads(response.text)['errors'][0]['message']}")

    except Exception as exc:
        print(f'API call (make_api_call) to {url} failed: {exc}')
        custom_logging(logger, 'ERROR', f'Error in make_api_call(). URL = {url}. Error = {e}.')
        raise


def insert_data(data):
    data['audit_creation_date'] = pd.Timestamp.today()

    records = data.to_dict(orient='records')

    #print(f'insertdata = {records}')

    # Create the insert statement
    insert_stmt = pg_insert(TABLE_MODEL_EQUITY_HISTORICAL_DATA).values(records)
    insert_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['exchange', 'isin_number', 'trade_date'])

    try:
        session.execute(insert_stmt)
        session.commit()
    except SQLAlchemyError as e:
        error = str(e)
        print("Error inserting data into the database:", e)
        custom_logging(logger, 'ERROR', f'Error in (1) insert_data(). Error = {e}.')
        raise

    except Exception as e:
        print("Error inserting data into the database:", e)
        custom_logging(logger, 'ERROR', f'Error in (2) insert_data(). Error = {e}.')
        raise

    #finally:
    #    custom_logging(logger, 'INFO', f'Completed insert_data().')
        #session.close()

def db_final_activities():
    # 1. Updating the MCap
    try:
        update_query = text("""
            UPDATE sm.equity_historical_data ehd
            SET mcap = el.market_capitalisation_in_crore
            FROM sm.equity_list el
            WHERE ehd.isin_number = el.isin_number
            AND ehd.trade_date BETWEEN :start_date AND :end_date;
        """)
        session.execute(update_query, {
            'start_date': f'{date_yyyy_mm_dd} 00:00:00',
            'end_date': f'{date_yyyy_mm_dd} 23:59:59'
        })
        session.commit()
    except Exception as e:
        print("Error updating data into the database:", e)
        custom_logging(logger, 'ERROR', f'Error in db_final_activities(mcap). Error = {e}.')
        raise
    else:
        custom_logging(logger, 'INFO', f'Completed db_final_activities(mcap).\n')

    #------------------------------------------------------------------------------------------
    # 2. Identify Missing ISIN
    try:
        sql_missing_isin_file_path = os.path.join(parent_dir, f"sql/sql_identify_missing_isin_for_current_trade_date.sql")  # '../sql/' + sql_file
        result_df = pd.read_sql_query(read_sql_file(sql_missing_isin_file_path), engine)
        # Log the results
        result_str = "Missing Stocks for today's run:\n"
        for _, row in result_df.iterrows():
            result_str += f"Exchange: {row['exchange']}, Trade Date: {row['trade_date']}, Count: {row['count']}, ISIN Numbers: {row['isin_numbers']} .\n\n"

        custom_logging(logger, 'INFO', result_str)

    except Exception as e:
        custom_logging(logger, 'ERROR', f"Error in db_final_activities(Missing ISIN). Error = {e}.")

def main():
    isin_df = generate_isin_str('sql_generate_isin_str.sql')
    upstox_token = get_token()
    api_get_data(upstox_token, isin_df)

if __name__ == "__main__":
    try:
        custom_logging(logger, 'INFO', f'Start time: {pd.Timestamp.today()}')
        main()
    except Exception as e:
        custom_logging(logger, 'ERROR', f'Error in __main__  {e} End time: {pd.Timestamp.today()}')
        exit(1)
    else:
        custom_logging(logger, 'INFO', f'End time: {pd.Timestamp.today()}')
