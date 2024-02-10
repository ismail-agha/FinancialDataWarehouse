import pandas as pd
import requests
import time
from datetime import datetime
import logging
import concurrent.futures
import json

from configs.config_urls import upstox_eq_full_market_quote, upstox_headers_market_quote
from db.database_and_models import engine, TABLE_MODEL_EQUITY_HISTORICAL_DATA, session, SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert as pg_insert

# Logs
# Create a timestamp string
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")


def main():
    isin_df = generate_isin_str('sql_generate_isin_str.sql')
    upstox_token = get_token()
    api_get_data(upstox_token, isin_df)


def get_token():
    with open('../token/token.txt', 'r') as file:
        sql_query = file.read()
    return sql_query


def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        sql_query = file.read()
    return sql_query


def generate_isin_str(sql_file):
    file_path = '../sql/' + sql_file
    result_df = pd.read_sql_query(read_sql_file(file_path), engine)
    return result_df


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
                                   'trade_date': datetime.fromtimestamp(
                                       int(data['data'][i]['last_trade_time']) / 1000),
                                   }

                        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

                    print(df.to_string())

                    insert_data(df)

            except Exception as exc:
                print(f'API call to {url} failed: {exc}')


def make_api_call(url, headers):
    print(f'URL = {url} | headers = {headers}')
    response = requests.get(url, headers=headers)
    return response.json()


def insert_data(data):
    data['audit_creation_date'] = pd.Timestamp.today()

    records = data.to_dict(orient='records')

    print(f'insertdata = {records}')

    # Create the insert statement
    insert_stmt = pg_insert(TABLE_MODEL_EQUITY_HISTORICAL_DATA).values(records)
    insert_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['exchange', 'isin_number', 'trade_date'])

    try:
        session.execute(insert_stmt)
        session.commit()
    except SQLAlchemyError as e:
        error = str(e)
        print("Error inserting data into the database:", e)

    except Exception as e:
        print("Error inserting data into the database:", e)

    finally:
        session.close()


if __name__ == "__main__":
    print(f'Start time: {pd.Timestamp.today()}')
    main()
    print(f'End time: {pd.Timestamp.today()}')
