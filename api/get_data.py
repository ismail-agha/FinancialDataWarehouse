import os
import sys

# Add parent directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

import pandas as pd
import requests, json
from datetime import datetime
from db.database_and_models import engine, session

def read_sql_file(file_path):
    try:
        with open(file_path, 'r') as file:
            sql_query = file.read()
        return sql_query
    except Exception as e:
        print(f'Error in read_sql_file({file_path}). Error = {e}.')


def get_top_n_equity_gainers_losers(trade_date, exchange, type, n=5):
    '''
    :param trade_date: Trade Date
    :param exchange: "BSE" / "NSE"
    :param type: "G" / "L" , where G - Gainers, and L - Losers
    :return: DataFrame
    '''

    try:
        file_path = os.path.join(parent_dir, f"sql/sql_get_top_n_equity.sql")
        sql_query = read_sql_file(file_path)
        params = {'trade_date': trade_date, 'exchange': exchange}
        order_clause = 'DESC' if type == 'G' else 'ASC'
        # Insert ORDER BY clause before LIMIT
        sql_query_with_order = f"{sql_query.rstrip(';')} ORDER BY percentage_change {order_clause} LIMIT {n}"

        #print(f'sql_query_with_order = \n {sql_query_with_order}')

        result_df = pd.read_sql_query(sql_query_with_order, engine, params=params)
        return result_df
    except Exception as e:
        print(f'Error in get_top_five_equity(). Error = {e}.')


if __name__ == "__main__":
    # Example usage
    trade_date = '2024-01-29'
    exchange = 'BSE'
    type = 'G'
    n = 5

    result = get_top_n_equity_gainers_losers(trade_date=trade_date, exchange=exchange, type=type, n=n)
    if result is not None:
        print(result)

    result = get_top_n_equity_gainers_losers(trade_date=trade_date, exchange=exchange, type='L', n=n)
    if result is not None:
        print(result)