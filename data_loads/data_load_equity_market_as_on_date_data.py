import pandas as pd
import requests
import time
import json
from sqlalchemy.exc import IntegrityError
from datetime import datetime
from configs.config_urls import headers, endpoint_equity_daily_json_part1, endpoint_equity_daily_json_part2, endpoint_equity_daily_json_part3, endpoint_equity_daily_json_part4
from db.database_and_models import engine, TABLE_MODEL_EQUITY_LIST, TABLE_MODEL_EQUITY_MARKET_HISTORICAL_DATA, TABLE_MODEL_EQUITY_MARKET_AS_ON_DATE_DATA, select, session

# Perform a SELECT query
query = select(TABLE_MODEL_EQUITY_LIST.security_code)\
    .where(TABLE_MODEL_EQUITY_LIST.status == 'Active').\
    filter(TABLE_MODEL_EQUITY_LIST.security_code.in_(['500002','500003','500008','500009']))

#with engine.connect() as connection:
#    result = connection.execute(query).fetchall()
#
# print(len(result))
#
# for row in result:
#     print(row[0])
#     security_code = row[0]


# Create a session object
request_session = requests.Session()

security_code = '500002'

for i in range(1,5):
    print(f'\nCounter = {i}')
    # Make a GET request to the API
    time.sleep(10)
    response_part1 = request_session.get(endpoint_equity_daily_json_part1.format(security_code), headers=headers)

    #response_part2 = request_session.get(endpoint_equity_daily_json_part2.format(security_code), headers=headers)

    #response_part3 = request_session.get(endpoint_equity_daily_json_part3.format(security_code), headers=headers)

    #response_part4 = request_session.get(endpoint_equity_daily_json_part4.format(security_code), headers=headers)

    print(f'response_part1.status_code={response_part1.status_code}')
    # print(f'response_part2.status_code={response_part2.status_code}')
    # print(f'response_part3.status_code={response_part3.status_code}')
    # print(f'response_part4.status_code={response_part4.status_code}')

    # Check if the request was successful (status code 200)
    #if response_part1.status_code == response_part2.status_code == response_part3.status_code == response_part4.status_code == 200:
    if response_part1.status_code == 200:
        # API-JSON
        print(f"Invalid json_data_part1: {response_part1.content.decode('utf-8', errors='ignore')}")
        try:
            json_data_part1 = response_part1.json()
            print(json_data_part1['Header']['PrevClose'])
            print(json_data_part1['Header']['Open'])
            print(json_data_part1['Header']['High'])
            print(json_data_part1['Header']['Low'])
            print(json_data_part1['Header']['LTP'])
            print(json_data_part1['Header']['Ason'])
        except json.JSONDecodeError as e:
            print(f"Invalid json_data_part1: {e}")
            print(f"Invalid json_data_part1: {response_part1.content.decode('utf-8', errors='ignore')}")

        # json_data_part2 = response_part2.json()
        # try:
        #     json_data_part2 = response_part2.json()
        #     print(json_data_part2['Fifty2WkHigh_adj'])
        #     print(json_data_part2['Fifty2WkHigh_adjDt'])
        #     print(json_data_part2['Fifty2WkLow_adj'])
        #     print(json_data_part2['Fifty2WkLow_adjDt'])
        #     print(json_data_part2['Fifty2WkHigh_unadj'])
        #     print(json_data_part2['Fifty2WkLow_unadj'])
        #     print(json_data_part2['MonthHighLow'])
        #     print(json_data_part2['WeekHighLow'])
        # except json.JSONDecodeError as e:
        #     print(f"Invalid json_data_part2: {e}")
        #
        # try:
        #     json_data_part3 = response_part3.json()
        #     print(json_data_part3['WAP'])
        #     print(json_data_part3['Turnoverin'])
        #     print(json_data_part3['Turnover'])
        #     print(json_data_part3['MktCapFull'])
        #     print(json_data_part3['MktCapFF'])
        # except json.JSONDecodeError as e:
        #     print(f"Invalid json_data_part3: {e}")
        #
        # try:
        #     json_data_part4 = response_part4.json()
        #     print(json_data_part4['Index'])
        #     print(json_data_part4['FaceVal'])
        #     print(json_data_part4['EPS'])
        #     print(json_data_part4['CEPS'])
        #     print(json_data_part4['PE'])
        #     print(json_data_part4['PB'])
        #     print(json_data_part4['ROE'])
        #     print(json_data_part4['SetlType'])
        # except json.JSONDecodeError as e:
        #     print(f"Invalid json_data_part4: {e}")

    data = TABLE_MODEL_EQUITY_MARKET_AS_ON_DATE_DATA(as_on_date='2023-5-29', security_code='500009') #Merge - Insert Or Update

    try:
        session.merge(data)
        session.commit()
    except IntegrityError:
        session.rollback()
    finally:
        session.close()

request_session.close()
