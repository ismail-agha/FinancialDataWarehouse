import pandas as pd
import requests
import io
from datetime import datetime
import logging

from configs.config_urls import endpoint_equity_historical_csv, headers
from db.database_and_models import engine, TABLE_MODEL_EQUITY_LIST, TABLE_MODEL_EQUITY_MARKET_HISTORICAL_DATA, select, session, SQLAlchemyError


#Logs
# Create a timestamp string
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Configure the logging settings
log_filename = f"../logs/data_load_equity_market_historical_data_{timestamp}.log"

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

# Perform a SELECT query
query = select(TABLE_MODEL_EQUITY_LIST.security_code)\
    .where(TABLE_MODEL_EQUITY_LIST.status == 'Active').\
    filter(TABLE_MODEL_EQUITY_LIST.security_code.in_(['500002', '500020'])) #'500002','500003','500008','500009'

with engine.connect() as connection:
    result = connection.execute(query).fetchall()

print(len(result))

from_date = '01/01/2023'
to_date = datetime.now().strftime("%d/%m/%Y")

errored = []
for row in result:
    print(row[0])
    security_code = row[0]

    # Make a GET request to the API
    response = requests.get(endpoint_equity_historical_csv.format(security_code, from_date, to_date), headers=headers)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # API-CSV
        data = response.content.decode('utf8')

        df = pd.read_csv(io.StringIO(data), index_col=False)  # , usecols = ['IQ','Scores']

        print(df)

        df.columns = ['trade_date', 'open', 'high', 'low', 'close', 'wap', 'number_of_shares',
                      'number_of_trades', 'total_turnover', 'deliverable_quantity','percentage_deliverable_quantity_to_traded_quantity',
                      'spread_high_low','spread_close_open']

        df['security_code'] = security_code
        df['audit_create_date'] = pd.Timestamp.today()

        data = df.to_dict(orient='records')
        #print(len(data))
        try:
            session.bulk_insert_mappings(TABLE_MODEL_EQUITY_MARKET_HISTORICAL_DATA, data)
            session.commit()
            log_msg_succ += f'SecurityCode={security_code} Loaded Successfuly.\n\n'
        except SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            log_msg_error += f'SecurityCode={security_code} Errored-Out: {error}\n\n'
        finally:
            print('Done')


    else:
        # Handle the error if the request was not successful
        print('Error:', response.status_code)


logger.error('\n\n' + log_msg_error)
logger.info('\n\n' + log_msg_succ)
