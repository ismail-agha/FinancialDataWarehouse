import pandas as pd
import requests
import io

from configs.config_urls import endpoint_equity_list_csv, headers
from db.database_and_models import engine, TABLE_MODEL_EQUITY_LIST, Table, session

# Make a GET request to the API
response = requests.get(endpoint_equity_list_csv, headers=headers)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # API-CSV
    data = response.content.decode('utf8')

    df = pd.read_csv(io.StringIO(data), index_col=False)  # , usecols = ['IQ','Scores']

    df.columns = ['security_code', 'issuer_name', 'security_id', 'security_name', 'status', 'security_group',
                  'face_value', 'isin_no', 'industry', 'instrument', 'sector_name', 'industry_new_name', 'igroup_name',
                  'isubgroup_name']

    df = df.drop(['instrument', 'isubgroup_name'], axis=1)

    df['face_value'] = pd.to_numeric(df['face_value'], errors='coerce').fillna(0).astype('int')

    df['audit_create_date'] = pd.Timestamp.today()

    data = df.to_dict(orient='records')

    session.query(TABLE_MODEL_EQUITY_LIST).delete()
    session.bulk_insert_mappings(TABLE_MODEL_EQUITY_LIST, data)
    session.commit()

    # obj = Table(TABLE_MODEL_EQUITY_LIST)
    # stmt = obj.insert().values(data)
    #
    # # Execute the insert statement
    # with engine.begin() as conn:
    #     conn.execute(obj.delete())
    #     conn.execute(stmt)

else:
    # Handle the error if the request was not successful
    print('Error:', response.status_code)
