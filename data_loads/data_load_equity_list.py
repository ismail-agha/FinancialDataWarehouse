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

# Create a timestamp string
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Configure the logging settings
log_filename = f"../logs/data_load_equity_daily_{timestamp}.log"

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=log_filename,
    filemode='w'
)

# Create a logger object
logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

def bse():
    try:
        # Execute the curl command and capture the output
        output  = subprocess.check_output(curl_cmd_bse_equity)

        # Convert the JSON string to a Python dictionary
        data = json.loads(output)

        for item in data:
            del item["NSURL"]  # drop "NSURL" column

        column_mapping = {
            "SCRIP_CD": "security_code",
            "Scrip_Name": "security_name",
            "Status": "status",
            "GROUP": "security_group",
            "FACE_VALUE": "face_value",
            "ISIN_NUMBER": "isin_number",
            "INDUSTRY": "industry",
            "scrip_id": "security_id",
            "Segment": "segment",
            "Issuer_Name": "issuer_name",
            "Mktcap": "market_capitalisation_in_crore"
        }

        # Create a DataFrame from the JSON data with renamed columns
        df = pd.DataFrame(data, columns=column_mapping.keys())
        df.rename(columns=column_mapping, inplace=True)

        df['face_value'] = pd.to_numeric(df['face_value'], errors='coerce').fillna(0).astype('int')
        df['market_capitalisation_in_crore'] = pd.to_numeric(df['market_capitalisation_in_crore'], errors='coerce').fillna(0).astype('float')

        #print(df)
        logger.info(f'Completed bse() - BSE Equity Dataframe created.')

        #print(f'BSE DF Created: {df.head()}')

        return df

    except subprocess.CalledProcessError as e:
        print(f"Error executing curl command for BSE: {e}")
        logger.error(f'Failed bse() - Error : {e}')

def nse():
    # Define column names
    columns = ["security_code", "issuer_name", "face_value", "isin_number", "status"]

    # Create an empty DataFrame with the specified columns
    empty_df = pd.DataFrame(columns=columns)

    logger.info(f'Completed - NSE Empty Equity Dataframe created.')

    return empty_df

def nse_xyz():
    # Send GET request to download the CSV file
    response = requests.get(**nse_equity_request_params)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Read the response content (CSV data) into a DataFrame
        df = pd.read_csv(StringIO(response.text))

        # Now you can work with the 'df' DataFrame as needed
        #print(df.head())  # Print the first few rows of the DataFrame

        df = df.loc[:, [' ISIN NUMBER', 'SYMBOL', 'NAME OF COMPANY', ' FACE VALUE']]
        df[' FACE VALUE'] = pd.to_numeric(df[' FACE VALUE'], errors='coerce').fillna(0).astype('int')

        column_mapping = {
            "SYMBOL": "security_code",
            "NAME OF COMPANY": "issuer_name",
            " FACE VALUE": "face_value",
            " ISIN NUMBER": "isin_number"
        }
        df['status'] = 'Active'

        df.rename(columns=column_mapping, inplace=True)

        return df
    else:
        print("Failed to download the file.")


def merge_bse_nse(df_bse, df_nse):
    # Perform full outer join
    df_final = pd.merge(df_bse, df_nse, on='isin_number', how='outer', suffixes=('_bse', '_nse'))

    # Set flags based on presence in each dataframe
    df_final['bse'] = df_final['issuer_name_bse'].notnull()
    df_final['nse'] = df_final['issuer_name_nse'].notnull()

    # Fill NaN values with appropriate values
    df_final.fillna({'issuer_name_bse': df_final['issuer_name_nse'], 'issuer_name_nse': df_final['issuer_name_bse']}, inplace=True)
    df_final.fillna({'face_value_bse': df_final['face_value_nse'], 'face_value_nse': df_final['face_value_bse']}, inplace=True)
    df_final.fillna({'status_bse': df_final['status_nse'], 'status_nse': df_final['status_bse']}, inplace=True)
    df_final['market_capitalisation_in_crore'].fillna(0, inplace=True)

    #print(df_final.head(50).to_string())

    column_mapping = {
        "issuer_name_bse": "issuer_name",
        "face_value_bse": "face_value",
        "status_bse": "status",
    }

    df_final.rename(columns=column_mapping, inplace=True)

    df_final['audit_create_date'] = pd.Timestamp.today()

    df_final = df_final[['bse', 'security_code_bse', 'nse', 'security_code_nse', 'issuer_name', 'security_id', 'security_name', 'status', 'security_group', 'face_value', 'isin_number', 'industry', 'market_capitalisation_in_crore', 'audit_create_date']]

    #print(df_final.head(100).to_string())

    logger.info(f'Completed merge_bse_nse() - BSE NSE DF Merging.')

    return df_final

def db_insert(df_final):

    data = df_final[df_final['isin_number'] == 'INE117A01022'].to_dict(orient='records')

    #Insert data into the database
    try:
        session.query(TABLE_MODEL_EQUITY_LIST).delete()
        data = df_final.to_dict(orient='records')
        session.bulk_insert_mappings(TABLE_MODEL_EQUITY_LIST, data)
        session.commit()
        print("Data inserted successfully.")
        logger.info(f'Completed db_insert() - Data inserted.')
    except Exception as e:
        session.rollback()
        print("Error inserting data into the database:", e)
        logger.error(f'Failed db_insert() - Data insertion failed. Error {e}.')


    # Create Partitions for Table - sm.equity_market_historical_data
    try:
        # Execute the stored procedure to create partitions
        session.execute(text("CALL sm.proc_create_partitions_ehd();"))

        # Commit the transaction
        session.commit()
        print("Partitions created for sm.equity_market_historical_data.")
        logger.info(f'Completed db_insert() - Partitions Created.')

    except Exception as e:
        print("Error:", e)
        logger.error(f'Completed db_insert() - Partitions Creation Failed. Error = {e}.')
        session.rollback()

    session.close()

def main():
    # Create a ThreadPoolExecutor with maximum workers as 2
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        # Submit bse() function to the executor
        future_bse = executor.submit(bse)

        # Submit nse() function to the executor
        future_nse = executor.submit(nse)

        # Retrieve the results when they are available
        df_bse = future_bse.result()
        df_nse = future_nse.result()

    df_final = merge_bse_nse(df_bse, df_nse)

    #print(df_final.to_string())

    db_insert(df_final)

if __name__ == "__main__":
    print(f'Start time: {pd.Timestamp.today()}')
    main()
    print(f'End time: {pd.Timestamp.today()}')
