import pandas as pd
from db.database_and_models import TABLE_MODEL_EQUITY_LIST, session
import json
from sqlalchemy import text
import concurrent.futures

def bse():
    file_path = '../files/BSE All-Securities.json'

    # Open the file and load the JSON data
    with open(file_path, 'r') as file:
        data = json.load(
            file)  # json.load() - to load JSON data from a file-like object (or any object with a read() method)

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

    return df

def nse():
    df = pd.read_csv('../files/NSE_EQUITY_L.csv')
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
    except Exception as e:
        session.rollback()
        print("Error inserting data into the database:", e)

    # Create Partitions for Table - sm.equity_market_historical_data
    try:
        # Execute the stored procedure to create partitions
        session.execute(text("CALL sm.proc_create_partitions_ehd();"))

        # Commit the transaction
        session.commit()
        print("Partitions created for sm.equity_market_historical_data.")

    except Exception as e:
        print("Error:", e)
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
    #print(df_final[df_final['isin_number'] == 'INE117A01022'].to_string())
    db_insert(df_final)

if __name__ == "__main__":
    print(f'Start time: {pd.Timestamp.today()}')
    main()
    print(f'End time: {pd.Timestamp.today()}')
