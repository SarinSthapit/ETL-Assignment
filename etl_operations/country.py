import snowflake.connector;
import csv
import os
from dotenv import load_dotenv

def truncate_tables(cursor, table_name):
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    print(f"Table {table_name} truncated")


def load_from_stage_to_table(cursor, stage_name, table_name):
    file_pattern = 'country_data.csv/country_data.csv.gz'
    skip_rows = 1

    copy_into_query = f"COPY INTO {table_name} FROM @{stage_name}/{file_pattern} FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = {skip_rows} COMPRESSION = 'gzip')"

    try:
        cursor.execute(copy_into_query)
        print("Data copied from file stage to table successfully.")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error: {e}")

    print(f"Data loaded into {table_name} staging table from stage {stage_name}")


def main():
    load_dotenv()

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    account = os.getenv('ACCOUNT')

    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account
    )
    cursor = conn.cursor()
    cursor.execute("USE DWH_BHATBHATENI")
    stage_name = 'DWH_FILE_STAGE'
    table_name = 'DWH_STG.STG_D_COUNTRY_LU'

    truncate_tables(cursor, table_name)
    load_from_stage_to_table(cursor, stage_name, table_name);

    cursor.close()
    conn.close()

main()