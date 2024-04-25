import snowflake.connector; # type: ignore
import csv
import os
from dotenv import load_dotenv # type: ignore

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

cursor.execute("USE BHATBHATENI_DWH")

stage_name = 'FILE_STAGE'
table_name = 'STG.STG_D_REGION_LU'

file_pattern = 'region_data.csv/region_data.csv.gz'
skip_rows = 1

copy_into_query = f"COPY INTO {table_name} FROM @{stage_name}/{file_pattern} FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = {skip_rows} COMPRESSION = 'gzip')"

try:
    cursor.execute(copy_into_query)
    print("Data copied from file stage to table successfully.")
except snowflake.connector.errors.ProgrammingError as e:
    print(f"Error: {e}")

print(f"Data loaded into {table_name} staging table from stage {stage_name}")

cursor.close()
conn.close()