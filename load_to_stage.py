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
cursor.execute("CREATE STAGE IF NOT EXISTS ETL_FILE_STAGE")

folder_path = 'C:/Users/HP/Desktop/ETL-Assignment/csv_files'

file_names = os.listdir(folder_path)

for file_name in file_names:
    csv_file_path = os.path.join(folder_path, file_name)
    
    cursor.execute(f"PUT file://{csv_file_path} @ETL_FILE_STAGE/{file_name}")

conn.commit()

cursor.close()
conn.close()

print("All CSV files uploaded to Snowflake stage successfully.")