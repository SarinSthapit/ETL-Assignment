import snowflake.connector
import csv
import os
from dotenv import load_dotenv

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

cursor.execute("SHOW TABLES")

table_names = [row[1] for row in cursor.fetchall()] 

folder_path = 'etl_operations'

if not os.path.exists(folder_path):
    os.makedirs(folder_path)

for table_name in table_names:
    file_path = os.path.join(folder_path, f"{table_name.lower()}.py")
    with open(file_path, 'w') as file:
        file.write("import snowflake.connector;")
    print(f"Empty Python file generated for table {table_name} at {file_path}")
