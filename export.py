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

cursor.execute("SHOW TABLES")

table_names = [row[1] for row in cursor.fetchall()]

folder_path = 'csv_files/'

if not os.path.exists(folder_path):
    os.makedirs(folder_path)

for table_name in table_names:
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    csv_file_path = os.path.join(folder_path, f"{table_name.lower()}_data.csv")

    with open(csv_file_path, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow([desc[0] for desc in cursor.description])
        csv_writer.writerows(rows)

    print(f"CSV file downloaded for table {table_name} to {csv_file_path}")

cursor.close()
conn.close()
