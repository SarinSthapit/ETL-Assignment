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

cursor.execute("USE BHATBHATENI_DB")
cursor.execute("SELECT * FROM SALES")

rows = cursor.fetchall()
for row in rows:
    print(row)

cursor.close()

conn.close()

