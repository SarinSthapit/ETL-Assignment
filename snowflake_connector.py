import snowflake.connector
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
cursor.execute("SELECT * FROM SALES")

rows = cursor.fetchall()
for row in rows:
    print(row)

cursor.close()

conn.close()

