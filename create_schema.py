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

stg_schema_sql = "CREATE SCHEMA IF NOT EXISTS STG"
tmp_schema_sql = "CREATE SCHEMA IF NOT EXISTS TMP"
tgt_schema_sql = "CREATE SCHEMA IF NOT EXISTS TGT"

cursor.execute(stg_schema_sql)
cursor.execute(tmp_schema_sql)
cursor.execute(tgt_schema_sql)

conn.commit()

cursor.close()
conn.close()

print("Schemas created successfully: STG, TMP, TGT")
