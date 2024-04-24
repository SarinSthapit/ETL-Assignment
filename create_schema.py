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

stg_schema_sql = "CREATE SCHEMA IF NOT EXISTS DWH_STG"
tmp_schema_sql = "CREATE SCHEMA IF NOT EXISTS DWH_TMP"
tgt_schema_sql = "CREATE SCHEMA IF NOT EXISTS DWH_TGT"

cursor.execute(stg_schema_sql)
cursor.execute(tmp_schema_sql)
cursor.execute(tgt_schema_sql)

conn.commit()

cursor.close()
conn.close()

print("Schemas created successfully: DWH_STG, DWH_TMP, DWH_TGT")
