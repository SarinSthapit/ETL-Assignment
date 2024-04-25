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

cursor.execute("CREATE DATABASE SARINSTHAPIT_BHATBATENI CLONE BHATBHATENI;")
cursor.execute("USE SARINSTHAPIT_BHATBATENI")

copy_query = f"""CREATE TABLE DWH_F_BHATBHATENI_SLS_TRXN_B (month DATE, total_sales_amount NUMBER(20, 2));"""
cursor.execute(copy_query);

insert_query = f"""INSERT INTO DWH_F_BHATBHATENI_SLS_TRXN_B (month, total_sales_amount)
                    SELECT TRUNC(transaction_time, 'MONTH'), SUM(amount)
                    FROM SALES
                    GROUP BY TRUNC(transaction_time, 'MONTH');"""
cursor.execute(insert_query);

cursor.close()

conn.close()

