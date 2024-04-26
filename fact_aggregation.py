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
cursor.execute('DROP DATABASE IF EXISTS SARINSTHAPIT_BHATBATENI_DWH')
cursor.execute("CREATE DATABASE SARINSTHAPIT_BHATBATENI_DWH CLONE BHATBHATENI_DWH;")
cursor.execute("USE SARINSTHAPIT_BHATBATENI_DWH")

copy_query = f"""CREATE TABLE SARINSTHAPIT_DWH_F_BHATBHATENI_AGG_SLS_PLC_MONTH_T (
    store_key NUMBER,
    product_key NUMBER,
    year_month VARCHAR(7),
    total_quantity NUMBER,
    total_amount NUMBER(20,2),
    active_flag BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (store_key, product_key, year_month),
    FOREIGN KEY (store_key) REFERENCES TGT.DWH_D_STORE_LU (store_key),
    FOREIGN KEY (product_key) REFERENCES TGT.DWH_D_PRODUCT_LU (product_key));"""
cursor.execute(copy_query);

insert_query = """
INSERT INTO SARINSTHAPIT_DWH_F_BHATBHATENI_AGG_SLS_PLC_MONTH_T (
    store_key,
    product_key,
    year_month,
    total_quantity,
    total_amount
)
SELECT 
    store_key,
    product_key,
    TO_CHAR(DATE_TRUNC('MONTH', transaction_time), 'YYYY-MM') AS year_month, 
    SUM(quantity) AS total_quantity, 
    SUM(amount) AS total_amount
FROM TMP.TMP_D_SALES_LU
GROUP BY TO_CHAR(DATE_TRUNC('MONTH', transaction_time), 'YYYY-MM'), store_key, product_key;
"""

cursor.execute(insert_query);

cursor.close()

conn.close()

