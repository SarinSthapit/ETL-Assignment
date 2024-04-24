import snowflake.connector;
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

cursor.execute("CREATE OR REPLACE TABLE DWH_TMP.TMP_D_COUNTRY_LU (id NUMBER, country_desc VARCHAR(256), PRIMARY KEY (id));")
print("Table created successfully: TMP_D_COUNTRY_LU")

cursor.execute("CREATE OR REPLACE TABLE DWH_TMP.TMP_D_REGION_LU (id NUMBER, country_key NUMBER, region_desc VARCHAR(256), PRIMARY KEY (id), FOREIGN KEY (country_key) references DWH_TMP.TMP_D_COUNTRY_LU(id));")
print("Table created successfully: TMP_D_REGION_LU")

cursor.execute("CREATE OR REPLACE TABLE DWH_TMP.TMP_D_STORE_LU (id NUMBER, region_key NUMBER, store_desc VARCHAR(256), PRIMARY KEY (id), FOREIGN KEY (region_key) references DWH_TMP.TMP_D_REGION_LU(id));")
print("Table created successfully: TMP_D_STORE_LU")

cursor.execute("CREATE OR REPLACE TABLE DWH_TMP.TMP_D_CATEGORY_LU (id NUMBER, category_desc VARCHAR(1024), PRIMARY KEY (id));")
print("Table created successfully: TMP_D_CATEGORY_LU")

cursor.execute("CREATE OR REPLACE TABLE DWH_TMP.TMP_D_SUBCATEGORY_LU (id NUMBER, category_key NUMBER, subcategory_desc VARCHAR(256), PRIMARY KEY (id), FOREIGN KEY (category_key) references DWH_TMP.TMP_D_CATEGORY_LU(id));")
print("Table created successfully: TMP_D_SUBCATEGORY_LU")

cursor.execute("CREATE OR REPLACE TABLE DWH_TMP.TMP_D_PRODUCT_LU (id NUMBER, subcategory_key NUMBER, product_desc VARCHAR(256), PRIMARY KEY (id), FOREIGN KEY (subcategory_key) references DWH_TMP.TMP_D_SUBCATEGORY_LU(id));")
print("Table created successfully: TMP_D_PRODUCT_LU")

cursor.execute("CREATE OR REPLACE TABLE DWH_TMP.TMP_D_CUSTOMER_LU (id NUMBER, customer_first_name VARCHAR(256), customer_middle_name VARCHAR(256), customer_last_name VARCHAR(256), customer_address VARCHAR(256), primary key (id));")
print("Table created successfully: TMP_D_CUSTOMER_LU")

cursor.execute("CREATE OR REPLACE TABLE DWH_TMP.TMP_D_SALES_LU (id NUMBER, store_key NUMBER NOT NULL, product_key NUMBER NOT NULL, customer_key NUMBER, transaction_time TIMESTAMP, quantity NUMBER, amount NUMBER(20,2), discount NUMBER(20,2), primary key (id), FOREIGN KEY (store_key) references DWH_TMP.TMP_D_STORE_LU(id), FOREIGN KEY (product_key) references DWH_TMP.TMP_D_PRODUCT_LU(id), FOREIGN KEY (customer_key) references DWH_TMP.TMP_D_CUSTOMER_LU(id));")
print("Table created successfully: TMP_D_SALES_LU")

cursor.execute("CREATE OR REPLACE TABLE DWH_TMP.TMP_D_LOCATION_HIERARCHY_LU (id NUMBER PRIMARY KEY, sales_key NUMBER, store_key NUMBER, region_key NUMBER, country_key NUMBER, FOREIGN KEY (store_key) REFERENCES DWH_TMP.TMP_D_STORE_LU(id), FOREIGN KEY (region_key) REFERENCES DWH_TMP.TMP_D_REGION_LU(id), FOREIGN KEY (country_key) REFERENCES DWH_TMP.TMP_D_COUNTRY_LU(id));")
print("Table created successfully: TMP_D_LOCATION_HIERARCHY_LU")

cursor.close()
conn.close()