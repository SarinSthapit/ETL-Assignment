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

cursor.execute("CREATE OR REPLACE TABLE STG.STG_D_COUNTRY_LU (id NUMBER, country_desc VARCHAR(256), PRIMARY KEY (id));")
print("Table created successfully: STG_D_COUNTRY_LU")

cursor.execute("CREATE OR REPLACE TABLE STG.STG_D_REGION_LU (id NUMBER, country_id NUMBER, region_desc VARCHAR(256), PRIMARY KEY (id), FOREIGN KEY (country_id) references STG.STG_D_COUNTRY_LU(id));")
print("Table created successfully: STG_D_REGION_LU")

cursor.execute("CREATE OR REPLACE TABLE STG.STG_D_STORE_LU (id NUMBER, region_id NUMBER, store_desc VARCHAR(256), PRIMARY KEY (id), FOREIGN KEY (region_id) references STG.STG_D_REGION_LU(id));")
print("Table created successfully: STG_D_STORE_LU")

cursor.execute("CREATE OR REPLACE TABLE STG.STG_D_CATEGORY_LU (id NUMBER, category_desc VARCHAR(1024), PRIMARY KEY (id));")
print("Table created successfully: STG_D_CATEGORY_LU")

cursor.execute("CREATE OR REPLACE TABLE STG.STG_D_SUBCATEGORY_LU (id NUMBER, category_id NUMBER, subcategory_desc VARCHAR(256), PRIMARY KEY (id), FOREIGN KEY (category_id) references STG.STG_D_CATEGORY_LU(id));")
print("Table created successfully: STG_D_SUBCATEGORY_LU")

cursor.execute("CREATE OR REPLACE TABLE STG.STG_D_PRODUCT_LU (id NUMBER, subcategory_id NUMBER, product_desc VARCHAR(256), PRIMARY KEY (id), FOREIGN KEY (subcategory_id) references STG.STG_D_SUBCATEGORY_LU(id));")
print("Table created successfully: STG_D_PRODUCT_LU")

cursor.execute("CREATE OR REPLACE TABLE STG.STG_D_CUSTOMER_LU (id NUMBER, customer_first_name VARCHAR(256), customer_middle_name VARCHAR(256), customer_last_name VARCHAR(256), customer_address VARCHAR(256), primary key (id));")
print("Table created successfully: STG_D_CUSTOMER_LU")

cursor.execute("CREATE OR REPLACE TABLE STG.STG_D_SALES_LU (id NUMBER, store_id NUMBER NOT NULL, product_id NUMBER NOT NULL, customer_id NUMBER, transaction_time TIMESTAMP, quantity NUMBER, amount NUMBER(20,2), discount NUMBER(20,2), primary key (id), FOREIGN KEY (store_id) references STG.STG_D_STORE_LU(id), FOREIGN KEY (product_id) references STG.STG_D_PRODUCT_LU(id), FOREIGN KEY (customer_id) references STG.STG_D_CUSTOMER_LU(id));")
print("Table created successfully: STG_D_SALES_LU")

cursor.execute("CREATE OR REPLACE TABLE STG.STG_D_LOCATION_HIERARCHY_LU (id NUMBER AUTOINCREMENT PRIMARY KEY, sales_id NUMBER, store_id NUMBER, region_id NUMBER, country_id NUMBER, FOREIGN KEY (store_id) REFERENCES STG.STG_D_STORE_LU(id), FOREIGN KEY (region_id) REFERENCES STG.STG_D_REGION_LU(id), FOREIGN KEY (country_id) REFERENCES STG.STG_D_COUNTRY_LU(id));")
print("Table created successfully: STG_D_LOCATION_HIERARCHY_LU")

cursor.close()
conn.close()