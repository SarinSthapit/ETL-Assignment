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

cursor.execute("CREATE OR REPLACE TABLE TGT.DWH_D_COUNTRY_LU (country_key NUMBER, id NUMBER, country_desc VARCHAR(256), active_flag BOOLEAN, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (country_key));")
print("Table created successfully: DWH_D_COUNTRY_LU")

cursor.execute("CREATE OR REPLACE TABLE TGT.DWH_D_REGION_LU (region_key NUMBER, id NUMBER, country_key NUMBER, region_desc VARCHAR(256), active_flag BOOLEAN, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (region_key), FOREIGN KEY (country_key) references TGT.DWH_D_COUNTRY_LU(country_key));")
print("Table created successfully: DWH_D_REGION_LU")

cursor.execute("CREATE OR REPLACE TABLE TGT.DWH_D_STORE_LU (store_key NUMBER, id NUMBER, region_key NUMBER, store_desc VARCHAR(256), active_flag BOOLEAN, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (store_key), FOREIGN KEY (region_key) references TGT.DWH_D_REGION_LU(region_key));")
print("Table created successfully: DWH_D_STORE_LU")

cursor.execute("CREATE OR REPLACE TABLE TGT.DWH_D_CATEGORY_LU (category_key NUMBER, id NUMBER, category_desc VARCHAR(1024), active_flag BOOLEAN, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (category_key));")
print("Table created successfully: DWH_D_CATEGORY_LU")

cursor.execute("CREATE OR REPLACE TABLE TGT.DWH_D_SUBCATEGORY_LU (subcategory_key NUMBER, id NUMBER, category_key NUMBER, subcategory_desc VARCHAR(256), active_flag BOOLEAN, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (subcategory_key), FOREIGN KEY (category_key) references TGT.DWH_D_CATEGORY_LU(category_key));")
print("Table created successfully: DWH_D_SUBCATEGORY_LU")

cursor.execute("CREATE OR REPLACE TABLE TGT.DWH_D_PRODUCT_LU (product_key NUMBER, id NUMBER, subcategory_key NUMBER, product_desc VARCHAR(256), active_flag BOOLEAN, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (product_key), FOREIGN KEY (subcategory_key) references TGT.DWH_D_SUBCATEGORY_LU(subcategory_key));")
print("Table created successfully: DWH_D_PRODUCT_LU")

cursor.execute("CREATE OR REPLACE TABLE TGT.DWH_D_CUSTOMER_LU (customer_key NUMBER, id NUMBER, customer_first_name VARCHAR(256), customer_middle_name VARCHAR(256), customer_last_name VARCHAR(256), customer_address VARCHAR(256), active_flag BOOLEAN, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (customer_key));")
print("Table created successfully: DWH_D_CUSTOMER_LU")

cursor.execute("CREATE OR REPLACE TABLE TGT.DWH_D_SALES_LU (sales_key NUMBER, id NUMBER, store_key NUMBER NOT NULL, product_key NUMBER NOT NULL, customer_key NUMBER, transaction_time TIMESTAMP, quantity NUMBER, amount NUMBER(20,2), discount NUMBER(20,2), active_flag BOOLEAN, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, primary key (sales_key), FOREIGN KEY (store_key) references TGT.DWH_D_STORE_LU(store_key), FOREIGN KEY (product_key) references TGT.DWH_D_PRODUCT_LU(product_key), FOREIGN KEY (customer_key) references TGT.DWH_D_CUSTOMER_LU(customer_key));")
print("Table created successfully: DWH_D_SALES_LU")

cursor.execute("CREATE OR REPLACE TABLE TGT.DWH_D_LOCATION_HIERARCHY_LU (location_key NUMBER PRIMARY KEY, id NUMBER, sales_key NUMBER, store_key NUMBER, region_key NUMBER, country_key NUMBER, active_flag BOOLEAN, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (store_key) REFERENCES TGT.DWH_D_STORE_LU(store_key), FOREIGN KEY (region_key) REFERENCES TGT.DWH_D_REGION_LU(region_key), FOREIGN KEY (country_key) REFERENCES TGT.DWH_D_COUNTRY_LU(country_key));")
print("Table created successfully: DWH_D_LOCATION_HIERARCHY_LU")

cursor.close()
conn.close()