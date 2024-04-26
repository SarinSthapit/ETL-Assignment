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
cursor.execute("CREATE DATABASE BHATBHATENI_DWH;")
cursor.execute("USE BHATBHATENI_DWH;")
cursor.execute("create or replace table COUNTRY (id NUMBER, country_desc VARCHAR(256), PRIMARY KEY (id));")
cursor.execute("create or replace table REGION (id NUMBER, country_id NUMBER, region_desc VARCHAR(256), PRIMARY KEY (id), FOREIGN KEY (country_id) references COUNTRY(id));")
cursor.execute("create or replace table STORE (id NUMBER, region_id NUMBER, store_desc VARCHAR(256), PRIMARY KEY (id), FOREIGN KEY (region_id) references REGION(id));")
cursor.execute("create or replace table CATEGORY (id NUMBER, category_desc VARCHAR(1024), PRIMARY KEY (id));")
cursor.execute("create or replace table SUBCATEGORY (id NUMBER, category_id NUMBER, subcategory_desc VARCHAR(256), PRIMARY KEY (id), FOREIGN KEY (category_id) references CATEGORY(id));")
cursor.execute("create or replace table PRODUCT (id NUMBER, subcategory_id NUMBER, product_desc VARCHAR(256), PRIMARY KEY (id), FOREIGN KEY (subcategory_id) references SUBCATEGORY(id));")
cursor.execute("create or replace table CUSTOMER (id NUMBER, customer_first_name VARCHAR(256), customer_middle_name VARCHAR(256), customer_last_name VARCHAR(256), customer_address VARCHAR(256), primary key (id));")
cursor.execute("create or replace table SALES (id NUMBER, store_id NUMBER NOT NULL, product_id NUMBER NOT NULL, customer_id NUMBER, transaction_time TIMESTAMP, quantity NUMBER, amount NUMBER(20,2), discount NUMBER(20,2), primary key (id), FOREIGN KEY (store_id) references STORE(id), FOREIGN KEY (product_id) references PRODUCT(id), FOREIGN KEY (customer_id) references CUSTOMER(id));")
cursor.execute(f"""INSERT INTO Country VALUES
(1, 'Nepal'),
(2, 'India');""")

cursor.execute(f"""
               INSERT INTO Region VALUES
                (1, 1, 'Kathmandu'),
                (2, 1, 'Lalitpur'),
                (3, 2, 'Punjab'),
                (4, 2, 'Bangalore');
               """)

cursor.execute(f"""
               INSERT INTO Store VALUES
                (1, 1, 'Bhatbhateni Chakrapath'),
                (2, 2, 'Bhabhateni Pulchowk'),
                (3, 3, 'Bhatbhateni Chandigarh'),
                (4, 4, 'Bhatbhateni Jayanagar');
               """)

cursor.execute(f"""
               INSERT INTO CATEGORY VALUES 
                (1,'Garment'),
                (2,'GROCERY'),
                (3,'KitchenWares');
               """)

cursor.execute(f"""
               Insert into Subcategory values
                (1, 1,'MENS WEAR'),
                (2, 1,'WOMENS WEAR'),
                (3,1,'KIDS WEAR'),
                (4,2,'Dairy'),
                (5,2,'Fruits'), 
                (6,2,'Packed Items'),
                (7,3,'Utensils') ,
                (8,3,'Glassware'),
                (9,3,'ElectricAppliances');
               """)

cursor.execute(f"""
               insert into Product values
                (1,1,'Jeans'),
                (2,2,'Shirt'),
                (3,1,'Shoes'),
                (4,5,'Apple'),
                (5,5,'Oranges'),
                (6,4,'Milk'),
                (7,4,'Butter'),
                (8,9, 'Microwave'),
                (9,9, 'Coffee Maker'),
                (10,7, 'Plate'),
                (11,6,'Wai Wai'),
                (12,6,'Oats'),
                (13,3,'Diaper'),
                (14,8,'Glass'),
                (15,8,'Jug'),
                (16,2,'Skirt'),
                (17,7,'Spoon'),
                (18,3,'Kids Jacket');
               """)

cursor.execute(f"""
                insert into Customer values
                (1,'Ram',null,'Adhikari','Kathmandu'),
                (2,'Shyam','Prasad','Sharma','lalitpur'),
                (3,'Hari',null,'Poudel','Bhaktapur'),
                (4, 'Saurav','Muhammed', 'Ali','Banglore'),
                (5,'Kannur','Lokesh','Rahul','Punjab'),
                (6, 'Mahendra','Singh','Dhoni','Chennai');
               """)

cursor.execute(f"""
Insert into Sales values
( 1 , 3 , 17 , 2 , '2021-11-11 13:39:14' , 12 , 28196.55 , 2819.66 ),
( 2 , 4 , 15 , 5 , '2021-11-10 13:23:1' , 9 , 18034.61 , 901.73 ),
( 3 , 2 , 11 , 5 , ' 2020-12-12 22:48:6 ' , 11 , 19433.22 , 971.66 ),
( 4 , 2 , 1 , 3 , '2021-12-13 6:27:49' , 6 , 3623.98 , 181.2 ),
( 5 , 4 , 4 , NULL , '2021-12-10 21:3:17' , 3 , 10707.11 , 535.36 ),
( 6 , 2 , 14 , 3 , ' 2020-11-13 9:41:33 ' , 5 , 1857.95 , 92.9 ),
( 7 , 2 , 12 , 1 , ' 2020-10-13 1:36:26 ' , 15 , 20792.43 , 2079.24 ),
( 8 , 4 , 13 , 6 , '2021-12-13 11:21:7' , 12 , 25160.25 , 2516.03 ),
( 9 , 2 , 3 , 1 , ' 2020-11-10 5:22:19 ' , 10 , 25718.2 , 2571.82 ),
( 10 , 1 , 15 , NULL , '2021-10-10 8:58:16' , 10 , 17857.01 , 892.85 ),
( 11 , 4 , 15 , 6 , '2021-10-10 15:48:33' , 8 , 28545.99 , 2854.6 ),
( 12 , 3 , 3 , 5 , '2021-11-12 1:32:15' , 12 , 21855.83 , 2185.58 ),
( 13 , 2 , 17 , 5 , '2021-12-12 16:17:53' , 1 , 18281.62 , 914.08 ),
( 14 , 4 , 11 , 2 , '2021-11-11 18:56:53' , 8 , 8119.77 , 405.99 ),
( 15 , 1 , 11 , NULL , ' 2020-12-12 15:29:0 ' , 9 , 26406.92 , 2640.69 ),
( 16 , 4 , 13 , 2 , ' 2020-10-13 5:58:9 ' , 10 , 13489.3 , 674.47 ),
( 17 , 2 , 7 , 6 , '2021-10-13 7:26:36' , 2 , 15331.48 , 766.57 ),
( 18 , 3 , 10 , 4 , '2021-9-13 3:7:23' , 17 , 7572.21 , 378.61 ),
( 19 , 3 , 4 , 3 , '2021-9-13 19:2:9' , 15 , 24542.8 , 2454.28 ),
( 20 , 2 , 6 , NULL , ' 2020-11-12 5:2:31 ' , 12 , 2786.59 , 139.33 ),
( 21 , 3 , 9 , 1 , '2021-11-11 7:39:57' , 1 , 19896.93 , 994.85 ),
( 22 , 4 , 2 , 1 , '2021-12-13 23:21:5' , 12 , 22424.7 , 2242.47 ),
( 23 , 4 , 3 , 6 , '2021-12-12 1:22:57' , 14 , 14760.96 , 738.05 ),
( 24 , 1 , 12 , 4 , '2021-10-10 23:5:12' , 8 , 5720.27 , 286.01 ),
( 25 , 2 , 5 , NULL , '2021-11-13 23:17:22' , 5 , 26363.45 , 2636.35 ),
( 26 , 2 , 2 , 2 , ' 2020-12-13 3:6:59 ' , 4 , 27198.82 , 2719.88 ),
( 27 , 1 , 17 , 6 , '2021-12-10 11:10:32' , 19 , 2604.81 , 130.24 ),
( 28 , 3 , 17 , 2 , ' 2020-12-10 20:51:5 ' , 3 , 27527.53 , 2752.75 ),
( 29 , 2 , 8 , 3 , '2021-9-10 0:21:9' , 9 , 4933.27 , 246.66 ),
( 30 , 2 , 12 , NULL , '2021-9-12 18:39:35' , 17 , 7677.3 , 383.87 ),
( 31 , 2 , 7 , 4 , ' 2020-11-12 23:9:28 ' , 3 , 7842.28 , 392.11 ),
( 32 , 3 , 12 , 1 , '2021-9-13 5:22:19' , 9 , 13386.08 , 669.3 ),
( 33 , 2 , 3 , 3 , '2021-12-12 18:35:37' , 2 , 19765.92 , 988.3 ),
( 34 , 3 , 7 , 3 , '2021-9-11 9:17:27' , 9 , 12611.95 , 630.6 ),
( 35 , 3 , 11 , NULL , ' 2020-12-11 10:19:1 ' , 1 , 881.22 , NULL ),
( 36 , 1 , 8 , 2 , ' 2020-11-11 21:27:29 ' , 17 , 22018.72 , 2201.87 ),
( 37 , 3 , 7 , 5 , '2021-10-11 10:39:27' , 6 , 19511.54 , 975.58 ),
( 38 , 3 , 16 , 4 , ' 2020-12-11 7:19:30 ' , 15 , 1497.62 , 74.88 ),
( 39 , 3 , 3 , 6 , '2021-9-13 13:27:50' , 18 , 7805.1 , 390.26 ),
( 40 , 2 , 15 , NULL , '2021-12-13 19:24:34' , 2 , 24934.45 , 2493.45 ),
( 41 , 2 , 2 , 1 , ' 2020-10-12 22:33:20 ' , 16 , 28761.05 , 2876.11 ),
( 42 , 4 , 14 , 3 , '2021-12-10 3:4:17' , 19 , 2676.76 , 133.84 ),
( 43 , 1 , 4 , 6 , '2021-12-12 8:22:55' , 14 , 334.56 , NULL ),
( 44 , 1 , 17 , 6 , ' 2020-11-13 17:57:19 ' , 7 , 25941.17 , 2594.12 ),
( 45 , 4 , 5 , NULL , '2021-12-12 22:7:10' , 14 , 10385.77 , 519.29 ),
( 46 , 1 , 9 , 4 , ' 2020-11-12 19:48:49 ' , 5 , 16426.41 , 821.32 ),
( 47 , 4 , 2 , 6 , ' 2020-12-13 1:8:33 ' , 17 , 28374.8 , 2837.48 ),
( 48 , 4 , 6 , 1 , ' 2020-9-12 12:36:0 ' , 17 , 15956.87 , 797.84 ),
( 49 , 4 , 10 , 4 , '2021-12-10 23:59:14' , 8 , 24363.07 , 2436.31 ),
( 50 , 3 , 16 , NULL , ' 2020-10-12 20:18:32 ' , 15 , 10834.32 , 541.72 ),
( 51 , 2 , 8 , 1 , '2021-10-13 3:46:34' , 1 , 27933.68 , 2793.37 ),
( 52 , 2 , 6 , 3 , '2021-11-12 17:46:33' , 11 , 19515.72 , 975.79 ),
( 53 , 2 , 1 , 4 , '2021-12-13 11:20:40' , 14 , 12510.67 , 625.53 ),
( 54 , 1 , 18 , 2 , ' 2020-9-10 14:28:4 ' , 3 , 17834.32 , 891.72 ),
( 55 , 2 , 15 , NULL , ' 2020-12-13 20:56:9 ' , 10 , 530.89 , NULL ),
( 56 , 1 , 9 , 3 , ' 2020-12-12 9:5:58 ' , 13 , 24821.99 , 2482.2 ),
( 57 , 2 , 5 , 2 , '2021-9-13 11:27:45' , 14 , 13273.6 , 663.68 ),
( 58 , 4 , 5 , 1 , '2021-9-12 5:37:43' , 16 , 7019.6 , 350.98 ),
( 59 , 3 , 2 , 4 , '2021-10-11 18:17:25' , 11 , 5843.05 , 292.15 ),
( 60 , 3 , 12 , NULL , ' 2020-10-13 10:44:16 ' , 13 , 17819.38 , 890.97 ),
( 61 , 4 , 14 , 1 , '2021-11-13 6:50:32' , 8 , 25297.71 , 2529.77 ),
( 62 , 2 , 11 , 4 , '2021-11-10 5:0:48' , 5 , 10363.42 , 518.17 ),
( 63 , 4 , 9 , 2 , '2021-12-12 0:31:17' , 10 , 4256.21 , 212.81 ),
( 64 , 3 , 4 , 2 , ' 2020-9-12 5:7:50 ' , 4 , 7248.99 , 362.45 ),
( 65 , 4 , 13 , NULL , '2021-10-12 19:46:16' , 12 , 17384.55 , 869.23 ),
( 66 , 1 , 12 , 4 , ' 2020-10-12 5:34:26 ' , 8 , 19182.61 , 959.13 ),
( 67 , 3 , 11 , 3 , '2021-10-13 4:56:18' , 1 , 11263.86 , 563.19 ),
( 68 , 4 , 12 , 6 , '2021-11-11 15:23:58' , 16 , 3755.26 , 187.76 ),
( 69 , 1 , 6 , 1 , '2021-11-13 21:25:18' , 13 , 21650.49 , 2165.05 ),
( 70 , 1 , 13 , NULL , '2021-9-12 2:19:0' , 18 , 20931.71 , 2093.17 ),
( 71 , 1 , 10 , 3 , ' 2020-9-12 16:13:41 ' , 18 , 21653.84 , 2165.38 ),
( 72 , 2 , 12 , 1 , '2021-12-13 16:45:45' , 4 , 5911.88 , 295.59 ),
( 73 , 3 , 3 , 2 , ' 2020-10-11 5:27:17 ' , 5 , 1511.31 , 75.57 ),
( 74 , 1 , 8 , 3 , '2021-11-10 9:7:59' , 2 , 20486.0 , 2048.6 ),
( 75 , 3 , 11 , NULL , ' 2020-11-11 10:19:6 ' , 5 , 13955.64 , 697.78 ),
( 76 , 2 , 6 , 4 , ' 2020-9-12 4:15:21 ' , 14 , 15812.64 , 790.63 ),
( 77 , 2 , 16 , 1 , '2021-11-11 4:35:39' , 11 , 10609.74 , 530.49 ),
( 78 , 1 , 2 , 2 , '2021-11-11 19:43:32' , 3 , 9909.1 , 495.46 ),
( 79 , 4 , 18 , 6 , '2021-10-13 8:11:32' , 19 , 21222.23 , 2122.22 ),
( 80 , 2 , 9 , NULL , '2021-9-12 23:11:14' , 8 , 4995.67 , 249.78 ),
( 81 , 4 , 3 , 1 , '2021-12-10 11:21:27' , 5 , 23939.63 , 2393.96 ),
( 82 , 3 , 3 , 6 , ' 2020-12-10 8:38:17 ' , 10 , 12986.79 , 649.34 ),
( 83 , 4 , 7 , 1 , ' 2020-10-12 1:39:11 ' , 14 , 19873.83 , 993.69 ),
( 84 , 3 , 8 , 6 , '2021-10-13 7:42:17' , 14 , 17294.24 , 864.71 ),
( 85 , 3 , 7 , NULL , '2021-12-13 2:24:1' , 17 , 24548.42 , 2454.84 ),
( 86 , 2 , 15 , 1 , '2021-12-10 1:40:18' , 9 , 17078.56 , 853.93 ),
( 87 , 1 , 11 , 4 , ' 2020-11-13 10:39:35 ' , 16 , 20844.84 , 2084.48 ),
( 88 , 4 , 17 , 4 , '2021-12-12 14:53:41' , 15 , 24909.58 , 2490.96 ),
( 89 , 1 , 14 , 1 , ' 2020-12-10 23:5:30 ' , 10 , 6031.23 , 301.56 ),
( 90 , 3 , 14 , NULL , ' 2020-12-11 2:15:37 ' , 18 , 20283.05 , 2028.31 ),
( 91 , 1 , 12 , 1 , '2021-12-11 23:24:16' , 8 , 8819.83 , 440.99 ),
( 92 , 1 , 17 , 3 , ' 2020-10-10 15:45:49 ' , 13 , 10647.71 , 532.39 ),
( 93 , 1 , 7 , 4 , '2021-12-11 16:14:19' , 7 , 22687.94 , 2268.79 ),
( 94 , 3 , 12 , 3 , ' 2020-10-13 19:53:18 ' , 9 , 473.69 , NULL ),
( 95 , 3 , 3 , NULL , ' 2020-11-13 15:9:30 ' , 18 , 29079.77 , 2907.98 ),
( 96 , 3 , 1 , 6 , ' 2020-11-11 19:4:13 ' , 7 , 2337.76 , 116.89 ),
( 97 , 2 , 17 , 3 , '2021-11-13 16:41:46' , 6 , 25751.48 , 2575.15 ),
( 98 , 1 , 14 , 6 , ' 2020-12-11 17:2:26 ' , 5 , 13475.45 , 673.77 ),
( 99 , 4 , 2 , 5 , ' 2020-12-11 0:11:49 ' , 4 , 17431.74 , 871.59 ),
( 100 , 4 , 9 , NULL , '2021-12-12 20:25:58' , 8 , 28518.2 , 2851.82 )
;
""")
cursor.execute(f"""CREATE OR REPLACE TABLE LOCATION_HIERARCHY (
    location_id NUMBER AUTOINCREMENT PRIMARY KEY,
    sales_id NUMBER,
    store_id NUMBER, 
    region_id NUMBER, 
    country_id NUMBER, 
    FOREIGN KEY (store_id) REFERENCES STORE(id),
    FOREIGN KEY (region_id) REFERENCES REGION(id),
    FOREIGN KEY (country_id) REFERENCES COUNTRY(id)
);""")

cursor.execute(f"""INSERT INTO LOCATION_HIERARCHY (sales_id, store_id, region_id, country_id)
SELECT sa.id, s.id, r.id, c.id
FROM STORE s, REGION r, COUNTRY c, SALES sa
WHERE r.id = s.region_id AND c.id = r.country_id AND sa.store_id = s.id;
""")

cursor.close()
conn.close()