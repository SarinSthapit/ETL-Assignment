import snowflake.connector; # type: ignore
import csv
import os
from dotenv import load_dotenv # type: ignore


def truncate_tables(cursor, table_name):
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    print(f"Table {table_name} truncated")


def load_from_stage_to_table(cursor, stage_name, table_name):
    file_pattern = 'sales_data.csv/sales_data.csv.gz'
    skip_rows = 1

    copy_into_query = f"COPY INTO {table_name} FROM @{stage_name}/{file_pattern} FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = {skip_rows} COMPRESSION = 'gzip')"

    try:
        cursor.execute(copy_into_query)
        print("Data copied from file stage to table successfully.")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error: {e}")



def handle_data_update(cursor, staging_table, temporary_table, key_column):
    query = f"""
            INSERT INTO {temporary_table} (
                id, 
                store_id, 
                store_key, 
                product_id, 
                product_key, 
                customer_id, 
                customer_key, 
                transaction_time, 
                quantity, 
                amount, 
                discount)
            SELECT 
                stg.id, 
                stg.store_id, 
                tgt_store.store_key, 
                stg.product_id, 
                tgt_product.product_key, 
                stg.customer_id, 
                tgt_customer.customer_key, 
                stg.transaction_time, 
                stg.quantity, 
                stg.amount, 
                stg.discount 
            FROM {staging_table} stg
            FULL JOIN TGT.DWH_D_STORE_LU AS tgt_store ON tgt_store.id = stg.store_id
            FULL JOIN TGT.DWH_D_PRODUCT_LU AS tgt_product ON tgt_product.id = stg.product_id
            FULL JOIN TGT.DWH_D_CUSTOMER_LU AS tgt_customer ON tgt_customer.id = stg.customer_id;
        """
    cursor.execute(query)
    print(f"Handling data updation for {temporary_table} completed.")


def reclassify_and_add_rows(cursor, staging_table, temporary_table, key_column):
    query = f"""
            INSERT INTO {temporary_table} (
                id, 
                store_id, 
                store_key, 
                product_id, 
                product_key, 
                customer_id, 
                customer_key, 
                transaction_time, 
                quantity, 
                amount, 
                discount)
            SELECT 
                stg.id, 
                stg.store_id, 
                tgt_store.store_key, 
                stg.product_id, 
                tgt_product.product_key, 
                stg.customer_id, 
                tgt_customer.customer_key, 
                stg.transaction_time, 
                stg.quantity, 
                stg.amount, 
                stg.discount 
            FROM {staging_table} stg
            FULL JOIN TGT.DWH_D_STORE_LU AS tgt_store ON tgt_store.id = stg.store_id
            FULL JOIN TGT.DWH_D_PRODUCT_LU AS tgt_product ON tgt_product.id = stg.product_id
            FULL JOIN TGT.DWH_D_CUSTOMER_LU AS tgt_customer ON tgt_customer.id = stg.customer_id
            WHERE stg.{key_column} NOT IN (SELECT {key_column} 
            FROM {temporary_table})
        """
    cursor.execute(query)
    print(f"Reclassification and addition of rows completed for {temporary_table}.")

def handle_closing_dimension(cursor, temporary_table, target_table, key_column):
    sequence_query = f"""
                       CREATE OR REPLACE SEQUENCE sequence_key
                        START = 1
                        INCREMENT = 1
                        NOORDER;
                    """
    cursor.execute(sequence_query)
    query = f"""
            MERGE INTO {target_table} AS tgt
                USING {temporary_table} AS tmp ON tgt.{key_column} = tmp.{key_column}
                WHEN NOT MATCHED THEN 
                    INSERT (sales_key, id, store_id, store_key, product_id, product_key, customer_id, customer_key, transaction_time, quantity, amount, discount, active_flag, created_at, updated_at)
                    VALUES (
                        sequence_key.NEXTVAL,
                        tmp.id, 
                        tmp.store_id,
                        tmp.store_key,
                        tmp.product_id, 
                        tmp.product_key,
                        tmp.customer_id, 
                        tmp.customer_key, 
                        tmp.transaction_time, 
                        tmp.quantity, 
                        tmp.amount, 
                        tmp.discount,
                        'N',
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP
                    )

                WHEN MATCHED THEN UPDATE SET 
                    tgt.sales_key = sequence_key.NEXTVAL,
                    tgt.id = tmp.id, 
                    tgt.store_id = tmp.store_id,
                    tgt.store_key = tmp.store_key,
                    tgt.product_id = tmp.product_id, 
                    tgt.product_key = tmp.product_key,
                    tgt.customer_id = tmp.customer_id, 
                    tgt.customer_key = tmp.customer_key, 
                    tgt.transaction_time = tmp.transaction_time, 
                    tgt.quantity = tmp.quantity, 
                    tgt.amount = tmp.amount, 
                    tgt.discount = tmp.discount,
                    tgt.active_flag = 'Y'

        """
    cursor.execute(query)
    drop_query = "DROP SEQUENCE sequence_key"
    cursor.execute(drop_query);
    print(f"Handling closing dimension for {target_table} completed.")



def main():
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

    cursor.execute("CREATE OR REPLACE TABLE STG.STG_D_SALES_LU (id NUMBER, store_id NUMBER NOT NULL, product_id NUMBER NOT NULL, customer_id NUMBER, transaction_time TIMESTAMP, quantity NUMBER, amount NUMBER(20,2), discount NUMBER(20,2), primary key (id), FOREIGN KEY (store_id) references STG.STG_D_STORE_LU(id), FOREIGN KEY (product_id) references STG.STG_D_PRODUCT_LU(id), FOREIGN KEY (customer_id) references STG.STG_D_CUSTOMER_LU(id));")
    print("Table created successfully: STG_D_SALES_LU")

    cursor.execute("CREATE OR REPLACE TABLE TMP.TMP_D_SALES_LU (id NUMBER, store_id NUMBER, store_key NUMBER NOT NULL, product_id NUMBER, product_key NUMBER NOT NULL, customer_id NUMBER, customer_key NUMBER, transaction_time TIMESTAMP, quantity NUMBER, amount NUMBER(20,2), discount NUMBER(20,2), primary key (id), FOREIGN KEY (store_key) references TGT.DWH_D_STORE_LU(store_key), FOREIGN KEY (product_key) references TGT.DWH_D_PRODUCT_LU(product_key), FOREIGN KEY (customer_key) references TGT.DWH_D_CUSTOMER_LU(customer_key));")
    print("Table created successfully: TMP_D_SALES_LU")

    cursor.execute("CREATE OR REPLACE TABLE TGT.DWH_D_SALES_LU (sales_key NUMBER, id NUMBER, store_id NUMBER, store_key NUMBER NOT NULL, product_id NUMBER, product_key NUMBER NOT NULL, customer_id NUMBER, customer_key NUMBER, transaction_time TIMESTAMP, quantity NUMBER, amount NUMBER(20,2), discount NUMBER(20,2), active_flag BOOLEAN, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, primary key (sales_key), FOREIGN KEY (store_key) references TGT.DWH_D_STORE_LU(store_key), FOREIGN KEY (product_key) references TGT.DWH_D_PRODUCT_LU(product_key), FOREIGN KEY (customer_key) references TGT.DWH_D_CUSTOMER_LU(customer_key));")
    print("Table created successfully: DWH_D_SALES_LU")

    stage_name = 'ETL_FILE_STAGE'
    staging_table = 'STG.STG_D_SALES_LU';
    temporary_table = 'TMP.TMP_D_SALES_LU';
    target_table = 'TGT.DWH_D_SALES_LU';
    key_column = 'id'

    truncate_tables(cursor, staging_table);
    load_from_stage_to_table(cursor, stage_name, staging_table);
    truncate_tables(cursor, temporary_table);
    handle_data_update(cursor, staging_table, temporary_table, key_column);
    # truncate_tables(cursor, target_table);
    handle_closing_dimension(cursor, temporary_table, target_table, key_column);


    cursor.close()
    conn.close()

main()