import snowflake.connector; # type: ignore
import csv
import os
from dotenv import load_dotenv # type: ignore


def truncate_tables(cursor, table_name):
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    print(f"Table {table_name} truncated")


def load_from_stage_to_table(cursor, stage_name, table_name):
    file_pattern = 'product_data.csv/product_data.csv.gz'
    skip_rows = 1

    copy_into_query = f"COPY INTO {table_name} FROM @{stage_name}/{file_pattern} FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = {skip_rows} COMPRESSION = 'gzip')"

    try:
        cursor.execute(copy_into_query)
        print("Data copied from file stage to table successfully.")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error: {e}")



def handle_data_update(cursor, staging_table, temporary_table, key_column):
    query = f"""
            INSERT INTO {temporary_table} (id, subcategory_id, subcategory_key, product_desc)
            SELECT stg.id, stg.subcategory_id, tgt.subcategory_key, stg.product_desc
            FROM {staging_table} stg 
            JOIN TGT.DWH_D_SUBCATEGORY_LU as tgt
            ON tgt.id = stg.subcategory_id;
        """
    cursor.execute(query)
    print(f"Handling data updation for {temporary_table} completed.")


def reclassify_and_add_rows(cursor, staging_table, temporary_table, key_column):
    query = f"""
            INSERT INTO {temporary_table} (id, subcategory_id, subcategory_key, product_desc)
            SELECT stg.{key_column}, stg.subcategory_id, tgt.subcategory_key, stg.product_desc
            FROM {staging_table} stg
            JOIN TGT.DWH_D_CATEGORY_LU as tgt
            ON tgt.id = stg.subcategory_id
            WHERE stg.{key_column} NOT IN 
            (SELECT {key_column} 
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
                    INSERT (product_key, id, subcategory_id, subcategory_key, product_desc, active_flag, created_at, updated_at)
                    VALUES (
                        sequence_key.NEXTVAL,
                        tmp.id, 
                        tmp.subcategory_id, 
                        tmp.subcategory_key,
                        tmp.product_desc,
                        'N',
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP
                    )

                WHEN MATCHED THEN UPDATE SET 
                    tgt.product_key = sequence_key.NEXTVAL,
                    tgt.id = tmp.id, 
                    tgt.subcategory_id = tmp.subcategory_id,
                    tgt.subcategory_key = tmp.subcategory_key,
                    tgt.product_desc = tmp.product_desc,
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

    cursor.execute("CREATE OR REPLACE TABLE STG.STG_D_PRODUCT_LU (id NUMBER, subcategory_id NUMBER, product_desc VARCHAR(256), PRIMARY KEY (id), FOREIGN KEY (subcategory_id) references STG.STG_D_SUBCATEGORY_LU(id));")
    print("Table created successfully: STG_D_PRODUCT_LU")

    cursor.execute("CREATE OR REPLACE TABLE TMP.TMP_D_PRODUCT_LU (id NUMBER, subcategory_id NUMBER, subcategory_key NUMBER, product_desc VARCHAR(256), PRIMARY KEY (id), FOREIGN KEY (subcategory_key) references TGT.DWH_D_SUBCATEGORY_LU(subcategory_key));")
    print("Table created successfully: TMP_D_PRODUCT_LU")

    cursor.execute("CREATE OR REPLACE TABLE TGT.DWH_D_PRODUCT_LU (product_key NUMBER, id NUMBER, subcategory_id NUMBER, subcategory_key NUMBER, product_desc VARCHAR(256), active_flag BOOLEAN, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (product_key), FOREIGN KEY (subcategory_key) references TGT.DWH_D_SUBCATEGORY_LU(subcategory_key));")
    print("Table created successfully: DWH_D_PRODUCT_LU")

    stage_name = 'ETL_FILE_STAGE'
    staging_table = 'STG.STG_D_PRODUCT_LU';
    temporary_table = 'TMP.TMP_D_PRODUCT_LU';
    target_table = 'TGT.DWH_D_PRODUCT_LU';
    key_column = 'id'

    truncate_tables(cursor, staging_table);
    load_from_stage_to_table(cursor, stage_name, staging_table);
    truncate_tables(cursor, temporary_table);
    handle_data_update(cursor, staging_table, temporary_table, key_column);
    handle_closing_dimension(cursor, temporary_table, target_table, key_column);


    cursor.close()
    conn.close()

main()