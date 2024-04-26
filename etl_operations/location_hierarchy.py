import snowflake.connector; # type: ignore
import csv
import os
from dotenv import load_dotenv # type: ignore


def truncate_tables(cursor, table_name):
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    print(f"Table {table_name} truncated")


def load_from_stage_to_table(cursor, stage_name, table_name):
    file_pattern = 'location_hierarchy_data.csv/location_hierarchy_data.csv.gz'
    skip_rows = 1

    copy_into_query = f"COPY INTO {table_name} FROM @{stage_name}/{file_pattern} FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = {skip_rows} COMPRESSION = 'gzip')"

    try:
        cursor.execute(copy_into_query)
        print("Data copied from file stage to table successfully.")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error: {e}")



def handle_data_update(cursor, staging_table, temporary_table, key_column):
    query = f"""
            INSERT INTO {temporary_table} (id, sales_id, sales_key, store_id, store_key, region_id, region_key, country_id, country_key)
            SELECT stg.id, stg.sales_id, tgt_sales.sales_key, stg.store_id, tgt_store.store_key, stg.region_id, tgt_region.region_key, stg.country_id, tgt_country.country_key
            FROM {staging_table} stg
            FULL JOIN TGT.DWH_D_SALES_LU AS tgt_sales ON tgt_sales.id = stg.sales_id
            FULL JOIN TGT.DWH_D_STORE_LU AS tgt_store ON tgt_store.id = stg.store_id
            FULL JOIN TGT.DWH_D_REGION_LU AS tgt_region ON tgt_region.id = stg.region_id
            FULL JOIN TGT.DWH_D_COUNTRY_LU AS tgt_country ON tgt_country.id = stg.country_id;
        """
    cursor.execute(query)
    print(f"Handling data updation for {temporary_table} completed.")


def reclassify_and_add_rows(cursor, staging_table, temporary_table, key_column):
    query = f"""
            INSERT INTO {temporary_table} (id, sales_id, sales_key, store_id, store_key, region_id, region_key, country_id, country_key)
            SELECT stg.id, stg.sales_id, tgt_sales.sales_key, stg.store_id, tgt_store.store_key, stg.region_id, tgt_region.region_key, stg.country_id, tgt_country.country_key
            FROM {staging_table} stg
            FULL JOIN TGT.DWH_D_SALES_LU AS tgt_sales ON tgt_sales.id = stg.sales_id
            FULL JOIN TGT.DWH_D_STORE_LU AS tgt_store ON tgt_store.id = stg.store_id
            FULL JOIN TGT.DWH_D_REGION_LU AS tgt_region ON tgt_region.id = stg.region_id
            FULL JOIN TGT.DWH_D_COUNTRY_LU AS tgt_country ON tgt_country.id = stg.country_id
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
                    INSERT (location_key, id, sales_id, sales_key, store_id, store_key, region_id, region_key, country_id, country_key, active_flag, created_at, updated_at)
                    VALUES (
                        sequence_key.NEXTVAL,
                        tmp.id, 
                        tmp.sales_id,
                        tmp.sales_key,
                        tmp.store_id, 
                        tmp.store_key,
                        tmp.region_id, 
                        tmp.region_key,
                        tmp.country_id, 
                        tmp.country_key,
                        'Y',
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP
                    )

                WHEN MATCHED THEN UPDATE SET 
                    tgt.location_key = sequence_key.NEXTVAL,
                    tgt.id = tmp.id, 
                    tgt.sales_key = tmp.sales_key,
                    tgt.store_key = tmp.store_key,
                    tgt.region_key = tmp.region_key,
                    tgt.country_key = tmp.country_key,
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

    cursor.execute("CREATE OR REPLACE TABLE STG.STG_D_LOCATION_HIERARCHY_LU (id NUMBER, sales_id NUMBER, store_id NUMBER, region_id NUMBER, country_id NUMBER, PRIMARY KEY (id), FOREIGN KEY (store_id) REFERENCES STG.STG_D_STORE_LU(id), FOREIGN KEY (region_id) REFERENCES STG.STG_D_REGION_LU(id), FOREIGN KEY (country_id) REFERENCES STG.STG_D_COUNTRY_LU(id));")
    print("Table created successfully: STG_D_LOCATION_HIERARCHY_LU")

    cursor.execute("CREATE OR REPLACE TABLE TMP.TMP_D_LOCATION_HIERARCHY_LU (id NUMBER, sales_id NUMBER, sales_key NUMBER, store_id NUMBER, store_key NUMBER, region_id NUMBER, region_key NUMBER, country_id NUMBER, country_key NUMBER, PRIMARY KEY (id), FOREIGN KEY (store_key) REFERENCES TGT.DWH_D_STORE_LU(store_key), FOREIGN KEY (region_key) REFERENCES TGT.DWH_D_REGION_LU(region_key), FOREIGN KEY (country_key) REFERENCES TGT.DWH_D_COUNTRY_LU(country_key));")
    print("Table created successfully: TMP_D_LOCATION_HIERARCHY_LU")

    cursor.execute("CREATE OR REPLACE TABLE TGT.DWH_D_LOCATION_HIERARCHY_LU (location_key NUMBER, id NUMBER, sales_id NUMBER, sales_key NUMBER, store_id NUMBER, store_key NUMBER, region_id NUMBER, region_key NUMBER, country_id NUMBER, country_key NUMBER, active_flag BOOLEAN, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (location_key), FOREIGN KEY (store_key) REFERENCES TGT.DWH_D_STORE_LU(store_key), FOREIGN KEY (region_key) REFERENCES TGT.DWH_D_REGION_LU(region_key), FOREIGN KEY (country_key) REFERENCES TGT.DWH_D_COUNTRY_LU(country_key));")
    print("Table created successfully: DWH_D_LOCATION_HIERARCHY_LU")

    stage_name = 'ETL_FILE_STAGE'
    staging_table = 'STG.STG_D_LOCATION_HIERARCHY_LU';
    temporary_table = 'TMP.TMP_D_LOCATION_HIERARCHY_LU';
    target_table = 'TGT.DWH_D_LOCATION_HIERARCHY_LU';
    key_column = 'id'

    truncate_tables(cursor, staging_table);
    load_from_stage_to_table(cursor, stage_name, staging_table);
    truncate_tables(cursor, temporary_table);
    handle_data_update(cursor, staging_table, temporary_table, key_column);
    handle_closing_dimension(cursor, temporary_table, target_table, key_column);


    cursor.close()
    conn.close()

main()