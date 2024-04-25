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
            MERGE INTO {temporary_table} tmp
            USING {staging_table} stg ON tmp.{key_column} = stg.{key_column}
            WHEN NOT MATCHED THEN 
                INSERT (tmp.id, tmp.sales_key, tmp.store_key, tmp.region_key, tmp.country_key)
                VALUES (stg.id, stg.sales_id, stg.store_id, stg.region_id, stg.country_id)
            WHEN MATCHED THEN UPDATE SET 
                tmp.id = stg.id, 
                tmp.sales_key = stg.sales_id, 
                tmp.store_key = stg.store_id, 
                tmp.region_key = stg.region_id, 
                tmp.country_key = stg.country_id
        """
    cursor.execute(query)
    print(f"Handling data updation for {temporary_table} completed.")


def reclassify_and_add_rows(cursor, staging_table, temporary_table, key_column):
    query = f"""
            INSERT INTO {temporary_table} (id, sales_key, store_key, region_key, country_key)
            SELECT stg.{key_column}, stg.sales_id, stg.store_id, stg.region_id, stg.country_id
            FROM {staging_table} stg
            WHERE stg.{key_column} NOT IN (SELECT {key_column} 
            FROM {temporary_table})
        """
    cursor.execute(query)
    print(f"Reclassification and addition of rows completed for {temporary_table}.")

def handle_closing_dimension(cursor, temporary_table, target_table, key_column):
    sequence_query = f"""
                       CREATE SEQUENCE sequence_key
                        START = 1
                        INCREMENT = 1
                        NOORDER;
                    """
    cursor.execute(sequence_query)
    query = f"""
            MERGE INTO {target_table} AS tgt
                USING {temporary_table} AS tmp ON tgt.{key_column} = tmp.{key_column}
                WHEN NOT MATCHED THEN 
                    INSERT (location_key, id, sales_key, store_key, region_key, country_key, active_flag, created_at, updated_at)
                    VALUES (
                        sequence_key.NEXTVAL,
                        tmp.id, 
                        tmp.sales_key, 
                        tmp.store_key, 
                        tmp.region_key, 
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
                    tgt.active_flag = 'Y',
                    tgt.created_at = CURRENT_TIMESTAMP,
                    tgt.updated_at = CURRENT_TIMESTAMP

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
    stage_name = 'ETL_FILE_STAGE'
    staging_table = 'STG.STG_D_LOCATION_HIERARCHY_LU';
    temporary_table = 'TMP.TMP_D_LOCATION_HIERARCHY_LU';
    target_table = 'TGT.DWH_D_LOCATION_HIERARCHY_LU';
    key_column = 'id'

    truncate_tables(cursor, staging_table);
    load_from_stage_to_table(cursor, stage_name, staging_table);
    truncate_tables(cursor, temporary_table);
    handle_data_update(cursor, staging_table, temporary_table, key_column);
    truncate_tables(cursor, target_table);
    handle_closing_dimension(cursor, temporary_table, target_table, key_column);


    cursor.close()
    conn.close()

main()