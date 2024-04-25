import snowflake.connector; # type: ignore
import csv
import os
from dotenv import load_dotenv # type: ignore


def truncate_tables(cursor, table_name):
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    print(f"Table {table_name} truncated")


def load_from_stage_to_table(cursor, stage_name, table_name):
    file_pattern = 'region_data.csv/region_data.csv.gz'
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
                INSERT (tmp.id, tmp.country_key, tmp.region_desc)
                VALUES (stg.id, stg.country_id, stg.region_desc)
            WHEN MATCHED THEN UPDATE SET 
                tmp.id = stg.id, 
                tmp.country_key = stg.country_id,
                tmp.region_desc = stg.region_desc
        """
    cursor.execute(query)
    print(f"Handling data updation for {temporary_table} completed.")


def reclassify_and_add_rows(cursor, staging_table, temporary_table, key_column):
    query = f"""
            INSERT INTO {temporary_table} (id, country_key, region_desc)
            SELECT stg.{key_column}, stg.country_key, stg.region_desc
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
                    INSERT (region_key, id, country_key, region_desc, active_flag, created_at, updated_at)
                    VALUES (
                        sequence_key.NEXTVAL,
                        tmp.id, 
                        tmp.country_key, 
                        tmp.region_desc, 
                        'Y',
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP
                    )

                WHEN MATCHED THEN UPDATE SET 
                    tgt.region_key = sequence_key.NEXTVAL,
                    tgt.id = tmp.id, 
                    tgt.country_key = tmp.country_key,
                    tgt.region_desc = tmp.region_desc,
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
    staging_table = 'STG.STG_D_REGION_LU';
    temporary_table = 'TMP.TMP_D_REGION_LU';
    target_table = 'TGT.DWH_D_REGION_LU';
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