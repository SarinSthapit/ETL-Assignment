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
            INSERT INTO {temporary_table} (id, country_id, country_key, region_desc)
            SELECT stg.id, stg.country_id, tgt.country_key, stg.region_desc
            FROM {staging_table} stg 
            JOIN TGT.DWH_D_COUNTRY_LU as tgt
            ON tgt.id = stg.country_id;
        """
    cursor.execute(query)
    print(f"Handling data updation for {temporary_table} completed.")


def reclassify_and_add_rows(cursor, staging_table, temporary_table, key_column):
    query = f"""
            INSERT INTO {temporary_table} (id, country_id, country_key, region_desc)
            SELECT stg.{key_column}, stg.country_id, tgt.country_key, stg.region_desc
            FROM {staging_table} stg
            JOIN TGT.DWH_D_COUNTRY_LU as tgt
            ON tgt.id = stg.country_id
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
                    INSERT (region_key, id, country_id, country_key, region_desc, active_flag, created_at, updated_at)
                    VALUES (
                        sequence_key.NEXTVAL,
                        tmp.id, 
                        tmp.country_id,
                        tmp.country_key, 
                        tmp.region_desc, 
                        'N',
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP
                    )

                WHEN MATCHED THEN UPDATE SET 
                    tgt.region_key = sequence_key.NEXTVAL,
                    tgt.id = tmp.id, 
                    tgt.country_id = tmp.country_id,
                    tgt.country_key = tmp.country_key,
                    tgt.region_desc = tmp.region_desc,
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

    cursor.execute("CREATE OR REPLACE TABLE STG.STG_D_REGION_LU (id NUMBER, country_id NUMBER, region_desc VARCHAR(256), PRIMARY KEY (id), FOREIGN KEY (country_id) references STG.STG_D_COUNTRY_LU(id));")
    print("Table created successfully: STG_D_REGION_LU")

    cursor.execute("CREATE OR REPLACE TABLE TMP.TMP_D_REGION_LU (id NUMBER, country_id NUMBER, country_key NUMBER, region_desc VARCHAR(256), PRIMARY KEY (id), FOREIGN KEY (country_key) references TGT.DWH_D_COUNTRY_LU(country_key));")
    print("Table created successfully: TMP_D_REGION_LU")

    cursor.execute("CREATE OR REPLACE TABLE TGT.DWH_D_REGION_LU (region_key NUMBER, id NUMBER, country_id NUMBER, country_key NUMBER, region_desc VARCHAR(256), active_flag BOOLEAN, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (region_key), FOREIGN KEY (country_key) references TGT.DWH_D_COUNTRY_LU(country_key));")
    print("Table created successfully: DWH_D_REGION_LU")

    stage_name = 'ETL_FILE_STAGE'
    staging_table = 'STG.STG_D_REGION_LU';
    temporary_table = 'TMP.TMP_D_REGION_LU';
    target_table = 'TGT.DWH_D_REGION_LU';
    key_column = 'id'

    truncate_tables(cursor, staging_table);
    load_from_stage_to_table(cursor, stage_name, staging_table);
    truncate_tables(cursor, temporary_table);
    handle_data_update(cursor, staging_table, temporary_table, key_column);
    handle_closing_dimension(cursor, temporary_table, target_table, key_column);


    cursor.close()
    conn.close()

main()