import pandas as pd
from sqlalchemy import create_engine, text
import urllib
from snowflake.sqlalchemy import URL
from datetime import datetime

# SQL Server Connection
DRIVER_NAME = 'SQL SERVER'
SERVER_NAME = 'DESKTOP-65CIF6J\\SQLEXPRESS'
DATABASE_NAME = 'ABHIJITDB'

params = urllib.parse.quote_plus(
    f"DRIVER={{{DRIVER_NAME}}};"
    f"SERVER={SERVER_NAME};"
    f"DATABASE={DATABASE_NAME};"
    f"Trust_Connection=yes;"
)

sql_server_engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# Snowflake Connection
snowflake_engine = create_engine(URL(
    account='CSJPTKJ-KI26303',
    user='PYTHON_STAGE',
    password='PYTHON_STAGE12#',
    database='SQL_SERVER_STAGE',
    schema='STAGE',
    warehouse='COMPUTE_WH',
    role='ACCOUNTADMIN'
))


# Tables with Primary Keys
tables = {
    'CUSTOMER': 'CustomerID',
    'PRODUCT': 'ProductID',
    'REGION': 'RegionID',
    'SALES': 'SaleID'
}


for table_name, primary_key in tables.items():
    try:
        print(f"\n===== Processing Table: {table_name} =====")

        # 1. Read last_updated_date from ETL_BATCH_CONTROL
        control_query = text(
            """
            SELECT last_updated_date
            FROM ETL_BATCH_CONTROL
            WHERE table_name = :table_name
            """
        )

        # 2. Now print it
        print("\n===== control_query Table: =====")
        print(control_query.text)

        # 3. And execute
        control_df = pd.read_sql_query(
            control_query,
            con=snowflake_engine,
            params={"table_name": table_name}
        )
        #last_updated_date = control_df['last_updated_date'][0]
        last_updated_date = control_df.loc[0, "last_updated_date"]
        print(f"Last Updated Date from Control Table")
        print(f"Last Updated Date from Control Table: {last_updated_date}")

        # 2. Extract Incremental Data
        extract_query = f"""
            SELECT * FROM {DATABASE_NAME}.dbo.{table_name}
            WHERE last_updated_date > CONVERT(DATETIME2(3), '{last_updated_date}', 21)
        """
        df = pd.read_sql(extract_query, sql_server_engine)
        print(f"Fetched {len(df)} records from SQL Server.")

        if df.empty:
            print(f"No new records found for {table_name}. Skipping...")
            continue

        # 3. Load into Snowflake Temp Table
        temp_table = f"{table_name}_STAGE_TEMP"
        
        extract_query1 = f"""
           CREATE OR REPLACE TEMPORARY TABLE  {temp_table} LIKE {table_name};
        """
        extract_query1 = extract_query1.lower()
        with snowflake_engine.connect() as conn:
            conn.execute(text(extract_query1))        
        
        df.to_sql(
            temp_table,
            con=snowflake_engine,
            index=False,
            if_exists='append',
            schema='STAGE'
        )
        print(f"Loaded data to Snowflake Temp Table: STAGE.{temp_table}")        
#        df.to_sql(temp_table, con=snowflake_engine, index=False, if_exists='replace', schema='STAGE')
        #df=df.lower()
        print(f"Loaded data to Snowflake Temp Table: {temp_table}")

        # 4. Perform Merge in Snowflake
        merge_sql = f"""
            MERGE INTO STAGE.{table_name} AS target
            USING STAGE.{temp_table} AS source
            ON target.{primary_key} = source.{primary_key}
            WHEN MATCHED THEN UPDATE SET {', '.join([f'target.{col} = source.{col}' for col in df.columns])}
            WHEN NOT MATCHED THEN INSERT ({', '.join(df.columns)}) VALUES ({', '.join(['source.' + col for col in df.columns])})
        """
        merge_sql =merge_sql.lower() 
        with snowflake_engine.connect() as connection:
            connection.execute(text(merge_sql))
            connection.execute(text(f"DROP TABLE IF EXISTS STAGE.{temp_table}"))

        print(f"Merge completed successfully for {table_name}")

        # 5. Update ETL_BATCH_CONTROL
        new_max_date = df['last_updated_date'].max()
        new_max_date_str = new_max_date.strftime('%Y-%m-%d %H:%M:%S')
        print(f"New last_updated_date to update: {new_max_date_str}")

        update_control_sql = f"""
            IF EXISTS (SELECT 1 FROM ETL_BATCH_CONTROL WHERE TABLE_NAME = '{table_name}')
                UPDATE ETL_BATCH_CONTROL
                SET last_updated_date = CONVERT(DATETIME2(3), '{new_max_date_str}', 21),
                    LOAD_DATE = GETDATE()
                WHERE TABLE_NAME = '{table_name}'            ELSE
                INSERT INTO ETL_BATCH_CONTROL (TABLE_NAME, last_updated_date, LOAD_DATE)
                VALUES ('{table_name}', CONVERT(DATETIME2(3), '{new_max_date_str}', 21), GETDATE())
        """

        with sql_server_engine.begin() as connection:
            connection.execute(text(update_control_sql))

        print(f"ETL_BATCH_CONTROL updated successfully for {table_name}")

    except Exception as e:
        print(f"Error processing table {table_name}: {e}")

print("\n===== ETL Incremental Load Completed Successfully =====")
