import pandas as pd
from sqlalchemy import create_engine, text
import urllib
from snowflake.sqlalchemy import URL
from datetime import datetime

# 1) SQL Server Connection
DRIVER_NAME   = 'SQL SERVER'
SERVER_NAME   = 'DESKTOP-65CIF6J\\SQLEXPRESS'
DATABASE_NAME = 'ABHIJITDB'
params = urllib.parse.quote_plus(
    f"DRIVER={{{DRIVER_NAME}}};"
    f"SERVER={SERVER_NAME};"
    f"DATABASE={DATABASE_NAME};"
    "Trust_Connection=yes;"
)
sql_server_engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# 2) Snowflake Connection
snowflake_engine = create_engine(URL(
    account   = 'CSJPTKJ-KI26303',
    user      = 'PYTHON_STAGE',
    password  = 'PYTHON_STAGE12#',
    database  = 'SQL_SERVER_STAGE',
    schema    = 'STAGE',
    warehouse = 'COMPUTE_WH',
    role      = 'ACCOUNTADMIN'
))

# 3) Tables with Primary Keys
tables = {
    'CUSTOMER': 'CustomerID',
    'PRODUCT':  'ProductID',
    'REGION':   'RegionID',
    'SALES':    'SaleID'
}

# 4) Main loop
for table_name, primary_key in tables.items():
    try:
        print(f"\n===== Processing Table: {table_name} =====")
        # 4.1) Build & print the control‐table query
        control_query = f"""
        SELECT last_updated_date
        FROM ETL_BATCH_CONTROL
        WHERE table_name = '{table_name}'
        """    
        print(control_query)
        control_df = pd.read_sql_query(control_query, con=snowflake_engine)
        if control_df.empty:
            last_updated_date = datetime(1900, 1, 1)
            print(f"No existing watermark found, defaulting to {last_updated_date}")
        else:
            last_updated_date = control_df.loc[0, "last_updated_date"]
            print(f"Last Updated Date from Control Table: {last_updated_date}")

        # 4.4) Extract Incremental Data from SQL Server
        extract_query = f"""
            SELECT *
            FROM {DATABASE_NAME}.dbo.{table_name}
            WHERE last_updated_date > CONVERT(DATETIME2(3), '{last_updated_date}', 21)
        """
        df = pd.read_sql(extract_query, sql_server_engine)
        print(f"Fetched {len(df)} records from SQL Server.")

        if df.empty:
            print(f"No new records for {table_name}. Skipping...")
            continue

        # 4.5) Pre-create temporary table in Snowflake (lowercase)
        temp_table = f"{table_name.lower()}_stage_temp"
        with snowflake_engine.connect() as conn:
            conn.execute(text(f"""
                CREATE OR REPLACE TEMPORARY TABLE stage.{temp_table}
                LIKE stage.{table_name.lower()};
            """))

        # 4.6) Lowercase DataFrame columns to match table
        df.columns = [c.lower() for c in df.columns]

        # 4.7) Load into Snowflake temp table
        df.to_sql(
            temp_table,
            con=snowflake_engine,
            index=False,
            if_exists='append',
            schema='stage'
        )
        print(f"Loaded data to Snowflake Temp Table: STAGE.{temp_table}")

        # 4.8) Perform the MERGE (all-lowercase SQL)
        cols     = df.columns.tolist()
        pk       = primary_key.lower()
        set_expr = ", ".join(f"target.{c}=source.{c}" for c in cols)
        ins_cols = ", ".join(cols)
        ins_vals = ", ".join(f"source.{c}" for c in cols)

        merge_sql = f"""
            merge into stage.{table_name.lower()} as target
            using stage.{temp_table}             as source
              on target.{pk} = source.{pk}
            when matched then update set {set_expr}
            when not matched then insert ({ins_cols}) values ({ins_vals});
            drop table if exists stage.{temp_table};
        """
        # merge_sql is already lowercase
        with snowflake_engine.connect() as conn:
            conn.execute(text(merge_sql))

        print(f"Merge completed successfully for {table_name}")

        # 4.9) Compute new watermark
        new_max_date = df['last_updated_date'].max()
        new_max_str  = new_max_date.strftime('%Y-%m-%d %H:%M:%S')
        print(f"New last_updated_date to update: {new_max_str}")

        # 4.10) Update watermark back in SQL Server
        update_control_sql = f"""
            IF EXISTS (SELECT 1 FROM ETL_BATCH_CONTROL WHERE table_name = '{table_name}')
                UPDATE ETL_BATCH_CONTROL
                SET last_updated_date = CONVERT(DATETIME2(3), '{new_max_str}', 21),
                    load_date         = GETDATE()
                WHERE table_name = '{table_name}';
            ELSE
                INSERT INTO ETL_BATCH_CONTROL (table_name, last_updated_date, load_date)
                VALUES ('{table_name}', CONVERT(DATETIME2(3), '{new_max_str}', 21), GETDATE());
        """
        with sql_server_engine.begin() as conn:
            conn.execute(text(update_control_sql))

        print(f"ETL_BATCH_CONTROL updated successfully for {table_name}")

    except Exception as e:
        print(f"Error processing table {table_name}: {e}")
        # optionally: break or continue

print("\n===== ETL Incremental Load Completed Successfully =====")