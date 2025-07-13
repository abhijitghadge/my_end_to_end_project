#!/usr/bin/env python3
import os
import urllib
import logging
import pandas as pd

from datetime import datetime
from sqlalchemy import create_engine, text
from snowflake.sqlalchemy import URL

# ─── 1) LOGGING ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

# ─── 2) SQL SERVER ENGINE (source) ────────────────────────────────────────────
DRIVER_NAME   = os.getenv("SQL_SERVER_DRIVER", "ODBC Driver 17 for SQL Server")
SERVER_NAME   = os.getenv("SQL_SERVER_SERVER")
DATABASE_NAME = os.getenv("SQL_SERVER_DATABASE")

if not (SERVER_NAME and DATABASE_NAME):
    raise RuntimeError("Missing one of: SQL_SERVER_SERVER, SQL_SERVER_DATABASE")

conn_str = (
    f"DRIVER={{{DRIVER_NAME}}};"
    f"SERVER={SERVER_NAME};"
    f"DATABASE={DATABASE_NAME};"
    "Trusted_Connection=yes;"
)
sql_server_engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(conn_str)}"
)
logger.info(f"Connected to SQL Server `{SERVER_NAME}` DB `{DATABASE_NAME}`")

# ─── 3) SNOWFLAKE CONNECTOR (target) ───────────────────────────────────────────
sf_account   = os.getenv("SNOWFLAKE_ACCOUNT")
sf_user      = os.getenv("SNOWFLAKE_USER")
sf_password  = os.getenv("SNOWFLAKE_PASSWORD")
sf_database  = os.getenv("SNOWFLAKE_DATABASE")
sf_schema    = os.getenv("SNOWFLAKE_SCHEMA")
sf_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
sf_role      = os.getenv("SNOWFLAKE_ROLE")

for var in ["sf_account","sf_user","sf_password","sf_database","sf_schema","sf_warehouse","sf_role"]:
    if not locals()[var]:
        raise RuntimeError(f"Missing Snowflake env var: {var}")

snowflake_engine = create_engine(URL(
    account   = sf_account,
    user      = sf_user,
    password  = sf_password,
    database  = sf_database,
    schema    = sf_schema,
    warehouse = sf_warehouse,
    role      = sf_role
))
logger.info(f"Connected to Snowflake {sf_account}/{sf_database}.{sf_schema} as {sf_user}")

# ─── 4) Tables with Primary Keys ───────────────────────────────────────────────
tables = {
    'CUSTOMER': 'CustomerID',
    'PRODUCT':  'ProductID',
    'REGION':   'RegionID',
    'SALES':    'SaleID'
}

# ─── 5) Main loop ──────────────────────────────────────────────────────────────
for table_name, primary_key in tables.items():
    try:
        logger.info(f"\n===== Processing Table: {table_name} =====")

        # 5.1) Fetch watermark
        control_sql = f"""
            SELECT last_updated_date
            FROM ETL_BATCH_CONTROL
            WHERE table_name = '{table_name}'
        """
        control_df = pd.read_sql_query(control_sql, snowflake_engine)
        if control_df.empty:
            last_updated_date = datetime(1900,1,1)
            logger.info(f"No watermark found; defaulting to {last_updated_date}")
        else:
            last_updated_date = control_df.loc[0, "last_updated_date"]
            logger.info(f"Last Updated Date: {last_updated_date}")

        # 5.2) Extract incremental data
        extract_sql = f"""
            SELECT *
            FROM {DATABASE_NAME}.dbo.{table_name}
            WHERE last_updated_date > CONVERT(DATETIME2(3), '{last_updated_date}', 21)
        """
        df = pd.read_sql(extract_sql, sql_server_engine)
        logger.info(f"Fetched {len(df)} rows from SQL Server")

        if df.empty:
            logger.info(f"No new rows for {table_name}, skipping.")
            continue

        # 5.3) Prep temp table in Snowflake
        temp_table = f"{table_name.lower()}_stage_temp"
        with snowflake_engine.connect() as conn:
            conn.execute(text(f"""
                CREATE OR REPLACE TEMPORARY TABLE {sf_schema}.{temp_table}
                LIKE {sf_schema}.{table_name.lower()};
            """))

        # 5.4) Lowercase cols
        df.columns = [c.lower() for c in df.columns]

        # 5.5) Load to temp
        df.to_sql(
            temp_table,
            con=snowflake_engine,
            index=False,
            if_exists='append',
            schema=sf_schema
        )
        logger.info(f"Staged {len(df)} rows into {sf_schema}.{temp_table}")

        # 5.6) Merge into target
        cols     = df.columns.tolist()
        pk       = primary_key.lower()
        set_expr = ", ".join(f"target.{c}=source.{c}" for c in cols)
        ins_cols = ", ".join(cols)
        ins_vals = ", ".join(f"source.{c}" for c in cols)

        merge_sql = f"""
            MERGE INTO {sf_schema}.{table_name.lower()} AS target
            USING {sf_schema}.{temp_table}          AS source
              ON target.{pk} = source.{pk}
            WHEN MATCHED THEN UPDATE SET {set_expr}
            WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals});
            DROP TABLE IF EXISTS {sf_schema}.{temp_table};
        """
        with snowflake_engine.connect() as conn:
            conn.execute(text(merge_sql))

        logger.info(f"Merged data into {table_name}")

        # 5.7) Update watermark back in SQL Server
        new_max = df['last_updated_date'].max().strftime('%Y-%m-%d %H:%M:%S')
        update_sql = f"""
            IF EXISTS (SELECT 1 FROM ETL_BATCH_CONTROL WHERE table_name = '{table_name}')
              UPDATE ETL_BATCH_CONTROL
                SET last_updated_date = CONVERT(DATETIME2(3), '{new_max}', 21),
                    load_date = GETDATE()
              WHERE table_name = '{table_name}';
            ELSE
              INSERT INTO ETL_BATCH_CONTROL (table_name, last_updated_date, load_date)
              VALUES ('{table_name}', CONVERT(DATETIME2(3), '{new_max}', 21), GETDATE());
        """
        with sql_server_engine.begin() as conn:
            conn.execute(text(update_sql))

        logger.info(f"Watermark updated for {table_name}")

    except Exception as e:
        logger.error(f"Error processing {table_name}: {e}")

logger.info("✅ Incremental load complete")
