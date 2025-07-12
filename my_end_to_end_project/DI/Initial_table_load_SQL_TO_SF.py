#!/usr/bin/env python3
import os
import urllib
import logging
import pandas as pd

from datetime import datetime
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from sqlalchemy import create_engine, text

# ─── 1) LOGGING ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

# ─── 2) SQL SERVER ENGINE (source) ────────────────────────────────────────────
DRIVER   = 'ODBC Driver 17 for SQL Server'
SERVER   = r'DESKTOP-65CIF6J\SQLEXPRESS'
DATABASE = 'ABHIJITDB'
conn_str = (
    f"DRIVER={{{SQL_SERVER_DRIVER}}};"
    f"SERVER={SQL_SERVER_SERVER};"
    f"DATABASE={SQL_SERVER_DATABASE};"
    "Trusted_Connection=yes;"
)
sqlserver_engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(conn_str)}"
)

# ─── 3) SNOWFLAKE CONNECTOR (target) ───────────────────────────────────────────
sf_conn = snowflake.connector.connect(
    account   = 'CSJPTKJ-KI26303',
    user      = 'PYTHON_STAGE',
    password  = 'PYTHON_STAGE12#',
    warehouse = 'COMPUTE_WH',
    database  = 'SQL_SERVER_STAGE',
    schema    = 'dbo',
    role      = 'ACCOUNTADMIN'
)

# derive once
with sf_conn.cursor() as cur:
    cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
    sf_db, sf_schema = cur.fetchone()
logger.info(f"Snowflake context: {sf_db}.{sf_schema}")

# ─── 4) TYPE MAPPING ──────────────────────────────────────────────────────────
def map_dtype_to_sf(row):
    import pandas as _pd
    dt     = row.DATA_TYPE.lower()
    prec   = int(row.NUMERIC_PRECISION)    if _pd.notnull(row.NUMERIC_PRECISION)    else 0
    scale  = int(row.NUMERIC_SCALE)        if _pd.notnull(row.NUMERIC_SCALE)        else 0
    length = int(row.CHARACTER_MAXIMUM_LENGTH) if _pd.notnull(row.CHARACTER_MAXIMUM_LENGTH) else 0

    if dt in ("int","bigint","smallint","tinyint"):
        return "NUMBER(38,0)"
    if dt in ("decimal","numeric"):
        return f"NUMBER({prec},{scale})"
    if dt in ("float","real"):
        return "FLOAT"
    if dt == "bit":
        return "BOOLEAN"
    if dt == "date":
        return "DATE"
    if dt in ("datetime","smalldatetime","datetime2"):
        # explicitly use 3-digit fractional seconds
        return "TIMESTAMP_LTZ(3)"
    if dt == "datetimeoffset":
        return "TIMESTAMP_TZ"
    if dt == "time":
        return "TIME"
    if dt in ("varchar","nvarchar","char","nchar"):
        return f"VARCHAR({length})" if 1 <= length <= 65535 else "VARCHAR"
    if dt in ("text","ntext"):
        return "VARCHAR"
    if dt in ("varbinary","binary"):
        return "BINARY"
    return "STRING"

# ─── 5) MAIN PIPELINE ─────────────────────────────────────────────────────────
TABLES = ["customer","product","region","sales"]

for tbl in TABLES:
    tbl_sf = tbl.upper()
    logger.info(f"▶ Processing `{tbl}` → {sf_db}.{sf_schema}.{tbl_sf}")

    # 5a) Introspect SQL Server schema
    sch_sql = text("""
        SELECT COLUMN_NAME, DATA_TYPE,
               CHARACTER_MAXIMUM_LENGTH,
               NUMERIC_PRECISION, NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = :tbl
        ORDER BY ORDINAL_POSITION
    """)
    sschema = pd.read_sql_query(sch_sql, sqlserver_engine, params={"tbl": tbl})
    if sschema.empty:
        logger.warning(f"  • No columns found for `{tbl}`; skipping.")
        continue

    # 5b) Get existing Snowflake columns
    with sf_conn.cursor() as cur:
        cur.execute(
            """
            SELECT COLUMN_NAME
              FROM INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_CATALOG = %s
               AND TABLE_SCHEMA  = %s
               AND TABLE_NAME    = %s
            """,
            (sf_db, sf_schema, tbl_sf)
        )
        rows = cur.fetchall()
    existing = {r[0].upper() for r in rows}

    # 5c) CREATE or ALTER DDL
    if not existing:
        defs = [
            f"{row.COLUMN_NAME.upper()} {map_dtype_to_sf(row)}"
            for _, row in sschema.iterrows()
        ]
        ddl = f"CREATE OR REPLACE TABLE {sf_schema}.{tbl_sf} (\n  " \
              + ",\n  ".join(defs) + "\n);"
        with sf_conn.cursor() as cur:
            cur.execute(ddl)
        logger.info(f"  • Created {sf_schema}.{tbl_sf} ({len(defs)} cols).")
    else:
        for _, row in sschema.iterrows():
            col = row.COLUMN_NAME.upper()
            if col not in existing:
                alter = (
                    f"ALTER TABLE {sf_schema}.{tbl_sf} "
                    f"ADD COLUMN {col} {map_dtype_to_sf(row)}"
                )
                with sf_conn.cursor() as cur:
                    cur.execute(alter)
                logger.info(f"  • Added column {col} to {sf_schema}.{tbl_sf}")

    # 5d) Read all data from SQL Server
    df = pd.read_sql(f"SELECT * FROM {DATABASE}.dbo.{tbl}", sqlserver_engine)
    if df.empty:
        logger.info(f"  • No data to load for `{tbl}`.")
        continue

    # 5e) Uppercase DataFrame columns
    df.columns = [c.upper() for c in df.columns]

    # 5f) Convert datetime columns to ISO strings with millisecond precision
    datetime_cols = [
        col for col, dtype in df.dtypes.items()
        if pd.api.types.is_datetime64_any_dtype(dtype)
    ]
    for col in datetime_cols:
        df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S.%f").str[:-3]

    # 5g) Bulk-load via write_pandas
    success, nchunks, nrows, _ = write_pandas(
        conn        = sf_conn,
        df          = df,
        table_name  = tbl_sf,
        schema      = sf_schema,
        database    = sf_db,
        chunk_size  = 16000
    )
    if not success:
        raise RuntimeError(f"write_pandas failed for {tbl_sf}")
    logger.info(f"  • Loaded {nrows} rows into {sf_schema}.{tbl_sf} ({nchunks} chunks)")

logger.info("✅ All tables created/altered and loaded, with TIMESTAMP_LTZ(3) values.")
