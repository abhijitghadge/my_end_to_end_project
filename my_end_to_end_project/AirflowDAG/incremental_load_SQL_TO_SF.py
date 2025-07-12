#!/usr/bin/env python3
import os, urllib, logging, pandas as pd
from sqlalchemy import create_engine, text
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ─── 1) LOGGING ───────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ─── 2) SQL SERVER ENGINE (source) ────────────────────────────────────────────
DRIVER   = 'ODBC Driver 17 for SQL Server'
SERVER   = r'DESKTOP-65CIF6J\SQLEXPRESS'
DATABASE = 'ABHIJITDB'
conn_str = (
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
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
with sf_conn.cursor() as cur:
    cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
    SF_DB, SF_SCHEMA = cur.fetchone()
logger.info(f"Snowflake context: {SF_DB}.{SF_SCHEMA}")

# ─── 4) TYPE MAPPING ──────────────────────────────────────────────────────────
def map_dtype_to_sf(r):
    dt = r.DATA_TYPE.lower()
    import pandas as _pd
    prec  = int(r.NUMERIC_PRECISION) if _pd.notnull(r.NUMERIC_PRECISION) else 0
    scale = int(r.NUMERIC_SCALE)     if _pd.notnull(r.NUMERIC_SCALE)     else 0
    length= int(r.CHARACTER_MAXIMUM_LENGTH) if _pd.notnull(r.CHARACTER_MAXIMUM_LENGTH) else 0

    if dt in ("int","bigint","smallint","tinyint"): return "NUMBER(38,0)"
    if dt in ("decimal","numeric"):                 return f"NUMBER({prec},{scale})"
    if dt in ("float","real"):                     return "FLOAT"
    if dt == "bit":                                return "BOOLEAN"
    if dt == "date":                               return "DATE"
    if dt in ("datetime","smalldatetime","datetime2"): return "TIMESTAMP_LTZ(3)"
    if dt == "datetimeoffset":                     return "TIMESTAMP_TZ"
    if dt == "time":                               return "TIME"
    if dt in ("varchar","nvarchar","char","nchar"):
        return f"VARCHAR({length})" if 1<=length<=65535 else "VARCHAR"
    if dt in ("text","ntext"):                     return "VARCHAR"
    if dt in ("varbinary","binary"):               return "BINARY"
    return "STRING"

# ─── 5) CONTROL ────────────────────────────────────────────────────────────────
TABLES = ["customer","product","region","sales"]
PKS    = {"customer":"CUSTOMERID","product":"PRODUCTID",
          "region":"REGIONID","sales":"SALEID"}

def get_sschema(tbl):
    sql = text("""
      SELECT COLUMN_NAME, DATA_TYPE,
             CHARACTER_MAXIMUM_LENGTH,
             NUMERIC_PRECISION, NUMERIC_SCALE
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_NAME=:tbl
      ORDER BY ORDINAL_POSITION
    """)
    return pd.read_sql_query(sql, sqlserver_engine, params={"tbl":tbl})

def get_sf_columns(tbl):
    with sf_conn.cursor() as cur:
        cur.execute("""
          SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_CATALOG=%s
             AND TABLE_SCHEMA =%s
             AND TABLE_NAME   =%s
        """, (SF_DB, SF_SCHEMA, tbl))
        return {r[0].upper() for r in cur.fetchall()}

# ─── 6) INCREMENTAL LOAD ──────────────────────────────────────────────────────
for tbl in TABLES:
    tgt = tbl.upper()
    pk  = PKS[tbl]
    logger.info(f"\n▶ Incremental load `{tbl}` → {SF_SCHEMA}.{tgt}")

    # ensure target schema
    ss = get_sschema(tbl)
    if ss.empty:
        logger.warning("  • No source cols; skipping.")
        continue

    sf_cols = get_sf_columns(tgt)
    if not sf_cols:
        # create table
        defs = [f"{r.COLUMN_NAME.upper()} {map_dtype_to_sf(r)}" for _,r in ss.iterrows()]
        ddl = f"CREATE TABLE {SF_SCHEMA}.{tgt} (\n  "+",\n  ".join(defs)+");"
        sf_conn.cursor().execute(ddl)
        sf_cols = get_sf_columns(tgt)
        logger.info(f"  • Created {tgt}")

    else:
        # add new columns
        for _,r in ss.iterrows():
            col = r.COLUMN_NAME.upper()
            if col not in sf_cols:
                alter = f"ALTER TABLE {SF_SCHEMA}.{tgt} ADD COLUMN {col} {map_dtype_to_sf(r)}"
                sf_conn.cursor().execute(alter)
                sf_cols.add(col)
                logger.info(f"  • Added column {col}")

    # find last load
    with sf_conn.cursor() as cur:
        cur.execute(f"SELECT COALESCE(MAX(LAST_UPDATED_DATE),'1900-01-01') FROM {SF_SCHEMA}.{tgt}")
        last = cur.fetchone()[0]
    logger.info(f"  • Last load = {last}")

    # extract delta
    df = pd.read_sql_query(
        text(f"""
          SELECT * FROM {DATABASE}.dbo.{tbl}
           WHERE LAST_UPDATED_DATE > CONVERT(DATETIME2(3), :ldt, 121)
        """),
        sqlserver_engine,
        params={"ldt":last}
    )
    if df.empty:
        logger.info("  • No new rows; skipping.")
        continue
    logger.info(f"  • Got {len(df)} new rows")

    # upper‐case columns & stringify datetimes
    df.columns = [c.upper() for c in df.columns]
    for c,d in df.dtypes.items():
        if pd.api.types.is_datetime64_any_dtype(d):
            df[c] = df[c].dt.strftime("%Y-%m-%d %H:%M:%S.%f").str[:-3]

    # stage into temp
    temp = f"TEMP_{tgt}"
    sf_conn.cursor().execute(f"CREATE TEMPORARY TABLE {temp} LIKE {SF_SCHEMA}.{tgt}")
    write_pandas(sf_conn, df, temp, schema=SF_SCHEMA, database=SF_DB, chunk_size=16000)
    logger.info("  • Staged into " + temp)

    # MERGE without aliases:
    cols       = df.columns.tolist()
    set_clause = ", ".join(f"{c}=SRC.{c}" for c in cols if c!=pk)
    ins_cols   = ", ".join(cols)
    ins_vals   = ", ".join(f"SRC.{c}" for c in cols)

    merge_sql = f"""
      MERGE INTO {SF_SCHEMA}.{tgt}
      USING {SF_SCHEMA}.{temp} SRC
        ON {SF_SCHEMA}.{tgt}.{pk} = SRC.{pk}
      WHEN MATCHED THEN
        UPDATE SET {set_clause}
      WHEN NOT MATCHED THEN
        INSERT ({ins_cols}) VALUES ({ins_vals});
    """

    with sf_conn.cursor() as cur:
        cur.execute(merge_sql)
    logger.info(f"  • Merged into {tgt}")

    sf_conn.cursor().execute(f"DROP TABLE IF EXISTS {temp}")
    logger.info("  • Dropped " + temp)

logger.info("\n✅ Done.")