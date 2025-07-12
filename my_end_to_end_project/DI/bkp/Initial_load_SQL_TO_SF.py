import pandas as pd
from sqlalchemy import create_engine, text
import urllib
from snowflake.sqlalchemy import URL

# SQL Server Connection
DRIVER_NAME ='SQL SERVER'
SERVER_NAME = 'DESKTOP-65CIF6J\SQLEXPRESS'
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

# List of tables to load
tables = ['customer', 'product', 'sales','region']

# Loop through each table
for table_name in tables:
    print(f"Processing table: {table_name}")

    # Read from SQL Server
    query = f"SELECT * FROM {DATABASE_NAME}.dbo.{table_name}"
    df = pd.read_sql(query, sql_server_engine)

    print(f"Loaded {len(df)} rows from SQL Server table: {table_name}")


    # Optional: Drop table in Snowflake before loading
    with snowflake_engine.connect() as connection:
           connection.execute(text(f"DROP TABLE IF EXISTS STAGE.{table_name}"))

    # Write to Snowflake
    df.to_sql(table_name, con=snowflake_engine, index=False, if_exists='replace', schema='STAGE')

    print(f"Loaded {len(df)} rows to Snowflake table: {table_name}")

print("Data Load Completed Successfully!")
