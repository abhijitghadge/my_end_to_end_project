import os
import urllib
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import json

def get_sql_server_connection():
    """
    Returns the SQL Server connection engine.
    """
    # Load configuration from environment variables or a config file
    DRIVER_NAME = os.getenv('SQL_SERVER_DRIVER', 'SQL SERVER')
    SERVER_NAME = os.getenv('SQL_SERVER_SERVER', 'DESKTOP-65CIF6J\\SQLEXPRESS')
    DATABASE_NAME = os.getenv('SQL_SERVER_DATABASE', 'ABHIJITDB')
    
    params = urllib.parse.quote_plus(
        f"DRIVER={{{DRIVER_NAME}}};"
        f"SERVER={SERVER_NAME};"
        f"DATABASE={DATABASE_NAME};"
        f"Trust_Connection=yes;"
    )
    
    sql_server_engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    
    return sql_server_engine


def get_snowflake_connection():
    """
    Returns the Snowflake connection engine.
    """
    # Load Snowflake configuration from environment variables or a config file
    snowflake_account = os.getenv('SNOWFLAKE_ACCOUNT', 'CSJPTKJ-KI26303')
    snowflake_user = os.getenv('SNOWFLAKE_USER', 'PYTHON_STAGE')
    snowflake_password = os.getenv('SNOWFLAKE_PASSWORD', 'PYTHON_STAGE12#')
    snowflake_database = os.getenv('SNOWFLAKE_DATABASE', 'SQL_SERVER_STAGE')
    snowflake_schema = os.getenv('SNOWFLAKE_SCHEMA', 'STAGE')
    snowflake_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
    snowflake_role = os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
    
    snowflake_engine = create_engine(URL(
        account=snowflake_account,
        user=snowflake_user,
        password=snowflake_password,
        database=snowflake_database,
        schema=snowflake_schema,
        warehouse=snowflake_warehouse,
        role=snowflake_role
    ))
    
    return snowflake_engine
