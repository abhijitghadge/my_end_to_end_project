import pypyodbc as odbc #pip
import pandas as pd #dataframe 
import os

DRIVER_NAME ='SQL SERVER'
SERVER_NAME = 'DESKTOP-65CIF6J\SQLEXPRESS'
DATABASE_NAME = 'ABHIJITDB'
#uid=<username>;
#pwd=<password>;

connection_string = f"""
    DRIVER={{{DRIVER_NAME}}};
    SERVER={SERVER_NAME};
    DATABSE={DATABASE_NAME};
    Trust_Connecion=yes;
"""

conn = odbc.connect(connection_string)
print(conn)

sql = "select * from ABHIJITDB.dbo.EMPLOYEE"
df = pd.read_sql(sql,conn)

#print(df)
df.to_csv("data.csv", index=False)
conn.close()