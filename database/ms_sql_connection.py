import pyodbc
import pandas as pd
from environment.settings import config

#Declare db connection 
conn=pyodbc.connect(
        'Driver={ODBC Driver 17 for SQL Server};'
        'Server=localhost;'
        f'Database={config.MS_SQL_DB};'
        'Trusted_Connection=yes;'
    )

def fetch_query(query):
    return pd.read_sql(query, conn)