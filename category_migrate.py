import pandas as pd
import pyodbc
import urllib
import sqlalchemy

from category_mapping import arxiv_category_mapping
from config import driver, server, database  
conn_str = (
    f"DRIVER={driver};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"Trusted_Connection=yes;"
)

params = urllib.parse.quote_plus(conn_str)

engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

df = pd.DataFrame({
    'Ar_ID': list(arxiv_category_mapping.keys()),
    'Category': list(arxiv_category_mapping.values())
})

df.insert(0, 'ID', range(1, len(df) + 1))

df.to_sql('category', engine, if_exists='append', index=False)

print("Data pushed to SQL Server table 'category' successfully!")
