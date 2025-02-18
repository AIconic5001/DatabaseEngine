import os
import json
import pandas as pd
import pyodbc
import urllib
import sqlalchemy

from config import driver, server, database


conn_str = (
    f"DRIVER={driver};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"Trusted_Connection=yes;"
)
params = urllib.parse.quote_plus(conn_str)
engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

existing_fields = pd.DataFrame(columns=["ID", "Field"])
existing_keywords = pd.DataFrame(columns=["ID", "Keyword"])

with engine.connect() as conn:
    if engine.dialect.has_table(conn, "fields"):
        existing_fields = pd.read_sql("SELECT ID, Field FROM fields", conn)
    if engine.dialect.has_table(conn, "keywords"):
        existing_keywords = pd.read_sql("SELECT ID, Keyword FROM keywords", conn)

existing_fields_set = set(existing_fields["Field"].tolist())
existing_keywords_set = set(existing_keywords["Keyword"].tolist())

folder_path = "./json"  
new_fields = []
new_keywords = []

for filename in os.listdir(folder_path):
    if filename.lower().endswith(".json"):
        filepath = os.path.join(folder_path, filename)
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        scientific_fields = data.get("scientific_fields", [])
        keywords = data.get("keywords", [])

        for field in scientific_fields:
            if field not in existing_fields_set:
                new_fields.append(field)
        for kw in keywords:
            if kw not in existing_keywords_set:
                new_keywords.append(kw)


new_fields_unique = list(set(new_fields))  #
if len(existing_fields) > 0:
    start_field_id = existing_fields["ID"].max() + 1
else:
    start_field_id = 1

df_new_fields = pd.DataFrame({
    "ID": range(start_field_id, start_field_id + len(new_fields_unique)),
    "Field": new_fields_unique
})

new_keywords_unique = list(set(new_keywords))
if len(existing_keywords) > 0:
    start_kw_id = existing_keywords["ID"].max() + 1
else:
    start_kw_id = 1

df_new_keywords = pd.DataFrame({
    "ID": range(start_kw_id, start_kw_id + len(new_keywords_unique)),
    "Keyword": new_keywords_unique
})


all_fields = pd.concat([existing_fields, df_new_fields], ignore_index=True)
all_keywords = pd.concat([existing_keywords, df_new_keywords], ignore_index=True)


all_fields.to_sql("fields", engine, if_exists="replace", index=False)
all_keywords.to_sql("keywords", engine, if_exists="replace", index=False)

print("Data successfully merged and updated in 'fields' and 'keywords' tables!")
