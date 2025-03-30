import pyodbc
import json
import config

server = config.server
database = config.database
driver = config.driver

conn = pyodbc.connect(
    f"DRIVER={driver};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"Trusted_Connection=yes;"
)
cursor = conn.cursor()

query = "SELECT TOP 10 Keyword, count FROM keywords ORDER BY count DESC"
cursor.execute(query)
rows = cursor.fetchall()

columns = [column[0] for column in cursor.description]

result = [dict(zip(columns, row)) for row in rows]

json_output = json.dumps(result, indent=4)
print(json_output)

conn.close()
