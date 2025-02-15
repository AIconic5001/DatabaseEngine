import pyodbc
import pandas as pd
from config import server, database, driver  
from category_mapping import arxiv_category_mapping  

conn = pyodbc.connect(
    f'DRIVER={driver};'
    f'SERVER={server};'
    f'DATABASE={database};'
    f'Trusted_Connection=yes;'
)
cursor = conn.cursor()

create_table_query = '''
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'arxiv')
BEGIN
    CREATE TABLE arxiv (
        ar_id INT IDENTITY(1,1) PRIMARY KEY,
        paper_name NVARCHAR(255) NOT NULL,
        author_name NVARCHAR(255) NOT NULL,
        ar_category NVARCHAR(255),
        category NVARCHAR(255),
        publish_date DATE,
        summary NVARCHAR(MAX),
        url NVARCHAR(500) NOT NULL
    );
END
'''

cursor.execute(create_table_query)
conn.commit()
print(" Checked/Created table 'arxiv' successfully.")

file_path = "arxiv_papers_1000.csv"  
df = pd.read_csv(file_path)

df_arxiv = df.rename(columns={
    "Title": "paper_name",
    "Authors": "author_name",
    "Categories": "ar_category",
    "URL": "url",
    "Published": "publish_date",
    "Summary": "summary"
})

df_arxiv["publish_date"] = pd.to_datetime(df_arxiv["publish_date"], errors='coerce').dt.date

def map_arxiv_category(arxiv_cat):
    if pd.isna(arxiv_cat):
        return None
    categories = arxiv_cat.split(", ")  # Handle multiple categories
    mapped_categories = [arxiv_category_mapping.get(cat, "Other") for cat in categories]
    return ", ".join(set(mapped_categories))  # Remove duplicates and join


df_arxiv["category"] = df_arxiv["ar_category"].apply(map_arxiv_category)
df_arxiv["author_name"] = df_arxiv["author_name"].astype(str).str[:255]

print(df_arxiv.head)


df_arxiv = df_arxiv[["paper_name", "author_name", "ar_category", "category", "publish_date", "summary", "url"]]

insert_query = '''
INSERT INTO arxiv (paper_name, author_name, ar_category, category, publish_date, summary, url)
VALUES (?, ?, ?, ?, ?, ?, ?)
'''

for index, row in df_arxiv.iterrows():
    cursor.execute(insert_query, row.paper_name, row.author_name, row.ar_category, row.category, row.publish_date, row.summary, row.url)

conn.commit()
cursor.close()
conn.close()

print("Complete" )
