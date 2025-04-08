import pyodbc
import json
import config

def export_top_keywords_and_categories(output_path="top_keywords_and_categories.json"):
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

    # Top 10 keywords
    query_keywords = "SELECT TOP 10 Keyword, count FROM keywords ORDER BY count DESC"
    cursor.execute(query_keywords)
    rows_keywords = cursor.fetchall()
    columns_keywords = [column[0] for column in cursor.description]
    result_keywords = [dict(zip(columns_keywords, row)) for row in rows_keywords]

    # Top 10 categories
    query_categories = """
    SELECT TOP 10 category, COUNT(*) AS total_entries
    FROM arxiv
    GROUP BY category
    ORDER BY total_entries DESC
    """
    cursor.execute(query_categories)
    rows_categories = cursor.fetchall()
    columns_categories = [column[0] for column in cursor.description]
    result_categories = [dict(zip(columns_categories, row)) for row in rows_categories]

    # Combine and export
    output = {
        "top_keywords": result_keywords,
        "top_categories": result_categories
    }

    with open(output_path, "w") as json_file:
        json.dump(output, json_file, indent=4)

    print(f"Exported to {output_path}")
    conn.close()
