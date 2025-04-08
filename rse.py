import pyodbc
import json
import config

def rse(keyword):
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

    query = "SELECT paper_name, url, publish_date, citation FROM arxiv WHERE summary LIKE ?"
    pattern = f"%{keyword}%"
    cursor.execute(query, (pattern,))
    rows = cursor.fetchall()

    results = []
    for row in rows:
        paper_name = row[0]
        url = row[1]
        publish_date = row[2]
        citation_raw = row[3]
        
        try:
            citations = json.loads(citation_raw) if citation_raw else []
        except Exception:
            citations = citation_raw.split(",") if citation_raw else []
            citations = [c.strip() for c in citations]
        
        results.append({
            'paper_name': paper_name,
            'url': url,
            'publish_date': publish_date,
            'citations': citations
        })

    conn.close()
    return results
