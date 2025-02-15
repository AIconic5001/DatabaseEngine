import requests
import pandas as pd
import time
import xml.etree.ElementTree as ET

class ArxivCrawler:
    def __init__(self, query, total_results=1000, batch_size=100, delay=4):
        self.query = query
        self.total_results = total_results
        self.batch_size = batch_size
        self.delay = delay
        self.base_url = "http://export.arxiv.org/api/query?"
        self.papers = []
        self.start_index = 0
    
    def fetch_batch(self):
        search_url = f"{self.base_url}search_query={self.query}&start={self.start_index}&max_results={self.batch_size}"
        response = requests.get(search_url)
        if response.status_code != 200:
            raise Exception(f"Error: Received status code {response.status_code}")
        
        root = ET.fromstring(response.content)
        return root
    
    def process_entries(self, root):
        for entry in root.findall("{http://www.w3.org/2005/Atom}entry"):
            if len(self.papers) >= self.total_results:
                break

            title = entry.find("{http://www.w3.org/2005/Atom}title").text
            summary = entry.find("{http://www.w3.org/2005/Atom}summary").text
            published = entry.find("{http://www.w3.org/2005/Atom}published").text
            categories = entry.find("{http://arxiv.org/schemas/atom}primary_category").attrib['term']
            authors = ', '.join([author.find("{http://www.w3.org/2005/Atom}name").text 
                                 for author in entry.findall("{http://www.w3.org/2005/Atom}author")])
            url = entry.find("{http://www.w3.org/2005/Atom}id").text

            paper_info = {
                'Title': title,
                'Authors': authors,
                'Summary': summary,
                'Categories': categories,
                'Published': published,
                'URL': url
            }

            self.papers.append(paper_info)

            print(f"Added: {len(self.papers)} / {self.total_results} - {title}")

    def crawl(self):
        while len(self.papers) < self.total_results:
            root = self.fetch_batch()
            self.process_entries(root)
            self.start_index += self.batch_size
            time.sleep(self.delay)
        
        return pd.DataFrame(self.papers)
    
    def save_to_csv(self, filename):
        df = pd.DataFrame(self.papers)
        df.to_csv(filename, index=False)
        print(f"Saved {len(self.papers)} papers to {filename}")

if __name__ == "__main__":
    query = "artificial intelligence"
    crawler = ArxivCrawler(query=query, total_results=1000, batch_size=100, delay=4)
    df = crawler.crawl()
    print(df.head())
    crawler.save_to_csv(f'arxiv_1000_{query}.csv')
