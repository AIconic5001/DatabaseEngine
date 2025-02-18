import os
import requests
import pandas as pd
import time
import xml.etree.ElementTree as ET

class ArxivCrawler:
    def __init__(self, query, total_results, batch_size, delay):
        self.query = query
        self.total_results = total_results
        self.batch_size = batch_size
        self.delay = delay
        self.base_url = "http://export.arxiv.org/api/query?"
        self.papers = []
        self.start_index = 0

    def fetch_batch(self):
        request_url = (
            f"{self.base_url}search_query={self.query}"
            f"&start={self.start_index}&max_results={self.batch_size}"
        )
        response = requests.get(request_url)
        if response.status_code != 200:
            raise Exception(f"Status code {response.status_code}")
        return ET.fromstring(response.content)

    def process_entries(self, root_element):
        for entry_element in root_element.findall("{http://www.w3.org/2005/Atom}entry"):
            if len(self.papers) >= self.total_results:
                break
            title_text = entry_element.find("{http://www.w3.org/2005/Atom}title").text
            summary_text = entry_element.find("{http://www.w3.org/2005/Atom}summary").text
            published_text = entry_element.find("{http://www.w3.org/2005/Atom}published").text

            primary_category_element = entry_element.find("{http://arxiv.org/schemas/atom}primary_category")
            category_text = primary_category_element.attrib.get('term') if primary_category_element is not None else ""

            authors_list = entry_element.findall("{http://www.w3.org/2005/Atom}author")
            authors_text = ', '.join(
                author_item.find("{http://www.w3.org/2005/Atom}name").text
                for author_item in authors_list
            )

            url_text = entry_element.find("{http://www.w3.org/2005/Atom}id").text

            self.papers.append({
                "Title": title_text,
                "Authors": authors_text,
                "Summary": summary_text,
                "Categories": category_text,
                "Published": published_text,
                "URL": url_text
            })

    def crawl(self):
        while len(self.papers) < self.total_results:
            root_element = self.fetch_batch()
            self.process_entries(root_element)
            self.start_index += self.batch_size
            time.sleep(self.delay)
        return pd.DataFrame(self.papers)

    def save_to_csv(self, folder="Arxiv_csv", filename=None):
        if not os.path.exists(folder):
            os.makedirs(folder)
        if filename is None:
            filename = f"arxiv_{self.total_results}_{self.query}.csv"
        csv_path = os.path.join(folder, filename)
        pd.DataFrame(self.papers).to_csv(csv_path, index=False)
        print(f"\nFile saved: {csv_path}")

