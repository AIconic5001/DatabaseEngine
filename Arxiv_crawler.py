import os
import requests
import pandas as pd
import time
import xml.etree.ElementTree as ET
from datetime import datetime

class ArxivCrawler:
    def __init__(self, query, total_results, batch_size, delay):
        self.query = query
        self.total_results = total_results
        self.batch_size = batch_size
        self.delay = delay
        self.base_url = "http://export.arxiv.org/api/query?"
        self.papers = []
        self.start_index = 0

    @staticmethod
    def format_author(name):
        """
        Formats a full name into APA style:
        e.g., "John Michael Doe" becomes "Doe, J. M."
        """
        parts = name.split()
        if len(parts) < 2:
            return name
        last_name = parts[-1]
        initials = " ".join(f"{p[0]}." for p in parts[:-1])
        return f"{last_name}, {initials}"

    @staticmethod
    def join_authors(author_list):
        """
        Joins a list of formatted authors into an APA-style string.
        For two authors, join with ' & '. For three or more, separate with commas and use ', & ' before the last author.
        """
        if not author_list:
            return ""
        if len(author_list) == 1:
            return author_list[0]
        elif len(author_list) == 2:
            return " & ".join(author_list)
        else:
            return ", ".join(author_list[:-1]) + ", & " + author_list[-1]

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

            title_text = entry_element.find("{http://www.w3.org/2005/Atom}title").text.strip()
            summary_text = entry_element.find("{http://www.w3.org/2005/Atom}summary").text.strip()
            published_text = entry_element.find("{http://www.w3.org/2005/Atom}published").text.strip()
            
            try:
                published_date = datetime.strptime(published_text, "%Y-%m-%dT%H:%M:%SZ")
                formatted_date = published_date.strftime("%Y, %B %d")
            except Exception:
                formatted_date = published_text[:4]  

            primary_category_element = entry_element.find("{http://arxiv.org/schemas/atom}primary_category")
            category_text = primary_category_element.attrib.get('term') if primary_category_element is not None else ""

            authors_elements = entry_element.findall("{http://www.w3.org/2005/Atom}author")
            authors_raw = [author.find("{http://www.w3.org/2005/Atom}name").text.strip() for author in authors_elements]
            authors_formatted = [self.format_author(author) for author in authors_raw]
            authors_apa = self.join_authors(authors_formatted)

            url_text = entry_element.find("{http://www.w3.org/2005/Atom}id").text.strip()


            citation = f"{authors_apa} ({formatted_date}). {title_text}. arXiv. {url_text}"

            self.papers.append({
                "Title": title_text,
                "Authors": authors_apa,
                "Summary": summary_text,
                "Categories": category_text,
                "Published": published_text,
                "URL": url_text,
                "Citation": citation,
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

