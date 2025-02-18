import subprocess
import threading
from tqdm import tqdm
from Arxiv_crawler import ArxivCrawler
from category_mapping import arxiv_category_mapping

def run_script(script_name):
    subprocess.run(["python", script_name], check=True)

if __name__ == "__main__":
    total_results = 1000
    batch_size = 1000
    delay = 4

    category_values = list(arxiv_category_mapping.values())


    total_steps = len(category_values) + 3

    with tqdm(total=total_steps, desc="ETL Pipeline Progress") as progress_bar:
        for category_value in category_values:
            merge_category_value = category_value.replace(" ", "_").replace("/", "_")
            crawler_instance = ArxivCrawler(
                query=category_value,
                total_results=total_results,
                batch_size=batch_size,
                delay=delay
            )
            df = crawler_instance.crawl()
            crawler_instance.save_to_csv(filename=f"arxiv_{total_results}_{merge_category_value}.csv")
            progress_bar.update(1)


        run_script("arxiv_migrate.py")
        progress_bar.update(1)


        category_thread = threading.Thread(target=run_script, args=("category_migrate.py",))
        field_category_thread = threading.Thread(target=run_script, args=("field_category_migrate.py",))

        category_thread.start()
        field_category_thread.start()


        category_thread.join()
        progress_bar.update(1)

        field_category_thread.join()
        progress_bar.update(1)

    print("ETL process completed.")
