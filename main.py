import requests
import sqlite3
import threading
import time
from bs4 import BeautifulSoup

class Scraper:
    def __init__(self, max_pages, pages_range=None, async_option=False, fastness=1):
        self.session = requests.Session()
        self.max_pages = max_pages
        self.pages_range = pages_range or (1, max_pages)
        self.async_option = async_option
        self.fastness = fastness
        self.db = Database()
        self.scrapedPages = 0
        self.lock = threading.Lock()

    def scrape(self):
        if self.async_option:
            self._scrape_async()
        else:
            self._scrape_sync()

    def _scrape_sync(self):
        for page in range(self.pages_range[0], self.pages_range[1] + 1):
            self._scrape_page(page)

    def _scrape_async(self):
        def _scrape_range(start, end):
            print(f"Scraping pages {start} to {end}..")
            for page in range(start, end + 1):
                print(f"Scraping page {page}..")
                self._scrape_page(page)

        threads = []
        for start_page in range(self.pages_range[0], self.pages_range[1] + 1, self.fastness):
            end_page = start_page + self.fastness - 1
            thread = threading.Thread(target=_scrape_range, args=(start_page, end_page))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()


    def _scrape_page(self, page):
        tempDb = Database()
        headers =  {
            'authority': 'www.amazon.com',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'max-age=0',
            'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        }
        # print(f"Scraping page {page}")
        url = f'https://www.amazon.com/s?i=electronics&s=price-asc-rank&dc&ref=sr_st_price-asc-rank&page={page}'
        response = self.session.get(url, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            products = soup.find_all('div', {'data-asin': lambda value: value and len(value) > 1})
            for product in products:
                try:
                    prodMeta1 = product.find('img', {'data-image-latency': 's-product-image'})
                    title = prodMeta1.get('alt').replace("Sponsored Ad", "")
                    image = prodMeta1.get('src')
                    price = product.find('span', {'class': 'a-price'}).find('span', {'class': 'a-offscreen'}).text
                    asin = product.get('data-asin')
                    tempDb.add_product(title, price, image, asin)
                except Exception as e:
                    print(f"Error on page {page}: {e}")
        else:
            print(f"Failed to scrape page {page}: Status code {response.status_code}")

        self.scrapedPages += 1

class Database:
    def __init__(self):
        # use self.lock to prevent multiple threads from accessing the database at the same time
        self.lock = threading.Lock()
        
        self.connection = sqlite3.connect("asins.db")
        self.cursor = self.connection.cursor()
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS products
                              (id INTEGER PRIMARY KEY, title TEXT, price REAL, image TEXT, asin TEXT)''')
        self.connection.commit()

    def add_product(self, title, price, image, asin):
        # print(f"Adding {title} to the database....")
        # use self.lock to prevent multiple threads from accessing the database at the same time
        with self.lock:
            self.cursor.execute('''INSERT INTO products (title, price, image, asin)
                                VALUES (?, ?, ?, ?)''', (title, price, image, asin))
            self.connection.commit()



    def remove_duplicates(self):
        # use self.lock to prevent multiple threads from accessing the database at the same time
        with self.lock:
            self.cursor.execute('''DELETE FROM products WHERE id NOT IN (SELECT MIN(id) FROM products GROUP BY asin)''')
            self.connection.commit()

    def close(self):
        self.connection.close()

if __name__ == "__main__":
    # Example usage
    print("Scraping...")
    scraper = Scraper(max_pages=2, pages_range=(80, 100), async_option=True, fastness=1)
    scraper.scrape()
    scraper.db.remove_duplicates()
    scraper.db.close()
    print("Done")
