import time
import requests
from bs4 import BeautifulSoup
from loguru import logger
from tqdm import tqdm
import json

# config
BASE_URL   = "https://mdcomputers.in/index.php?route=extension/ultimate_filters/module/filter&module_id=3188&stock_status=in&category_id=76"
PAGE_PARAM = "%3Bpage%3D5&page={}"
MAX_PAGES  = 9

HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Fedora; Linux x86_64)"}
DELAY   = 1.5

def get_product_urls():
    urls = []

    for page in range(1, MAX_PAGES + 1):
        url = BASE_URL + PAGE_PARAM.format(page)
        resp = requests.get(url, headers=HEADERS, timeout=10)

        if resp.status_code != 200:
            logger.warning(f"Failed to fetch page {page}: {resp.status_code}")
            continue                     # ← was missing indentation

        soup = BeautifulSoup(resp.text, "lxml")
        links = [a["href"] for a in soup.select("div.product-grid-item.product-hover-icons.product-with-labels.product-no-swatches a")]
        if not links:
            break
        
        urls.extend(links)
        time.sleep(DELAY)

    return urls

def scrape_product(url: str) -> dict | None:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        name  = soup.select_one("h1.product-name-title").get_text(strip=True)
        price = soup.select_one("h2.special-price").get_text(strip=True)

        specs = {}
        for row in soup.select("table.table tr"):
            cols = row.find_all("td")
            if len(cols) == 2:
                key = cols[0].get_text(strip=True).lower().replace(" ", "_")
                specs[key] = cols[1].get_text(strip=True)

        return {"name": name, "price": price, "url": url, **specs}

    except Exception as e:
        logger.error(f"Error scraping {url}: {e}")
        return None

def scrape_all() -> list[dict]:
    product_urls = get_product_urls()
    logger.info(f"Total URLs collected: {len(product_urls)}")

    records = []
    for url in tqdm(product_urls, desc="Scraping products"):
        data = scrape_product(url)
        if data:
            records.append(data)
        time.sleep(DELAY)

    logger.success(f"Done. {len(records)} products scraped.")
    return records

if __name__ == "__main__":
    data = scrape_all()
