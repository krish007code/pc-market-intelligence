'''
this script is for monitor data from websites scraping using dlt and beautiful soup it goes to website take the links and extracts the details
from html. It defines resources for each of the 4 websites[MD Computers, Vedant Computers, Prime AGBG and the id depot],
but before filling it cleans previous data to avoid duplicates and use transformers to extract the relevant details.
The pipeline is configured to write Parquet files to MinIO which is just a local object storage, with credentials red from .env.
'''


import os
import re
import time
import socket
import dlt
import requests
from bs4 import BeautifulSoup

# --- CONFIG ---
def get_minio_endpoint():
    try:
        socket.gethostbyname("minio")
        return "http://minio:9000"
    except socket.gaierror:
        return "http://localhost:9000"

S3_ENDPOINT = get_minio_endpoint()
BUCKET_NAME = os.environ.get("DESTINATION__S3__BUCKET_NAME", "pc-parts-bronze")
DATASET_NAME = "monitors"
COMMON_HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Fedora; Linux x86_64)"}
CRAWL_DELAY = 0.5

# --- HELPERS ---
def _clean_price(text: str) -> int:
    cleaned = re.sub(r"[^\d]", "", text.split(".")[0])
    return int(cleaned) if cleaned else 0

def _base_record(name: str, price_inr: int, url: str, source: str) -> dict:
    return {
        "name": name,
        "price_inr": price_inr,
        "url": url,
        "source": source,
        "category": "monitor",
    }

# --- MD COMPUTERS ---
@dlt.resource(name="md_monitor_links", write_disposition="replace")
def get_md_monitor_links():
    url = "https://mdcomputers.in/catalog/monitor"
    try:
        response = requests.get(url, headers=COMMON_HEADERS, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        links = []
        for p in soup.find_all("h3", class_="product-entities-title"):
            link_tag = p.find("a", href=True)
            if link_tag:
                links.append(link_tag["href"])
        return links[:20]
    except requests.exceptions.Timeout:
        print("⏰ MD Computers timed out! Skipping...")
        return []

@dlt.transformer(data_from=get_md_monitor_links, name="md_monitor_details")
def get_md_monitor_specs(product_url: str):
    time.sleep(CRAWL_DELAY)
    response = requests.get(product_url, headers=COMMON_HEADERS, timeout=15)
    soup = BeautifulSoup(response.text, "html.parser")
    name = soup.find("h1").get_text(strip=True) if soup.find("h1") else "N/A"
    price_tag = soup.find("span", class_="price-new") or soup.find("ul", class_="list-unstyled")
    price_text = price_tag.get_text(strip=True) if price_tag else "0"
    data = _base_record(name, _clean_price(price_text), product_url, "MD Computers")

    spec_table = soup.find("div", id="tab-specification")
    if spec_table:
        for row in spec_table.find_all("tr"):
            cols = row.find_all("td")
            if len(cols) == 2:
                key = cols[0].get_text(strip=True).replace(" ", "_").lower()
                data[key] = cols[1].get_text(strip=True)
    yield data

# --- VEDANT COMPUTERS ---
@dlt.resource(name="vedant_monitor_links", write_disposition="replace")
def get_vedant_monitor_links():
    url = "https://www.vedantcomputers.com/monitor"
    try:
        response = requests.get(url, headers=COMMON_HEADERS, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        links = []
        for a in soup.select("div.name a"):
            if a.has_attr("href"):
                links.append(a["href"])
        return links[:20]
    except requests.exceptions.Timeout:
        print("⏰ Vedant Computers timed out! Skipping...")
        return []

@dlt.transformer(data_from=get_vedant_monitor_links, name="vedant_monitor_details")
def get_vedant_monitor_specs(product_url: str):
    time.sleep(CRAWL_DELAY)
    response = requests.get(product_url, headers=COMMON_HEADERS, timeout=15)
    soup = BeautifulSoup(response.text, "html.parser")
    name = soup.find("h1").get_text(strip=True) if soup.find("h1") else "N/A"
    price_tag = soup.find("span", class_="price-new")
    price_text = price_tag.get_text(strip=True) if price_tag else "0"
    data = _base_record(name, _clean_price(price_text), product_url, "Vedant")

    spec_table = soup.find("table", class_="MsoNormalTable")
    if spec_table:
        for row in spec_table.find_all("tr"):
            cols = row.find_all("td")
            if len(cols) >= 2:
                key = cols[0].get_text(strip=True).replace(" ", "_").lower().replace(":", "")
                if key and key != "specification":
                    data[key] = cols[1].get_text(strip=True)
    yield data

# --- PRIMEABGB ---
@dlt.resource(name="prime_monitor_links", write_disposition="replace")
def get_prime_monitor_links():
    url = "https://www.primeabgb.com/buy-online-price-india/led-monitors/"
    try:
        response = requests.get(url, headers=COMMON_HEADERS, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        links = []
        products = soup.select("h3.product-title a") or soup.select(".product-name a")
        for a in products:
            if a.has_attr("href"):
                links.append(a["href"])
        return links[:20]
    except requests.exceptions.Timeout:
        print("⏰ PrimeABGB timed out! Skipping...")
        return []

@dlt.transformer(data_from=get_prime_monitor_links, name="prime_monitor_details")
def get_prime_monitor_specs(product_url: str):
    time.sleep(CRAWL_DELAY)
    response = requests.get(product_url, headers=COMMON_HEADERS, timeout=15)
    soup = BeautifulSoup(response.text, "html.parser")
    name = soup.find("h1").get_text(strip=True) if soup.find("h1") else "N/A"
    price_tag = (
        soup.select_one("p.price ins span.woocommerce-Price-amount")
        or soup.select_one("p.price span.woocommerce-Price-amount")
    )
    price_text = price_tag.get_text(strip=True) if price_tag else "0"
    data = _base_record(name, _clean_price(price_text), product_url, "PrimeABGB")

    spec_table = soup.find("table", class_="woocommerce-product-attributes")
    if spec_table:
        for row in spec_table.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                key = th.get_text(strip=True).replace(" ", "_").lower()
                data[key] = td.get_text(strip=True)
    yield data

# --- THEITDEPOT ---
@dlt.resource(name="itdepot_monitor_links", write_disposition="replace")
def get_itdepot_monitor_links():
    url = "https://www.theitdepot.com/Monitor"
    try:
        response = requests.get(url, headers=COMMON_HEADERS, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        links = []
        for a in soup.select(".product-thumb .name a"):
            link = a["href"]
            if not link.startswith("http"):
                link = "https://www.theitdepot.com" + (link if link.startswith("/") else "/" + link)
            links.append(link)
        return links[:20]
    except requests.exceptions.Timeout:
        print("⏰ TheITDepot timed out! Skipping...")
        return []

@dlt.transformer(data_from=get_itdepot_monitor_links, name="itdepot_monitor_details")
def get_itdepot_monitor_specs(product_url: str):
    time.sleep(CRAWL_DELAY)
    response = requests.get(product_url, headers=COMMON_HEADERS, timeout=15)
    soup = BeautifulSoup(response.text, "html.parser")
    name = soup.find("h1").get_text(strip=True) if soup.find("h1") else "N/A"
    price_tag = soup.select_one(".product-price-new")
    price_text = price_tag.get_text(strip=True) if price_tag else "0"
    data = _base_record(name, _clean_price(price_text), product_url, "TheITDepot")

    for div in soup.find_all("div", style=lambda x: x and "text-align: justify" in x):
        text = div.get_text(strip=True)
        if ":" in text:
            parts = text.split(":", 1)
            key = parts[0].strip().replace(" ", "_").lower()
            data[key] = parts[1].strip()
    yield data

# --- PIPELINE ---
def build_monitors_pipeline():
    return dlt.pipeline(
        pipeline_name="monitor_pipeline",
        destination=dlt.destinations.filesystem(
            bucket_url=f"s3://{BUCKET_NAME}/{DATASET_NAME}",
            credentials={
                "aws_access_key_id": os.environ["DESTINATION__S3__ACCESS_KEY_ID"],
                "aws_secret_access_key": os.environ["DESTINATION__S3__SECRET_ACCESS_KEY"],
                "endpoint_url": S3_ENDPOINT,
            },
        ),
        dataset_name="bronze_layer",
    )

if __name__ == "__main__":
    pipeline = build_monitors_pipeline()
    sources = [
        ("MD Computers",  get_md_monitor_links     | get_md_monitor_specs),
        ("Vedant",        get_vedant_monitor_links  | get_vedant_monitor_specs),
        ("PrimeABGB",     get_prime_monitor_links   | get_prime_monitor_specs),
        ("TheITDepot",    get_itdepot_monitor_links | get_itdepot_monitor_specs),
    ]

    print(f"📡 Using MinIO endpoint: {S3_ENDPOINT}")
    for label, resource in sources:
        pipeline.run(resource, loader_file_format="parquet")
        print(f"{label} ingested.")