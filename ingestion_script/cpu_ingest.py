'''
this script is written by claude ai to mimic the logic used in ingestion.py but for cpu data instead of monitors.
It scrapes the same 2 websites (MD Computers and Prime ABGB) but goes to their CPU/Processor category pages, extracts product links,
and then visits each product page to extract details like name, price, brand, and highlights. 
The cleaned data is then written as Parquet files to MinIO, similar to the monitor pipeline.
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
DATASET_NAME = "cpus"
COMMON_HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Fedora; Linux x86_64)"}
CRAWL_DELAY = 0.5

# Keywords that indicate combo/bundle deals — skip these
COMBO_KEYWORDS = ["combo", "bundle", "kit with", "+ motherboard", "+ mobo"]

# --- HELPERS ---
def _clean_price(text: str) -> int:
    cleaned = re.sub(r"[^\d]", "", text.split(".")[0])
    return int(cleaned) if cleaned else 0

def _detect_brand(name: str) -> str:
    name_lower = name.lower()
    if "intel" in name_lower or "core i" in name_lower or "xeon" in name_lower or "pentium" in name_lower:
        return "Intel"
    elif "amd" in name_lower or "ryzen" in name_lower or "threadripper" in name_lower or "athlon" in name_lower:
        return "AMD"
    return "Unknown"

def _is_combo(name: str) -> bool:
    name_lower = name.lower()
    return any(kw in name_lower for kw in COMBO_KEYWORDS)

def _base_record(name: str, price_inr: int, url: str, source: str) -> dict:
    return {
        "name": name,
        "price_inr": price_inr,
        "url": url,
        "source": source,
        "category": "cpu",
        "brand": _detect_brand(name),
    }

# --- RESOURCES ---
@dlt.resource(name="md_cpu_links", write_disposition="replace")
def get_md_cpu_links():
    url = "https://mdcomputers.in/catalog/processor"
    try:
        response = requests.get(url, headers=COMMON_HEADERS, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        links = []
        for p in soup.find_all("h3", class_="product-entities-title"):
            link_tag = p.find("a", href=True)
            if link_tag:
                name = link_tag.get_text(strip=True)
                # Filter combos at link stage if name is available
                if not _is_combo(name):
                    links.append(link_tag["href"])
        return links[:20]
    except requests.exceptions.Timeout:
        print("⏰ MD Computers timed out! Skipping...")
        return []

@dlt.transformer(data_from=get_md_cpu_links, name="md_cpu_details")
def get_md_cpu_specs(product_url: str):
    time.sleep(CRAWL_DELAY)
    response = requests.get(product_url, headers=COMMON_HEADERS, timeout=15)
    soup = BeautifulSoup(response.text, "html.parser")
    name = soup.find("h1").get_text(strip=True) if soup.find("h1") else "N/A"

    # Skip combos that slipped through link-stage filtering
    if _is_combo(name):
        return

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

@dlt.resource(name="prime_cpu_links", write_disposition="replace")
def get_prime_cpu_links():
    url = "https://www.primeabgb.com/buy-online-price-india/cpu-processor/"
    response = requests.get(url, headers=COMMON_HEADERS, timeout=15)
    soup = BeautifulSoup(response.text, "html.parser")
    products = soup.select("h3.product-title a")
    for a in products:
        if a.has_attr("href"):
            name = a.get_text(strip=True)
            if not _is_combo(name):
                yield a["href"]

@dlt.transformer(data_from=get_prime_cpu_links, name="prime_cpu_details")
def get_prime_cpu_specs(product_url: str):
    time.sleep(CRAWL_DELAY)
    response = requests.get(product_url, headers=COMMON_HEADERS, timeout=15)
    soup = BeautifulSoup(response.text, "html.parser")
    name = soup.find("h1").get_text(strip=True) if soup.find("h1") else "N/A"

    if _is_combo(name):
        return

    price_tag = soup.select_one("p.price span.woocommerce-Price-amount")
    price_text = price_tag.get_text(strip=True) if price_tag else "0"
    data = _base_record(name, _clean_price(price_text), product_url, "PrimeABGB")
    highlights = soup.find("div", class_="woocommerce-product-details__short-description")
    if highlights:
        data["highlights"] = highlights.get_text(separator=" | ", strip=True)
    yield data

# --- PIPELINE ---
def build_cpu_pipeline():
    return dlt.pipeline(
        pipeline_name="cpu_pipeline",
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
    pipeline = build_cpu_pipeline()
    sources = [
        ("MD CPUs", get_md_cpu_links | get_md_cpu_specs),
        ("Prime CPUs", get_prime_cpu_links | get_prime_cpu_specs),
    ]

    print(f"📡 Using MinIO endpoint: {S3_ENDPOINT}")
    for label, resource in sources:
        pipeline.run(resource, loader_file_format="parquet")
        print(f"✅ {label} ingested.")
