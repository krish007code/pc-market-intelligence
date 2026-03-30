import os
import re
import time
import socket

import dlt
import requests
from bs4 import BeautifulSoup
from minio import Minio
from minio.error import S3Error

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────

def _get_minio_endpoint_url() -> str:
    """Prefer the Docker service name; fall back to localhost for local dev."""
    try:
        socket.gethostbyname("minio")
        return "http://minio:9000"
    except socket.gaierror:
        return "http://localhost:9000"

S3_ENDPOINT  = _get_minio_endpoint_url()
MINIO_HOST   = S3_ENDPOINT.replace("http://", "").replace("https://", "")
MINIO_SECURE = S3_ENDPOINT.startswith("https")

BUCKET_NAME  = os.environ.get("DESTINATION__S3__BUCKET_NAME", "pc-parts-bronze")
DATASET_NAME = "monitors"

# Credential Safety: Check both Kestra and local env patterns
MINIO_USER = os.environ.get("MINIO_USER", os.environ.get("DESTINATION__S3__ACCESS_KEY_ID", ""))
MINIO_PASS = os.environ.get("MINIO_PASSWORD", os.environ.get("DESTINATION__S3__SECRET_ACCESS_KEY", ""))

COMMON_HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Fedora; Linux x86_64)"}
CRAWL_DELAY    = 0.5
PAGE_LIMIT     = 20

# ──────────────────────────────────────────────────────────────────────────────
# STEP 0 – IDEMPOTENT BUCKET SETUP
# ──────────────────────────────────────────────────────────────────────────────

def ensure_bucket_exists() -> None:
    """Create the MinIO bucket if it does not already exist."""
    print(f"📡 Connecting to MinIO at {MINIO_HOST} …")

    if not MINIO_USER or not MINIO_PASS:
        print("⚠️  MINIO_USER / MINIO_PASSWORD not set – skipping bucket check.")
        return

    client = Minio(MINIO_HOST, access_key=MINIO_USER, secret_key=MINIO_PASS,
                   secure=MINIO_SECURE)
    try:
        if client.bucket_exists(BUCKET_NAME):
            print(f"✅ Bucket '{BUCKET_NAME}' already exists.")
        else:
            client.make_bucket(BUCKET_NAME)
            print(f"🪣  Bucket '{BUCKET_NAME}' created.")
    except S3Error as exc:
        print(f"⚠️  Could not verify/create bucket: {exc}")

# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def _clean_price(text: str) -> int:
    """Robust price cleaning with whitespace stripping."""
    cleaned = re.sub(r"[^\d]", "", text.strip().split(".")[0])
    return int(cleaned) if cleaned else 0


def _base_record(name: str, price_inr: int, url: str, source: str) -> dict:
    return {
        "name":      name,
        "price_inr": price_inr,
        "url":       url,
        "source":    source,
        "category":  "monitor",
    }

# ──────────────────────────────────────────────────────────────────────────────
# SOURCE 1 – MD COMPUTERS
# ──────────────────────────────────────────────────────────────────────────────

@dlt.resource(name="md_monitor_links", write_disposition="replace")
def get_md_monitor_links():
    listing_url = "https://mdcomputers.in/catalog/monitor"
    print(f"🔍 Fetching MD Computers listing: {listing_url}")
    try:
        resp = requests.get(listing_url, headers=COMMON_HEADERS, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        links = [p.find("a")["href"] for p in soup.find_all("h3", class_="product-entities-title") if p.find("a")]
        return links[:PAGE_LIMIT]
    except requests.exceptions.RequestException as exc:
        print(f"⚠️  MD Computers error: {exc}")
        return []

@dlt.transformer(data_from=get_md_monitor_links, name="md_monitor_details")
def get_md_monitor_specs(product_url: str):
    time.sleep(CRAWL_DELAY)
    try:
        resp = requests.get(product_url, headers=COMMON_HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
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
        
        print(f"  📄 MD  | {name[:50]} | ₹{data['price_inr']:,}")
        yield data
    except Exception as exc:
        print(f"⚠️  Skipping MD product {product_url}: {exc}")

# ──────────────────────────────────────────────────────────────────────────────
# SOURCE 2 – VEDANT COMPUTERS
# ──────────────────────────────────────────────────────────────────────────────

@dlt.resource(name="vedant_monitor_links", write_disposition="replace")
def get_vedant_monitor_links():
    listing_url = "https://www.vedantcomputers.com/monitor"
    print(f"🔍 Fetching Vedant listing: {listing_url}")
    try:
        resp = requests.get(listing_url, headers=COMMON_HEADERS, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        links = [a["href"] for a in soup.select("div.name a") if a.has_attr("href")]
        return links[:PAGE_LIMIT]
    except requests.exceptions.RequestException as exc:
        print(f"⚠️  Vedant error: {exc}")
        return []

@dlt.transformer(data_from=get_vedant_monitor_links, name="vedant_monitor_details")
def get_vedant_monitor_specs(product_url: str):
    time.sleep(CRAWL_DELAY)
    try:
        resp = requests.get(product_url, headers=COMMON_HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
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
        
        print(f"  📄 VED | {name[:50]} | ₹{data['price_inr']:,}")
        yield data
    except Exception as exc:
        print(f"⚠️  Skipping Vedant product {product_url}: {exc}")

# ──────────────────────────────────────────────────────────────────────────────
# SOURCE 3 – PRIME ABGB
# ──────────────────────────────────────────────────────────────────────────────

@dlt.resource(name="prime_monitor_links", write_disposition="replace")
def get_prime_monitor_links():
    listing_url = "https://www.primeabgb.com/buy-online-price-india/led-monitors/"
    print(f"🔍 Fetching PrimeABGB listing: {listing_url}")
    try:
        resp = requests.get(listing_url, headers=COMMON_HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        links = [a["href"] for a in (soup.select("h3.product-title a") or soup.select(".product-name a")) if a.has_attr("href")]
        return links[:PAGE_LIMIT]
    except requests.exceptions.RequestException as exc:
        print(f"⚠️  PrimeABGB error: {exc}")
        return []

@dlt.transformer(data_from=get_prime_monitor_links, name="prime_monitor_details")
def get_prime_monitor_specs(product_url: str):
    time.sleep(CRAWL_DELAY)
    try:
        resp = requests.get(product_url, headers=COMMON_HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        name = soup.find("h1").get_text(strip=True) if soup.find("h1") else "N/A"
        price_tag = soup.select_one("p.price ins span.woocommerce-Price-amount") or soup.select_one("p.price span.woocommerce-Price-amount")
        price_text = price_tag.get_text(strip=True) if price_tag else "0"
        
        data = _base_record(name, _clean_price(price_text), product_url, "PrimeABGB")
        spec_table = soup.find("table", class_="woocommerce-product-attributes")
        if spec_table:
            for row in spec_table.find_all("tr"):
                th, td = row.find("th"), row.find("td")
                if th and td:
                    key = th.get_text(strip=True).replace(" ", "_").lower()
                    data[key] = td.get_text(strip=True)
        
        print(f"  📄 PRM | {name[:50]} | ₹{data['price_inr']:,}")
        yield data
    except Exception as exc:
        print(f"⚠️  Skipping Prime product {product_url}: {exc}")

# ──────────────────────────────────────────────────────────────────────────────
# PIPELINE FACTORY & MAIN
# ──────────────────────────────────────────────────────────────────────────────

def build_monitors_pipeline() -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name="monitor_pipeline",
        destination=dlt.destinations.filesystem(
            bucket_url=f"s3://{BUCKET_NAME}",
            credentials={
                "aws_access_key_id":     MINIO_USER,
                "aws_secret_access_key": MINIO_PASS,
                "endpoint_url":          S3_ENDPOINT,
            },
        ),
        dataset_name="bronze_layer",
    )

if __name__ == "__main__":
    print("=" * 60)
    print("🚀 Monitor Ingestion Pipeline – Starting")
    print("=" * 60)
    
    ensure_bucket_exists()
    pipeline = build_monitors_pipeline()
    
    sources = [
        ("MD Computers", get_md_monitor_links     | get_md_monitor_specs),
        ("Vedant",       get_vedant_monitor_links  | get_vedant_monitor_specs),
        ("PrimeABGB",    get_prime_monitor_links   | get_prime_monitor_specs),
    ]

    for label, resource in sources:
        print(f"\n⏳ Ingesting: {label} …")
        pipeline.run(resource, loader_file_format="parquet")
        print(f"✅ {label} complete.")

    print("\n🏁 All Monitor sources ingested successfully.")