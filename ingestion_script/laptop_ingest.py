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
    """Detect if running in Docker network ('minio') or locally."""
    try:
        socket.gethostbyname("minio")
        return "http://minio:9000"
    except socket.gaierror:
        return "http://localhost:9000"

S3_ENDPOINT  = _get_minio_endpoint_url()
MINIO_HOST   = S3_ENDPOINT.replace("http://", "").replace("https://", "")
MINIO_SECURE = S3_ENDPOINT.startswith("https")

BUCKET_NAME  = os.environ.get("DESTINATION__S3__BUCKET_NAME", "pc-parts-bronze")
DATASET_NAME = "laptops"

# Credential Safety: Supports both Kestra and local dlt env patterns
MINIO_USER = os.environ.get("MINIO_USER", os.environ.get("DESTINATION__S3__ACCESS_KEY_ID", ""))
MINIO_PASS = os.environ.get("MINIO_PASSWORD", os.environ.get("DESTINATION__S3__SECRET_ACCESS_KEY", ""))

COMMON_HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Fedora; Linux x86_64)"}
CRAWL_DELAY    = 0.5
LAPTOP_LIMIT   = 30

# ──────────────────────────────────────────────────────────────────────────────
# STEP 0 – IDEMPOTENT BUCKET SETUP
# ──────────────────────────────────────────────────────────────────────────────

def ensure_bucket_exists() -> None:
    """Ensures MinIO is ready before ingestion starts."""
    print(f"📡 Connecting to MinIO at {MINIO_HOST} …")

    if not MINIO_USER or not MINIO_PASS:
        print("⚠️  MINIO credentials not set – skipping bucket check.")
        return

    client = Minio(MINIO_HOST, access_key=MINIO_USER, secret_key=MINIO_PASS, secure=MINIO_SECURE)
    try:
        if client.bucket_exists(BUCKET_NAME):
            print(f"✅ Bucket '{BUCKET_NAME}' found.")
        else:
            client.make_bucket(BUCKET_NAME)
            print(f"🪣  Bucket '{BUCKET_NAME}' created successfully.")
    except S3Error as exc:
        print(f"⚠️  MinIO Verification Error: {exc}")

# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def _clean_price(text: str) -> int:
    cleaned = re.sub(r"[^\d]", "", text.strip().split(".")[0])
    return int(cleaned) if cleaned else 0

def _base_record(name: str, price_inr: int, url: str, source: str) -> dict:
    return {
        "name":      name,
        "price_inr": price_inr,
        "url":       url,
        "source":    source,
        "category":  "laptop",
    }

# ──────────────────────────────────────────────────────────────────────────────
# SOURCES
# ──────────────────────────────────────────────────────────────────────────────

@dlt.resource(name="md_laptop_links", write_disposition="replace")
def get_md_laptop_links():
    listing_url = "https://mdcomputers.in/catalog/laptop"
    print(f"🔍 Fetching MD Computers Laptops: {listing_url}")
    try:
        resp = requests.get(listing_url, headers=COMMON_HEADERS, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        links = [h3.find("a")["href"] for h3 in soup.find_all("h3", class_="product-entities-title") if h3.find("a")]
        return links[:LAPTOP_LIMIT]
    except Exception as exc:
        print(f"⚠️  MD Computers Listing Error: {exc}")
        return []

@dlt.transformer(data_from=get_md_laptop_links, name="md_laptop_details")
def get_md_laptop_specs(product_url: str):
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
        print(f"⚠️  Skipping MD Product: {exc}")

@dlt.resource(name="prime_laptop_links", write_disposition="replace")
def get_prime_laptop_links():
    listing_url = "https://www.primeabgb.com/buy-online-price-india/gaming-laptop/"
    print(f"🔍 Fetching PrimeABGB Laptops: {listing_url}")
    try:
        resp = requests.get(listing_url, headers=COMMON_HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        links = [a["href"] for a in soup.select("h3.product-title a") if a.has_attr("href")]
        return links[:LAPTOP_LIMIT]
    except Exception as exc:
        print(f"⚠️  PrimeABGB Listing Error: {exc}")
        return []

@dlt.transformer(data_from=get_prime_laptop_links, name="prime_laptop_details")
def get_prime_laptop_specs(product_url: str):
    time.sleep(CRAWL_DELAY)
    try:
        resp = requests.get(product_url, headers=COMMON_HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        
        name = soup.find("h1").get_text(strip=True) if soup.find("h1") else "N/A"
        price_tag = soup.select_one("p.price span.woocommerce-Price-amount")
        price_text = price_tag.get_text(strip=True) if price_tag else "0"
        
        data = _base_record(name, _clean_price(price_text), product_url, "PrimeABGB")
        
        highlights = soup.find("div", class_="woocommerce-product-details__short-description")
        if highlights:
            data["highlights"] = highlights.get_text(separator=" | ", strip=True)
            
        print(f"  📄 PRM | {name[:50]} | ₹{data['price_inr']:,}")
        yield data
    except Exception as exc:
        print(f"⚠️  Skipping Prime Product: {exc}")

# ──────────────────────────────────────────────────────────────────────────────
# PIPELINE
# ──────────────────────────────────────────────────────────────────────────────

def build_laptop_pipeline() -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name="laptop_pipeline",
        destination=dlt.destinations.filesystem(
            bucket_url=f"s3://{BUCKET_NAME}/{DATASET_NAME}",
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
    print("🚀 Laptop Ingestion Pipeline – Starting")
    print("=" * 60)
    
    ensure_bucket_exists()
    pipeline = build_laptop_pipeline()
    
    sources = [
        ("MD Laptops", get_md_laptop_links | get_md_laptop_specs),
        ("Prime Laptops", get_prime_laptop_links | get_prime_laptop_specs),
    ]

    for label, resource in sources:
        print(f"\n⏳ Ingesting: {label} …")
        pipeline.run(resource, loader_file_format="parquet")
        print(f"✅ {label} ingestion finished.")

    print("\n🏁 All Laptop sources ingested successfully.")