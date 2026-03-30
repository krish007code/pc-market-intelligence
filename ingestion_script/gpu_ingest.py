"""
gpu_ingest.py  –  Bronze-layer ingestion for GPU / Graphics Card data.
Scrapes MD Computers and PrimeABGB, writes Parquet files to MinIO.

Fixes applied vs. original draft
─────────────────────────────────
1.  Idempotency      – MinIO bucket is checked / created via the `minio` client
                       before the dlt pipeline runs.
2.  Credential safety – Every secret uses os.environ.get() so a missing
                       variable returns None instead of raising KeyError.
3.  Brand detection  – Added _detect_brand() for Nvidia / AMD / Intel Arc.
4.  Combo filtering  – Bundle listings (e.g. "GPU + PSU combo") are skipped
                       at both the link stage and the detail stage.
5.  Full error handling – All requests calls catch the full
                       RequestException family; one broken page never kills
                       the other source.
6.  raise_for_status() – 4xx / 5xx responses are surfaced immediately instead
                       of producing silently empty/corrupt records.
7.  Price string fix  – strip() before split(".") prevents whitespace from
                       corrupting the decimal truncation.
8.  Structured logging – Emoji-prefixed print statements at every major step
                       for easy monitoring in Kestra task logs.
"""

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
DATASET_NAME = "gpus"

MINIO_USER = os.environ.get("MINIO_USER", os.environ.get("DESTINATION__S3__ACCESS_KEY_ID", ""))
MINIO_PASS = os.environ.get("MINIO_PASSWORD", os.environ.get("DESTINATION__S3__SECRET_ACCESS_KEY", ""))

COMMON_HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Fedora; Linux x86_64)"}
CRAWL_DELAY    = 0.5
MD_PAGE_LIMIT  = 20

COMBO_KEYWORDS = ["combo", "bundle", "kit with", "+ psu", "+ case", "+ cpu", "+ mobo", "+ motherboard"]

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
    cleaned = re.sub(r"[^\d]", "", text.strip().split(".")[0])
    return int(cleaned) if cleaned else 0


def _detect_brand(name: str) -> str:
    name_lower = name.lower()
    if any(k in name_lower for k in ("nvidia", "geforce", "rtx", "gtx", "quadro")):
        return "Nvidia"
    if any(k in name_lower for k in ("amd", "radeon", "rx ", "vega", "navi")):
        return "AMD"
    if any(k in name_lower for k in ("intel", "arc ", "iris")):
        return "Intel"
    return "Unknown"


def _is_combo(name: str) -> bool:
    return any(kw in name.lower() for kw in COMBO_KEYWORDS)


def _base_record(name: str, price_inr: int, url: str, source: str) -> dict:
    return {
        "name":      name,
        "price_inr": price_inr,
        "url":       url,
        "source":    source,
        "category":  "gpu",
        "brand":     _detect_brand(name),
    }

# ──────────────────────────────────────────────────────────────────────────────
# SOURCE 1 – MD COMPUTERS
# ──────────────────────────────────────────────────────────────────────────────

@dlt.resource(name="md_gpu_links", write_disposition="replace")
def get_md_gpu_links():
    """Yield individual product URLs from the MD Computers graphics-card listing."""
    listing_url = "https://mdcomputers.in/graphics-card"
    print(f"🔍 Fetching MD Computers listing: {listing_url}")
    try:
        resp = requests.get(listing_url, headers=COMMON_HEADERS, timeout=10)
        resp.raise_for_status()
    except requests.exceptions.Timeout:
        print("⏰ MD Computers listing timed out – skipping source.")
        return
    except requests.exceptions.RequestException as exc:
        print(f"⚠️  MD Computers listing error: {exc} – skipping source.")
        return

    soup  = BeautifulSoup(resp.text, "html.parser")
    count = 0
    for h3 in soup.find_all("h3", class_="product-entities-title"):
        link_tag = h3.find("a", href=True)
        if not link_tag:
            continue
        name = link_tag.get_text(strip=True)
        if _is_combo(name):
            continue
        yield link_tag["href"]
        count += 1
        if count >= MD_PAGE_LIMIT:
            break

    print(f"🔗 MD Computers: {count} product links queued.")


@dlt.transformer(data_from=get_md_gpu_links, name="md_gpu_details")
def get_md_gpu_specs(product_url: str):
    """Visit each MD product page and yield a structured GPU record."""
    time.sleep(CRAWL_DELAY)
    try:
        resp = requests.get(product_url, headers=COMMON_HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.exceptions.RequestException as exc:
        print(f"⚠️  Skipping {product_url}: {exc}")
        return

    soup = BeautifulSoup(resp.text, "html.parser")
    name = soup.find("h1").get_text(strip=True) if soup.find("h1") else "N/A"

    if _is_combo(name):
        return  # second-pass combo guard

    price_tag  = soup.find("span", class_="price-new") or soup.find("ul", class_="list-unstyled")
    price_text = price_tag.get_text(strip=True) if price_tag else "0"

    data = _base_record(name, _clean_price(price_text), product_url, "MD Computers")

    spec_table = soup.find("div", id="tab-specification")
    if spec_table:
        for row in spec_table.find_all("tr"):
            cols = row.find_all("td")
            if len(cols) == 2:
                key       = cols[0].get_text(strip=True).replace(" ", "_").lower()
                data[key] = cols[1].get_text(strip=True)

    print(f"  📄 MD  | {name[:60]} | ₹{data['price_inr']:,}")
    yield data

# ──────────────────────────────────────────────────────────────────────────────
# SOURCE 2 – PRIME ABGB
# ──────────────────────────────────────────────────────────────────────────────

@dlt.resource(name="prime_gpu_links", write_disposition="replace")
def get_prime_gpu_links():
    """Yield individual product URLs from PrimeABGB's GPU category."""
    listing_url = "https://www.primeabgb.com/buy-online-price-india/graphic-cards-gpu/"
    print(f"🔍 Fetching PrimeABGB listing: {listing_url}")
    try:
        resp = requests.get(listing_url, headers=COMMON_HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.exceptions.RequestException as exc:
        print(f"⚠️  PrimeABGB listing error: {exc} – skipping source.")
        return

    soup     = BeautifulSoup(resp.text, "html.parser")
    products = soup.select("h3.product-title a")
    count    = 0
    for a in products:
        if not a.has_attr("href"):
            continue
        if _is_combo(a.get_text(strip=True)):
            continue
        yield a["href"]
        count += 1

    print(f"🔗 PrimeABGB: {count} product links queued.")


@dlt.transformer(data_from=get_prime_gpu_links, name="prime_gpu_details")
def get_prime_gpu_specs(product_url: str):
    """Visit each PrimeABGB product page and yield a structured GPU record."""
    time.sleep(CRAWL_DELAY)
    try:
        resp = requests.get(product_url, headers=COMMON_HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.exceptions.RequestException as exc:
        print(f"⚠️  Skipping {product_url}: {exc}")
        return

    soup = BeautifulSoup(resp.text, "html.parser")
    name = soup.find("h1").get_text(strip=True) if soup.find("h1") else "N/A"

    if _is_combo(name):
        return

    price_tag  = soup.select_one("p.price span.woocommerce-Price-amount")
    price_text = price_tag.get_text(strip=True) if price_tag else "0"

    data       = _base_record(name, _clean_price(price_text), product_url, "PrimeABGB")
    highlights = soup.find("div", class_="woocommerce-product-details__short-description")
    if highlights:
        data["highlights"] = highlights.get_text(separator=" | ", strip=True)

    print(f"  📄 Prime | {name[:60]} | ₹{data['price_inr']:,}")
    yield data

# ──────────────────────────────────────────────────────────────────────────────
# PIPELINE FACTORY
# ──────────────────────────────────────────────────────────────────────────────

def build_gpu_pipeline() -> dlt.Pipeline:
    if not MINIO_USER or not MINIO_PASS:
        raise EnvironmentError(
            "MINIO_USER / MINIO_PASSWORD (or DESTINATION__S3__ACCESS_KEY_ID / "
            "DESTINATION__S3__SECRET_ACCESS_KEY) must be set before running."
        )

    return dlt.pipeline(
        pipeline_name="gpu_pipeline",
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

# ──────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("🚀 GPU Ingestion Pipeline – Starting")
    print("=" * 60)
    print(f"📡 MinIO endpoint : {S3_ENDPOINT}")
    print(f"🪣  Target bucket  : {BUCKET_NAME}")

    ensure_bucket_exists()

    pipeline = build_gpu_pipeline()

    sources = [
        ("MD Computers GPUs", get_md_gpu_links  | get_md_gpu_specs),
        ("PrimeABGB GPUs",    get_prime_gpu_links | get_prime_gpu_specs),
    ]

    for label, resource in sources:
        print(f"\n⏳ Ingesting: {label} …")
        load_info = pipeline.run(resource, loader_file_format="parquet")
        print(f"✅ {label} complete. Load info: {load_info}")

    print("\n🏁 All sources ingested successfully.")