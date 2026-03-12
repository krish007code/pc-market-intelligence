import os
import re
import time

import dlt
import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Environment-variable driven credentials (set these in your Docker env / Kestra)
# ---------------------------------------------------------------------------
# DESTINATION__S3__ACCESS_KEY_ID
# DESTINATION__S3__SECRET_ACCESS_KEY
# DESTINATION__S3__BUCKET_NAME
#
# dlt picks up DESTINATION__S3__* vars automatically; we also expose them here
# for the explicit endpoint override required by MinIO.
# ---------------------------------------------------------------------------

S3_ENDPOINT = "http://minio:9000"          # Internal Docker network
BUCKET_NAME  = os.environ["DESTINATION__S3__BUCKET_NAME"]   # e.g. "pc-parts-bronze"
DATASET_NAME = "monitors"                  # sub-folder inside the bucket

COMMON_HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Fedora; Linux x86_64)"}
CRAWL_DELAY    = 0.5


# ===========================================================================
# Helper
# ===========================================================================

def _clean_price(text: str) -> int:
    """Strip everything except digits from a price string."""
    cleaned = re.sub(r"[^\d]", "", text.split(".")[0])
    return int(cleaned) if cleaned else 0


def _base_record(name: str, price_inr: int, url: str, source: str) -> dict:
    """Return the mandatory fields present in every record."""
    return {
        "name":       name,
        "price_inr":  price_inr,
        "url":        url,
        "source":     source,
        "category":   "monitor",   # ← enables multi-component filtering later
    }


# ===========================================================================
# MD Computers
# ===========================================================================

@dlt.resource(name="md_links", write_disposition="replace")
def get_md_links():
    url = "https://mdcomputers.in/catalog/monitor"
    response = requests.get(url, headers=COMMON_HEADERS, timeout=15)
    soup = BeautifulSoup(response.text, "html.parser")
    for p in soup.find_all("h3", class_="product-entities-title"):
        link_tag = p.find("a", href=True)
        if link_tag:
            yield link_tag["href"]


@dlt.transformer(data_from=get_md_links, name="md_monitor_details")
def get_md_specs(product_url: str):
    time.sleep(CRAWL_DELAY)
    response = requests.get(product_url, headers=COMMON_HEADERS, timeout=15)
    soup = BeautifulSoup(response.text, "html.parser")

    name       = soup.find("h1").get_text(strip=True) if soup.find("h1") else "N/A"
    price_tag  = soup.find("span", class_="price-new") or soup.find("ul", class_="list-unstyled")
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


# ===========================================================================
# Vedant Computers
# ===========================================================================

@dlt.resource(name="vedant_links", write_disposition="replace")
def get_vedant_links():
    url = "https://www.vedantcomputers.com/monitor"
    response = requests.get(url, headers=COMMON_HEADERS, timeout=15)
    soup = BeautifulSoup(response.text, "html.parser")
    for a in soup.select("div.name a"):
        if a.has_attr("href"):
            yield a["href"]


@dlt.transformer(data_from=get_vedant_links, name="vedant_monitor_details")
def get_vedant_specs(product_url: str):
    time.sleep(CRAWL_DELAY)
    response = requests.get(product_url, headers=COMMON_HEADERS, timeout=15)
    soup = BeautifulSoup(response.text, "html.parser")

    name       = soup.find("h1").get_text(strip=True) if soup.find("h1") else "N/A"
    price_tag  = soup.find("span", class_="price-new")
    price_text = price_tag.get_text(strip=True) if price_tag else "0"

    data = _base_record(name, _clean_price(price_text), product_url, "Vedant")

    spec_table = soup.find("table", class_="MsoNormalTable")
    if spec_table:
        for row in spec_table.find_all("tr"):
            cols = row.find_all("td")
            if len(cols) >= 2:
                key = (
                    cols[0].get_text(strip=True)
                    .replace(" ", "_").lower().replace(":", "")
                )
                if key and key != "specification":
                    data[key] = cols[1].get_text(strip=True)

    yield data


# ===========================================================================
# PrimeABGB
# ===========================================================================

@dlt.resource(name="prime_links", write_disposition="replace")
def get_prime_links():
    url = "https://www.primeabgb.com/buy-online-price-india/led-monitors/"
    response = requests.get(url, headers=COMMON_HEADERS, timeout=15)
    soup = BeautifulSoup(response.text, "html.parser")
    products = soup.select("h3.product-title a") or soup.select(".product-name a")
    for a in products:
        if a.has_attr("href"):
            yield a["href"]


@dlt.transformer(data_from=get_prime_links, name="prime_monitor_details")
def get_prime_specs(product_url: str):
    time.sleep(CRAWL_DELAY)
    response = requests.get(product_url, headers=COMMON_HEADERS, timeout=15)
    soup = BeautifulSoup(response.text, "html.parser")

    name      = soup.find("h1").get_text(strip=True) if soup.find("h1") else "N/A"
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


# ===========================================================================
# TheITDepot
# ===========================================================================

@dlt.resource(name="itdepot_links", write_disposition="replace")
def get_itdepot_links():
    url = "https://www.theitdepot.com/Monitor"
    response = requests.get(url, headers=COMMON_HEADERS, timeout=15)
    soup = BeautifulSoup(response.text, "html.parser")
    for a in soup.select(".product-thumb .name a"):
        link = a["href"]
        if not link.startswith("http"):
            link = "https://www.theitdepot.com" + (link if link.startswith("/") else "/" + link)
        yield link


@dlt.transformer(data_from=get_itdepot_links, name="itdepot_monitor_details")
def get_itdepot_specs(product_url: str):
    time.sleep(CRAWL_DELAY)
    response = requests.get(product_url, headers=COMMON_HEADERS, timeout=15)
    soup = BeautifulSoup(response.text, "html.parser")

    name      = soup.find("h1").get_text(strip=True) if soup.find("h1") else "N/A"
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


# ===========================================================================
# Pipeline factory
# ===========================================================================

def build_pipeline() -> dlt.Pipeline:
    """
    Construct a dlt pipeline targeting MinIO via the S3-compatible endpoint.

    Credentials are read from the environment:
        DESTINATION__S3__ACCESS_KEY_ID
        DESTINATION__S3__SECRET_ACCESS_KEY
        DESTINATION__S3__BUCKET_NAME

    dlt resolves DESTINATION__S3__* variables automatically, but we must
    supply the custom endpoint_url for MinIO (not native AWS S3).
    """
    s3_destination = dlt.destinations.filesystem(
        bucket_url=f"s3://{BUCKET_NAME}/{DATASET_NAME}",
        credentials={
            "aws_access_key_id":     os.environ["DESTINATION__S3__ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["DESTINATION__S3__SECRET_ACCESS_KEY"],
            "endpoint_url":          S3_ENDPOINT,
        },
    )

    return dlt.pipeline(
        pipeline_name="monitor_pipeline",
        destination=s3_destination,
        dataset_name="bronze_layer",
    )


# ===========================================================================
# Entry point
# ===========================================================================

if __name__ == "__main__":
    pipeline = build_pipeline()

    sources = [
        ("MD Computers",   get_md_links      | get_md_specs),
        ("Vedant",         get_vedant_links   | get_vedant_specs),
        ("PrimeABGB",      get_prime_links    | get_prime_specs),
        ("TheITDepot",     get_itdepot_links  | get_itdepot_specs),
    ]

    print("🚀  Starting Multi-Source Scrape → Bronze Layer (MinIO / Parquet)")
    for label, resource in sources:
        print(f"   ↳ Ingesting: {label} …")
        pipeline.run(resource, loader_file_format="parquet")
        print(f"   ✅ {label} done.")

    print(f"\n🎉  All sources written to s3://{BUCKET_NAME}/{DATASET_NAME}/ as Parquet.")