import pandas as pd
from playwright.sync_api import sync_playwright
import datetime
import os

# The category URLs from your screenshot
CATEGORY_SOURCES = [
    {"url": "https://www.91mobiles.com/list-of-laptops/laptops-for-coding", "purpose": "coding"},
    {"url": "https://www.91mobiles.com/list-of-laptops/gaming-laptops-for-esports", "purpose": "gaming"},
    {"url": "https://www.91mobiles.com/list-of-laptops/lightweight-laptops", "purpose": "education"}
]

import json

import re

def get_product_links(page, category_url):
    print(f"🔍 Searching in: {category_url}")
    try:
        # 1. Go to the page and wait for it to actually load
        page.goto(category_url, wait_until="domcontentloaded", timeout=60000)
        
        # 2. THE SECRET SAUCE: Scroll down to trigger the "Lazy Load"
        # 91mobiles won't show the products until you scroll!
        for _ in range(3):
            page.mouse.wheel(0, 800)
            page.wait_for_timeout(1000)

        # 3. Find all links that look like a laptop product page
        # Usually they contain '/laptops/' and end with '-laptop' or have a name
        all_links = page.locator("a").evaluate_all(
            "elements => elements.map(e => e.href)"
        )
        
        # Filter for laptop detail pages specifically
        laptop_links = [
            link for link in all_links 
            if "/laptops/" in link and any(char.isdigit() for char in link)
            and "list-of-laptops" not in link # Avoid recirculating the category page
        ]
        
        # Unique links only
        found = list(set(laptop_links))[:10]
        print(f"🎯 Found {len(found)} laptops!")
        return found
        
    except Exception as e:
        print(f"❌ Error during discovery: {e}")
        return []
    
    try:
        page.wait_for_selector(selector, timeout=10000)
        links = page.locator(selector).evaluate_all(
            "elements => elements.map(e => e.href)"
        )
        return list(set(links))[:10]
    except Exception:
        print("Timeout! 91mobiles might be blocking us or the layout changed.")
        # Fallback: take a screenshot to see what the script is actually seeing
        page.screenshot(path="debug_screenshot.png")
        return []

def scrape_laptop_details(page, url, purpose):
    page.goto(url, wait_until="networkidle")
    
    # Targeting the "Key Specs" section from your screenshot
    specs = {
        "name": page.locator("h1").inner_text().strip(),
        "price": page.locator(".price_ padding_top_5").first.inner_text().strip(),
        "display": page.locator("li:has-text('Display')").inner_text().strip(),
        "processor": page.locator("li:has-text('Ghz')").inner_text().strip(),
        "ram_ssd": page.locator("li:has-text('RAM')").inner_text().strip(),
        "gpu": page.locator("li:has-text('GPU')").inner_text().strip(),
        "purpose": purpose,
        "scraped_at": datetime.datetime.now().isoformat(),
        "source_url": url
    }
    return specs

def run_pipeline():
    all_data = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        for source in CATEGORY_SOURCES:
            print(f"Finding laptops in: {source['purpose']}...")
            links = get_product_links(page, source['url'])
            
            for link in links:
                print(f"Scraping details for: {link}")
                details = scrape_laptop_details(page, link, source['purpose'])
                all_data.append(details)
        
        browser.close()
    
    # Save to Parquet as we discussed for your MotherDuck/MinIO flow
    df = pd.DataFrame(all_data)
    df.to_parquet("laptops_91mobiles.parquet", index=False)
    print("✅ Ingestion complete. Data saved to laptops_91mobiles.parquet")

if __name__ == "__main__":
    run_pipeline()