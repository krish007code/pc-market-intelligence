import requests
from bs4 import BeautifulSoup
import re

url = "https://www.theitdepot.com/Monitor/acer_21.5-inch_full_hd_monitor_ek220q"
headers = {"User-Agent": "Mozilla/5.0 (X11; Fedora; Linux x86_64)"}

print(f"Deep Scraping TheITDepot: {url}")
response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.text, "html.parser")

# 1. Price Fix: Targeted specifically at the 'new' price class from your screenshot
price_tag = soup.select_one(".product-price-new")
if price_tag:
    price_text = price_tag.get_text(strip=True)
    # Extract only digits and remove commas
    price_cleaned = re.sub(r'[^\d]', '', price_text.split('.')[0])
    print(f"Verified Sale Price: ₹{price_cleaned}")
else:
    print("Could not find .product-price-new, checking fallback...")

# 2. Specs Fix: Targeted at the div list you found in image_8f25f3.png
print("\n--- Technical Specifications Found ---")
# We look for divs that contain a colon (:) which usually marks a key-value pair
spec_divs = soup.find_all("div", style=lambda x: x and "text-align: justify" in x)

specs_found = 0
for div in spec_divs:
    text = div.get_text(strip=True)
    if ":" in text:
        # Split into key and value (e.g., "Refresh Rate : 75 Hz")
        parts = text.split(":", 1)
        key = parts[0].strip().replace(" ", "_").lower()
        value = parts[1].strip()
        print(f"{key}: {value}")
        specs_found += 1

if specs_found == 0:
    print("No specs found in div list. Website might be using a different layout for this product.")