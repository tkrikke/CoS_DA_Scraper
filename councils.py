import requests
from bs4 import BeautifulSoup
import csv
import os
from urllib.parse import urljoin
import re
from datetime import datetime
from tqdm import tqdm

# List of Morph.io scraper URLs and optional API keys
SCRAPERS = [
    {"name": "City of Sydney", "url": "https://api.morph.io/planningalerts-scrapers/city_of_sydney/data.json", "key": "JQLGaiTjLU/8qg8RhfZL"},
    {"name": "ACT", "url": "https://api.morph.io/planningalerts-scrapers/act/data.json", "key": "JQLGaiTjLU/8qg8RhfZL"},
    # Add other council scrapers here...
]

KEYWORDS = ["acoustic", "noise"]
OUTPUT_CSV = "all_councils_acoustic_reports.csv"

# Create a timestamped reports folder
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
REPORTS_DIR = os.path.join("reports", timestamp)
os.makedirs(REPORTS_DIR, exist_ok=True)

def sanitize_filename(name):
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = re.sub(r'\s+', '_', name)
    return name

def deduplicate_filename(path):
    base, ext = os.path.splitext(path)
    counter = 1
    while os.path.exists(path):
        path = f"{base}_{counter}{ext}"
        counter += 1
    return path

def fetch(scraper):
    q = 'select * from "data" order by date_received desc limit 200'
    params = {"query": q, "key": scraper.get("key", "")}
    r = requests.get(scraper["url"], params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def check_documents(info_url, ref, council_name):
    """Check PDF/DOC/DOCX links and download matching acoustic/noise files"""
    try:
        r = requests.get(info_url, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"    ⚠️ Failed to fetch DA page {info_url}: {e}")
        return [], []

    soup = BeautifulSoup(r.text, "html.parser")
    docs = []
    matches = []

    links = [a for a in soup.find_all("a", href=True)
             if any(ext in a["href"].lower() or ext in a.get_text(" ", strip=True).lower()
                    for ext in [".pdf", ".doc", ".docx"])]

    for a in links:
        href = a["href"]
        full_url = urljoin(info_url, href)
        text = a.get_text(" ", strip=True).lower()

        if any(kw in text for kw in KEYWORDS):
            try:
                base_name = os.path.basename(full_url).split('?')[0]
                filename_safe = sanitize_filename(f"{council_name}_{ref}_{base_name}")
                filename_safe = os.path.splitext(filename_safe)[0] + ".pdf"
                file_path = os.path.join(REPORTS_DIR, filename_safe)
                file_path = deduplicate_filename(file_path)

                file_resp = requests.get(full_url, timeout=30)
                with open(file_path, "wb") as f:
                    f.write(file_resp.content)
                matches.append(file_path)
            except Exception as e:
                print(f"    ⚠️ Failed to download {full_url}: {e}")

    return docs, matches

if __name__ == "__main__":
    rows_to_save = []

    # Outer progress bar: councils
    for scraper in tqdm(SCRAPERS, desc="Processing councils", unit="council"):
        print(f"\nProcessing scraper: {scraper['name']}")
        try:
            data = fetch(scraper)
        except Exception as e:
            print(f"  ❌ Skipping {scraper['name']} due to error: {e}")
            continue

        # Inner progress bar: DAs within the council
        for i, row in enumerate(tqdm(data, desc=f"DAs in {scraper['name']}", unit="DA", leave=False)):
            ref = row.get("council_reference") or ""
            addr = row.get("address") or row.get("description") or ""
            info_url = row.get("info_url") or ""

            if info_url:
                docs, matches = check_documents(info_url, ref, scraper["name"])
                if matches:
                    rows_to_save.append({
                        "council": scraper["name"],
                        "council_reference": ref,
                        "address": addr,
                        "info_url": info_url,
                        "matching_documents": "; ".join(matches)
                    })
                    print(f"  {i+1}. {ref} — {addr} (acoustic/noise report found, {len(matches)} file(s) downloaded)")

    # Write all results to CSV
    with open(OUTPUT_CSV, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["council", "council_reference", "address", "info_url", "matching_documents"])
        writer.writeheader()
        writer.writerows(rows_to_save)

    print(f"\nTotal DAs with acoustic/noise reports: {len(rows_to_save)}")
    print(f"Saved to {OUTPUT_CSV}")
    print(f"Downloaded files saved in {REPORTS_DIR}/")
