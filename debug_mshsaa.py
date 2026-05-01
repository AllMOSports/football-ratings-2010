"""
Debug script — prints the rendered HTML of one MSHSAA schedule page
so we can see exactly what Selenium is capturing.
 
Run this once, then look at the output to find the real table structure.
"""
 
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
 
# Cabool (ID=23) as a test — a small Class 1 team
TEST_URL = "https://www.mshsaa.org/MySchool/Schedule.aspx?s=23&alg=19&year=2010"
 
def build_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)
 
driver = build_driver()
print(f"Loading: {TEST_URL}")
driver.get(TEST_URL)
 
# Wait up to 20 seconds for ANY table to appear
try:
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "table"))
    )
    print("A <table> element was found on the page.")
except Exception:
    print("WARNING: No <table> found after 20 seconds.")
 
html = driver.page_source
driver.quit()
 
soup = BeautifulSoup(html, "html.parser")
 
# Print page title so we know we got the right page
print(f"\nPage title: {soup.title.string if soup.title else 'none'}")
 
# How many tables?
tables = soup.find_all("table")
print(f"Total <table> elements found: {len(tables)}")
 
# Print all table contents so we can see what's there
for i, table in enumerate(tables):
    rows = table.find_all("tr")
    print(f"\n--- Table {i+1} ({len(rows)} rows) ---")
    for tr in rows[:15]:  # first 15 rows of each table
        cells = tr.find_all(["td", "th"])
        if cells:
            print(" | ".join(c.get_text(strip=True)[:40] for c in cells))
 
# Also print a chunk of the raw HTML around "Schedule" keyword
raw = html
idx = raw.find("Schedule")
if idx > -1:
    print(f"\n--- Raw HTML snippet around 'Schedule' (chars {idx} to {idx+2000}) ---")
    print(raw[idx:idx+2000])
 
# Save full HTML to file for inspection
with open("debug_page.html", "w", encoding="utf-8") as f:
    f.write(html)
print("\nFull rendered HTML saved to: debug_page.html")
