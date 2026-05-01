"""
MSHSAA Schedule Checker — 2010 Football Season
===============================================
Uses Selenium + headless Chrome to fully render each MSHSAA schedule page
(including JavaScript), then compares games against the existing scoreboard.
 
Only games where the opponent is also a ranked team (in classifications.json)
are flagged as missing.
 
Outputs
-------
mshsaa_missing_games.csv   – every unique missing game detected
mshsaa_school_ids.csv      – team-name to MSHSAA school ID map (for review)
 
Requirements
------------
    pip install selenium requests beautifulsoup4 pandas webdriver-manager
 
Usage
-----
    python mshsaa_schedule_checker.py
"""
 
import json
import re
import time
import unicodedata
import pandas as pd
import requests
from bs4 import BeautifulSoup
 
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
 
# ── File paths ────────────────────────────────────────────────────────────────
TEAMS_FILE      = "classifications.json"
SCOREBOARD_FILE = "football_scoreboard_2010.csv"
OUTPUT_MISSING  = "mshsaa_missing_games.csv"
OUTPUT_IDS      = "mshsaa_school_ids.csv"
 
# alg=19 is the MSHSAA 11-Man Football activity code
SCHEDULE_URL = "https://www.mshsaa.org/MySchool/Schedule.aspx?s={sid}&alg=19&year=2010"
LISTING_URL  = "https://www.mshsaa.org/Schools/SchoolListing.aspx"
 
# Seconds to wait for JS table to appear on each page
JS_WAIT_TIMEOUT = 15
# Seconds to pause between page loads (be polite to the server)
REQUEST_DELAY   = 2.0
 
HEADERS = {"User-Agent": "Mozilla/5.0 (MSHSAA-ScheduleChecker/1.0)"}
 
 
# ─────────────────────────────────────────────────────────────────────────────
#  Utilities
# ─────────────────────────────────────────────────────────────────────────────
 
def normalize(name):
    name = str(name).strip()
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    name = re.sub(r"[''`\u2018\u2019]", "", name)
    name = re.sub(r"[^a-z0-9 ]", " ", name.lower())
    return re.sub(r"\s+", " ", name).strip()
 
 
def strip_suffix(norm_key):
    for suffix in (" junior high school", " high school"):
        if norm_key.endswith(suffix):
            return norm_key[: -len(suffix)].strip()
    return norm_key
 
 
# ─────────────────────────────────────────────────────────────────────────────
#  Data loading
# ─────────────────────────────────────────────────────────────────────────────
 
def load_ranked_teams(path):
    with open(path, "r") as f:
        data = json.load(f)
    df = pd.DataFrame(data["teams"])
    df = df.rename(columns={"school": "Team Name", "classification": "Class"})
    df["Team Name"] = df["Team Name"].astype(str).str.strip()
    df["norm"]      = df["Team Name"].apply(normalize)
    return df.reset_index(drop=True)
 
 
def load_scoreboard(path):
    df = pd.read_csv(path)
    df = df[["Date", "Home Team", "Away Team"]].dropna(subset=["Home Team", "Away Team"])
    df["Date"]      = df["Date"].astype(str).str.strip()
    df["norm_home"] = df["Home Team"].apply(normalize)
    df["norm_away"] = df["Away Team"].apply(normalize)
 
    game_keys = set()
    for _, row in df.iterrows():
        game_keys.add(frozenset([row["norm_home"], row["norm_away"], row["Date"]]))
    return game_keys, df
 
 
# ─────────────────────────────────────────────────────────────────────────────
#  Selenium browser setup
# ─────────────────────────────────────────────────────────────────────────────
 
def build_driver():
    """Return a headless Chrome WebDriver."""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(f"user-agent=Mozilla/5.0 (MSHSAA-ScheduleChecker/1.0)")
 
    service = Service(ChromeDriverManager().install())
    driver  = webdriver.Chrome(service=service, options=options)
    return driver
 
 
def get_rendered_html(driver, url, wait_selector="table"):
    """
    Load a URL in the headless browser, wait for a CSS selector to appear,
    then return the fully-rendered page HTML.
    """
    driver.get(url)
    try:
        WebDriverWait(driver, JS_WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, wait_selector))
        )
    except Exception:
        pass  # Return whatever HTML we have even if timeout
    return driver.page_source
 
 
# ─────────────────────────────────────────────────────────────────────────────
#  School-ID lookup  (uses plain requests — listing page is server-rendered)
# ─────────────────────────────────────────────────────────────────────────────
 
def fetch_school_id_map():
    print("Fetching MSHSAA school listing ...")
    session = requests.Session()
    session.headers.update(HEADERS)
    resp = session.get(LISTING_URL, timeout=30)
    resp.raise_for_status()
    soup   = BeautifulSoup(resp.text, "html.parser")
    id_map = {}
    for a in soup.select("a[href*='MySchool']"):
        m = re.search(r"[?&]s=(\d+)", a.get("href", ""))
        if not m:
            continue
        id_map[normalize(a.get_text(strip=True))] = int(m.group(1))
    print(f"  {len(id_map)} school entries found.")
    return id_map
 
 
def find_school_id(team_name, norm, id_map):
    stripped = {strip_suffix(k): v for k, v in id_map.items()}
 
    if norm in id_map:      return id_map[norm]
    if norm in stripped:    return stripped[norm]
 
    candidates = [(k, v) for k, v in stripped.items()
                  if k.startswith(norm) or norm in k]
    if len(candidates) == 1:
        return candidates[0][1]
 
    words = [w for w in norm.split() if len(w) > 3]
    if words:
        wm = [(k, v) for k, v in stripped.items() if all(w in k for w in words)]
        if len(wm) == 1:
            return wm[0][1]
    return None
 
 
# ─────────────────────────────────────────────────────────────────────────────
#  Schedule page parser
# ─────────────────────────────────────────────────────────────────────────────
 
def parse_schedule_page(html):
    soup  = BeautifulSoup(html, "html.parser")
    games = []
    for tr in soup.select("tr"):
        cells = tr.find_all("td")
        if len(cells) < 4:
            continue
 
        date_text = cells[0].get_text(strip=True)
        if not re.match(r"^\d{1,2}/\d{1,2}", date_text):
            continue
 
        row_text = tr.get_text()
        if "Tournament" in row_text or "\u21b7" in row_text or "⤷" in row_text:
            continue
 
        opp_link = cells[1].find("a")
        if not opp_link:
            continue
        opp_name     = opp_link.get_text(strip=True)
        opp_raw_text = cells[1].get_text(" ", strip=True)
        home_away    = "away" if re.match(r"at\s", opp_raw_text.strip()) else "home"
 
        score_text  = cells[3].get_text(strip=True)
        score_match = re.search(r"(\d+)\s*[-\u2013]\s*(\d+)", score_text)
        score_team  = int(score_match.group(1)) if score_match else None
        score_opp   = int(score_match.group(2)) if score_match else None
 
        date_clean = re.match(r"(\d{1,2}/\d{1,2})", date_text).group(1)
        games.append({
            "date":          date_clean + "/2010",
            "opponent":      opp_name,
            "opponent_norm": normalize(opp_name),
            "home_away":     home_away,
            "score_team":    score_team,
            "score_opp":     score_opp,
        })
    return games
 
 
def opponent_in_rankings(opp_norm, ranked_norms):
    if opp_norm in ranked_norms:
        return True
    for rn in ranked_norms:
        if rn and opp_norm and (rn in opp_norm or opp_norm in rn) and len(min(rn, opp_norm, key=len)) > 4:
            return True
    return False
 
 
# ─────────────────────────────────────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────────────────────────────────────
 
def main():
    print("Loading ranked teams ...")
    teams_df     = load_ranked_teams(TEAMS_FILE)
    ranked_norms = set(teams_df["norm"].tolist())
    print(f"  {len(teams_df)} ranked teams.")
 
    print("Loading existing scoreboard ...")
    game_keys, _ = load_scoreboard(SCOREBOARD_FILE)
    print(f"  {len(game_keys)} games in scoreboard.")
 
    id_map = fetch_school_id_map()
 
    teams_df["school_id"] = None
    teams_df["id_found"]  = False
    for idx, row in teams_df.iterrows():
        sid = find_school_id(row["Team Name"], row["norm"], id_map)
        teams_df.at[idx, "school_id"] = sid
        teams_df.at[idx, "id_found"]  = sid is not None
 
    teams_df[["Team Name", "Class", "district", "school_id", "id_found"]].to_csv(OUTPUT_IDS, index=False)
 
    n_found = int(teams_df["id_found"].sum())
    print(f"\nSchool IDs resolved: {n_found}/{len(teams_df)}")
    for nm in teams_df[~teams_df["id_found"]]["Team Name"].tolist():
        print(f"  No ID found: {nm}")
 
    print("\nStarting headless browser ...")
    driver = build_driver()
 
    missing_rows  = []
    teams_with_id = teams_df[teams_df["id_found"]].copy()
    total         = len(teams_with_id)
 
    try:
        for i, (_, team_row) in enumerate(teams_with_id.iterrows(), 1):
            team_name = team_row["Team Name"]
            team_norm = team_row["norm"]
            sid       = int(team_row["school_id"])
            url       = SCHEDULE_URL.format(sid=sid)
 
            print(f"\n[{i}/{total}] {team_name}  (ID={sid})")
            try:
                html = get_rendered_html(driver, url)
            except Exception as exc:
                print(f"  WARNING: Skipped — {exc}")
                time.sleep(REQUEST_DELAY)
                continue
 
            games = parse_schedule_page(html)
            time.sleep(REQUEST_DELAY)
 
            if not games:
                print("  (no game rows parsed)")
                continue
 
            print(f"  {len(games)} games on MSHSAA page.")
            for game in games:
                opp_norm = game["opponent_norm"]
                if not opponent_in_rankings(opp_norm, ranked_norms):
                    continue
 
                game_key = frozenset([team_norm, opp_norm, game["date"]])
                if game_key not in game_keys:
                    print(f"  MISSING: {game['date']}  vs  {game['opponent']}"
                          f"  ({game['home_away']})  {game['score_team']}-{game['score_opp']}")
                    missing_rows.append({
                        "Ranked Team":    team_name,
                        "Team School ID": sid,
                        "Date":           game["date"],
                        "Opponent":       game["opponent"],
                        "Opponent Norm":  opp_norm,
                        "Home/Away":      game["home_away"],
                        "Team Score":     game["score_team"],
                        "Opp Score":      game["score_opp"],
                        "MSHSAA URL":     url,
                    })
                else:
                    print(f"  OK: {game['date']}  vs  {game['opponent']}")
 
    finally:
        driver.quit()
 
    print(f"\n{'='*60}")
    if missing_rows:
        missing_df = pd.DataFrame(missing_rows)
        missing_df["_key"] = missing_df.apply(
            lambda r: str(frozenset([normalize(r["Ranked Team"]), r["Opponent Norm"], r["Date"]])),
            axis=1,
        )
        missing_df = (missing_df
                      .drop_duplicates(subset="_key")
                      .drop(columns=["_key", "Opponent Norm"])
                      .reset_index(drop=True))
        missing_df.to_csv(OUTPUT_MISSING, index=False)
        print(f"Done. {len(missing_df)} unique missing games -> {OUTPUT_MISSING}")
    else:
        # Always write the file so the GitHub Actions artifact upload doesn't fail
        pd.DataFrame(columns=["Ranked Team","Team School ID","Date","Opponent",
                               "Home/Away","Team Score","Opp Score","MSHSAA URL"]
                     ).to_csv(OUTPUT_MISSING, index=False)
        print("No missing games detected.")
 
    print(f"School ID map -> {OUTPUT_IDS}")
 
 
if __name__ == "__main__":
    main()
