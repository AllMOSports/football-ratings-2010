"""
MSHSAA Schedule Checker — 2010 Football Season
===============================================
Uses Selenium + headless Chrome to fully render each MSHSAA schedule page
(including JavaScript), then compares games against the existing scoreboard.

Only games where BOTH teams are in classifications.json are flagged as missing.

Outputs
-------
mshsaa_missing_games.csv   – every unique missing game detected
mshsaa_school_ids.csv      – team-name to MSHSAA school ID map (for review)

Requirements
------------
    pip install selenium requests beautifulsoup4 pandas webdriver-manager

Usage
-----
    python Schedule_Checker.py
"""

import json
import re
import time
import unicodedata
from collections import defaultdict
from datetime import datetime
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# -- File paths ----------------------------------------------------------------
TEAMS_FILE      = "classifications.json"
SCHOOLS_CSV     = "mshsaa_schools.csv"
SCOREBOARD_FILE = "football_scoreboard_2010.csv"
OUTPUT_MISSING  = "mshsaa_missing_games.csv"
OUTPUT_IDS      = "mshsaa_school_ids.csv"

# alg=19 is the MSHSAA 11-Man Football activity code
SCHEDULE_URL = "https://www.mshsaa.org/MySchool/Schedule.aspx?s={sid}&alg=19&year=2010"

JS_WAIT_TIMEOUT  = 15
REQUEST_DELAY    = 2.0
DATE_WINDOW_DAYS = 3   # games within this many days of each other are considered the same

# -- School ID overrides -------------------------------------------------------
#
# CLASSNAME_TO_ID: maps the exact classifications.json school name -> MSHSAA ID.
# Needed when mshsaa_schools.csv doesn't list the school (closed / co-op / renamed)
# or when the CSV name doesn't match the classifications.json name.
#
# If a team is missing from this list and from mshsaa_schools.csv, its schedule
# page will be skipped and the script will print a warning.  Add the correct ID
# (find it at https://www.mshsaa.org/Schools/SchoolListing.aspx, s= in the URL).
#
CLASSNAME_TO_ID = {
    # -- originally in Schedule_Checker ----------------------------------------
    "Cleveland NJROTC":                          61,   # TODO: verify correct ID
    "Lockwood with Golden City":                 126,
    "Sweet Springs with Malta Bend":             469,
    "Rich Hill with Hume":                       424,
    "Princeton with Mercer":                     421,
    "St. Mary's (Independence)":                 548,
    "Wentworth Military Academy":                563,
    "King City with Pattonsburg":                331,
    "Barat Academy":                             781,
    "Carnahan":                                  777,
    "Transportation and Law":                    776,
    "McAuley Catholic with New Heights Christian": 568,
    "Clopton with Elsberry":                     271,
    "Cole Camp with Green Ridge":                272,
    "John F. Kennedy":                           525,
    "Imagine College Prep Charter":              51,
    "Trinity Catholic":                          557,
    "O'Hara":                                    537,
    "Renaissance Academy":                       575,
    "Beaumont":                                  11,
    "SLUH":                                      547,
    "St. Vincent":                              554,
    "Valle Catholic":                           559,
    "Sacred Heart":                             573,
    "Windsor":                                  500,
    "Santa Fe":                                 432,
    "South Shelby":                             451,
    "Schuyler County":                          183,
    "Scotland County":                          434,
    "Wellington-Napoleon":                      492,
    "South Harrison":                           446,
    "West Platte":                              495,
    "Willow Springs":                           498,
    "Strafford":                                466,
    "Veritas Christian Academy":                812,
    "Sherwood":                                 439,
    "St. Paul Lutheran (Concordia)":            550,
    "Ste. Genevieve":                           459,
    "McCluer South-Berkeley":                   118,
    "Winfield":                                 501,
    "Southern Boone":                           2,
    "Salem":                                    174,
    "Versailles":                               485,
    "Seneca":                                   190,
    "St. Pius X (Kansas City)":                 551,
    "Sikeston":                                 191,
    "Windsor (Imperial)":                       499,
    "Soldan International Studies":             195,
    "St. Charles":                              168,
    "Westminster Christian Academy":            564,
    "St. Charles West":                         169,
    "St. Dominic":                              542,
    "Warrenton":                                489,
    "West Plains":                              230,
    "Webb City":                                227,
    "Warrensburg":                              218,
    "Savannah":                                 177,
    "Seckman":                                  184,
    "Chaminade College Prep":                   512,
    "Webster Groves":                           228,
    "Timberland":                               472,
    "Vianney":                                  560,
    "Washington":                               491,
    "Waynesville":                              224,
    "William Chrisman":                         235,
    "Winnetonka":                               209,
    "Staley":                                   805,
    # -- from football_ratings_2010.py MANUAL_OVERRIDES ------------------------
    "Salisbury":                                 431,
    "Scott City":                                435,
    "Skyline":                                   443,
    "Slater":                                    193,
    "Smith-Cotton":                              194,
    "South Callaway":                            197,
    "St. Mary's South Side":                     549,
    "Stockton":                                  463,
    "Sullivan":                                  207,
    "Sumner":                                    208,
    "Truman":                                    198,
    "University Academy Charter":                479,
    "Van Horn":                                  204,
    "Vashon":                                    206,
    "Appleton City":                             20,
    "Drexel":                                    275,
    "St. James":                                 172,
    "DeSoto":                                    35,
    "Liberal":                                   342,
    "Van-Far":                                   483,
    "St. Vincent":                               554,
    "Valle Catholic":                            559,
    "Sacred Heart":                              573,
    "Windsor":                                   500,
    "Santa Fe":                                  432,
    "South Shelby":                              451,
    "Schuyler County":                           183,
    "Scotland County":                           434,
    "Wellington-Napoleon":                       492,
    "South Harrison":                            446,
    "West Platte":                               495,
    "Willow Springs":                            498,
    "Strafford":                                 466,
    "Veritas Christian Academy":                 812,
    "Sherwood":                                  439,
    "St. Paul Lutheran (Concordia)":             550,
    "Ste. Genevieve":                            459,
    "McCluer South-Berkeley":                    118,
    "Winfield":                                  501,
    "Southern Boone":                            2,
    "Salem":                                     174,
    "Versailles":                                485,
    "Seneca":                                    190,
    "St. Pius X (Kansas City)":                  551,
    "Sikeston":                                  191,
    "Windsor (Imperial)":                        499,
    "Soldan International Studies":              195,
    "St. Charles":                               168,
    "Westminster Christian Academy":             564,
    "St. Charles West":                          169,
    "St. Dominic":                               542,
    "Warrenton":                                 489,
    "West Plains":                               230,
    "Webb City":                                 227,
    "Warrensburg":                               218,
    "Savannah":                                  177,
    "Seckman":                                   184,
    "Chaminade College Prep":                    512,
    "Webster Groves":                            228,
    "Timberland":                                472,
    "Vianney":                                   560,
    "Washington":                                491,
    "Waynesville":                               224,
    "William Chrisman":                          235,
    "Winnetonka":                                209,
    "Staley":                                    805,
}

# ID_TO_SCOREBOARD_NAME: maps MSHSAA school ID (as str) -> the name that
# football_ratings_2010.py wrote into football_scoreboard_2010.csv.
# For most teams this equals the classifications.json name, but seven co-op
# schools are stored in the scoreboard under their full co-op name
# (e.g. "Appleton City with Montrose") while classifications.json uses a
# shorter form ("Appleton City").  The Schedule Checker must use these same
# scoreboard names so that the pair-matching against football_scoreboard_2010.csv
# works correctly.
ID_TO_SCOREBOARD_NAME = {
    "271": "Clopton with Elsberry",
    "331": "King City with Pattonsburg",
    "126": "Lockwood with Golden City",
    "421": "Princeton with Mercer",
    "424": "Rich Hill with Hume",
    "431": "Salisbury",
    "435": "Scott City",
    "443": "Skyline",
    "193": "Slater",
    "194": "Smith-Cotton",
    "197": "South Callaway",
    "549": "St. Mary's South Side",
    "463": "Stockton",
    "207": "Sullivan",
    "208": "Sumner",
    "469": "Sweet Springs with Malta Bend",
    "198": "Truman",
    "479": "University Academy Charter",
    "204": "Van Horn",
    "206": "Vashon",
    "20":  "Appleton City with Montrose",
    "275": "Drexel with Miami (Amoret)",
    "575": "Renaissance Academy Charter",
    "172": "St. James",
    "35":  "DeSoto with Kingston",
    "342": "Liberal with Bronaugh",
    "776": "Transportation and Law with Beaumont",
    "483": "Van-Far with Community",
    "61":  "Cleveland NJROTC",
    "548": "St. Mary's (Independence)",
    "563": "Wentworth Military Academy",
    "781": "Barat Academy",
    "777": "Carnahan",
    "568": "McAuley Catholic with New Heights Christian",
    "272": "Cole Camp with Green Ridge",
    "525": "John F. Kennedy",
    "51":  "Imagine College Prep Charter",
    "557": "Trinity Catholic",
    "537": "O'Hara",
    "11":  "Beaumont",
    "547": "SLUH",
    "554": "St. Vincent",
    "559": "Valle Catholic",
    "573": "Sacred Heart",
    "500": "Windsor",
    "432": "Santa Fe",
    "451": "South Shelby",
    "183": "Schuyler County",
    "434": "Scotland County",
    "492": "Wellington-Napoleon",
    "446": "South Harrison",
    "495": "West Platte",
    "498": "Willow Springs",
    "466": "Strafford",
    "812": "Veritas Christian Academy",
    "439": "Sherwood",
    "550": "St. Paul Lutheran (Concordia)",
    "459": "Ste. Genevieve",
    "118": "McCluer South-Berkeley",
    "501": "Winfield",
    "2":   "Southern Boone",
    "174": "Salem",
    "485": "Versailles",
    "190": "Seneca",
    "551": "St. Pius X (Kansas City)",
    "191": "Sikeston",
    "499": "Windsor (Imperial)",
    "195": "Soldan International Studies",
    "168": "St. Charles",
    "564": "Westminster Christian Academy",
    "169": "St. Charles West",
    "542": "St. Dominic",
    "489": "Warrenton",
    "230": "West Plains",
    "227": "Webb City",
    "218": "Warrensburg",
    "177": "Savannah",
    "184": "Seckman",
    "512": "Chaminade College Prep",
    "228": "Webster Groves",
    "472": "Timberland",
    "560": "Vianney",
    "491": "Washington",
    "224": "Waynesville",
    "235": "William Chrisman",
    "209": "Winnetonka",
    "805": "Staley",
}

# -----------------------------------------------------------------------------
#  Utilities
# -----------------------------------------------------------------------------

def normalize(name):
    name = str(name).strip()
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    name = re.sub(r"[''`\u2018\u2019]", "", name)
    name = re.sub(r"[^a-z0-9 ]", " ", name.lower())
    return re.sub(r"\s+", " ", name).strip()


# -----------------------------------------------------------------------------
#  ID map construction
# -----------------------------------------------------------------------------

def build_id_maps(known_class_names):
    df = pd.read_csv(SCHOOLS_CSV)

    id_to_sb_name = {}
    for _, row in df.iterrows():
        full_name = str(row["school_name"])
        sid       = str(int(row["school_id"]))
        stripped  = full_name.replace(" High School", "").strip()

        if stripped in known_class_names:
            id_to_sb_name[sid] = stripped
        elif full_name in known_class_names:
            id_to_sb_name[sid] = full_name

    id_to_sb_name.update(ID_TO_SCOREBOARD_NAME)

    classname_to_id = {}
    for sid, sb_name in id_to_sb_name.items():
        if sb_name in known_class_names:
            classname_to_id[sb_name] = int(sid)

    classname_to_id.update(CLASSNAME_TO_ID)

    return id_to_sb_name, classname_to_id


# -----------------------------------------------------------------------------
#  Data loading
# -----------------------------------------------------------------------------

def load_ranked_teams(path):
    with open(path, "r") as f:
        data = json.load(f)
    return [
        {
            "Team Name": entry["school"],
            "Class":     entry["classification"],
            "district":  entry["district"],
        }
        for entry in data["teams"]
    ]


def build_ranked_norms(teams, id_to_sb_name):
    class_names = {t["Team Name"] for t in teams}
    sb_names    = set(id_to_sb_name.values())
    return {normalize(n) for n in (class_names | sb_names)}


def load_scoreboard(path):
    df = pd.read_csv(path)
    df = df[["Date", "Home Team", "Away Team"]].dropna(subset=["Home Team", "Away Team"])
    df["Date"]      = df["Date"].astype(str).str.strip()
    df["norm_home"] = df["Home Team"].apply(normalize)
    df["norm_away"] = df["Away Team"].apply(normalize)

    pair_dates = defaultdict(list)
    for _, row in df.iterrows():
        pair   = frozenset([row["norm_home"], row["norm_away"]])
        parsed = None
        for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
            try:
                parsed = datetime.strptime(row["Date"], fmt)
                break
            except Exception:
                pass
        pair_dates[pair].append(parsed)
    return pair_dates


def game_in_scoreboard(team_norm, opp_norm, game_date_str, pair_dates,
                        window_days=DATE_WINDOW_DAYS):
    pair = frozenset([team_norm, opp_norm])
    if pair not in pair_dates:
        return False
    try:
        gdate = datetime.strptime(game_date_str, "%m/%d/%Y")
    except Exception:
        return False
    return any(
        sbdate is not None and abs((sbdate - gdate).days) <= window_days
        for sbdate in pair_dates[pair]
    )


# -----------------------------------------------------------------------------
#  Selenium browser setup
# -----------------------------------------------------------------------------

def build_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("user-agent=Mozilla/5.0 (MSHSAA-ScheduleChecker/1.0)")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


def get_rendered_html(driver, url, wait_selector="table"):
    driver.get(url)
    try:
        WebDriverWait(driver, JS_WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, wait_selector))
        )
    except Exception:
        pass
    return driver.page_source


# -----------------------------------------------------------------------------
#  Schedule page parser
# -----------------------------------------------------------------------------

def parse_schedule_page(html):
    from bs4 import BeautifulSoup
    soup  = BeautifulSoup(html, "html.parser")
    games = []

    levels_ul = soup.find("ul", id="LevelsOfPlay")
    if levels_ul:
        active_li = levels_ul.find("li", class_="current")
        if active_li:
            active_text = active_li.get_text(strip=True).lower()
            if "varsity" not in active_text:
                print(f"  (skipping - active tab is not Varsity: {active_text!r})")
                return games

    schedule_div = soup.find("div", id="ctl00_contentMain_divSchedule")
    if not schedule_div:
        return games

    schedule_table = None
    for table in schedule_div.find_all("table"):
        if "Date" in table.get_text() and "Opponent" in table.get_text():
            schedule_table = table
            break

    if not schedule_table:
        return games

    rows = schedule_table.find_all("tr")
    for tr in rows[1:]:
        cells = tr.find_all(["td", "th"])
        if len(cells) < 5:
            continue

        date_text  = cells[1].get_text(strip=True)
        opp_cell   = cells[2]
        score_text = cells[4].get_text(strip=True)

        if not re.match(r"^\d{1,2}/\d{1,2}", date_text):
            continue
        if "Tournament" in opp_cell.get_text() or "Tournament" in tr.get_text():
            continue

        opp_text = opp_cell.get_text(strip=True)

        opp_link     = opp_cell.find("a", href=lambda h: h and "/MySchool/Schedule.aspx" in h)
        opponent_sid = None
        if opp_link:
            m = re.search(r"[?&]s=(\d+)", opp_link.get("href", ""), re.IGNORECASE)
            if m:
                opponent_sid = m.group(1)

        home_away = "away" if opp_text.startswith("at") else "home"
        opp_clean = re.sub(r"^at", "", opp_text).strip()
        opp_clean = re.sub(r"\(\d+-\d+\)\s*$", "", opp_clean).strip()

        score_match = re.search(r"(\d+)\s*[-\u2013]\s*(\d+)", score_text)
        score_team  = int(score_match.group(1)) if score_match else None
        score_opp   = int(score_match.group(2)) if score_match else None

        date_clean = re.match(r"(\d{1,2}/\d{1,2})", date_text).group(1)
        games.append({
            "date":          date_clean + "/2010",
            "opponent":      opp_clean,
            "opponent_sid":  opponent_sid,
            "opponent_norm": normalize(opp_clean),
            "home_away":     home_away,
            "score_team":    score_team,
            "score_opp":     score_opp,
        })
    return games


# -----------------------------------------------------------------------------
#  Opponent resolution
# -----------------------------------------------------------------------------

def resolve_opponent(game, id_to_sb_name, ranked_norms):
    sid = game.get("opponent_sid")
    if sid and sid in id_to_sb_name:
        sb_name  = id_to_sb_name[sid]
        opp_norm = normalize(sb_name)
        if opp_norm in ranked_norms:
            return sb_name, opp_norm
        return None

    opp_norm = game["opponent_norm"]
    if opp_norm in ranked_norms:
        return game["opponent"], opp_norm
    return None


# -----------------------------------------------------------------------------
#  Main
# -----------------------------------------------------------------------------

def main():
    print("Loading ranked teams ...")
    teams = load_ranked_teams(TEAMS_FILE)
    known_class_names = {t["Team Name"] for t in teams}
    print(f"  {len(teams)} ranked teams.")

    print("Building school ID maps ...")
    id_to_sb_name, classname_to_id = build_id_maps(known_class_names)
    ranked_norms = build_ranked_norms(teams, id_to_sb_name)
    print(f"  {len(id_to_sb_name)} IDs in scoreboard-name map.")
    print(f"  {len(classname_to_id)} ranked teams with resolved school IDs.")

    print("Loading existing scoreboard ...")
    pair_dates = load_scoreboard(SCOREBOARD_FILE)
    print(f"  {len(pair_dates)} unique team pairs in scoreboard.")

    team_records = []
    for t in teams:
        name    = t["Team Name"]
        sid     = classname_to_id.get(name)
        sb_name = id_to_sb_name.get(str(sid), name) if sid else name
        team_records.append({**t, "school_id": sid, "sb_name": sb_name})

    id_df = pd.DataFrame([
        {
            "Team Name": r["Team Name"],
            "Class":     r["Class"],
            "district":  r["district"],
            "school_id": r["school_id"],
            "id_found":  r["school_id"] is not None,
            "sb_name":   r["sb_name"],
        }
        for r in team_records
    ])
    id_df.to_csv(OUTPUT_IDS, index=False)

    n_found = int(id_df["id_found"].sum())
    print(f"\nSchool IDs resolved: {n_found}/{len(team_records)}")
    still_missing = id_df[~id_df["id_found"]]["Team Name"].tolist()
    if still_missing:
        print("  Teams still needing a manual ID (add to CLASSNAME_TO_ID at top of script):")
        for nm in still_missing:
            print(f"    No ID: {nm}")

    print("\nStarting headless browser ...")
    driver = build_driver()

    missing_rows  = []
    teams_with_id = [r for r in team_records if r["school_id"] is not None]
    total         = len(teams_with_id)

    try:
        for i, team_row in enumerate(teams_with_id, 1):
            team_name = team_row["Team Name"]
            team_sb   = team_row["sb_name"]
            team_norm = normalize(team_sb)
            sid       = int(team_row["school_id"])
            url       = SCHEDULE_URL.format(sid=sid)

            print(f"\n[{i}/{total}] {team_name}  (ID={sid})")
            try:
                html = get_rendered_html(driver, url)
            except Exception as exc:
                print(f"  WARNING: Skipped - {exc}")
                time.sleep(REQUEST_DELAY)
                continue

            games = parse_schedule_page(html)
            time.sleep(REQUEST_DELAY)

            if not games:
                print("  (no game rows parsed)")
                continue

            print(f"  {len(games)} games on MSHSAA page.")
            for game in games:
                result = resolve_opponent(game, id_to_sb_name, ranked_norms)
                if result is None:
                    continue

                opp_sb_name, opp_norm = result

                if not game_in_scoreboard(team_norm, opp_norm, game["date"], pair_dates):
                    print(f"  MISSING: {game['date']}  vs  {opp_sb_name}"
                          f"  ({game['home_away']})  {game['score_team']}-{game['score_opp']}")
                    missing_rows.append({
                        "Ranked Team":    team_name,
                        "Team School ID": sid,
                        "Date":           game["date"],
                        "Opponent":       opp_sb_name,
                        "Home/Away":      game["home_away"],
                        "Team Score":     game["score_team"],
                        "Opp Score":      game["score_opp"],
                        "MSHSAA URL":     url,
                    })
                else:
                    print(f"  OK: {game['date']}  vs  {opp_sb_name}")

    finally:
        driver.quit()

    print(f"\n{'='*60}")
    if missing_rows:
        missing_df = pd.DataFrame(missing_rows)
        missing_df["_key"] = missing_df.apply(
            lambda r: str(frozenset([normalize(r["Ranked Team"]),
                                     normalize(r["Opponent"]),
                                     r["Date"]])),
            axis=1,
        )
        missing_df = (missing_df
                      .drop_duplicates(subset="_key")
                      .drop(columns=["_key"])
                      .reset_index(drop=True))
        missing_df.to_csv(OUTPUT_MISSING, index=False)
        print(f"Done. {len(missing_df)} unique missing games -> {OUTPUT_MISSING}")
    else:
        pd.DataFrame(columns=[
            "Ranked Team", "Team School ID", "Date", "Opponent",
            "Home/Away", "Team Score", "Opp Score", "MSHSAA URL",
        ]).to_csv(OUTPUT_MISSING, index=False)
        print("No missing games detected.")

    print(f"School ID map -> {OUTPUT_IDS}")


if __name__ == "__main__":
    main()
