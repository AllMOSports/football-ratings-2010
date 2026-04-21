import requests
from bs4 import BeautifulSoup
import json
import csv
import re
import pandas as pd
from datetime import datetime, date
import time
 
# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
 
SEASON_YEAR   = 2010
SEASON_START  = date(2010, 8, 1)
SEASON_END    = date(2010, 12, 15)
MAX_POINTS    = 100
OUTPUT_PATH   = "football_ratings_2010.json"
CSV_PATH      = "football_scoreboard_2010.csv"
CLASSIFICATIONS_PATH  = "classifications.json"
SCHOOLS_CSV           = "mshsaa_schools.csv"
MAPPING_CSV           = "mshsaa_school_sport_mapping.csv"
ITERATIONS            = 1000
LEARNING_RATE         = 0.1
COMPETITIVE_THRESHOLD = 35
 
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.mshsaa.org/"
}
 
# ---------------------------------------------------------------------------
# LOAD SUPPORT FILES
# ---------------------------------------------------------------------------
 
def load_classifications(path=CLASSIFICATIONS_PATH):
    """Return team_to_class and team_to_district dicts keyed by school name."""
    with open(path) as f:
        data = json.load(f)
    team_to_class    = {}
    team_to_district = {}
    for entry in data["teams"]:
        school = entry["school"]
        team_to_class[school]    = entry["classification"]
        team_to_district[school] = entry["district"]
    return team_to_class, team_to_district
 
 
def load_school_id_map(schools_csv=SCHOOLS_CSV):
    """
    Return a dict { school_id_str : school_name } from mshsaa_schools.csv.
    Used to resolve opponent names by their s= ID during schedule scraping.
    """
    df = pd.read_csv(schools_csv)
    return dict(zip(df["school_id"].astype(str), df["school_name"]))
 
 
def load_football_schedule_urls(mapping_csv=MAPPING_CSV):
    """
    Return a dict { school_name : { url, school_id } } for Football only.
    Appends &year=SEASON_YEAR to each URL to pull the correct historical season.
    """
    df = pd.read_csv(mapping_csv)
    football = df[df["sport"] == "Football"]
    urls = {}
    for _, row in football.iterrows():
        base_url = row["schedule_url"].strip()
        url = f"{base_url}&year={SEASON_YEAR}"
        urls[row["school_name"]] = {
            "url": url,
            "school_id": str(row["school_id"])
        }
    return urls
 
 
# ---------------------------------------------------------------------------
# NAME RESOLUTION
# ---------------------------------------------------------------------------
 
def build_classname_lookup(team_to_class, schools_csv=SCHOOLS_CSV):
    """
    Build two lookups:
      1. school_id → classification_name  (resolves opponent IDs on schedule pages)
      2. csv_school_name → classification_name  (resolves the home school name)
 
    Only exact matches after stripping ' High School' are used.
    No fuzzy matching — fuzzy matching caused silent wrong mappings.
    """
    df = pd.read_csv(schools_csv)
    known_class_names = set(team_to_class.keys())
 
    # full CSV name → classification name
    csv_to_classname = {}
    for full_name in df["school_name"]:
        stripped = full_name.replace(" High School", "").strip()
        if stripped in known_class_names:
            csv_to_classname[full_name] = stripped
        elif full_name in known_class_names:
            csv_to_classname[full_name] = full_name
 
    # school_id → classification name
    id_to_classname = {}
    for _, row in df.iterrows():
        full_name = row["school_name"]
        sid = str(row["school_id"])
        if full_name in csv_to_classname:
            id_to_classname[sid] = csv_to_classname[full_name]
 
    # Report unresolved
    resolved_classnames = set(id_to_classname.values())
    unresolved = sorted(known_class_names - resolved_classnames)
    if unresolved:
        print(f"\n  WARNING: {len(unresolved)} classification schools could not be "
              f"mapped to a school ID.\n"
              f"  These schools will still be scraped via their own schedule page\n"
              f"  but opponent names may not resolve correctly for their games.\n"
              f"  Unresolved: {unresolved}\n")
    else:
        print("  [name-resolve] All classification schools resolved successfully.")
 
    print(f"  [name-resolve] {len(id_to_classname)} schools mapped by ID")
    return id_to_classname, csv_to_classname
 
 
def resolve_csv_name(csv_name, csv_to_classname, team_to_class):
    """
    Resolve a full CSV school name to its classifications.json name.
    Returns None if not found.
    """
    if csv_name in csv_to_classname:
        return csv_to_classname[csv_name]
    if csv_name in team_to_class:
        return csv_name
    return None
 
 
# ---------------------------------------------------------------------------
# SCHEDULE PAGE SCRAPING
# ---------------------------------------------------------------------------
 
def parse_game_date(date_text):
    """Parse a date string from the MSHSAA schedule page."""
    date_text = date_text.strip()
    for fmt in ("%m/%d/%Y", "%b %d, %Y", "%B %d, %Y", "%m/%d/%y"):
        try:
            return datetime.strptime(date_text, fmt).date()
        except ValueError:
            continue
    return None
 
 
def parse_score(text):
    text = text.strip()
    if not text:
        return None
    try:
        score = int(text)
    except ValueError:
        return None
    return score if 0 <= score <= MAX_POINTS else None
 
 
def get_school_id_from_link(tag):
    """Extract the s= school ID from an anchor tag href."""
    if not tag:
        return None
    href = tag.get("href", "")
    match = re.search(r"[?&]s=(\d+)", href, re.IGNORECASE)
    return match.group(1) if match else None
 
 
def parse_date_from_cells(cells):
    """Scan row cells for a recognizable date string."""
    for cell in cells:
        text = cell.get_text(strip=True)
        if re.match(r'\d{1,2}/\d{1,2}/\d{2,4}', text):
            return parse_game_date(text)
        if re.match(r'[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}', text):
            return parse_game_date(text)
    return None
 
 
def parse_scores_from_cells(cells):
    """
    Extract home and opponent scores from a row.
    MSHSAA schedule pages show scores as 'W 42-14' or 'L 7-28'.
    Returns (home_score, opp_score) or (None, None).
    """
    for cell in cells:
        text = cell.get_text(strip=True)
        match = re.search(r'([WL])\s+(\d+)-(\d+)', text, re.IGNORECASE)
        if match:
            result = match.group(1).upper()
            s1, s2 = int(match.group(2)), int(match.group(3))
            home, opp = (s1, s2) if result == 'W' else (s2, s1)
            if 0 <= home <= MAX_POINTS and 0 <= opp <= MAX_POINTS:
                return home, opp
    return None, None
 
 
def parse_opponent_from_cells(cells, id_to_classname):
    """
    Find the opponent link in a row and resolve it to a classification name
    via school ID. Falls back to display text if ID not found in map.
    """
    for cell in cells:
        link = cell.find("a", href=lambda h: h and "/MySchool/Schedule.aspx" in h)
        if link:
            sid = get_school_id_from_link(link)
            if sid and sid in id_to_classname:
                return id_to_classname[sid]
            return link.get_text(strip=True) or None
    return None
 
 
def scrape_schedule(home_classname, url, id_to_classname):
    """
    Scrape one school's schedule page and return completed games within
    the season date range as tuples:
        (date_str, home_classname, home_score, opp_classname, opp_score)
 
    Because we scrape from the home school's own page, the home school
    name is always the exact classification name — no ID resolution needed
    for the home side. Only the opponent needs ID resolution.
    """
    try:
        resp = requests.get(url, timeout=20, headers=HEADERS)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"    Failed {home_classname}: {e}")
        return []
 
    soup  = BeautifulSoup(resp.text, "html.parser")
    games = []
 
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 4:
                continue
 
            # Date
            game_date = parse_date_from_cells(cells)
            if game_date is None:
                continue
            if not (SEASON_START <= game_date <= SEASON_END):
                continue
 
            # Skip forfeits
            row_text = " ".join(c.get_text() for c in cells).lower()
            if "forfeit" in row_text:
                continue
 
            # Scores
            home_score, opp_score = parse_scores_from_cells(cells)
            if home_score is None or opp_score is None:
                continue
 
            # Opponent
            opp_classname = parse_opponent_from_cells(cells, id_to_classname)
            if opp_classname is None:
                continue
 
            games.append((
                game_date.strftime("%Y-%m-%d"),
                home_classname,
                home_score,
                opp_classname,
                opp_score
            ))
 
    return games
 
 
# ---------------------------------------------------------------------------
# FULL SEASON SCRAPE
# ---------------------------------------------------------------------------
 
def scrape_full_season(team_to_class, id_to_classname,
                       csv_to_classname, schedule_urls):
    """
    Scrape every classification school's schedule page.
    Deduplicates games since each game appears on both teams' pages.
    Only keeps games where both teams exist in classifications.json.
    """
    all_games  = []
    seen_games = set()
    known_teams = set(team_to_class.keys())
 
    for csv_name, info in schedule_urls.items():
        classname = resolve_csv_name(csv_name, csv_to_classname, team_to_class)
        if classname is None:
            # Not in classifications.json — skip
            continue
 
        print(f"  Scraping {classname}...", end=" ", flush=True)
        games = scrape_schedule(classname, info["url"], id_to_classname)
 
        new_count = 0
        for game in games:
            date_str, home, h_score, away, a_score = game
 
            # Only keep games where both teams are in classifications
            if away not in known_teams:
                continue
 
            # Deduplicate — same game appears on both schools' pages
            key = frozenset([date_str, home, away])
            if key in seen_games:
                continue
            seen_games.add(key)
 
            all_games.append(game)
            new_count += 1
 
        print(f"{new_count} new games ({len(games)} on schedule page)")
        time.sleep(0.5)
 
    return all_games
 
 
# ---------------------------------------------------------------------------
# CSV OUTPUT
# ---------------------------------------------------------------------------
 
def save_csv(all_games):
    with open(CSV_PATH, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Date", "Home Team", "Home Score", "Away Team", "Away Score"])
        for date_str, t1, s1, t2, s2 in all_games:
            writer.writerow([date_str, t1, s1, t2, s2])
    print(f"Saved {len(all_games)} games to {CSV_PATH}")
 
 
# ---------------------------------------------------------------------------
# RATING ENGINE
# ---------------------------------------------------------------------------
 
def run_iterations(games, teams, off_rating, def_rating, league_avg,
                   iterations, phase_label, ovr_filter=None):
    for iteration in range(iterations):
        off_error    = {t: 0.0 for t in teams}
        def_error    = {t: 0.0 for t in teams}
        games_played = {t: 0   for t in teams}
 
        eligible_games = games
        if ovr_filter is not None:
            eligible_games = [
                (t1, t2, s1, s2) for t1, t2, s1, s2 in games
                if abs((off_rating[t1] + def_rating[t1]) -
                       (off_rating[t2] + def_rating[t2])) <= ovr_filter
            ]
 
        for t1, t2, actual_s1, actual_s2 in eligible_games:
            predicted_s1 = off_rating[t1] - def_rating[t2] + league_avg
            predicted_s2 = off_rating[t2] - def_rating[t1] + league_avg
 
            error_s1 = actual_s1 - predicted_s1
            error_s2 = actual_s2 - predicted_s2
 
            off_error[t1] += error_s1
            off_error[t2] += error_s2
            def_error[t1] += -error_s2
            def_error[t2] += -error_s1
 
            games_played[t1] += 1
            games_played[t2] += 1
 
        for team in teams:
            if games_played[team] > 0:
                off_rating[team] += (
                    (off_error[team] / games_played[team]) * LEARNING_RATE
                )
                def_rating[team] += (
                    (def_error[team] / games_played[team]) * LEARNING_RATE
                )
 
        if (iteration + 1) % 100 == 0:
            eligible_count = (
                len(eligible_games) if ovr_filter is not None else len(games)
            )
            print(
                f"  [{phase_label}] Iteration {iteration + 1}/{iterations} complete"
                + (f" | Competitive games: {eligible_count}" if ovr_filter else "")
            )
 
 
def calculate_ratings(all_games, iterations=ITERATIONS):
    games = [(t1, t2, s1, s2) for _, t1, s1, t2, s2 in all_games]
 
    teams = list({t for t1, t2, _, _ in games for t in (t1, t2)})
    if not teams:
        return {}, {}, {}, 0
 
    all_scores = [s for _, _, s1, s2 in games for s in (s1, s2)]
    league_avg = sum(all_scores) / len(all_scores)
    print(f"  League average: {league_avg:.2f} points per game")
 
    off_rating = {t: 0.0 for t in teams}
    def_rating = {t: 0.0 for t in teams}
 
    print(f"\n  Running Phase 1 ({iterations} iterations, all games)...")
    run_iterations(games, teams, off_rating, def_rating, league_avg,
                   iterations=iterations, phase_label="Phase 1", ovr_filter=None)
 
    print(f"\n  Running Phase 2 ({iterations} iterations, "
          f"competitive games within {COMPETITIVE_THRESHOLD} OVR pts)...")
    run_iterations(games, teams, off_rating, def_rating, league_avg,
                   iterations=iterations, phase_label="Phase 2",
                   ovr_filter=COMPETITIVE_THRESHOLD)
 
    ovr_rating = {t: round(off_rating[t] + def_rating[t], 2) for t in teams}
    return off_rating, def_rating, ovr_rating, league_avg
 
 
# ---------------------------------------------------------------------------
# JSON OUTPUT
# ---------------------------------------------------------------------------
 
def build_team_entries(off_rating, def_rating, ovr_rating,
                       team_to_class, team_to_district,
                       class_filter=None):
    all_teams = list(ovr_rating.keys())
 
    pool = (
        [t for t in all_teams if team_to_class.get(t) == class_filter]
        if class_filter is not None
        else all_teams
    )
 
    ovr_sorted = sorted(pool, key=lambda t: ovr_rating[t],  reverse=True)
    off_sorted = sorted(pool, key=lambda t: off_rating[t],  reverse=True)
    def_sorted = sorted(pool, key=lambda t: def_rating[t],  reverse=True)
 
    ovr_rank = {t: i + 1 for i, t in enumerate(ovr_sorted)}
    off_rank = {t: i + 1 for i, t in enumerate(off_sorted)}
    def_rank = {t: i + 1 for i, t in enumerate(def_sorted)}
 
    return [
        {
            "ovr_rank":       ovr_rank[t],
            "school":         t,
            "classification": team_to_class.get(t),
            "district":       team_to_district.get(t),
            "ovr_rating":     ovr_rating[t],
            "off_rating":     round(off_rating[t], 2),
            "off_rank":       off_rank[t],
            "def_rating":     round(def_rating[t], 2),
            "def_rank":       def_rank[t],
        }
        for t in ovr_sorted
    ]
 
 
def save_overall_json(off_rating, def_rating, ovr_rating, league_avg,
                      team_to_class, team_to_district):
    entries = build_team_entries(off_rating, def_rating, ovr_rating,
                                 team_to_class, team_to_district)
    output = {
        "last_updated":   datetime.now().strftime("%B %d, %Y at %I:%M %p"),
        "league_average": round(league_avg, 2),
        "teams": entries,
    }
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)
 
    print(f"Saved {len(entries)} teams to {OUTPUT_PATH}")
    print("Top 5 overall:")
    for e in entries[:5]:
        print(f"  {e['ovr_rank']}. {e['school']} (Class {e['classification']}) "
              f"| OVR: {e['ovr_rating']:+.2f} "
              f"| OFF: {e['off_rating']:+.2f} "
              f"| DEF: {e['def_rating']:+.2f}")
 
 
def save_class_jsons(off_rating, def_rating, ovr_rating, league_avg,
                     team_to_class, team_to_district):
    for cls in range(1, 7):
        entries = build_team_entries(off_rating, def_rating, ovr_rating,
                                     team_to_class, team_to_district,
                                     class_filter=cls)
        if not entries:
            print(f"  Class {cls}: no teams found — skipping.")
            continue
 
        path = f"football_ratings_2010_class{cls}.json"
        output = {
            "last_updated":   datetime.now().strftime("%B %d, %Y at %I:%M %p"),
            "league_average": round(league_avg, 2),
            "classification": cls,
            "teams": entries,
        }
        with open(path, "w") as f:
            json.dump(output, f, indent=2)
 
        print(f"  Class {cls}: {len(entries)} teams → {path}")
        print("    Top 3: " + " | ".join(
            f"{e['ovr_rank']}. {e['school']} ({e['ovr_rating']:+.2f})"
            for e in entries[:3]
        ))
 
 
# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
 
if __name__ == "__main__":
    print("=== MSHSAA Football Ratings 2010 ===")
 
    print("\nLoading classifications...")
    team_to_class, team_to_district = load_classifications()
    print(f"  Loaded {len(team_to_class)} teams from {CLASSIFICATIONS_PATH}")
 
    print("\nBuilding school ID → classification name lookup...")
    id_to_classname, csv_to_classname = build_classname_lookup(
        team_to_class, SCHOOLS_CSV
    )
 
    print("\nLoading football schedule URLs...")
    schedule_urls = load_football_schedule_urls()
    print(f"  Loaded {len(schedule_urls)} football schedule URLs")
 
    print("\nScraping individual school schedules...")
    all_games = scrape_full_season(
        team_to_class, id_to_classname, csv_to_classname, schedule_urls
    )
    print(f"\nTotal unique valid games: {len(all_games)}")
    if not all_games:
        print("No games found — exiting.")
        exit(1)
 
    print("\nSaving scoreboard CSV...")
    save_csv(all_games)
 
    print(f"\nRunning ratings engine "
          f"({ITERATIONS} Phase 1 + {ITERATIONS} Phase 2 iterations)...")
    off_rating, def_rating, ovr_rating, league_avg = calculate_ratings(all_games)
 
    print("\nSaving overall ratings JSON...")
    save_overall_json(off_rating, def_rating, ovr_rating, league_avg,
                      team_to_class, team_to_district)
 
    print("\nSaving per-class ratings JSONs...")
    save_class_jsons(off_rating, def_rating, ovr_rating, league_avg,
                     team_to_class, team_to_district)
 
    print("\n=== Done ===")
