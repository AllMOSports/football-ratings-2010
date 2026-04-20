import requests
from bs4 import BeautifulSoup
import json
import csv
from datetime import datetime, date, timedelta
import time
 
SEASON_START  = date(2010, 8, 1)
SEASON_END    = date(2010, 12, 15)
BASE_URL      = "https://www.mshsaa.org/activities/scoreboard.aspx?alg=19&date={}"
MAX_POINTS      = 100
OUTPUT_PATH   = "football_ratings_2010.json"
CSV_PATH      = "football_scoreboard_2010.csv"
ITERATIONS    = 1000
LEARNING_RATE = 0.1
COMPETITIVE_THRESHOLD = 35  # Max OVR gap for Phase 2 iterations
 
# --- SCRAPING ---
 
def is_mshsaa_team(cell):
    return cell.find("a", href=lambda h: h and "/MySchool/Schedule.aspx" in h) is not None
 
def parse_score(text):
    text = text.strip()
    if not text:
        return None
    try:
        score = int(text)
    except ValueError:
        return None
    return score if 0 <= score <= MAX_POINTS else None
 
def is_forfeit(c1, c2):
    return "forfeit" in (c1.get_text() + c2.get_text()).lower()
 
def scrape_date(target_date):
    url = BASE_URL.format(target_date.strftime("%m%d%Y"))
    try:
        resp = requests.get(url, timeout=20, headers={
            "User-Agent": "Mozilla/5.0 (compatible; FootballRatingsBot/1.0)"
        })
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  Failed {target_date}: {e}")
        return []
 
    soup  = BeautifulSoup(resp.text, "html.parser")
    games = []
 
    print(f"  Page length: {len(resp.text)} chars")
    print(f"  Tables found: {len(soup.find_all('table'))}")
 
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 3:
            continue
        if "final" not in rows[-1].get_text().lower():
            continue
        t1c = rows[1].find_all("td")
        t2c = rows[2].find_all("td")
        if len(t1c) < 3 or len(t2c) < 3:
            continue
        if not is_mshsaa_team(t1c[1]) or not is_mshsaa_team(t2c[1]):
            continue
        if is_forfeit(t1c[1], t2c[1]):
            continue
        l1 = t1c[1].find("a")
        l2 = t2c[1].find("a")
        if not l1 or not l2:
            continue
        s1 = parse_score(t1c[2].get_text())
        s2 = parse_score(t2c[2].get_text())
        if s1 is None or s2 is None:
            continue
        games.append((
            target_date.strftime("%Y-%m-%d"),
            l1.get_text().strip(),
            s1,
            l2.get_text().strip(),
            s2
        ))
 
    return games
 
def scrape_full_season():
    all_games = []
    current   = SEASON_START
    while current <= min(SEASON_END, date.today()):
        print(f"  Scraping {current}...", end=" ", flush=True)
        day_games = scrape_date(current)
        all_games.extend(day_games)
        print(f"{len(day_games)} games")
        current += timedelta(days=1)
        time.sleep(0.5)
    return all_games
 
def save_csv(all_games):
    with open(CSV_PATH, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Date", "Home Team", "Home Score", "Away Team", "Away Score"])
        for date_str, t1, s1, t2, s2 in all_games:
            writer.writerow([date_str, t1, s1, t2, s2])
    print(f"Saved {len(all_games)} games to {CSV_PATH}")
 
# --- RATING ENGINE ---
 
def run_iterations(games, teams, off_rating, def_rating, league_avg,
                   iterations, phase_label, ovr_filter=None):
    """
    Run a block of gradient-descent iterations.
 
    Phase 1 (ovr_filter=None): every game is used every iteration.
 
    Phase 2 (ovr_filter=float): the eligible game list is rebuilt at the START
    of each iteration using that iteration's current ratings, so a game can
    enter or leave the pool from one iteration to the next as ratings shift.
    """
 
    for iteration in range(iterations):
        off_error    = {t: 0.0 for t in teams}
        def_error    = {t: 0.0 for t in teams}
        games_played = {t: 0   for t in teams}
 
        # Rebuild the competitive game list fresh each iteration for Phase 2
        if ovr_filter is not None:
            eligible_games = [
                (t1, t2, s1, s2) for t1, t2, s1, s2 in games
                if abs((off_rating[t1] + def_rating[t1]) -
                        (off_rating[t2] + def_rating[t2])) <= ovr_filter
            ]
        else:
            eligible_games = games
 
        for t1, t2, actual_s1, actual_s2 in eligible_games:
            predicted_s1 = off_rating[t1] - def_rating[t2] + league_avg
            predicted_s2 = off_rating[t2] - def_rating[t1] + league_avg
 
            error_s1 = actual_s1 - predicted_s1
            error_s2 = actual_s2 - predicted_s2
 
            off_error[t1]    += error_s1
            off_error[t2]    += error_s2
            def_error[t1]    += -error_s2
            def_error[t2]    += -error_s1
 
            games_played[t1] += 1
            games_played[t2] += 1
 
        for team in teams:
            if games_played[team] > 0:
                off_rating[team] += (off_error[team] / games_played[team]) * LEARNING_RATE
                def_rating[team] += (def_error[team] / games_played[team]) * LEARNING_RATE
 
        if (iteration + 1) % 100 == 0:
            eligible_count = len(eligible_games) if ovr_filter is not None else len(games)
            print(f"  [{phase_label}] Iteration {iteration + 1}/{iterations} complete"
                  + (f" | Competitive games this iteration: {eligible_count}" if ovr_filter else ""))
 
 
def calculate_ratings(all_games, iterations=ITERATIONS):
    # Strip date from games for rating calculations
    games = [(t1, t2, s1, s2) for _, t1, s1, t2, s2 in all_games]
 
    teams = list({t for t1, t2, _, _ in games for t in (t1, t2)})
    if not teams:
        return {}, {}, {}, 0
 
    # Calculate league average points per game
    all_scores = [s for _, _, s1, s2 in games for s in (s1, s2)]
    league_avg = sum(all_scores) / len(all_scores)
    print(f"  League average: {league_avg:.2f} points per game")
 
    # Shared rating dicts — Phase 2 picks up exactly where Phase 1 ends
    off_rating = {t: 0.0 for t in teams}
    def_rating = {t: 0.0 for t in teams}
 
    # --- Phase 1: All games ---
    print(f"\n  Running Phase 1 ({iterations} iterations, all games)...")
    run_iterations(games, teams, off_rating, def_rating, league_avg,
                   iterations=iterations, phase_label="Phase 1", ovr_filter=None)
 
    # --- Phase 2: Competitive games only, filter re-evaluated every iteration ---
    print(f"\n  Running Phase 2 ({iterations} iterations, "
          f"competitive games within {COMPETITIVE_THRESHOLD} OVR pts, dynamic filter)...")
    run_iterations(games, teams, off_rating, def_rating, league_avg,
                   iterations=iterations, phase_label="Phase 2",
                   ovr_filter=COMPETITIVE_THRESHOLD)
 
    # OVR = OFF + DEF (raw sum in points above/below average)
    ovr_rating = {t: round(off_rating[t] + def_rating[t], 2) for t in teams}
 
    return off_rating, def_rating, ovr_rating, league_avg
 
 
def save_json(off_rating, def_rating, ovr_rating, league_avg):
    teams      = sorted(ovr_rating, key=lambda t: ovr_rating[t], reverse=True)
    off_ranked = sorted(teams, key=lambda t: off_rating[t], reverse=True)
    def_ranked = sorted(teams, key=lambda t: def_rating[t], reverse=True)
    off_rank   = {t: i+1 for i, t in enumerate(off_ranked)}
    def_rank   = {t: i+1 for i, t in enumerate(def_ranked)}
 
    output = {
        "last_updated":   datetime.now().strftime("%B %d, %Y at %I:%M %p"),
        "league_average": round(league_avg, 2),
        "teams": [{
            "ovr_rank":   i + 1,
            "school":     t,
            "ovr_rating": ovr_rating[t],
            "off_rating": round(off_rating[t], 2),
            "off_rank":   off_rank[t],
            "def_rating": round(def_rating[t], 2),
            "def_rank":   def_rank[t]
        } for i, t in enumerate(teams)]
    }
 
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)
 
    print(f"Saved {len(teams)} teams to {OUTPUT_PATH}")
    print(f"League average: {league_avg:.2f} points/game")
    print(f"Top 5 teams:")
    for entry in output["teams"][:5]:
        print(f"  {entry['ovr_rank']}. {entry['school']} "
              f"| OVR: {entry['ovr_rating']:+.2f} "
              f"| OFF: {entry['off_rating']:+.2f} "
              f"| DEF: {entry['def_rating']:+.2f}")
 
 
if __name__ == "__main__":
    print("=== MSHSAA Football Ratings ===")
    all_games = scrape_full_season()
    print(f"\nTotal valid games: {len(all_games)}")
    if not all_games:
        print("No games found — exiting.")
        exit(1)
 
    print("\nSaving scoreboard CSV...")
    save_csv(all_games)
 
    print(f"\nRunning {ITERATIONS} Phase 1 + {ITERATIONS} Phase 2 iterations...")
    off_rating, def_rating, ovr_rating, league_avg = calculate_ratings(all_games)
 
    print("\nSaving ratings JSON...")
    save_json(off_rating, def_rating, ovr_rating, league_avg)
    print("\n=== Done ===")
