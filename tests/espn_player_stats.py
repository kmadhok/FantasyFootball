import random, time, re, sys, json
from typing import List, Dict, Any
import requests
import pandas as pd

SEASON = 2025
SEASON_TYPE = 2  # 2 = regular, 3 = postseason
N_PLAYERS = 50
UA = {"User-Agent": "kanu-ffl-demo/0.1 (+https://example.com)"}

def get_active_athletes() -> List[Dict[str, Any]]:
    """Return list of {'id': '####', 'fullName': '...'} for active NFL athletes."""
    url = "https://sports.core.api.espn.com/v3/sports/football/nfl/athletes"
    params = {"limit": 20000, "active": "true"}
    r = requests.get(url, params=params, headers=UA, timeout=30)
    r.raise_for_status()
    data = r.json()

    items = data.get("items", []) or []
    out = []
    for it in items:
        # v3 usually includes id/fullName directly; if not, try to parse from href
        aid = str(it.get("id") or "")
        name = it.get("fullName") or it.get("displayName") or ""
        if not aid:
            href = it.get("href") or it.get("$ref") or ""
            m = re.search(r"/athletes/(\d+)", href)
            if m:
                aid = m.group(1)
        if aid:
            out.append({"id": aid, "fullName": name})
    return out

def fetch_gamelog(athlete_id: str) -> Dict[str, Any]:
    """Return raw gamelog JSON for an athlete for a season."""
    url = f"https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/{athlete_id}/gamelog"
    params = {"season": SEASON, "seasonType": SEASON_TYPE}
    r = requests.get(url, params=params, headers=UA, timeout=30)
    # some IDs may 404 if player never logged NFL games this season
    if r.status_code == 404:
        return {}
    r.raise_for_status()
    return r.json()

def flatten_entry(athlete, entry) -> Dict[str, Any]:
    """
    Try to coerce one game 'entry' into a flat row.
    ESPN responses vary a bit by sport/year; this tries to be forgiving.
    """
    row = {
        "athlete_id": athlete["id"],
        "athlete_name": athlete.get("fullName", ""),
        "season": SEASON,
        "season_type": SEASON_TYPE,
    }

    # Common places for metadata
    # Many gamelog payloads wrap entries like: {"entries":[{...},{...}]}
    # Game identifiers and dates show up under "game", "event", or nested links.
    game = entry.get("game") or entry.get("event") or {}
    row["game_id"] = game.get("id") or game.get("uid", "").split(":")[-1] or None
    row["game_date"] = game.get("date") or game.get("startDate") or entry.get("date")

    # Week can be in different spots
    wk = entry.get("week") or {}
    row["week"] = wk.get("number") if isinstance(wk, dict) else wk

    # Opponent shorthand (best effort)
    opp = entry.get("opponent") or entry.get("against") or {}
    if isinstance(opp, dict):
        row["opponent_abbr"] = opp.get("abbreviation")
        row["opponent_id"] = opp.get("id")
    else:
        row["opponent_abbr"] = opp

    # Result string if present
    row["result"] = entry.get("result") or entry.get("gameResult")

    # Stats can be in several shapes:
    # - entry["stats"] = [{"name":"passingYards","value":275}, ...]
    # - entry["statistics"] = [{"name":"passing","displayName":"Passing","stats":[{"name":"yards","value":275}, ...]}, ...]
    # We'll promote a handful of common numeric stats, and keep a JSON dump of the raw stat objects.
    promoted = {}
    if "stats" in entry and isinstance(entry["stats"], list):
        for s in entry["stats"]:
            n = s.get("name")
            v = s.get("value")
            if n and isinstance(v, (int, float)):
                promoted[n] = v

    if "statistics" in entry and isinstance(entry["statistics"], list):
        for cat in entry["statistics"]:
            cat_name = cat.get("name")
            for s in cat.get("stats", []):
                n = s.get("name")
                v = s.get("value")
                if cat_name and n and isinstance(v, (int, float)):
                    promoted[f"{cat_name}_{n}"] = v

    # Common aliases we like to see in NFL box scores
    aliases = {
        "passingYards": None, "passingTouchdowns": None, "interceptions": None,
        "rushingYards": None, "rushingTouchdowns": None,
        "receivingYards": None, "receivingTouchdowns": None,
        "receptions": None, "targets": None, "fumblesLost": None, "snaps": None,
    }
    for k in aliases:
        row[k] = promoted.get(k)

    row["all_promoted_stats"] = json.dumps(promoted, separators=(",", ":"))
    # Keep a raw snapshot if the caller wants to dig further
    row["raw_entry"] = json.dumps(entry, separators=(",", ":"))
    return row

def entries_from_gamelog(gl_json) -> List[Dict[str, Any]]:
    # Different seasons might wrap differently. Try a few shapes.
    if not gl_json:
        return []
    # Typical shape: {"gamelogs":[{"entries":[{...}, ...]}], "season":{"year":...}}
    if "gamelogs" in gl_json and isinstance(gl_json["gamelogs"], list):
        out = []
        for block in gl_json["gamelogs"]:
            ents = block.get("entries", [])
            out.extend(ents if isinstance(ents, list) else [])
        if out:
            return out
    # Some payloads expose "events" at top-level
    if "events" in gl_json and isinstance(gl_json["events"], list):
        return gl_json["events"]
    # Last resort – if there's a single "entries" somewhere
    if "entries" in gl_json and isinstance(gl_json["entries"], list):
        return gl_json["entries"]
    return []

def main():
    print("Fetching active NFL athletes index...")
    athletes = get_active_athletes()
    if len(athletes) < N_PLAYERS:
        print(f"Only found {len(athletes)} athletes; aborting.", file=sys.stderr)
        sys.exit(1)

    sample = random.sample(athletes, N_PLAYERS)
    rows = []

    for i, ath in enumerate(sample, 1):
        try:
            gl = fetch_gamelog(ath["id"])
            ents = entries_from_gamelog(gl)
            for e in ents:
                rows.append(flatten_entry(ath, e))
        except Exception as ex:
            print(f"[WARN] {ath['id']} gamelog failed: {ex}", file=sys.stderr)
        # courteous delay; ESPN endpoints are public/undocumented and may rate-limit
        time.sleep(0.2)

        if i % 10 == 0:
            print(f"...processed {i} players ({len(rows)} game rows so far)")

    if not rows:
        print("No rows collected (players may have no 2025 logs yet).", file=sys.stderr)
        sys.exit(0)

    df = pd.DataFrame(rows)
    # Basic sort for readability
    df.sort_values(["athlete_name", "week", "game_date"], inplace=True, na_position="last")
    df.reset_index(drop=True, inplace=True)
    df.to_csv("espn_weeklies_50players.csv", index=False)
    print(f"Done. Wrote {len(df)} rows to espn_weeklies_50players.csv")

if __name__ == "__main__":
    main()
