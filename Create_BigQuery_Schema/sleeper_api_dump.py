#!/usr/bin/env python3
"""
Sleeper API → Tidy CSV dump

Fetches comprehensive data for a Sleeper league and organizes it into CSV files:
- players.csv (optional)
- league.csv, users.csv, rosters.csv, roster_players_current.csv
- matchups_week_{w}.csv and matchup_lineups_week_{w}.csv (derived)
- transactions_week_{w}.csv, transaction_players_week_{w}.csv,
  transaction_faab_transfers_week_{w}.csv, transaction_draft_picks_week_{w}.csv
- drafts.csv and draft_picks.csv

Usage:
  python Create_BigQuery_Schema/sleeper_api_dump.py \
    --league-id 1257071160403709954 \
    --weeks 1-18 \
    --outdir out/sleeper_dump \
    --include-players

Notes:
- Writes CSV files; ensures the output directory exists.
- Adds helpful derived tables (roster_players_current, lineup) to make analysis easier.
- Retries network calls with light backoff.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests

SLEEPER = "https://api.sleeper.app/v1"


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Dump Sleeper league data to CSV")
    ap.add_argument("--league-id", required=True, help="Sleeper league id")
    ap.add_argument("--weeks", default="1-18", help="Weeks to fetch, e.g., '1-18' or '1 3 5'")
    ap.add_argument("--outdir", default="out/sleeper_dump", help="Output directory for CSVs")
    ap.add_argument("--include-players", action="store_true", help="Also dump the global players dataset (large)")
    return ap.parse_args()


def weeks_arg_to_list(s: str) -> List[int]:
    s = s.strip()
    if "-" in s:
        a, b = s.split("-", 1)
        return list(range(int(a), int(b) + 1))
    return [int(x) for x in s.split()]


def to_json_str(obj: Any) -> Optional[str]:
    if obj is None:
        return None
    try:
        return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        return None


def backoff_get(url: str, timeout: int = 30, attempts: int = 5) -> Any:
    last_err: Optional[Exception] = None
    for i in range(attempts):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            sleep = min(8, 0.5 * (2 ** i))
            time.sleep(sleep)
    raise last_err or RuntimeError(f"GET failed for {url}")


def ensure_outdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_csv(df: pd.DataFrame, path: str) -> None:
    ensure_outdir(os.path.dirname(path) or ".")
    df.to_csv(path, index=False)
    print(f"[OK] wrote {path} ({len(df)} rows)")


def sleeper_players_df() -> pd.DataFrame:
    print("[FETCH] players dump (this can be large)...")
    data = backoff_get(f"{SLEEPER}/players/nfl") or {}
    rows = []
    for pid, obj in data.items():
        if not isinstance(obj, dict):
            continue
        o = obj.copy()
        o["player_id"] = pid
        # Flatten heavy dicts to JSON
        for k in ("metadata", "advanced_stats", "team_data", "practice_participation"):
            if k in o:
                o[k] = to_json_str(o.get(k))
        rows.append(o)
    df = pd.DataFrame(rows)
    # choose friendly subset if present
    keep = [
        "player_id", "full_name", "first_name", "last_name",
        "team", "position", "fantasy_positions",
        "status", "injury_status", "injury_start_date", "practice_participation",
        "age", "height", "weight", "metadata",
    ]
    existing = [c for c in keep if c in df.columns]
    return df[existing] if existing else df


def sleeper_league_core(league_id: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    league = backoff_get(f"{SLEEPER}/league/{league_id}") or {}
    df_league = pd.DataFrame([{
        "league_id": str(league_id),
        "name": league.get("name"),
        "season": int(league.get("season")) if league.get("season") else None,
        "sport": league.get("sport"),
        "status": league.get("status"),
        "total_rosters": league.get("total_rosters"),
        "draft_id": league.get("draft_id"),
        "scoring_settings": to_json_str(league.get("scoring_settings")),
        "roster_positions": to_json_str(league.get("roster_positions")),
        "metadata": to_json_str(league.get("metadata")),
        "created": league.get("created")
    }])

    users = backoff_get(f"{SLEEPER}/league/{league_id}/users") or []
    df_users = pd.DataFrame([{
        "user_id": u.get("user_id"),
        "username": u.get("username"),
        "display_name": u.get("display_name"),
        "avatar": u.get("avatar"),
        "metadata": to_json_str(u.get("metadata")),
        "is_commissioner": bool(u.get("is_owner")),
        "league_id": str(league_id),
    } for u in users])

    rosters = backoff_get(f"{SLEEPER}/league/{league_id}/rosters") or []
    df_rosters = pd.DataFrame([{
        "league_id": r.get("league_id"),
        "roster_id": r.get("roster_id"),
        "owner_id": r.get("owner_id"),
        "co_owner_ids": to_json_str(r.get("co_owners")),
        "starters": to_json_str(r.get("starters")),
        "players": to_json_str(r.get("players")),
        "reserve": to_json_str(r.get("reserve")),
        "settings": to_json_str(r.get("settings")),
    } for r in rosters])

    return df_league, df_users, df_rosters


def derive_roster_players_current(df_rosters: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for _, r in df_rosters.iterrows():
        league_id = r.get("league_id")
        roster_id = r.get("roster_id")
        try:
            plist = json.loads(r.get("players") or "null") or []
        except Exception:
            plist = []
        for pid in plist:
            rows.append({
                "league_id": league_id,
                "roster_id": roster_id,
                "player_id": pid,
            })
    return pd.DataFrame(rows)


def sleeper_matchups_df(league_id: str, week: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    data = backoff_get(f"{SLEEPER}/league/{league_id}/matchups/{week}") or []
    rows: List[Dict[str, Any]] = []
    lineups: List[Dict[str, Any]] = []
    for obj in data:
        rows.append({
            "league_id": league_id,
            "week": week,
            "matchup_id": obj.get("matchup_id"),
            "roster_id": obj.get("roster_id"),
            "points": obj.get("points"),
            "custom_points": obj.get("custom_points"),
            "players": to_json_str(obj.get("players")),
            "starters": to_json_str(obj.get("starters")),
        })
        starters = obj.get("starters") or []
        players = obj.get("players") or []
        bench = [p for p in players if p not in starters]
        for idx, pid in enumerate(starters):
            lineups.append({
                "league_id": league_id, "week": week, "matchup_id": obj.get("matchup_id"),
                "roster_id": obj.get("roster_id"), "slot_index": idx,
                "player_id": pid, "is_starter": True
            })
        for idx, pid in enumerate(bench):
            lineups.append({
                "league_id": league_id, "week": week, "matchup_id": obj.get("matchup_id"),
                "roster_id": obj.get("roster_id"), "slot_index": 1000 + idx,
                "player_id": pid, "is_starter": False
            })
    return pd.DataFrame(rows), pd.DataFrame(lineups)


def sleeper_transactions_for_week(league_id: str, week: int) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    data = backoff_get(f"{SLEEPER}/league/{league_id}/transactions/{week}") or []
    tx_rows: List[Dict[str, Any]] = []
    leg_rows: List[Dict[str, Any]] = []
    faab_rows: List[Dict[str, Any]] = []
    pick_rows: List[Dict[str, Any]] = []

    for t in data:
        tx_rows.append({
            "league_id": league_id,
            "transaction_id": t.get("transaction_id"),
            "type": t.get("type"),
            "status": t.get("status"),
            "notes": t.get("notes"),
            "creator": t.get("creator"),
            "created": t.get("created"),
            "executed": t.get("executed"),
            "consenter_ids": to_json_str(t.get("consenter_ids")),
            "roster_ids": to_json_str(t.get("roster_ids")),
            "leg": t.get("leg"),
            "draft_id": t.get("draft_id"),
            "waiver_budget": to_json_str(t.get("waiver_budget")),
            "adds": to_json_str(t.get("adds")),
            "drops": to_json_str(t.get("drops")),
            "metadata": to_json_str(t.get("metadata")),
        })

        players = t.get("players") or []
        for seq, pid in enumerate(players, 1):
            leg_rows.append({
                "league_id": league_id,
                "transaction_id": t.get("transaction_id"),
                "leg_no": seq,
                "player_id": pid,
                "type": t.get("type"),
                "status": t.get("status"),
            })

        faab = t.get("waiver_budget") or []
        for seq, fb in enumerate(faab, 1):
            faab_rows.append({
                "league_id": league_id,
                "transaction_id": t.get("transaction_id"),
                "seq": seq,
                "sender": fb.get("sender"),
                "receiver": fb.get("receiver"),
                "amount": fb.get("amount"),
            })

        picks = t.get("draft_picks") or []
        for seq, pk in enumerate(picks, 1):
            pick_rows.append({
                "league_id": league_id,
                "transaction_id": t.get("transaction_id"),
                "seq": seq,
                "season": pk.get("season"),
                "round": pk.get("round"),
                "roster_id": pk.get("roster_id"),
                "previous_owner_id": pk.get("previous_owner_id"),
                "owner_id": pk.get("owner_id"),
            })

    return (
        pd.DataFrame(tx_rows),
        pd.DataFrame(leg_rows),
        pd.DataFrame(faab_rows),
        pd.DataFrame(pick_rows),
    )


def sleeper_drafts_df(league_id: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    drafts = backoff_get(f"{SLEEPER}/league/{league_id}/drafts") or []
    df_drafts = pd.DataFrame([{
        "league_id": league_id,
        "draft_id": d.get("draft_id"),
        "status": d.get("status"),
        "start_time": d.get("start_time"),
        "sport": d.get("sport"),
        "type": d.get("type"),
        "settings": to_json_str(d.get("settings")),
        "slots_wr": d.get("slots_wr"),
        "slots_rb": d.get("slots_rb"),
        "teams": d.get("teams"),
    } for d in drafts])

    picks_all: List[Dict[str, Any]] = []
    for d in drafts:
        did = d.get("draft_id")
        if not did:
            continue
        picks = backoff_get(f"{SLEEPER}/draft/{did}/picks") or []
        for p in picks:
            picks_all.append({
                "draft_id": did,
                "round": p.get("round"),
                "pick_no": p.get("pick_no"),
                "roster_id": p.get("roster_id"),
                "player_id": p.get("player_id"),
                "metadata": to_json_str(p.get("metadata")),
                "is_keeper": p.get("is_keeper"),
            })

    df_picks = pd.DataFrame(picks_all)
    return df_drafts, df_picks


def main() -> int:
    args = parse_args()
    weeks = weeks_arg_to_list(args.weeks)
    outdir = args.outdir.rstrip("/")
    ensure_outdir(outdir)

    # League core
    print("[FETCH] league core …")
    df_league, df_users, df_rosters = sleeper_league_core(args.league_id)
    write_csv(df_league, f"{outdir}/league.csv")
    write_csv(df_users, f"{outdir}/users.csv")
    write_csv(df_rosters, f"{outdir}/rosters.csv")
    write_csv(derive_roster_players_current(df_rosters), f"{outdir}/roster_players_current.csv")

    # Players (optional)
    if args.include_players:
        df_players = sleeper_players_df()
        write_csv(df_players, f"{outdir}/players.csv")

    # Matchups & Transactions per week
    for w in weeks:
        print(f"[FETCH] matchups week {w} …")
        m, l = sleeper_matchups_df(args.league_id, w)
        write_csv(m, f"{outdir}/matchups_week_{w}.csv")
        write_csv(l, f"{outdir}/matchup_lineups_week_{w}.csv")

        print(f"[FETCH] transactions week {w} …")
        tx, legs, faab, picks = sleeper_transactions_for_week(args.league_id, w)
        write_csv(tx, f"{outdir}/transactions_week_{w}.csv")
        write_csv(legs, f"{outdir}/transaction_players_week_{w}.csv")
        write_csv(faab, f"{outdir}/transaction_faab_transfers_week_{w}.csv")
        write_csv(picks, f"{outdir}/transaction_draft_picks_week_{w}.csv")

    # Drafts & picks
    print("[FETCH] drafts …")
    d, p = sleeper_drafts_df(args.league_id)
    write_csv(d, f"{outdir}/drafts.csv")
    write_csv(p, f"{outdir}/draft_picks.csv")

    print("\n[DONE] Sleeper CSV dump in:", outdir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
