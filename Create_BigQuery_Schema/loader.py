#!/usr/bin/env python3
"""
Sleeper + nfl_data_py  → BigQuery loader (idempotent upserts)

Prereqs:
  pip install google-cloud-bigquery pandas requests tenacity nfl_data_py python-dateutil

Auth:
  - Application Default Credentials (ADC) or a service account with BigQuery Data Editor
  - The script assumes the BigQuery client is already initialized with permissions.

What it loads:
  Sleeper (dataset: SLP): players, users, leagues, league_users, rosters, roster_players_current,
                           matchups, matchup_lineups (derived), transactions (+legs/faab/picks),
                           drafts, draft_picks
  nfl_data_py (dataset: NFL): nfl_ff_playerids (ID map), nfl_players_dim (from seasonal rosters),
                              nfl_player_stats_weekly, nfl_player_stats_seasonal,
                              nfl_depth_charts_weekly, nfl_injuries_weekly, nfl_snap_counts_weekly,
                              nfl_schedules_games, nfl_ngs_player_weekly (optional)
  Bridge (dataset: BRIDGE): player_xref view tying Sleeper IDs to nflverse ids (gsis_id, etc.)

Joins:
  sleeper.* tables (player_id)  → BRIDGE.player_xref.sleeper_id
  nfl.* tables (gsis_id/pfr_id) → BRIDGE.player_xref.(gsis_id/pfr_id)

Usage:
  python loader.py \
    --project brainrot-453319 \
    --dataset-slp sleeper \
    --dataset-nfl nfl \
    --dataset-bridge bridge \
    --league-ids 1257071160403709954 \
    --seasons 2024 2025 \
    --weeks 1-18

"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import requests
from dateutil import tz
from google.cloud import bigquery
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

import nfl_data_py as nfl  # library of record for nflverse imports

UTC = tz.tzutc()

# ------------------------------
# Config & CLI
# ------------------------------

def parse_args():
    ap = argparse.ArgumentParser(description="Sleeper + nfl_data_py -> BigQuery")
    ap.add_argument("--project", required=True, help="GCP project id")
    ap.add_argument("--dataset-slp", default="sleeper", help="BigQuery dataset for Sleeper tables")
    ap.add_argument("--dataset-nfl", default="nfl", help="BigQuery dataset for nfl_data_py tables")
    ap.add_argument("--dataset-bridge", default="bridge", help="BigQuery dataset for xref view")
    ap.add_argument("--location", default="US", help="BigQuery dataset location")
    ap.add_argument("--league-ids", nargs="+", required=True, help="Sleeper league ids")
    ap.add_argument("--seasons", nargs="+", type=int, required=True, help="Seasons to load (e.g., 2024 2025)")
    ap.add_argument("--weeks", default="1-18", help="Week list or range (e.g., '1-18' or '1 2 3')")
    ap.add_argument("--load-ngs", action="store_true", help="Also load nfl_ngs_player_weekly")
    return ap.parse_args()


# ------------------------------
# Helpers
# ------------------------------

def weeks_arg_to_list(s: str) -> List[int]:
    s = s.strip()
    if "-" in s:
        a, b = s.split("-", 1)
        return list(range(int(a), int(b) + 1))
    return [int(x) for x in s.split()]

def now_ts() -> pd.Timestamp:
    return pd.Timestamp(datetime.now(tz=UTC))

def to_json_str(obj: Any) -> Optional[str]:
    if obj is None:
        return None
    try:
        return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        return None

def ensure_datasets(bq: bigquery.Client, project: str, datasets: Sequence[Tuple[str, str]]):
    for dataset_id, location in datasets:
        ds_ref = bigquery.Dataset(f"{project}.{dataset_id}")
        try:
            bq.get_dataset(ds_ref)
        except Exception:
            ds_ref.location = location
            bq.create_dataset(ds_ref)

def table_id(project: str, dataset: str, table: str) -> str:
    return f"`{project}.{dataset}.{table}`"

def load_df_to_temp_and_merge(
    bq: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
    df: pd.DataFrame,
    key_cols: Sequence[str],
    write_disposition: str = "WRITE_TRUNCATE",
):
    """
    Upsert df into {project}.{dataset}.{table} with MERGE using a temp table.
    - Creates destination table on first run (schema inferred from df)
    - Updates all columns on key match, inserts when not matched.
    """
    if df is None or df.empty:
        return

    dest = f"{project}.{dataset}.{table}"
    tmp = f"{project}.{dataset}._tmp_{table}_{int(time.time()*1000)}"

    # Load temp
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    load_job = bq.load_table_from_dataframe(df, tmp, job_config=job_config)
    load_job.result()

    # Ensure destination exists (create with df schema if needed)
    try:
        bq.get_table(dest)
    except Exception:
        schema = bq.get_table(tmp).schema
        tbl = bigquery.Table(dest, schema=schema)
        bq.create_table(tbl)

    # Build MERGE SQL
    src_cols = [schema_field.name for schema_field in bq.get_table(tmp).schema]
    # Filter out non-mergeable pseudo columns if any
    src_cols = [c for c in src_cols if not c.startswith("_")]

    on_clause = " AND ".join([f"T.{c}=S.{c}" for c in key_cols if c in src_cols])

    update_set = ", ".join([f"{'T.'+c}=S.{c}" for c in src_cols if c not in key_cols])
    insert_cols = ", ".join(src_cols)
    insert_vals = ", ".join([f"S.{c}" for c in src_cols])

    sql = f"""
    MERGE {table_id(project, dataset, table)} T
    USING {table_id(project, dataset, '_tmp_' + table + '_' + str(int(time.time()*1000)))} S
    ON {on_clause}
    WHEN MATCHED THEN UPDATE SET {update_set}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """

    # We must reference the exact tmp table we created; recompose tmp id string
    sql = sql.replace("_tmp_" + table + "_" + str(int(time.time()*1000)),
                      tmp.split(".")[-1])

    bq.query(sql).result()
    # Drop tmp
    bq.delete_table(tmp, not_found_ok=True)

# ------------------------------
# Sleeper fetchers
# ------------------------------

SLEEPER = "https://api.sleeper.app/v1"

@retry(wait=wait_exponential_jitter(initial=0.5, max=8), stop=stop_after_attempt(5))
def _get(url: str) -> Any:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

def sleeper_players_df() -> pd.DataFrame:
    data = _get(f"{SLEEPER}/players/nfl")
    # data is dict keyed by player_id
    rows = []
    for pid, obj in data.items():
        if not isinstance(obj, dict):
            continue
        obj = obj.copy()
        obj["player_id"] = pid  # Sleeper id as string
        # Flatten some heavy dicts to JSON
        for k in ("metadata", "advanced_stats", "team_data", "practice_participation"):
            if k in obj:
                obj[k] = to_json_str(obj.get(k))
        rows.append(obj)
    df = pd.DataFrame(rows)
    # Normalize some common columns if present
    rename = {
        "full_name": "full_name",
        "first_name": "first_name",
        "last_name": "last_name",
        "team": "team",
        "position": "position",
        "fantasy_positions": "fantasy_positions",
        "injury_status": "injury_status",
    }
    keep = list(set(["player_id"] + list(rename.keys()) + ["metadata", "age", "height", "weight"]))
    existing = [c for c in keep if c in df.columns]
    return df[existing]

def sleeper_league_core(league_id: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    league = _get(f"{SLEEPER}/league/{league_id}")
    df_league = pd.DataFrame([{
        "league_id": league_id,
        "name": league.get("name"),
        "season": int(league.get("season")) if league.get("season") else None,
        "sport": league.get("sport"),
        "status": league.get("status"),
        "total_rosters": league.get("total_rosters"),
        "draft_id": league.get("draft_id"),
        "scoring_settings": to_json_str(league.get("scoring_settings")),
        "roster_positions": league.get("roster_positions"),
        "metadata": to_json_str(league.get("metadata")),
        "created_at": pd.to_datetime(league.get("created"), unit="ms", utc=True) if league.get("created") else None,
        "ingested_at": now_ts(),
    }])

    users = _get(f"{SLEEPER}/league/{league_id}/users")
    df_users = pd.DataFrame([{
        "user_id": u.get("user_id"),
        "username": u.get("username"),
        "display_name": u.get("display_name"),
        "avatar": u.get("avatar"),
        "metadata": to_json_str(u.get("metadata")),
        "is_commissioner": bool(u.get("is_owner")),
        "league_id": league_id,
        "ingested_at": now_ts(),
    } for u in users]) if users else pd.DataFrame(columns=[
        "user_id","username","display_name","avatar","metadata","is_commissioner","league_id","ingested_at"
    ])

    rosters = _get(f"{SLEEPER}/league/{league_id}/rosters")
    df_rosters = pd.DataFrame([{
        "league_id": r.get("league_id"),
        "roster_id": r.get("roster_id"),
        "owner_id": r.get("owner_id"),
        "co_owner_ids": r.get("co_owners"),
        "settings": to_json_str(r.get("settings")),
        "starters": r.get("starters"),
        "players": r.get("players"),
        "reserve": r.get("reserve"),
        "ingested_at": now_ts(),
    } for r in rosters]) if rosters else pd.DataFrame(columns=[
        "league_id","roster_id","owner_id","co_owner_ids","settings","starters","players","reserve","ingested_at"
    ])

    return df_league, df_users, df_rosters

def derive_roster_players_current(df_rosters: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in df_rosters.iterrows():
        plist = r.get("players") or []
        for pid in plist:
            rows.append({
                "league_id": r["league_id"],
                "roster_id": r["roster_id"],
                "player_id": pid,
                "ingested_at": now_ts()
            })
    return pd.DataFrame(rows)

def sleeper_matchups_df(league_id: str, week: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    data = _get(f"{SLEEPER}/league/{league_id}/matchups/{week}")
    rows = []
    lineups = []
    for obj in data or []:
        rows.append({
            "league_id": league_id,
            "week": week,
            "matchup_id": obj.get("matchup_id"),
            "roster_id": obj.get("roster_id"),
            "points": obj.get("points"),
            "custom_points": obj.get("custom_points"),
            "players": obj.get("players"),
            "starters": obj.get("starters"),
            "ingested_at": now_ts()
        })
        starters = obj.get("starters") or []
        players = obj.get("players") or []
        # Derive bench
        bench = [p for p in players if p not in starters]
        for idx, pid in enumerate(starters):
            lineups.append({
                "league_id": league_id, "week": week, "matchup_id": obj.get("matchup_id"),
                "roster_id": obj.get("roster_id"), "slot_index": idx,
                "player_id": pid, "is_starter": True, "ingested_at": now_ts()
            })
        for idx, pid in enumerate(bench):
            lineups.append({
                "league_id": league_id, "week": week, "matchup_id": obj.get("matchup_id"),
                "roster_id": obj.get("roster_id"), "slot_index": 1000 + idx,
                "player_id": pid, "is_starter": False, "ingested_at": now_ts()
            })
    return pd.DataFrame(rows), pd.DataFrame(lineups)

def sleeper_transactions_for_week(league_id: str, week: int) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    data = _get(f"{SLEEPER}/league/{league_id}/transactions/{week}")
    tx_rows, leg_rows, faab_rows, pick_rows = [], [], [], []
    for t in data or []:
        tx_rows.append({
            "league_id": league_id,
            "transaction_id": t.get("transaction_id"),
            "type": t.get("type"),
            "status": t.get("status"),
            "notes": t.get("notes"),
            "created_ts": pd.to_datetime(t.get("created"), unit="ms", utc=True) if t.get("created") else None,
            "executed_ts": pd.to_datetime(t.get("execute"), unit="ms", utc=True) if t.get("execute") else None,
            "ingested_at": now_ts(),
            "raw": to_json_str(t)
        })
        # Players (adds/drops/trades)
        adds = (t.get("adds") or {})
        drops = (t.get("drops") or {})
        leg_no = 0
        for pid, to_roster in adds.items():
            leg_no += 1
            leg_rows.append({
                "league_id": league_id, "transaction_id": t.get("transaction_id"), "leg_no": leg_no,
                "action": "ADD" if t.get("type") in ("waiver", "free_agent") else "TRADE_ADD",
                "player_id": pid, "from_roster_id": None, "to_roster_id": to_roster
            })
        for pid, from_roster in drops.items():
            leg_no += 1
            leg_rows.append({
                "league_id": league_id, "transaction_id": t.get("transaction_id"), "leg_no": leg_no,
                "action": "DROP" if t.get("type") in ("waiver", "free_agent") else "TRADE_DROP",
                "player_id": pid, "from_roster_id": from_roster, "to_roster_id": None
            })
        # FAAB transfers
        for tr in (t.get("waiver_budget") or []):
            faab_rows.append({
                "league_id": league_id, "transaction_id": t.get("transaction_id"),
                "seq": len(faab_rows)+1,
                "from_roster_id": tr.get("sender"),
                "to_roster_id": tr.get("receiver"),
                "amount": tr.get("amount")
            })
        # Draft picks moved
        for p in (t.get("draft_picks") or []):
            pick_rows.append({
                "league_id": league_id, "transaction_id": t.get("transaction_id"),
                "seq": len(pick_rows)+1,
                "season": p.get("season"), "round": p.get("round"),
                "original_roster_id": p.get("owner_id"),
                "previous_owner_roster_id": p.get("previous_owner_id"),
                "new_owner_roster_id": p.get("receiver_id")
            })
    return pd.DataFrame(tx_rows), pd.DataFrame(leg_rows), pd.DataFrame(faab_rows), pd.DataFrame(pick_rows)

def sleeper_drafts_df(league_id: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    drafts = _get(f"{SLEEPER}/league/{league_id}/drafts")
    drows, pick_rows = [], []
    for d in drafts or []:
        drows.append({
            "draft_id": d.get("draft_id"),
            "league_id": d.get("league_id"),
            "status": d.get("status"),
            "type": d.get("type"),
            "rounds": (d.get("settings") or {}).get("rounds"),
            "start_time": pd.to_datetime(d.get("start_time"), unit="ms", utc=True) if d.get("start_time") else None,
            "draft_order": to_json_str(d.get("draft_order")),
            "slot_to_roster_id": to_json_str(d.get("slot_to_roster_id")),
            "metadata": to_json_str(d.get("metadata")),
            "ingested_at": now_ts()
        })
        try:
            picks = _get(f"{SLEEPER}/draft/{d.get('draft_id')}/picks")
        except Exception:
            picks = []
        for i, p in enumerate(picks or []):
            pick_rows.append({
                "draft_id": d.get("draft_id"),
                "pick_no": i+1,
                "round": p.get("round"),
                "pick": p.get("pick_no") or p.get("pick"),
                "player_id": p.get("player_id"),
                "picked_by_roster_id": p.get("roster_id"),
                "is_keeper": p.get("is_keeper"),
                "picked_at": pd.to_datetime(p.get("picked_at"), unit="ms", utc=True) if p.get("picked_at") else None,
                "metadata": to_json_str(p)
            })
    return pd.DataFrame(drows), pd.DataFrame(pick_rows)

# ------------------------------
# nfl_data_py fetchers
# ------------------------------

def nfl_ids_df(columns: Optional[List[str]] = None) -> pd.DataFrame:
    # Includes sleeper_id, gsis_id, pfr_id, espn_id, yahoo_id, sportradar_id, etc.
    ids = nfl.import_ids(columns=columns)
    ids = ids.rename(columns={"sleeper_id": "sleeper_id"})
    return ids

def nfl_players_dim_from_rosters(years: List[int]) -> pd.DataFrame:
    # Use seasonal rosters as a stable dimension (dedupe by gsis_id)
    rosters = nfl.import_seasonal_rosters(years=years, columns=None)
    keep = [
        "gsis_id","pfr_id","espn_id","yahoo_id","sportradar_id",
        "player_name","position","team","birth_date","college","draft_year","draft_round","draft_pick"
    ]
    for c in keep:
        if c not in rosters.columns:
            rosters[c] = None
    dim = (rosters[keep]
           .drop_duplicates(subset=["gsis_id"], keep="last")
           .rename(columns={"player_name":"full_name","birth_date":"birthdate"}))
    dim["updated_at"] = now_ts()
    return dim

def nfl_weekly_stats(years: List[int]) -> pd.DataFrame:
    df = nfl.import_weekly_data(years=years)
    # key columns are: season, week, player_id or gsis_id? nfl_data_py returns 'player_id' as gsis?
    # Standardize gsis_id column
    if "player_id" in df.columns and "gsis_id" not in df.columns:
        df = df.rename(columns={"player_id": "gsis_id"})
    df["ingested_at"] = now_ts()
    return df

def nfl_seasonal_stats(years: List[int]) -> pd.DataFrame:
    df = nfl.import_seasonal_data(years=years)
    if "player_id" in df.columns and "gsis_id" not in df.columns:
        df = df.rename(columns={"player_id": "gsis_id"})
    df["ingested_at"] = now_ts()
    return df

def nfl_depth_charts(years: List[int]) -> pd.DataFrame:
    df = nfl.import_depth_charts(years=years)
    # Should have gsis_id + team/pos/depth
    if "player_id" in df.columns and "gsis_id" not in df.columns:
        df = df.rename(columns={"player_id": "gsis_id"})
    df["ingested_at"] = now_ts()
    return df

def nfl_injuries(years: List[int]) -> pd.DataFrame:
    df = nfl.import_injuries(years=years)
    if "player_id" in df.columns and "gsis_id" not in df.columns:
        df = df.rename(columns={"player_id": "gsis_id"})
    df["ingested_at"] = now_ts()
    return df

def nfl_snap_counts(years: List[int]) -> pd.DataFrame:
    df = nfl.import_snap_counts(years=years)
    if "player_id" in df.columns and "gsis_id" not in df.columns:
        df = df.rename(columns={"player_id": "gsis_id"})
    df["ingested_at"] = now_ts()
    return df

def nfl_schedules(years: List[int]) -> pd.DataFrame:
    df = nfl.import_schedules(years=years)
    df["ingested_at"] = now_ts()
    return df

def nfl_ngs_weekly(years: List[int]) -> pd.DataFrame:
    # Combine passing/rushing/receiving into one long table with 'stat_type'
    parts = []
    for typ in ("passing", "rushing", "receiving"):
        t = nfl.import_ngs_data(stat_type=typ, years=years).copy()
        t["stat_type"] = typ
        if "player_id" in t.columns and "gsis_id" not in t.columns:
            t = t.rename(columns={"player_id": "gsis_id"})
        parts.append(t)
    df = pd.concat(parts, ignore_index=True)
    df["ingested_at"] = now_ts()
    return df

# ------------------------------
# Bridge: player_xref
# ------------------------------

def create_or_replace_player_xref_view(
    bq: bigquery.Client, project: str, dataset_bridge: str, dataset_nfl: str
):
    sql = f"""
    CREATE OR REPLACE VIEW {table_id(project, dataset_bridge, "player_xref")} AS
    SELECT
      ids.full_name,
      ids.sleeper_id,
      ids.gsis_id,
      ids.pfr_id,
      ids.espn_id,
      ids.yahoo_id,
      ids.sportradar_id,
      CURRENT_TIMESTAMP() AS updated_at
    FROM {table_id(project, dataset_nfl, "nfl_ff_playerids")} ids
    WHERE ids.gsis_id IS NOT NULL OR ids.sleeper_id IS NOT NULL
    """
    bq.query(sql).result()

# ------------------------------
# Main load routine
# ------------------------------

def main():
    args = parse_args()
    project = args.project
    ds_slp = args.dataset_slp
    ds_nfl = args.dataset_nfl
    ds_bridge = args.dataset_bridge
    location = args.location
    league_ids = args.league_ids
    seasons = args.seasons
    weeks = weeks_arg_to_list(args.weeks)

    bq = bigquery.Client(project=project)

    ensure_datasets(
        bq, project,
        [(ds_slp, location), (ds_nfl, location), (ds_bridge, location)]
    )

    # ---------- Sleeper ----------
    print("Loading Sleeper players ...")
    slp_players = sleeper_players_df()
    load_df_to_temp_and_merge(bq, project, ds_slp, "players_dim", slp_players, key_cols=["player_id"])

    for league_id in league_ids:
        print(f"Loading league core: {league_id}")
        df_league, df_users, df_rosters = sleeper_league_core(league_id)

        load_df_to_temp_and_merge(bq, project, ds_slp, "leagues", df_league, key_cols=["league_id"])
        if not df_users.empty:
            # users table (user dimension)
            users_dim = (df_users[["user_id","username","display_name","avatar"]]
                         .drop_duplicates(subset=["user_id"]))
            load_df_to_temp_and_merge(bq, project, ds_slp, "users", users_dim, key_cols=["user_id"])
            # membership table
            league_users = df_users[["league_id","user_id","is_commissioner","metadata","ingested_at"]]
            load_df_to_temp_and_merge(bq, project, ds_slp, "league_users",
                                      league_users, key_cols=["league_id","user_id"])

        if not df_rosters.empty:
            load_df_to_temp_and_merge(bq, project, ds_slp, "rosters", df_rosters,
                                      key_cols=["league_id","roster_id"])
            # current holdings
            rpc = derive_roster_players_current(df_rosters)
            load_df_to_temp_and_merge(bq, project, ds_slp, "roster_players_current", rpc,
                                      key_cols=["league_id","roster_id","player_id"])

        print(f"Loading matchups & transactions: {league_id} weeks={weeks}")
        all_matchups, all_lineups = [], []
        all_tx, all_legs, all_faab, all_picks = [], [], [], []
        for w in weeks:
            try:
                m, l = sleeper_matchups_df(league_id, w)
                if not m.empty: all_matchups.append(m)
                if not l.empty: all_lineups.append(l)
            except Exception as e:
                print(f"matchups week {w} error: {e}")

            try:
                tx, legs, faab, picks = sleeper_transactions_for_week(league_id, w)
                if not tx.empty: all_tx.append(tx)
                if not legs.empty: all_legs.append(legs)
                if not faab.empty: all_faab.append(faab)
                if not picks.empty: all_picks.append(picks)
            except Exception as e:
                print(f"transactions week {w} error: {e}")

        if all_matchups:
            dfm = pd.concat(all_matchups, ignore_index=True)
            load_df_to_temp_and_merge(bq, project, ds_slp, "matchups", dfm,
                                      key_cols=["league_id","week","matchup_id","roster_id"])
        if all_lineups:
            dfl = pd.concat(all_lineups, ignore_index=True)
            load_df_to_temp_and_merge(bq, project, ds_slp, "matchup_lineups", dfl,
                                      key_cols=["league_id","week","matchup_id","roster_id","slot_index"])

        if all_tx:
            dft = pd.concat(all_tx, ignore_index=True)
            load_df_to_temp_and_merge(bq, project, ds_slp, "transactions", dft,
                                      key_cols=["league_id","transaction_id"])
        if all_legs:
            dflg = pd.concat(all_legs, ignore_index=True)
            load_df_to_temp_and_merge(bq, project, ds_slp, "transaction_players", dflg,
                                      key_cols=["league_id","transaction_id","leg_no"])
        if all_faab:
            dff = pd.concat(all_faab, ignore_index=True)
            load_df_to_temp_and_merge(bq, project, ds_slp, "transaction_faab_transfers", dff,
                                      key_cols=["league_id","transaction_id","seq"])
        if all_picks:
            dfp = pd.concat(all_picks, ignore_index=True)
            load_df_to_temp_and_merge(bq, project, ds_slp, "transaction_draft_picks", dfp,
                                      key_cols=["league_id","transaction_id","seq"])

        print(f"Loading drafts: {league_id}")
        d_drafts, d_picks = sleeper_drafts_df(league_id)
        if not d_drafts.empty:
            load_df_to_temp_and_merge(bq, project, ds_slp, "drafts", d_drafts, key_cols=["draft_id"])
        if not d_picks.empty:
            load_df_to_temp_and_merge(bq, project, ds_slp, "draft_picks", d_picks,
                                      key_cols=["draft_id","pick_no"])

    # ---------- nfl_data_py ----------
    print("Loading nfl_data_py: ids, players dim, weekly/seasonal/situational ...")

    ids = nfl_ids_df()
    # Subset to standard columns if present
    id_keep = [c for c in ["full_name","sleeper_id","gsis_id","pfr_id","espn_id","yahoo_id","sportradar_id"] if c in ids.columns]
    ids = ids[id_keep].drop_duplicates()
    load_df_to_temp_and_merge(bq, project, ds_nfl, "nfl_ff_playerids", ids,
                              key_cols=["gsis_id"] if "gsis_id" in ids.columns else ["pfr_id","full_name"])

    players_dim = nfl_players_dim_from_rosters(seasons)
    load_df_to_temp_and_merge(bq, project, ds_nfl, "nfl_players_dim", players_dim,
                              key_cols=["gsis_id"])

    weekly = nfl_weekly_stats(seasons)
    load_df_to_temp_and_merge(bq, project, ds_nfl, "nfl_player_stats_weekly", weekly,
                              key_cols=["season","week","gsis_id"])

    seasonal = nfl_seasonal_stats(seasons)
    load_df_to_temp_and_merge(bq, project, ds_nfl, "nfl_player_stats_seasonal", seasonal,
                              key_cols=["season","gsis_id"])

    depth = nfl_depth_charts(seasons)
    # Pick a minimal PK to avoid dup rows across timestamps
    depth_keys = [k for k in ["season","week","team","gsis_id","depth_position","depth_order"] if k in depth.columns]
    load_df_to_temp_and_merge(bq, project, ds_nfl, "nfl_depth_charts_weekly", depth,
                              key_cols=depth_keys or ["season","week","gsis_id"])

    inj = nfl_injuries(seasons)
    inj_keys = [k for k in ["season","week","team","gsis_id"] if k in inj.columns]
    load_df_to_temp_and_merge(bq, project, ds_nfl, "nfl_injuries_weekly", inj,
                              key_cols=inj_keys or ["season","week","gsis_id"])

    snaps = nfl_snap_counts(seasons)
    load_df_to_temp_and_merge(bq, project, ds_nfl, "nfl_snap_counts_weekly", snaps,
                              key_cols=["season","week","gsis_id"])

    sched = nfl_schedules(seasons)
    # Best-effort keys (nfl_data_py schedule has game_id, season, week, home_team, away_team)
    sched_keys = [k for k in ["game_id","season","week","home_team","away_team"] if k in sched.columns]
    load_df_to_temp_and_merge(bq, project, ds_nfl, "nfl_schedules_games", sched,
                              key_cols=sched_keys or ["season","week","home_team","away_team"])

    if args.load_ngs:
        ngs = nfl_ngs_weekly(seasons)
        load_df_to_temp_and_merge(bq, project, ds_nfl, "nfl_ngs_player_weekly", ngs,
                                  key_cols=["season","week","gsis_id","stat_type"])

    # ---------- Bridge View ----------
    print("Creating/refreshing player_xref view ...")
    create_or_replace_player_xref_view(bq, project, ds_bridge, ds_nfl)

    print("Done.")


if __name__ == "__main__":
    main()
