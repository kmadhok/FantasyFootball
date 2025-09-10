#!/usr/bin/env python3
"""
Sleeper + nfl_data_py  → BigQuery loader (idempotent upserts)

Prereqs (suggested):
  pip install --upgrade google-cloud-bigquery pandas requests tenacity nfl_data_py python-dateutil pyarrow
  # Optional (silences a future warning from google-cloud-bigquery):
  pip install --upgrade pandas-gbq

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
    --project YOUR_GCP_PROJECT \
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
import time
from datetime import datetime
from typing import Any, List, Optional, Sequence, Tuple

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

import re

def _sanitize_bq_columns(df: pd.DataFrame):
    """
    Return (df_renamed, mapping) where df_renamed has BigQuery-safe column names:
      - replace non [A-Za-z0-9_] with _
      - prefix with _ if the first char is not [A-Za-z_]
      - truncate to 300 chars
      - ensure uniqueness by adding _1, _2 ... if needed
    """
    if df is None or df.empty:
        return df, {}
    mapping = {}
    used = set()
    new_cols = []
    for col in df.columns:
        col_str = str(col)
        new = re.sub(r'[^A-Za-z0-9_]', '_', col_str)
        if not re.match(r'^[A-Za-z_]', new):
            new = '_' + new
        new = new[:300]
        base = new
        i = 1
        while new in used:
            suf = f"_{i}"
            new = base[: (300 - len(suf))] + suf
            i += 1
        used.add(new)
        mapping[col] = new
        new_cols.append(new)
    df2 = df.copy()
    df2.columns = new_cols
    return df2, mapping


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

# def load_df_to_temp_and_merge(
#     bq: bigquery.Client,
#     project: str,
#     dataset: str,
#     table: str,
#     df: pd.DataFrame,
#     key_cols: Sequence[str],
#     write_disposition: str = "WRITE_TRUNCATE",
# ):
#     """
#     Upsert df into {project}.{dataset}.{table} with MERGE using a temp table.
#     - Creates destination table on first run (schema inferred from df)
#     - Updates all columns on key match, inserts when not matched.
#     """
#     if df is None or df.empty:
#         return

#     dest = f"{project}.{dataset}.{table}"
#     tmp_name = f"_tmp_{table}_{int(time.time()*1000)}"
#     tmp = f"{project}.{dataset}.{tmp_name}"

#     job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
#     bq.load_table_from_dataframe(df, tmp, job_config=job_config).result()

#     # Ensure destination exists (create with df schema if needed)
#     try:
#         bq.get_table(dest)
#     except Exception:
#         schema = bq.get_table(tmp).schema
#         bq.create_table(bigquery.Table(dest, schema=schema))

#     src_cols = [f.name for f in bq.get_table(tmp).schema if not f.name.startswith("_")]
#     on_clause = " AND ".join([f"T.{c}=S.{c}" for c in key_cols if c in src_cols])
#     update_cols = [c for c in src_cols if c not in key_cols]
#     update_set = ", ".join([f"T.{c}=S.{c}" for c in update_cols]) if update_cols else None
#     insert_cols = ", ".join(src_cols)
#     insert_vals = ", ".join([f"S.{c}" for c in src_cols])

#     sql = f"""
#     MERGE `{dest}` T
#     USING `{tmp}` S
#     ON {on_clause}
#     """
#     if update_set:
#         sql += f"WHEN MATCHED THEN UPDATE SET {update_set}\n"
#     else:
#         # If there are only key columns, do nothing on match
#         sql += "WHEN MATCHED THEN UPDATE SET " + " , ".join([f"T.{k}=S.{k}" for k in key_cols[:1]]) + "\n"
#     sql += f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
#     bq.query(sql).result()
#     bq.delete_table(tmp, not_found_ok=True)
def load_df_to_temp_and_merge(
    bq: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
    df: pd.DataFrame,
    key_cols: Sequence[str],
    write_disposition: str = "WRITE_TRUNCATE",
):
    if df is None or df.empty:
        return

    # Sanitize columns for BigQuery
    df, colmap = _sanitize_bq_columns(df)
    # Map key columns to their sanitized names (if any changed)
    sanitized_keys = [colmap.get(k, k) for k in key_cols]

    dest = f"{project}.{dataset}.{table}"
    tmp_name = f"_tmp_{table}_{int(time.time()*1000)}"
    tmp = f"{project}.{dataset}.{tmp_name}"

    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    bq.load_table_from_dataframe(df, tmp, job_config=job_config).result()

    # Ensure destination exists (create with df schema if needed)
    try:
        bq.get_table(dest)
    except Exception:
        schema = bq.get_table(tmp).schema
        bq.create_table(bigquery.Table(dest, schema=schema))

    # Use ALL columns from the temp table schema (don’t drop leading-underscore names)
    src_cols = [f.name for f in bq.get_table(tmp).schema]

    # Build MERGE
    on_parts = [f"T.{c}=S.{c}" for c in sanitized_keys if c in src_cols]
    if not on_parts:
        raise ValueError(f"No MERGE keys found in source columns for {dest}. Keys: {sanitized_keys} Src: {src_cols}")
    on_clause = " AND ".join(on_parts)

    update_cols = [c for c in src_cols if c not in sanitized_keys]
    update_set = ", ".join([f"T.{c}=S.{c}" for c in update_cols]) if update_cols else None
    insert_cols = ", ".join(src_cols)
    insert_vals = ", ".join([f"S.{c}" for c in src_cols])

    sql = f"MERGE `{dest}` T USING `{tmp}` S ON {on_clause}\n"
    if update_set:
        sql += f"WHEN MATCHED THEN UPDATE SET {update_set}\n"
    else:
        # no non-key columns; no-op on match
        sql += f"WHEN MATCHED THEN UPDATE SET {sanitized_keys[0]} = S.{sanitized_keys[0]}\n"
    sql += f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"

    bq.query(sql).result()
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
    rows = []
    for pid, obj in (data or {}).items():
        if not isinstance(obj, dict):
            continue
        o = obj.copy()
        o["player_id"] = pid  # Sleeper id as string
        for k in ("metadata", "advanced_stats", "team_data", "practice_participation"):
            if k in o:
                o[k] = to_json_str(o.get(k))
        rows.append(o)
    df = pd.DataFrame(rows)

    # Pick a tidy subset; coerce a few types for stable schema
    keep = ["player_id", "full_name", "first_name", "last_name",
            "team", "position", "fantasy_positions", "injury_status",
            "metadata", "age", "height", "weight"]
    keep = [c for c in keep if c in df.columns]
    df = df[keep]
    for c in ("player_id","full_name","first_name","last_name","team","position","injury_status","height","weight"):
        if c in df.columns:
            df[c] = df[c].astype("string")
    if "age" in df.columns:
        df["age"] = pd.to_numeric(df["age"], errors="coerce").astype("Int64")
    # fantasy_positions is a list already; leave as-is
    return df

def sleeper_league_core(league_id: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    league = _get(f"{SLEEPER}/league/{league_id}")
    df_league = pd.DataFrame([{
        "league_id": str(league_id),
        "name": league.get("name"),
        "season": int(league.get("season")) if league.get("season") else None,
        "sport": league.get("sport"),
        "status": league.get("status"),
        "total_rosters": league.get("total_rosters"),
        "draft_id": league.get("draft_id"),
        "scoring_settings": to_json_str(league.get("scoring_settings")),
        "roster_positions": league.get("roster_positions"),
        "metadata": to_json_str(league.get("metadata")),
        "created_at": pd.to_datetime(league.get("created"), unit="ms", utc=True, errors="coerce") if league.get("created") else None,
        "ingested_at": now_ts(),
    }])
    df_league["league_id"] = df_league["league_id"].astype("string")

    users = _get(f"{SLEEPER}/league/{league_id}/users") or []
    df_users = pd.DataFrame([{
        "user_id": u.get("user_id"),
        "username": u.get("username"),
        "display_name": u.get("display_name"),
        "avatar": u.get("avatar"),
        "metadata": to_json_str(u.get("metadata")),
        "is_commissioner": bool(u.get("is_owner")),
        "league_id": str(league_id),
        "ingested_at": now_ts(),
    } for u in users])

    if not df_users.empty:
        for c in ("user_id","username","display_name","avatar","league_id"):
            if c in df_users.columns:
                df_users[c] = df_users[c].astype("string")

    rosters = _get(f"{SLEEPER}/league/{league_id}/rosters") or []
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
    } for r in rosters])

    if not df_rosters.empty:
        for c in ("league_id","owner_id"):
            if c in df_rosters.columns:
                df_rosters[c] = df_rosters[c].astype("string")
        if "co_owner_ids" in df_rosters.columns:
            df_rosters["co_owner_ids"] = df_rosters["co_owner_ids"].apply(
                lambda x: x if isinstance(x, list) else ([] if pd.isna(x) else [str(x)])
            )

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
    df = pd.DataFrame(rows)
    if not df.empty:
        df["league_id"] = df["league_id"].astype("string")
        df["player_id"] = df["player_id"].astype("string")
    return df

def sleeper_matchups_df(league_id: str, week: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    data = _get(f"{SLEEPER}/league/{league_id}/matchups/{week}") or []
    rows, lineups = [], []
    for obj in data:
        rows.append({
            "league_id": str(league_id),
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
        bench = [p for p in players if p not in starters]
        for idx, pid in enumerate(starters):
            lineups.append({
                "league_id": str(league_id), "week": week, "matchup_id": obj.get("matchup_id"),
                "roster_id": obj.get("roster_id"), "slot_index": idx,
                "player_id": pid, "is_starter": True, "ingested_at": now_ts()
            })
        for idx, pid in enumerate(bench):
            lineups.append({
                "league_id": str(league_id), "week": week, "matchup_id": obj.get("matchup_id"),
                "roster_id": obj.get("roster_id"), "slot_index": 1000 + idx,
                "player_id": pid, "is_starter": False, "ingested_at": now_ts()
            })
    dfm = pd.DataFrame(rows)
    dfl = pd.DataFrame(lineups)
    if not dfm.empty and "custom_points" in dfm.columns:
        dfm["custom_points"] = pd.to_numeric(dfm["custom_points"], errors="coerce")
    if not dfl.empty:
        dfl["player_id"] = dfl["player_id"].astype("string")
        dfl["league_id"] = dfl["league_id"].astype("string")
    return dfm, dfl

def sleeper_transactions_for_week(league_id: str, week: int) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    data = _get(f"{SLEEPER}/league/{league_id}/transactions/{week}") or []
    tx_rows, leg_rows, faab_rows, pick_rows = [], [], [], []
    for t in data:
        tx_rows.append({
            "league_id": str(league_id),
            "transaction_id": t.get("transaction_id"),
            "type": t.get("type"),
            "status": t.get("status"),
            "notes": t.get("notes"),
            "created_ts": pd.to_datetime(t.get("created"), unit="ms", utc=True, errors="coerce") if t.get("created") else None,
            "executed_ts": pd.to_datetime(t.get("execute"), unit="ms", utc=True, errors="coerce") if t.get("execute") else None,
            "ingested_at": now_ts(),
            "raw": to_json_str(t)
        })
        adds = (t.get("adds") or {})
        drops = (t.get("drops") or {})
        leg_no = 0
        for pid, to_roster in adds.items():
            leg_no += 1
            leg_rows.append({
                "league_id": str(league_id), "transaction_id": t.get("transaction_id"), "leg_no": leg_no,
                "action": "ADD" if t.get("type") in ("waiver", "free_agent") else "TRADE_ADD",
                "player_id": pid, "from_roster_id": None, "to_roster_id": to_roster
            })
        for pid, from_roster in drops.items():
            leg_no += 1
            leg_rows.append({
                "league_id": str(league_id), "transaction_id": t.get("transaction_id"), "leg_no": leg_no,
                "action": "DROP" if t.get("type") in ("waiver", "free_agent") else "TRADE_DROP",
                "player_id": pid, "from_roster_id": from_roster, "to_roster_id": None
            })
        for tr in (t.get("waiver_budget") or []):
            faab_rows.append({
                "league_id": str(league_id), "transaction_id": t.get("transaction_id"),
                "seq": len(faab_rows)+1,
                "from_roster_id": tr.get("sender"),
                "to_roster_id": tr.get("receiver"),
                "amount": tr.get("amount")
            })
        for p in (t.get("draft_picks") or []):
            pick_rows.append({
                "league_id": str(league_id), "transaction_id": t.get("transaction_id"),
                "seq": len(pick_rows)+1,
                "season": p.get("season"), "round": p.get("round"),
                "original_roster_id": p.get("owner_id"),
                "previous_owner_roster_id": p.get("previous_owner_id"),
                "new_owner_roster_id": p.get("receiver_id")
            })
    dft = pd.DataFrame(tx_rows)
    dflg = pd.DataFrame(leg_rows)
    dff = pd.DataFrame(faab_rows)
    dfp = pd.DataFrame(pick_rows)

    if not dft.empty:
        dft["league_id"] = dft["league_id"].astype("string")
        if "notes" in dft.columns:
            dft["notes"] = dft["notes"].astype("string")
    return dft, dflg, dff, dfp

def sleeper_drafts_df(league_id: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    drafts = _get(f"{SLEEPER}/league/{league_id}/drafts") or []
    drows, pick_rows = [], []
    for d in drafts:
        drows.append({
            "draft_id": d.get("draft_id"),
            "league_id": d.get("league_id"),
            "status": d.get("status"),
            "type": d.get("type"),
            "rounds": (d.get("settings") or {}).get("rounds"),
            "start_time": pd.to_datetime(d.get("start_time"), unit="ms", utc=True, errors="coerce") if d.get("start_time") else None,
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
                "picked_at": pd.to_datetime(p.get("picked_at"), unit="ms", utc=True, errors="coerce") if p.get("picked_at") else None,
                "metadata": to_json_str(p)
            })
    df_drafts = pd.DataFrame(drows)
    df_picks = pd.DataFrame(pick_rows)

    if not df_drafts.empty and "slot_to_roster_id" in df_drafts.columns:
        df_drafts["slot_to_roster_id"] = df_drafts["slot_to_roster_id"].astype("string")
    if not df_picks.empty:
        if "is_keeper" in df_picks.columns:
            df_picks["is_keeper"] = df_picks["is_keeper"].astype("boolean")
    return df_drafts, df_picks


# ------------------------------
# nfl_data_py fetchers (robust)
# ------------------------------

def _concat_years_safe(fetch_fn, years: List[int], rename_player_id=True, label=None) -> pd.DataFrame:
    frames = []
    for y in years:
        try:
            df = fetch_fn(years=[y])
        except Exception as e:
            msg = str(e)
            if "404" in msg or "Not Found" in msg:
                print(f"[SKIP] {label or fetch_fn.__name__} {y}: {e}")
                continue
            raise
        if rename_player_id and "player_id" in df.columns and "gsis_id" not in df.columns:
            df = df.rename(columns={"player_id": "gsis_id"})
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def nfl_ids_df(columns: Optional[List[str]] = None) -> pd.DataFrame:
    ids = nfl.import_ids(columns=columns)
    # Inspect available columns for visibility during runs
    try:
        print("[nfl_ids_df] columns from nfl_data_py.import_ids():",
              sorted(list(ids.columns)))
    except Exception:
        pass

    # Ensure a name column called full_name always exists in the output,
    # regardless of upstream nfl_data_py version (which may expose name or player_name).
    if "full_name" not in ids.columns:
        if "player_name" in ids.columns:
            ids = ids.rename(columns={"player_name": "full_name"})
        elif "name" in ids.columns:
            ids["full_name"] = ids["name"].astype("string")
        elif "merge_name" in ids.columns:
            ids = ids.rename(columns={"merge_name": "full_name"})
        else:
            # Create an empty string column to satisfy downstream schema
            ids["full_name"] = pd.Series(dtype="string")

    # Common id columns (keep only those that exist)
    wanted = [
        "full_name", "sleeper_id", "gsis_id", "pfr_id", "espn_id", "yahoo_id", "sportradar_id"
    ]
    keep = [c for c in wanted if c in ids.columns]
    ids = ids[keep].drop_duplicates()

    # Normalize types for stable BigQuery schemas
    for c in keep:
        # Cast id/name-like columns to STRING
        ids[c] = ids[c].astype("string")

    # Log the final columns we are about to load
    try:
        print("[nfl_ids_df] columns to load into nfl_ff_playerids:", keep)
    except Exception:
        pass

    return ids

def nfl_players_dim_from_rosters(years: List[int]) -> pd.DataFrame:
    # Support multiple nfl_data_py versions
    try:
        rosters = nfl.import_rosters(years=years)
    except Exception:
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

    for c in ["gsis_id","pfr_id","espn_id","yahoo_id","sportradar_id","college","position","team","full_name"]:
        if c in dim.columns:
            dim[c] = dim[c].astype("string")
    for c in ["draft_year","draft_round","draft_pick"]:
        if c in dim.columns:
            dim[c] = pd.to_numeric(dim[c], errors="coerce").astype("Int64")
    dim["updated_at"] = now_ts()
    return dim

def nfl_weekly_stats(years: List[int]) -> pd.DataFrame:
    # Prefer canonical multi-season parquet (includes the latest season); fall back to per-year API
    try:
        url = "https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats.parquet"
        df = pd.read_parquet(url)
        if years:
            df = df[df["season"].isin(years)]
        if "player_id" in df.columns and "gsis_id" not in df.columns:
            df = df.rename(columns={"player_id": "gsis_id"})
        df["ingested_at"] = now_ts()
        return df
    except Exception as e:
        print(f"[WARN] canonical weekly parquet failed; falling back to per-year: {e}")
        df = _concat_years_safe(nfl.import_weekly_data, years, label="weekly")
        if not df.empty:
            df["ingested_at"] = now_ts()
        return df

def nfl_seasonal_stats(years: List[int]) -> pd.DataFrame:
    df = _concat_years_safe(nfl.import_seasonal_data, years, label="seasonal")
    if not df.empty:
        df["ingested_at"] = now_ts()
    return df

def nfl_depth_charts(years: List[int]) -> pd.DataFrame:
    df = _concat_years_safe(nfl.import_depth_charts, years, label="depth_charts")
    if not df.empty:
        df["ingested_at"] = now_ts()
    return df

def nfl_injuries(years: List[int]) -> pd.DataFrame:
    df = _concat_years_safe(nfl.import_injuries, years, label="injuries")
    if not df.empty:
        df["ingested_at"] = now_ts()
    return df

def nfl_snap_counts(years: List[int]) -> pd.DataFrame:
    df = _concat_years_safe(nfl.import_snap_counts, years, label="snap_counts")
    if not df.empty:
        df["ingested_at"] = now_ts()
    return df

def nfl_schedules(years: List[int]) -> pd.DataFrame:
    df = _concat_years_safe(nfl.import_schedules, years, rename_player_id=False, label="schedules")
    if not df.empty:
        df["ingested_at"] = now_ts()
    return df

def nfl_ngs_weekly(years: List[int]) -> pd.DataFrame:
    parts = []
    for typ in ("passing", "rushing", "receiving"):
        t = _concat_years_safe(
            lambda ys: nfl.import_ngs_data(stat_type=typ, years=ys),
            years, label=f"ngs_{typ}"
        )
        if not t.empty:
            if "player_id" in t.columns and "gsis_id" not in t.columns:
                t = t.rename(columns={"player_id": "gsis_id"})
            t["stat_type"] = typ
            t["ingested_at"] = now_ts()
            parts.append(t)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


# ------------------------------
# Bridge: player_xref
# ------------------------------

def create_or_replace_player_xref_view(
    bq: bigquery.Client, project: str, dataset_bridge: str, dataset_nfl: str
):
    # Build the view to avoid referencing a column that may not exist in
    # nfl_ff_playerids across nfl_data_py versions. We source the display
    # name from nfl_players_dim (which we control to always contain full_name)
    # and only use id columns from nfl_ff_playerids.
    sql = f"""
    CREATE OR REPLACE VIEW {table_id(project, dataset_bridge, "player_xref")} AS
    SELECT
      COALESCE(p.full_name, p2.full_name) AS full_name,
      ids.sleeper_id,
      ids.gsis_id,
      ids.pfr_id,
      ids.espn_id,
      ids.yahoo_id,
      ids.sportradar_id,
      CURRENT_TIMESTAMP() AS updated_at
    FROM {table_id(project, dataset_nfl, "nfl_ff_playerids")} ids
    LEFT JOIN {table_id(project, dataset_nfl, "nfl_players_dim")} p
      ON p.gsis_id = ids.gsis_id
    LEFT JOIN {table_id(project, dataset_nfl, "nfl_players_dim")} p2
      ON p2.pfr_id = ids.pfr_id
    WHERE ids.gsis_id IS NOT NULL OR ids.sleeper_id IS NOT NULL OR ids.pfr_id IS NOT NULL
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
            users_dim = (df_users[["user_id","username","display_name","avatar"]]
                         .drop_duplicates(subset=["user_id"]))
            load_df_to_temp_and_merge(bq, project, ds_slp, "users", users_dim, key_cols=["user_id"])

            league_users = df_users[["league_id","user_id","is_commissioner","metadata","ingested_at"]]
            load_df_to_temp_and_merge(bq, project, ds_slp, "league_users",
                                      league_users, key_cols=["league_id","user_id"])

        if not df_rosters.empty:
            load_df_to_temp_and_merge(bq, project, ds_slp, "rosters", df_rosters,
                                      key_cols=["league_id","roster_id"])
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
    load_df_to_temp_and_merge(
        bq, project, ds_nfl, "nfl_ff_playerids", ids,
        key_cols=["gsis_id"] if "gsis_id" in ids.columns else ["pfr_id","full_name"]
    )

    players_dim = nfl_players_dim_from_rosters(seasons)
    load_df_to_temp_and_merge(bq, project, ds_nfl, "nfl_players_dim", players_dim,
                              key_cols=["gsis_id"])

    weekly = nfl_weekly_stats(seasons)
    if not weekly.empty:
        load_df_to_temp_and_merge(bq, project, ds_nfl, "nfl_player_stats_weekly", weekly,
                                  key_cols=["season","week","gsis_id"])
    else:
        print("[INFO] weekly stats empty after filtering/skip; nothing to load.")

    seasonal = nfl_seasonal_stats(seasons)
    if not seasonal.empty:
        load_df_to_temp_and_merge(bq, project, ds_nfl, "nfl_player_stats_seasonal", seasonal,
                                  key_cols=["season","gsis_id"])

    depth = nfl_depth_charts(seasons)
    if not depth.empty:
        depth_keys = [k for k in ["season","week","team","gsis_id","depth_position","depth_order"] if k in depth.columns]
        load_df_to_temp_and_merge(bq, project, ds_nfl, "nfl_depth_charts_weekly", depth,
                                  key_cols=depth_keys or ["season","week","gsis_id"])

    inj = nfl_injuries(seasons)
    if not inj.empty:
        inj_keys = [k for k in ["season","week","team","gsis_id"] if k in inj.columns]
        load_df_to_temp_and_merge(bq, project, ds_nfl, "nfl_injuries_weekly", inj,
                                  key_cols=inj_keys or ["season","week","gsis_id"])

    snaps = nfl_snap_counts(seasons)
    if not snaps.empty:
        load_df_to_temp_and_merge(bq, project, ds_nfl, "nfl_snap_counts_weekly", snaps,
                                  key_cols=["season","week","gsis_id"])

    sched = nfl_schedules(seasons)
    if not sched.empty:
        sched_keys = [k for k in ["game_id","season","week","home_team","away_team"] if k in sched.columns]
        load_df_to_temp_and_merge(bq, project, ds_nfl, "nfl_schedules_games", sched,
                                  key_cols=sched_keys or ["season","week","home_team","away_team"])

    if args.load_ngs:
        ngs = nfl_ngs_weekly(seasons)
        if not ngs.empty:
            load_df_to_temp_and_merge(bq, project, ds_nfl, "nfl_ngs_player_weekly", ngs,
                                      key_cols=["season","week","gsis_id","stat_type"])

    # ---------- Bridge View ----------
    print("Creating/refreshing player_xref view ...")
    create_or_replace_player_xref_view(bq, project, ds_bridge, ds_nfl)

    print("Done.")


if __name__ == "__main__":
    main()
