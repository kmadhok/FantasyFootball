#!/usr/bin/env python3
"""
Load Sleeper CSV dump files into a dedicated BigQuery dataset.

Scans an output directory (produced by sleeper_api_dump.py) and uploads
CSV files into canonical tables in a target dataset. Weekly files are
appended (with week inferred from filename if missing). Static dimension
files (players, league, users, rosters, drafts) default to replace.

Usage:
  python Create_BigQuery_Schema/bq_load_sleeper_dump.py \
    --project brainrot-453319 \
    --dataset sleeper_raw \
    --location US \
    --path out/sleeper_dump \
    [--replace-all]

Tables (auto-detected by filename):
  players.csv -> players
  league.csv -> league
  users.csv -> users
  rosters.csv -> rosters
  roster_players_current.csv -> roster_players_current
  matchups_week_{w}.csv -> matchups
  matchup_lineups_week_{w}.csv -> matchup_lineups
  transactions_week_{w}.csv -> transactions
  transaction_players_week_{w}.csv -> transaction_players
  transaction_faab_transfers_week_{w}.csv -> transaction_faab_transfers
  transaction_draft_picks_week_{w}.csv -> transaction_draft_picks
  drafts.csv -> drafts
  draft_picks.csv -> draft_picks
"""
from __future__ import annotations

import argparse
import os
import re
from typing import Dict, List, Optional, Tuple

import pandas as pd
from google.cloud import bigquery


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Load Sleeper CSV dump into BigQuery")
    ap.add_argument("--project", required=True, help="GCP project id")
    ap.add_argument("--dataset", default="sleeper_raw", help="BigQuery dataset for destination tables")
    ap.add_argument("--location", default="US", help="BigQuery dataset location")
    ap.add_argument("--path", default="out/sleeper_dump", help="Directory with CSV files from sleeper_api_dump.py")
    ap.add_argument("--replace-all", action="store_true", help="Use WRITE_TRUNCATE for all tables (default: replace static only)")
    return ap.parse_args()


def ensure_dataset(client: bigquery.Client, project: str, dataset: str, location: str) -> None:
    ds_ref = bigquery.Dataset(f"{project}.{dataset}")
    try:
        client.get_dataset(ds_ref)
    except Exception:
        ds_ref.location = location
        client.create_dataset(ds_ref)


def sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    used = set()
    new_cols: List[str] = []
    for col in df.columns:
        name = re.sub(r"[^A-Za-z0-9_]", "_", str(col)).strip("_")
        if not re.match(r"^[A-Za-z_]", name):
            name = "_" + name
        base = name[:300]
        name = base
        i = 1
        while name in used:
            suf = f"_{i}"
            name = base[: (300 - len(suf))] + suf
            i += 1
        used.add(name)
        new_cols.append(name)
    out = df.copy()
    out.columns = new_cols
    return out


def infer_table_and_week(filename: str) -> Tuple[str, Optional[int]]:
    fname = os.path.basename(filename).lower()
    m = re.search(r"_week_([0-9]+)\.csv$", fname)
    week = int(m.group(1)) if m else None

    mapping: List[Tuple[str, str]] = [
        ("players.csv", "players"),
        ("league.csv", "league"),
        ("users.csv", "users"),
        ("rosters.csv", "rosters"),
        ("roster_players_current.csv", "roster_players_current"),
        ("matchups_week_", "matchups"),
        ("matchup_lineups_week_", "matchup_lineups"),
        ("transactions_week_", "transactions"),
        ("transaction_players_week_", "transaction_players"),
        ("transaction_faab_transfers_week_", "transaction_faab_transfers"),
        ("transaction_draft_picks_week_", "transaction_draft_picks"),
        ("drafts.csv", "drafts"),
        ("draft_picks.csv", "draft_picks"),
    ]
    for pat, table in mapping:
        if pat.endswith(".csv"):
            if fname == pat:
                return table, week
        else:
            if pat in fname:
                return table, week
    # default to base name without extension
    table = re.sub(r"\.csv$", "", fname)
    return table, week


def write_disposition_for(table: str, replace_all: bool) -> str:
    static_tables = {
        "players", "league", "users", "rosters", "roster_players_current", "drafts", "draft_picks"
    }
    if replace_all:
        return "WRITE_TRUNCATE"
    return "WRITE_TRUNCATE" if table in static_tables else "WRITE_APPEND"


def main() -> int:
    args = parse_args()
    client = bigquery.Client(project=args.project, location=args.location)
    ensure_dataset(client, args.project, args.dataset, args.location)

    if not os.path.isdir(args.path):
        print(f"[ERR] Path not found: {args.path}")
        return 1

    files = [os.path.join(args.path, f) for f in os.listdir(args.path) if f.lower().endswith('.csv')]
    if not files:
        print(f"[WARN] No CSV files found in {args.path}")
        return 0

    # Load each file into its canonical table
    for csv_path in sorted(files):
        table, week = infer_table_and_week(csv_path)
        try:
            df = pd.read_csv(csv_path)
        except Exception as e:
            print(f"[SKIP] Failed to read {csv_path}: {e}")
            continue

        # Add week if inferred and not present
        if week is not None and 'week' not in df.columns:
            df['week'] = week

        df = sanitize_columns(df)

        dest = f"{args.project}.{args.dataset}.{table}"
        job_cfg = bigquery.LoadJobConfig(write_disposition=write_disposition_for(table, args.replace_all))
        print(f"[LOAD] {csv_path} -> {dest} rows={len(df)} mode={job_cfg.write_disposition}")
        client.load_table_from_dataframe(df, dest, job_config=job_cfg).result()
        print(f"[OK] Loaded {len(df)} rows into {dest}")

    print("[DONE]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

