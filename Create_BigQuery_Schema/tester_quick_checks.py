#!/usr/bin/env python3
"""
Quick BigQuery availability checks for nfl 2025 data.

Usage:
  python Create_BigQuery_Schema/tester_quick_checks.py \
    --project brainrot-453319 \
    --dataset-nfl nfl \
    --location US

This prints, per table, whether it exists, which seasons are present,
and how many rows exist for season=2025 (fast queries with small results).
"""
from __future__ import annotations

import argparse
from typing import List, Dict, Any

from google.cloud import bigquery


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Check 2025 availability in BigQuery nfl dataset")
    ap.add_argument("--project", required=True, help="GCP project id")
    ap.add_argument("--dataset-nfl", default="nfl", help="BigQuery dataset for nfl_data_py tables")
    ap.add_argument("--location", default="US", help="BigQuery dataset location")
    return ap.parse_args()


def table_exists(client: bigquery.Client, table_fqn: str) -> bool:
    try:
        client.get_table(table_fqn)
        return True
    except Exception:
        return False


def run_checks(client: bigquery.Client, project: str, dataset: str) -> List[Dict[str, Any]]:
    tables = [
        "nfl_player_stats_weekly",
        "nfl_player_stats_seasonal",
        "nfl_snap_counts_weekly",
        "nfl_injuries_weekly",
        "nfl_depth_charts_weekly",
        "nfl_schedules_games",
        "nfl_ngs_player_weekly",
        "nfl_players_dim",  # no season column typically
        "nfl_ff_playerids",  # id map only
    ]

    results: List[Dict[str, Any]] = []
    for t in tables:
        fqn = f"{project}.{dataset}.{t}"
        exists = table_exists(client, fqn)
        rec: Dict[str, Any] = {"table": t, "exists": exists}
        if not exists:
            results.append(rec)
            continue

        # seasons present (if table has a season column)
        try:
            sql_seasons = f"""
                SELECT ARRAY_AGG(DISTINCT season ORDER BY season DESC LIMIT 10) AS seasons
                FROM `{fqn}`
            """
            seasons_row = list(client.query(sql_seasons).result())[0]
            rec["seasons"] = seasons_row.get("seasons")
        except Exception:
            # likely no season column
            rec["seasons"] = None

        # count rows for 2025 (if season exists)
        try:
            sql_cnt_2025 = f"SELECT COUNT(1) AS c FROM `{fqn}` WHERE season = 2025"
            cnt_row = list(client.query(sql_cnt_2025).result())[0]
            rec["rows_2025"] = int(cnt_row.get("c"))
        except Exception:
            rec["rows_2025"] = None

        results.append(rec)
    return results


def main() -> int:
    args = parse_args()
    client = bigquery.Client(project=args.project, location=args.location)

    rows = run_checks(client, args.project, args.dataset_nfl)

    print("\nBigQuery 2025 Availability (dataset: {}):".format(args.dataset_nfl))
    print("=" * 60)
    for r in rows:
        tbl = r["table"]
        exists = r["exists"]
        seasons = r.get("seasons")
        rows_2025 = r.get("rows_2025")
        print(f"- {tbl}:")
        print(f"  exists: {exists}")
        if seasons is not None:
            print(f"  seasons: {seasons}")
        else:
            print("  seasons: (no 'season' column)")
        if rows_2025 is not None:
            print(f"  rows_2025: {rows_2025}")
        else:
            print("  rows_2025: n/a")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

