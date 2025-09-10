#!/usr/bin/env python3
"""
ProReferenceFootballStats Excel → BigQuery loader (Week-level actuals)

Loads passing/rushing/receiving Excel exports for a given season/week into a long table:
  {project}.{dataset}.external_weekly_stats_prf

Key features:
- Robust column mapping across slightly different header names
- Computes fantasy_ppr (basic PPR scoring) when inputs available
- Idempotent upsert (MERGE) keyed by (season, week, stat_type, full_name, team)

Usage:
  python Create_BigQuery_Schema/prf_excel_loader.py \
    --project brainrot-453319 \
    --dataset nfl \
    --location US \
    --season 2025 \
    --week 1 \
    --receiving ProReferenceFootballStats/Receiving_Stats_Week_1.xls \
    --passing   ProReferenceFootballStats/Passing_Stats_Week_1.xls \
    --rushing   ProReferenceFootballStats/Rushing_Stats_Week_1.xls
"""
from __future__ import annotations

import argparse
import re
from typing import Dict, List, Optional, Tuple

import pandas as pd
from google.cloud import bigquery


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Load ProReferenceFootballStats Excel to BigQuery")
    ap.add_argument("--project", required=True, help="GCP project id")
    ap.add_argument("--dataset", default="nfl", help="BigQuery dataset for destination table")
    ap.add_argument("--location", default="US", help="BigQuery dataset location")
    ap.add_argument("--season", type=int, required=True, help="Season (e.g., 2025)")
    ap.add_argument("--week", type=int, required=True, help="Week number (e.g., 1)")
    ap.add_argument("--receiving", default="ProReferenceFootballStats/Receiving_Stats_Week_1.xls",
                    help="Path to receiving stats .xls")
    ap.add_argument("--passing", default="ProReferenceFootballStats/Passing_Stats_Week_1.xls",
                    help="Path to passing stats .xls")
    ap.add_argument("--rushing", default="ProReferenceFootballStats/Rushing_Stats_Week_1.xls",
                    help="Path to rushing stats .xls")
    return ap.parse_args()


def _sanitize_bq_columns(df: pd.DataFrame):
    if df is None or df.empty:
        return df, {}
    mapping: Dict[str, str] = {}
    used = set()
    new_cols: List[str] = []
    for col in df.columns:
        col_str = str(col)
        new = re.sub(r"[^A-Za-z0-9_]", "_", col_str)
        if not re.match(r"^[A-Za-z_]", new):
            new = "_" + new
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
    out = df.copy()
    out.columns = new_cols
    return out, mapping


def _load_merge(
    client: bigquery.Client,
    df: pd.DataFrame,
    project: str,
    dataset: str,
    table: str,
    keys: List[str],
):
    if df is None or df.empty:
        print("[INFO] No rows to load; skipping MERGE")
        return

    df, colmap = _sanitize_bq_columns(df)
    sanitized_keys = [colmap.get(k, k) for k in keys]

    dest = f"{project}.{dataset}.{table}"
    tmp_name = f"_tmp_{table}"
    tmp = f"{project}.{dataset}.{tmp_name}"

    # Load to temp (truncate temp each run)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(df, tmp, job_config=job_config).result()

    # Ensure destination exists
    try:
        client.get_table(dest)
    except Exception:
        schema = client.get_table(tmp).schema
        client.create_table(bigquery.Table(dest, schema=schema))

    # Build MERGE
    src_cols = [f.name for f in client.get_table(tmp).schema]
    on_parts = [f"T.{c}=S.{c}" for c in sanitized_keys if c in src_cols]
    if not on_parts:
        raise ValueError(f"No MERGE keys found in source. Keys={sanitized_keys}, src_cols={src_cols}")
    on_clause = " AND ".join(on_parts)

    update_cols = [c for c in src_cols if c not in sanitized_keys]
    update_set = ", ".join([f"T.{c}=S.{c}" for c in update_cols]) if update_cols else None
    insert_cols = ", ".join(src_cols)
    insert_vals = ", ".join([f"S.{c}" for c in src_cols])

    sql = f"MERGE `{dest}` T USING `{tmp}` S ON {on_clause}\n"
    if update_set:
        sql += f"WHEN MATCHED THEN UPDATE SET {update_set}\n"
    else:
        sql += f"WHEN MATCHED THEN UPDATE SET {sanitized_keys[0]} = S.{sanitized_keys[0]}\n"
    sql += f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"

    client.query(sql).result()
    client.delete_table(tmp, not_found_ok=True)
    print(f"[OK] Upserted {len(df)} rows into {dest}")


def _get(df: pd.DataFrame, names: List[str]):
    for n in names:
        if n in df.columns:
            return df[n]
    return pd.Series([None] * len(df))


def _read_table(path: str) -> pd.DataFrame:
    """Read an Excel-like file that may actually be HTML-disguised .xls.

    Strategy:
    1) Try pandas.read_excel with engine='xlrd' (legacy .xls)
    2) If that fails, try pandas.read_html (many sites export HTML tables with .xls extension)
    """
    # Support CSV directly
    pl = path.lower()
    if pl.endswith(".csv"):
        return pd.read_csv(path)
    try:
        return pd.read_excel(path, engine="xlrd")
    except Exception as e:
        # Fallback to HTML table parsing
        try:
            tables = pd.read_html(path)  # requires lxml/bs4
            if tables:
                # Choose the table that looks like a player stat table
                chosen = _choose_best_table(tables)
                return chosen
        except Exception:
            pass
        # Re-raise original for visibility
        raise e


def _choose_best_table(tables: List[pd.DataFrame]) -> pd.DataFrame:
    """Pick the most plausible stats table among read_html outputs.

    Heuristic: prefer a table containing a column like 'Player' or 'Name' and 'Tm'/'Team'.
    Fallback: the widest table (most columns).
    """
    def has_cols(df: pd.DataFrame, needles: List[str]) -> bool:
        cols = [str(c).strip().lower() for c in df.columns]
        return all(any(n in c for c in cols) for n in needles)

    # First pass: player + team present
    for df in tables:
        if has_cols(df, ["player"]) and (has_cols(df, ["tm"]) or has_cols(df, ["team"])):
            return df
    # Second pass: player only
    for df in tables:
        if has_cols(df, ["player"]) or has_cols(df, ["name"]):
            return df
    # Fallback: pick the widest table
    tables_sorted = sorted(tables, key=lambda d: d.shape[1], reverse=True)
    return tables_sorted[0]


def _match_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """Return the best matching column name in df for any of candidates.

    Matching is case-insensitive, ignores non-word characters, and allows substring matches.
    """
    norm = lambda s: re.sub(r"\W+", "", str(s).strip().lower())
    cols = list(df.columns)
    norm_map = {c: norm(c) for c in cols}
    cand_norms = [norm(c) for c in candidates]
    # Exact normalized match
    for c in cols:
        for cn in cand_norms:
            if norm_map[c] == cn:
                return c
    # Substring match
    for c in cols:
        for cn in cand_norms:
            if cn in norm_map[c]:
                return c
    return None


def _safe_series(df: pd.DataFrame, colname: Optional[str]) -> pd.Series:
    if colname and colname in df.columns:
        return df[colname]
    # Return a NULL series of appropriate length
    return pd.Series([None] * len(df))


def read_receiving(path: str, season: int, week: int) -> pd.DataFrame:
    df = _read_table(path)
    df.columns = [str(c).strip() for c in df.columns]
    out = pd.DataFrame()
    # Resolve core columns with flexible matching
    name_col = _match_column(df, ["Player", "Name"]) or "Player"
    team_col = _match_column(df, ["Tm", "Team"]) or "Tm"
    opp_col = _match_column(df, ["Opp", "Opponent"]) or "Opp"

    out["full_name"] = _safe_series(df, name_col).astype("string")
    out["team"] = _safe_series(df, team_col).astype("string")
    out["opponent"] = _safe_series(df, opp_col).astype("string")
    out["season"] = season
    out["week"] = week
    out["stat_type"] = "receiving"
    out["targets"] = pd.to_numeric(_safe_series(df, _match_column(df, ["Tgt", "Tar"]) or "Tgt"), errors="coerce")
    out["receptions"] = pd.to_numeric(_safe_series(df, _match_column(df, ["Rec"]) or "Rec"), errors="coerce")
    out["rec_yds"] = pd.to_numeric(_safe_series(df, _match_column(df, ["Yds"]) or "Yds"), errors="coerce")
    out["rec_td"] = pd.to_numeric(_safe_series(df, _match_column(df, ["TD"]) or "TD"), errors="coerce")
    return out


def read_rushing(path: str, season: int, week: int) -> pd.DataFrame:
    df = _read_table(path)
    df.columns = [str(c).strip() for c in df.columns]
    out = pd.DataFrame()
    name_col = _match_column(df, ["Player", "Name"]) or "Player"
    team_col = _match_column(df, ["Tm", "Team"]) or "Tm"
    opp_col = _match_column(df, ["Opp", "Opponent"]) or "Opp"

    out["full_name"] = _safe_series(df, name_col).astype("string")
    out["team"] = _safe_series(df, team_col).astype("string")
    out["opponent"] = _safe_series(df, opp_col).astype("string")
    out["season"] = season
    out["week"] = week
    out["stat_type"] = "rushing"
    out["rush_att"] = pd.to_numeric(_safe_series(df, _match_column(df, ["Att"]) or "Att"), errors="coerce")
    out["rush_yds"] = pd.to_numeric(_safe_series(df, _match_column(df, ["Yds"]) or "Yds"), errors="coerce")
    out["rush_td"] = pd.to_numeric(_safe_series(df, _match_column(df, ["TD"]) or "TD"), errors="coerce")
    return out


def read_passing(path: str, season: int, week: int) -> pd.DataFrame:
    df = _read_table(path)
    df.columns = [str(c).strip() for c in df.columns]
    out = pd.DataFrame()
    name_col = _match_column(df, ["Player", "Name"]) or "Player"
    team_col = _match_column(df, ["Tm", "Team"]) or "Tm"
    opp_col = _match_column(df, ["Opp", "Opponent"]) or "Opp"

    out["full_name"] = _safe_series(df, name_col).astype("string")
    out["team"] = _safe_series(df, team_col).astype("string")
    out["opponent"] = _safe_series(df, opp_col).astype("string")
    out["season"] = season
    out["week"] = week
    out["stat_type"] = "passing"
    out["pass_att"] = pd.to_numeric(_safe_series(df, _match_column(df, ["Passing_Att", "Att"]) or _match_column(df, ["Att"]) or "Att"), errors="coerce")
    out["pass_cmp"] = pd.to_numeric(_safe_series(df, _match_column(df, ["Passing_Cmp", "Cmp", "Comp"]) or _match_column(df, ["Cmp", "Comp"]) or "Cmp"), errors="coerce")
    # Prefer explicit passing yards if present; else derive from CAY + YAC if available
    ycol = (
        _match_column(df, ["Passing_Yds", "PassYds"]) or
        _match_column(df, ["Yds"])  # generic, may match wrong group
    )
    out["pass_yds"] = pd.to_numeric(_safe_series(df, ycol), errors="coerce") if ycol else pd.Series([None] * len(df))
    out["pass_td"] = pd.to_numeric(_safe_series(df, _match_column(df, ["TD"]) or "TD"), errors="coerce")
    out["interceptions"] = pd.to_numeric(_safe_series(df, _match_column(df, ["Int", "Int."]) or "Int"), errors="coerce")
    if out["pass_yds"].isna().all():
        cay = pd.to_numeric(_safe_series(df, _match_column(df, ["Air_Yards_CAY", "CAY"]) or "CAY"), errors="coerce").fillna(0)
        yac = pd.to_numeric(_safe_series(df, _match_column(df, ["Air_Yards_YAC", "YAC"]) or "YAC"), errors="coerce").fillna(0)
        # Derive total passing yards approximation
        out["pass_yds"] = (cay + yac).where((cay + yac) > 0, None)
    return out


def compute_ppr(df: pd.DataFrame) -> pd.Series:
    # Basic PPR scoring
    return (
        0.04 * df.get("pass_yds", 0).fillna(0)
        + 4.0 * df.get("pass_td", 0).fillna(0)
        - 2.0 * df.get("interceptions", 0).fillna(0)
        + 0.1 * df.get("rush_yds", 0).fillna(0)
        + 6.0 * df.get("rush_td", 0).fillna(0)
        + 0.1 * df.get("rec_yds", 0).fillna(0)
        + 6.0 * df.get("rec_td", 0).fillna(0)
        + 1.0 * df.get("receptions", 0).fillna(0)
    )


def main() -> int:
    args = parse_args()

    # If inputs are CSVs, upload raw CSVs (plus season/week/stat_type/source_file) and overwrite table
    csv_mode = all([
        (not args.passing or args.passing.lower().endswith('.csv')),
        (not args.rushing or args.rushing.lower().endswith('.csv')),
        (not args.receiving or args.receiving.lower().endswith('.csv')),
    ])

    if csv_mode:
        parts: List[pd.DataFrame] = []
        for path, typ in [
            (args.passing, 'passing'),
            (args.rushing, 'rushing'),
            (args.receiving, 'receiving'),
        ]:
            if not path:
                continue
            print(f"[READ CSV] {typ}: {path}")
            df = pd.read_csv(path)
            # Normalize column headers lightly
            df.columns = [str(c).strip() for c in df.columns]
            df["season"] = args.season
            df["week"] = args.week
            df["stat_type"] = typ
            df["source_file"] = path
            parts.append(df)

        if not parts:
            print("[ERR] No input files provided")
            return 1

        raw_df = pd.concat(parts, ignore_index=True, sort=False)
        # Sanitize column names to BigQuery-safe schema (no dots, spaces; dedupe)
        raw_df, _ = _sanitize_bq_columns(raw_df)

        client = bigquery.Client(project=args.project, location=args.location)
        from google.cloud.bigquery import LoadJobConfig
        job_cfg = LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        dest = f"{args.project}.{args.dataset}.external_weekly_stats_prf"
        print(f"[LOAD] Uploading {len(raw_df)} rows to {dest} (WRITE_TRUNCATE)")
        client.load_table_from_dataframe(raw_df, dest, job_config=job_cfg).result()
        print("[OK] Load complete")
        return 0

    # Read sheets
    frames: List[pd.DataFrame] = []
    if args.passing:
        print(f"[READ] passing: {args.passing}")
        frames.append(read_passing(args.passing, args.season, args.week))
    if args.rushing:
        print(f"[READ] rushing: {args.rushing}")
        frames.append(read_rushing(args.rushing, args.season, args.week))
    if args.receiving:
        print(f"[READ] receiving: {args.receiving}")
        frames.append(read_receiving(args.receiving, args.season, args.week))

    if not frames:
        print("[ERR] No input files provided")
        return 1

    long_df = pd.concat(frames, ignore_index=True)

    # Compute fantasy_ppr
    long_df["fantasy_ppr"] = compute_ppr(long_df)

    # Normalize dtypes (names/teams to string)
    for c in ["full_name", "team", "opponent", "stat_type"]:
        if c in long_df.columns:
            long_df[c] = long_df[c].astype("string")

    # Basic visibility/debugging
    total_rows = len(long_df)
    null_names = int(long_df["full_name"].isna().sum()) if "full_name" in long_df.columns else total_rows
    if null_names:
        print(f"[WARN] full_name is NULL for {null_names}/{total_rows} rows. Check source headers.")

    # Upsert into BigQuery
    client = bigquery.Client(project=args.project, location=args.location)
    _load_merge(
        client,
        long_df,
        project=args.project,
        dataset=args.dataset,
        table="external_weekly_stats_prf",
        keys=["season", "week", "stat_type", "full_name", "team"],
    )

    # Quick summary
    print("[SUMMARY] Rows by stat_type:")
    print(long_df.groupby("stat_type").size())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
