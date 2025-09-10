#!/usr/bin/env python3
"""
Convert ProReferenceFootballStats .xls (often HTML) into CSV files.

Selects the most plausible stats table per file and writes it to CSV with original headers.

Usage:
  python Create_BigQuery_Schema/prf_convert_to_csv.py \
    --receiving ProReferenceFootballStats/Receiving_Stats_Week_1.xls \
    --passing   ProReferenceFootballStats/Passing_Stats_Week_1.xls \
    --rushing   ProReferenceFootballStats/Rushing_Stats_Week_1.xls

Outputs CSVs next to sources by default (same basename, .csv extension). Use --out-* to override.
"""
from __future__ import annotations

import argparse
import os
import re
from typing import List

import pandas as pd


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Convert PRF .xls (HTML) to CSV")
    ap.add_argument("--receiving", help="Path to receiving .xls")
    ap.add_argument("--passing", help="Path to passing .xls")
    ap.add_argument("--rushing", help="Path to rushing .xls")
    ap.add_argument("--out-receiving", help="Output CSV for receiving")
    ap.add_argument("--out-passing", help="Output CSV for passing")
    ap.add_argument("--out-rushing", help="Output CSV for rushing")
    return ap.parse_args()


def _choose_best_table(tables: List[pd.DataFrame]) -> pd.DataFrame:
    def has_cols(df: pd.DataFrame, needles: List[str]) -> bool:
        cols = [str(c).strip().lower() for c in df.columns]
        return all(any(n in c for c in cols) for n in needles)

    for df in tables:
        if has_cols(df, ["player"]) and (has_cols(df, ["tm"]) or has_cols(df, ["team"])):
            return df
    for df in tables:
        if has_cols(df, ["player"]) or has_cols(df, ["name"]):
            return df
    return sorted(tables, key=lambda d: d.shape[1], reverse=True)[0]


def convert_one(src: str, dest_csv: str) -> None:
    print(f"[READ] {src}")
    pl = src.lower()
    if pl.endswith(".csv"):
        df = pd.read_csv(src)
    else:
        # Many PRF .xls are HTML; prefer read_html
        try:
            tables = pd.read_html(src)
            if not tables:
                raise ValueError("No tables found via read_html")
            df = _choose_best_table(tables)
        except Exception:
            # Fallback to old xlrd engine for true .xls
            df = pd.read_excel(src, engine="xlrd")

    # Flatten MultiIndex headers into single, informative names
    if isinstance(df.columns, pd.MultiIndex):
        def clean_token(s: str) -> str:
            s = re.sub(r"\W+", "_", s.strip())
            return re.sub(r"_+", "_", s).strip("_")

        new_cols = []
        for a, b in df.columns.tolist():
            a_str = str(a) if a is not None else ""
            b_str = str(b) if b is not None else ""
            a_is_unnamed = a_str.lower().startswith("unnamed") or a_str.strip() == ""
            b_is_unnamed = b_str.lower().startswith("unnamed") or b_str.strip() == ""

            if not a_is_unnamed and not b_is_unnamed:
                # Use prefix for disambiguation (e.g., RPO_Yds, PlayAction_PassYds)
                name = f"{clean_token(a_str)}_{clean_token(b_str)}"
            elif a_is_unnamed and not b_is_unnamed:
                name = clean_token(b_str)
            elif not a_is_unnamed and b_is_unnamed:
                name = clean_token(a_str)
            else:
                name = "col"
            new_cols.append(name)
        df.columns = new_cols
    else:
        df.columns = [str(c).strip() for c in df.columns]

    # Drop obvious summary/footer rows like "League Average"
    for name_col in ("Player", "Name", "player", "name"):
        if name_col in df.columns:
            df = df[~df[name_col].astype(str).str.contains(r"^\s*League\s+Average\s*$", case=False, na=False)]
            break
    print(f"[INFO] columns -> {list(df.columns)}; rows={len(df)}")
    os.makedirs(os.path.dirname(dest_csv) or ".", exist_ok=True)
    df.to_csv(dest_csv, index=False)
    print(f"[OK] wrote {dest_csv}")


def default_out(path: str) -> str:
    root, _ = os.path.splitext(path)
    return root + ".csv"


def main() -> int:
    args = parse_args()
    tasks = []
    if args.passing:
        tasks.append((args.passing, args.out_passing or default_out(args.passing)))
    if args.rushing:
        tasks.append((args.rushing, args.out_rushing or default_out(args.rushing)))
    if args.receiving:
        tasks.append((args.receiving, args.out_receiving or default_out(args.receiving)))

    if not tasks:
        print("No input files provided.")
        return 1

    for src, dest in tasks:
        convert_one(src, dest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
