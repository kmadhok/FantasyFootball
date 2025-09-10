#!/usr/bin/env python3
"""
Enhanced Canonical Fantasy Football Data Model

Integrates CSV projections data with Sleeper, MFL, and nfl_data_py to create a unified 
fantasy football data model. Adapted from ChatGPT skeleton to work with your specific
CSV structure and existing infrastructure.
"""

import argparse
import re
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timezone

import pandas as pd
import requests
import nfl_data_py as nfl

# Import your existing infrastructure
try:
    from src.database import SessionLocal, Player, PlayerRankings
    from src.utils.player_id_mapper import PlayerIDMapper
    from src.config.config import get_config
    HAS_LOCAL_INFRASTRUCTURE = True
except ImportError:
    print("Warning: Local infrastructure not available, using standalone mode")
    HAS_LOCAL_INFRASTRUCTURE = False

# Output directory
OUT_DIR = Path("out")
OUT_DIR.mkdir(exist_ok=True)

# -----------------------------
# Enhanced Utilities
# -----------------------------

def norm_name(s: Optional[str]) -> Optional[str]:
    """Enhanced name normalizer for robust player matching."""
    if s is None or pd.isna(s):
        return None
    s = str(s).strip().lower()
    
    # Remove common suffixes
    s = re.sub(r'\s+(jr\.?|sr\.?|iii|iv)$', '', s)
    
    # Remove punctuation except hyphens and apostrophes in names
    s = re.sub(r"[^\w\s'-]", "", s)
    
    # Normalize spaces
    s = re.sub(r'\s+', ' ', s).strip()
    
    return s if s else None

def norm_team(team: Optional[str]) -> Optional[str]:
    """Normalize team abbreviations."""
    if not team or pd.isna(team):
        return None
    
    team = str(team).upper().strip()
    
    # Common team mappings
    team_map = {
        'WSH': 'WAS',  # Washington
        'JAC': 'JAX',  # Jacksonville  
        'LV': 'LVR',   # Las Vegas (some systems)
        'LAR': 'LA',   # LA Rams (some systems)
    }
    
    return team_map.get(team, team)

def norm_position(pos: Optional[str]) -> Optional[str]:
    """Normalize position abbreviations."""
    if not pos or pd.isna(pos):
        return None
    
    pos = str(pos).upper().strip()
    
    # Position mappings
    pos_map = {
        'DEF': 'DST',
        'D/ST': 'DST', 
        'D': 'DST',
        'PK': 'K',
        'FLEX': None,  # Remove flex designations
    }
    
    return pos_map.get(pos, pos)

def is_team_defense_like(player_id: str) -> bool:
    """Check if ID represents a team defense."""
    return bool(re.fullmatch(r"[A-Z]{2,4}", player_id or ""))

def parse_name_components(full_name: str) -> Tuple[str, str]:
    """Split full name into first and last components."""
    if not full_name or pd.isna(full_name):
        return None, None
    
    parts = str(full_name).strip().split()
    if len(parts) == 1:
        return parts[0], ""
    elif len(parts) >= 2:
        return parts[0], " ".join(parts[1:])
    
    return full_name, ""

# -----------------------------
# Enhanced Data Loaders
# -----------------------------

def load_sleeper_rosters(league_id: str) -> pd.DataFrame:
    """Load Sleeper roster data."""
    try:
        url = f"https://api.sleeper.app/v1/league/{league_id}/rosters"
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        df = pd.json_normalize(r.json())
        
        # Ensure expected columns exist
        for col in ["roster_id", "owner_id", "players", "starters"]:
            if col not in df.columns:
                df[col] = None
                
        return df[["roster_id", "owner_id", "players", "starters"]]
    except Exception as e:
        print(f"Warning: Could not load Sleeper rosters: {e}")
        return pd.DataFrame(columns=["roster_id", "owner_id", "players", "starters"])

def load_sleeper_players_dump(optional: bool = True) -> pd.DataFrame:
    """Load comprehensive Sleeper player database."""
    if not optional:
        return pd.DataFrame(columns=["sleeper_id"])
    
    try:
        print("Loading Sleeper players dump (this may take a moment)...")
        r = requests.get("https://api.sleeper.app/v1/players/nfl", timeout=120)
        r.raise_for_status()
        data = r.json()
        
        rows = []
        for sid, rec in data.items():
            rec = rec or {}
            rec["sleeper_id"] = sid
            rows.append(rec)
            
        df = pd.json_normalize(rows)
        print(f"Loaded {len(df)} players from Sleeper")
        return df
        
    except Exception as e:
        print(f"Warning: Could not load Sleeper players dump: {e}")
        return pd.DataFrame(columns=["sleeper_id"])

def load_mfl_players(season: int) -> pd.DataFrame:
    """Load MFL players database."""
    try:
        url = f"https://api.myfantasyleague.com/{season}/export"
        params = {"TYPE": "players", "DETAILS": 1, "JSON": 1}
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        
        payload = r.json() or {}
        players = (payload.get("players") or {}).get("player") or []
        df = pd.json_normalize(players)
        
        # Normalize column names
        rename_map = {
            "id": "mfl_id", 
            "name": "full_name", 
            "position": "position", 
            "team": "team"
        }
        df = df.rename(columns=rename_map)
        
        for col in ["mfl_id", "full_name", "position", "team"]:
            if col not in df.columns:
                df[col] = None
                
        print(f"Loaded {len(df)} players from MFL")
        return df[["mfl_id", "full_name", "position", "team"]].drop_duplicates()
        
    except Exception as e:
        print(f"Warning: Could not load MFL players: {e}")
        return pd.DataFrame(columns=["mfl_id", "full_name", "position", "team"])

def load_nfl_ids() -> pd.DataFrame:
    """Load comprehensive NFL ID crosswalk."""
    try:
        print("Loading NFL ID crosswalk...")
        df = nfl.import_ids()
        
        # Map NFL data columns to our expected names
        if 'name' in df.columns:
            df['player_name'] = df['name']
        
        # Parse first_name and last_name from full name if not present
        if 'player_name' in df.columns and 'first_name' not in df.columns:
            df[['first_name', 'last_name']] = df['player_name'].apply(
                lambda x: pd.Series(parse_name_components(x))
            )
        
        # Standardize expected columns
        expected_cols = [
            "gsis_id", "pfr_id", "sportradar_id", "sleeper_id", 
            "yahoo_id", "rotowire_id", "rotoworld_id", "mfl_id", 
            "fantasypros_id", "player_name", "first_name", "last_name", "position", "team"
        ]
        
        for col in expected_cols:
            if col not in df.columns:
                df[col] = None
                
        print(f"Loaded {len(df)} player ID mappings")
        return df[expected_cols].drop_duplicates()
        
    except Exception as e:
        print(f"Error loading NFL IDs: {e}")
        raise

def load_enhanced_projections_csv(csv_path: Path) -> pd.DataFrame:
    """Load and parse the specific CSV structure with enhanced column mapping."""
    try:
        df = pd.read_csv(csv_path)
        print(f"Loaded CSV with {len(df)} rows and columns: {list(df.columns)}")
        
        # Map your specific CSV columns to standardized names
        column_mapping = {
            "Player Name": "player_name",
            "Pos": "position", 
            "Opp": "opponent",
            "$": "salary",
            "AVG ADP": "avg_adp",
            "Trend": "trend",
            "Points": "proj_points",
            "YDS": "proj_yards", 
            "YPC": "proj_ypc",
            "Rush": "proj_rush",
            "REC": "proj_rec", 
            "TD": "proj_td",
            "YPR": "proj_ypr",
            "Rec/Tar Game": "proj_rec_tar",
            "QB Att/Game": "proj_qb_att",
            "QB YPC": "proj_qb_ypc",
            "Rank": "rank"
        }
        
        # Rename columns
        df = df.rename(columns=column_mapping)
        
        # Clean and normalize key fields
        df["player_name"] = df["player_name"].astype(str)
        df["position"] = df["position"].map(norm_position)
        df["rank"] = pd.to_numeric(df.get("rank"), errors="coerce")
        df["proj_points"] = pd.to_numeric(df.get("proj_points"), errors="coerce")
        
        # Add name components
        name_parts = df["player_name"].apply(parse_name_components)
        df["first_name"] = [parts[0] for parts in name_parts]
        df["last_name"] = [parts[1] for parts in name_parts]
        
        # Add normalized name for matching
        df["norm_name"] = df["player_name"].map(norm_name)
        
        print(f"Processed CSV: {len(df)} players with positions {df['position'].value_counts().to_dict()}")
        return df
        
    except Exception as e:
        print(f"Error loading projections CSV: {e}")
        raise

def load_nfl_weekly_stats(seasons: List[int]) -> pd.DataFrame:
    """Load NFL weekly statistics."""
    try:
        print(f"Loading weekly stats for seasons: {seasons}")
        df = nfl.import_weekly_data(seasons)
        
        # Keep essential columns
        keep_cols = [
            "season", "week", "player_id", "player_name", "position", "recent_team",
            "attempts", "completions", "passing_yards", "passing_tds", "interceptions",
            "rushing_yards", "rushing_tds", "receptions", "receiving_yards", "receiving_tds",
            "fumbles_lost", "fantasy_points_ppr"
        ]
        
        for col in keep_cols:
            if col not in df.columns:
                df[col] = None
                
        print(f"Loaded {len(df)} weekly stat records")
        return df[keep_cols]
        
    except Exception as e:
        print(f"Warning: Could not load weekly stats: {e}")
        keep_cols = [
            "season", "week", "player_id", "player_name", "position", "recent_team",
            "attempts", "completions", "passing_yards", "passing_tds", "interceptions",
            "rushing_yards", "rushing_tds", "receptions", "receiving_yards", "receiving_tds",
            "fumbles_lost", "fantasy_points_ppr"
        ]
        return pd.DataFrame(columns=keep_cols)

# -----------------------------
# Enhanced Model Builders
# -----------------------------

def build_enhanced_player_bridge(nfl_ids: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Build enhanced player ID bridge with better clustering."""
    print("Building player ID bridge...")
    
    # Create clusters based on available IDs
    key_cols = ["gsis_id", "mfl_id", "sleeper_id", "pfr_id", "fantasypros_id", "yahoo_id"]
    
    for col in key_cols:
        if col not in nfl_ids.columns:
            nfl_ids[col] = None
    
    # Create cluster key by combining available IDs
    nfl_ids["_cluster_key"] = (
        nfl_ids[key_cols]
        .astype(str)
        .apply(lambda x: "|".join(x), axis=1)
        .pipe(pd.factorize)
    )[0] + 1
    
    # Build wide format with best available data per cluster
    wide_ids = (
        nfl_ids
        .groupby("_cluster_key", as_index=False)
        .agg({
            "player_name": "first",
            "first_name": "first", 
            "last_name": "first",
            "position": "first",
            "team": "first",
            "gsis_id": lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else None,
            "mfl_id": lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else None,
            "sleeper_id": lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else None,
            "pfr_id": lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else None,
            "fantasypros_id": lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else None,
            "yahoo_id": lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else None,
        })
        .rename(columns={"_cluster_key": "player_sk"})
    )
    
    # Add normalized names
    wide_ids["norm_name"] = wide_ids["player_name"].map(norm_name)
    wide_ids["position"] = wide_ids["position"].map(norm_position)
    wide_ids["team"] = wide_ids["team"].map(norm_team)
    
    # Build long-form bridge
    bridge_rows = []
    for _, row in wide_ids.iterrows():
        player_sk = int(row["player_sk"])
        
        for system, col in [
            ("gsis", "gsis_id"), ("mfl", "mfl_id"), ("sleeper", "sleeper_id"),
            ("pfr", "pfr_id"), ("fantasypros", "fantasypros_id"), ("yahoo", "yahoo_id")
        ]:
            source_id = row.get(col)
            if pd.notna(source_id) and str(source_id) not in ("", "None", "nan"):
                bridge_rows.append({
                    "player_sk": player_sk,
                    "source_system": system, 
                    "source_player_id": str(source_id)
                })
    
    bridge_long = pd.DataFrame(bridge_rows) if bridge_rows else pd.DataFrame(
        columns=["player_sk", "source_system", "source_player_id"]
    )
    
    print(f"Created {len(wide_ids)} player clusters with {len(bridge_long)} ID mappings")
    return wide_ids, bridge_long

def build_enhanced_projections_fact(
    season: int, 
    week: int,
    projections_df: pd.DataFrame,
    wide_ids: pd.DataFrame
) -> pd.DataFrame:
    """Build projections fact table with enhanced matching."""
    print("Building projections fact table...")
    
    # Debug: Show sample data for matching
    print(f"Sample projections names: {projections_df['norm_name'].head().tolist()}")
    print(f"Sample ID names: {wide_ids['norm_name'].head().tolist()}")
    print(f"Projections positions: {projections_df['position'].value_counts().head().to_dict()}")
    print(f"ID positions: {wide_ids['position'].value_counts().head().to_dict()}")
    
    # First try exact name matching
    proj = projections_df.copy()
    ids_for_matching = wide_ids[["player_sk", "norm_name", "position", "team"]].copy()
    
    # Exact normalized name match
    matched = proj.merge(
        ids_for_matching,
        left_on=["norm_name", "position"], 
        right_on=["norm_name", "position"],
        how="left"
    )
    
    print(f"Exact match rate: {matched['player_sk'].notna().mean():.2%}")
    
    # Try name-only matching (ignore position temporarily)
    unmatched = matched[matched["player_sk"].isna()].copy()
    if len(unmatched) > 0:
        print(f"Trying name-only matching for {len(unmatched)} unmatched players...")
        
        name_only_matches = unmatched[["norm_name"]].merge(
            ids_for_matching[["player_sk", "norm_name"]].drop_duplicates(),
            on="norm_name", 
            how="left"
        )
        
        # Update matched with name-only results
        for i, row in name_only_matches.iterrows():
            if pd.notna(row["player_sk"]):
                orig_idx = unmatched.index[i] if i < len(unmatched.index) else None
                if orig_idx is not None:
                    matched.loc[orig_idx, "player_sk"] = row["player_sk"]
        
        print(f"Name-only match rate: {matched['player_sk'].notna().mean():.2%}")
        
        # Try fuzzy matching for still unmatched players
        still_unmatched = matched[matched["player_sk"].isna()].copy()
        if len(still_unmatched) > 0:
            print(f"Attempting fuzzy matching for {len(still_unmatched)} remaining unmatched players...")
            
            # Simple fuzzy matching - look for first name matches
            for idx, row in still_unmatched.iterrows():
                name = row["norm_name"]
                pos = row["position"]
                
                if name and len(name.split()) >= 2:
                    first_name = name.split()[0]
                    last_name = name.split()[-1]
                    
                    # Look for first+last name matches
                    candidates = ids_for_matching[
                        (ids_for_matching["norm_name"].str.contains(first_name, na=False)) &
                        (ids_for_matching["norm_name"].str.contains(last_name, na=False))
                    ]
                    
                    # If same position available, prefer it
                    pos_candidates = candidates[candidates["position"] == pos]
                    if len(pos_candidates) == 1:
                        matched.loc[idx, "player_sk"] = pos_candidates.iloc[0]["player_sk"]
                    elif len(candidates) == 1:
                        matched.loc[idx, "player_sk"] = candidates.iloc[0]["player_sk"]
    
    # Build fact table
    fact_projections = matched[[
        "player_sk", "rank", "proj_points", "proj_yards", "proj_rush", 
        "proj_rec", "proj_td", "proj_ypc", "proj_ypr", "avg_adp", "salary"
    ]].copy()
    
    fact_projections["season"] = season
    fact_projections["week"] = week
    fact_projections["source"] = "csv_rankings"
    fact_projections["load_timestamp"] = datetime.now(timezone.utc)
    
    # Remove rows without player matches
    fact_projections = fact_projections.dropna(subset=["player_sk"])
    
    final_match_rate = len(fact_projections) / len(projections_df)
    print(f"Final projections match rate: {final_match_rate:.2%} ({len(fact_projections)}/{len(projections_df)})")
    
    return fact_projections

def build_data_quality_report(
    projections_df: pd.DataFrame,
    fact_projections: pd.DataFrame,
    wide_ids: pd.DataFrame
) -> pd.DataFrame:
    """Build comprehensive data quality report."""
    
    total_csv_players = len(projections_df)
    matched_players = len(fact_projections)
    total_id_mappings = len(wide_ids)
    
    # Position breakdown
    csv_by_pos = projections_df.groupby("position").size().to_dict()
    matched_by_pos = fact_projections.merge(
        wide_ids[["player_sk", "position"]], on="player_sk", how="left"
    ).groupby("position").size().to_dict()
    
    # Build report rows
    report_rows = [
        {"metric": "total_csv_players", "value": total_csv_players},
        {"metric": "total_id_mappings", "value": total_id_mappings},
        {"metric": "matched_players", "value": matched_players},
        {"metric": "match_rate", "value": matched_players / total_csv_players if total_csv_players > 0 else 0},
    ]
    
    # Add position-specific metrics
    for pos in ["QB", "RB", "WR", "TE", "K", "DST"]:
        csv_count = csv_by_pos.get(pos, 0)
        matched_count = matched_by_pos.get(pos, 0)
        match_rate = matched_count / csv_count if csv_count > 0 else 0
        
        report_rows.extend([
            {"metric": f"csv_players_{pos}", "value": csv_count},
            {"metric": f"matched_players_{pos}", "value": matched_count},
            {"metric": f"match_rate_{pos}", "value": match_rate},
        ])
    
    return pd.DataFrame(report_rows)

# -----------------------------
# Main Orchestration
# -----------------------------

def main():
    parser = argparse.ArgumentParser(description="Build enhanced canonical fantasy football model")
    parser.add_argument("--season", type=int, required=True, help="NFL season year")
    parser.add_argument("--week", type=int, required=True, help="NFL week number")
    parser.add_argument("--projections-csv", type=str, required=True, help="Path to projections CSV file")
    parser.add_argument("--sleeper-league-id", type=str, help="Sleeper league ID")
    parser.add_argument("--mfl-league-id", type=str, help="MFL league ID") 
    parser.add_argument("--skip-sleeper-dump", action="store_true", help="Skip loading full Sleeper players dump")
    parser.add_argument("--skip-weekly-stats", action="store_true", help="Skip loading weekly stats")
    parser.add_argument("--output-dir", type=str, default="out", help="Output directory")
    
    args = parser.parse_args()
    
    global OUT_DIR
    OUT_DIR = Path(args.output_dir)
    OUT_DIR.mkdir(exist_ok=True)
    
    season = args.season
    week = args.week
    
    print(f"Building canonical model for {season} season, week {week}")
    print("=" * 60)
    
    # Load core data sources
    print("\n1. Loading NFL ID crosswalk...")
    nfl_ids = load_nfl_ids()
    
    print("\n2. Loading projections CSV...")
    projections_df = load_enhanced_projections_csv(Path(args.projections_csv))
    
    print("\n3. Building player ID bridge...")
    wide_ids, bridge_long = build_enhanced_player_bridge(nfl_ids)
    
    print("\n4. Building projections fact table...")
    fact_projections = build_enhanced_projections_fact(season, week, projections_df, wide_ids)
    
    print("\n5. Building data quality report...")
    dq_report = build_data_quality_report(projections_df, fact_projections, wide_ids)
    
    # Optional: Load additional data sources
    fact_roster_sleeper = pd.DataFrame()
    if args.sleeper_league_id:
        print("\n6. Loading Sleeper roster data...")
        sleeper_rosters = load_sleeper_rosters(args.sleeper_league_id)
        # TODO: Implement roster fact building
    
    weekly_stats = pd.DataFrame()
    if not args.skip_weekly_stats:
        print("\n7. Loading weekly stats...")
        weekly_stats = load_nfl_weekly_stats([season])
    
    # Save outputs
    print(f"\n8. Saving outputs to {OUT_DIR}...")
    
    # Core dimension tables
    wide_ids.to_parquet(OUT_DIR / "dim_player.parquet", index=False)
    bridge_long.to_parquet(OUT_DIR / "bridge_player_ids.parquet", index=False)
    
    # Fact tables
    fact_projections.to_parquet(OUT_DIR / "fact_projections.parquet", index=False)
    if not weekly_stats.empty:
        weekly_stats.to_parquet(OUT_DIR / "fact_weekly_stats.parquet", index=False)
    
    # Data quality
    dq_report.to_parquet(OUT_DIR / "data_quality_report.parquet", index=False)
    
    # Raw data for reference
    projections_df.to_parquet(OUT_DIR / "raw_projections.parquet", index=False)
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Players in CSV: {len(projections_df)}")
    print(f"Players matched: {len(fact_projections)}")
    print(f"Match rate: {len(fact_projections)/len(projections_df):.2%}")
    print(f"\nFiles saved to: {OUT_DIR}")
    
    # Print key metrics
    print("\nData Quality Metrics:")
    for _, row in dq_report.iterrows():
        metric = row["metric"]
        value = row["value"]
        if "rate" in metric:
            print(f"  {metric}: {value:.2%}")
        else:
            print(f"  {metric}: {value}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())