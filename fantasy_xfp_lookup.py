#!/usr/bin/env python3
"""fantasy_xfp_lookup.py
Simple Expected Fantasy Points Lookup Tool

Based on the provided xFP dashboard script but simplified for basic player lookups.
Downloads Ben Baldwin's expected fantasy points data and allows lookup of specific players.

Usage:
    python fantasy_xfp_lookup.py
    Enter player name when prompted
"""

from __future__ import annotations

import io
import logging
import sys
from pathlib import Path
from typing import List, Optional

try:
    import pandas as pd
    import requests
    CORE_DEPENDENCIES = True
except ImportError as e:
    print(f"❌ Missing core dependencies: {e}")
    print("Install with: pip install pandas requests")
    CORE_DEPENDENCIES = False

DEPENDENCIES_AVAILABLE = CORE_DEPENDENCIES

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

SEASONS: List[int] = [2024]  # Current season
CACHE_FILE = Path("weekly_player_metrics.csv")

XFP_URL = (
    "https://github.com/ffverse/ffopportunity/releases/download/"
    "latest-data/ep_weekly_2024.csv"
)

# Fantasy scoring weights (PPR)
RUSH_YARD = 0.1
REC_YARD = 0.1
PASS_YARD = 0.04
RUSH_TD = REC_TD = 6
PASS_TD = 4
RECEPTION = 1
FUMBLE_LOST = INTERCEPTION = -2

DELTA_THRESHOLD = 4.0  # points

# ---------------------------------------------------------------------------
# LOGGING SETUP
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DATA HELPERS
# ---------------------------------------------------------------------------

def load_weekly_xfp(seasons: List[int]) -> pd.DataFrame:
    """Download and filter ffopportunity weekly xFP data for the given seasons."""
    log.info("Downloading weekly xFP data from ffopportunity...")
    try:
        resp = requests.get(XFP_URL, timeout=60)
        resp.raise_for_status()
        xfp_df = pd.read_csv(io.BytesIO(resp.content))

        # Simple column mapping - use ffopportunity data as-is
        column_mapping = {
            'full_name': 'player_name',
            'total_fantasy_points_exp': 'exp_fpts',
            'total_fantasy_points': 'actual_fpts', 
            'total_fantasy_points_diff': 'delta',
            'rush_attempt': 'carries',
            'rec_attempt': 'targets'
        }
        
        # Rename columns
        xfp_df = xfp_df.rename(columns=column_mapping)
        
        # Calculate touches
        xfp_df['touches'] = xfp_df['carries'].fillna(0) + xfp_df['receptions'].fillna(0)

        # Keep only data for requested seasons
        xfp_df = xfp_df.loc[xfp_df["season"].isin(seasons)]
        
        log.info("xFP rows loaded: %s", len(xfp_df))
        return xfp_df
    except Exception as e:
        log.error(f"Failed to download xFP data: {e}")
        raise


def label_delta(delta: float | None) -> str:
    """Classify delta into Buy-Low / Sell-High / Neutral."""
    if delta is None or pd.isna(delta):
        return "Unknown"
    if delta >= DELTA_THRESHOLD:
        return "Buy-Low"
    if delta <= -DELTA_THRESHOLD:
        return "Sell-High"
    return "Neutral"

def build_player_metrics() -> pd.DataFrame:
    """Build the complete player metrics dataset."""
    if CACHE_FILE.exists():
        log.info("Loading cached data from %s", CACHE_FILE)
        return pd.read_csv(CACHE_FILE)
    
    log.info("Building fresh player metrics data...")
    df = load_weekly_xfp(SEASONS)
    
    # Add flag based on delta (delta is already actual - expected from ffopportunity)
    df["flag"] = df["delta"].apply(label_delta)

    # Select columns we want to keep
    columns_to_keep = [
        "player_id",
        "player_name", 
        "position",
        "season",
        "week",
        "carries",
        "targets",
        "receptions", 
        "touches",
        "actual_fpts",
        "exp_fpts",
        "delta",
        "flag",
    ]
    
    # Only keep columns that exist in the dataframe
    available_cols = [col for col in columns_to_keep if col in df.columns]
    result_df = df[available_cols].copy()

    # Save cache
    log.info("Saving cache to %s", CACHE_FILE)
    result_df.to_csv(CACHE_FILE, index=False)
    log.info("Player metrics built: %s rows", len(result_df))
    
    return result_df

# ---------------------------------------------------------------------------
# LOOKUP FUNCTIONS
# ---------------------------------------------------------------------------

def lookup_player_xfp(player_name: str, df: pd.DataFrame, week: Optional[int] = None) -> List[dict]:
    """
    Look up expected fantasy points for a specific player.
    
    Args:
        player_name: Name of player to search for
        df: Player metrics dataframe
        week: Specific week to filter (optional)
    
    Returns:
        List of matching player records
    """
    # Case-insensitive search
    mask = df['player_name'].str.contains(player_name, case=False, na=False)
    
    if week is not None:
        mask = mask & (df['week'] == week)
    
    results = df[mask].to_dict('records')
    return results

def display_player_results(player_name: str, results: List[dict]):
    """Display player lookup results in a readable format."""
    if not results:
        print(f"❌ No data found for player: {player_name}")
        return
    
    print(f"\n🏈 FANTASY POINTS DATA FOR: {player_name}")
    print("=" * 60)
    
    # Group by player name (in case of partial matches)
    players = {}
    for result in results:
        name = result['player_name']
        if name not in players:
            players[name] = []
        players[name].append(result)
    
    for name, player_data in players.items():
        print(f"\n📊 {name} ({player_data[0].get('position', 'Unknown')})")
        print("-" * 40)
        
        # Show recent weeks first
        player_data = sorted(player_data, key=lambda x: (x.get('season', 0), x.get('week', 0)), reverse=True)
        
        total_exp = 0
        total_actual = 0
        weeks_shown = 0
        
        for data in player_data[:4]:  # Show last 4 weeks
            week = data.get('week', 'N/A')
            season = data.get('season', 'N/A')
            exp_fpts = data.get('exp_fpts', 0) or 0
            actual_fpts = data.get('actual_fpts', 0) or 0
            delta = data.get('delta', 0) or 0
            flag = data.get('flag', 'Unknown')
            
            total_exp += exp_fpts
            total_actual += actual_fpts
            weeks_shown += 1
            
            flag_emoji = {
                'Buy-Low': '📈',
                'Sell-High': '📉', 
                'Neutral': '➡️',
                'Unknown': '❓'
            }.get(flag, '❓')
            
            print(f"  Week {week}: Expected {exp_fpts:.1f}, Actual {actual_fpts:.1f}, "
                  f"Delta {delta:+.1f} {flag_emoji} {flag}")
        
        if weeks_shown > 1:
            avg_exp = total_exp / weeks_shown
            avg_actual = total_actual / weeks_shown  
            avg_delta = avg_exp - avg_actual
            print(f"\n  📈 AVERAGES (last {weeks_shown} weeks):")
            print(f"     Expected: {avg_exp:.1f} pts/week")
            print(f"     Actual: {avg_actual:.1f} pts/week")
            print(f"     Delta: {avg_delta:+.1f} pts/week")
            
            if avg_delta >= DELTA_THRESHOLD:
                print(f"     🎯 RECOMMENDATION: BUY-LOW candidate (underperforming by {avg_delta:.1f} pts)")
            elif avg_delta <= -DELTA_THRESHOLD:
                print(f"     🎯 RECOMMENDATION: SELL-HIGH candidate (overperforming by {abs(avg_delta):.1f} pts)")
            else:
                print(f"     🎯 RECOMMENDATION: NEUTRAL (performing close to expectations)")

def main():
    """Main function with interactive player lookup."""
    if not DEPENDENCIES_AVAILABLE:
        sys.exit(1)
        
    print("🏈 EXPECTED FANTASY POINTS LOOKUP TOOL")
    print("=" * 50)
    print("Loading player data (this may take a few minutes on first run)...")
    
    try:
        # Build or load player metrics
        df = build_player_metrics()
        print(f"✅ Loaded data for {len(df)} player-week records")
        
        # Interactive lookup
        while True:
            print("\n" + "-" * 50)
            player_input = input("Enter player name (or 'quit' to exit): ").strip()
            
            if player_input.lower() in ['quit', 'exit', 'q']:
                print("👋 Goodbye!")
                break
                
            if not player_input:
                print("Please enter a player name.")
                continue
            
            # Optional week filter
            week_input = input("Enter specific week (or press Enter for all weeks): ").strip()
            week = None
            if week_input.isdigit():
                week = int(week_input)
            
            # Lookup and display results
            results = lookup_player_xfp(player_input, df, week)
            display_player_results(player_input, results)
            
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
    except Exception as e:
        log.error("Error during execution: %s", e)
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()