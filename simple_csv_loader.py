#!/usr/bin/env python3
"""
Simple CSV Projection Loader

Loads CSV projections into the database using LEFT JOIN logic:
- Only creates projections for players that already exist in the database
- No new player records created
- No mock data generated
"""

import argparse
import sys
import pandas as pd
from typing import Dict, Optional
from datetime import datetime, timezone

# Import existing infrastructure
from src.database import SessionLocal, Player, PlayerProjections


class SimpleCsvLoader:
    def __init__(self):
        self.session = SessionLocal()
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()
        
    def normalize_name(self, name: str) -> str:
        """Simple name normalization."""
        return name.lower().strip()
        
    def load_csv_projections(self, csv_path: str, season: int, week: int) -> Dict[str, int]:
        """Load CSV projections with LEFT JOIN logic."""
        results = {
            'csv_players': 0,
            'matched_players': 0,
            'projections_created': 0,
            'projections_updated': 0,
            'unmatched_players': 0
        }
        
        # Load CSV
        print(f"📊 Loading CSV from: {csv_path}")
        csv_df = pd.read_csv(csv_path)
        results['csv_players'] = len(csv_df)
        print(f"Found {len(csv_df)} players in CSV")
        
        # Process each player
        unmatched_players = []
        
        for _, row in csv_df.iterrows():
            player_name = row['Player Name']
            position = row['Pos']
            projected_points = row['Points']
            
            # Skip players with invalid data
            if pd.isna(projected_points) or pd.isna(player_name):
                continue
                
            # Find matching player in database (LEFT JOIN logic)
            existing_player = self._find_player(player_name, position)
            
            if existing_player:
                results['matched_players'] += 1
                
                # Check if projection already exists
                existing_projection = self.session.query(PlayerProjections).filter(
                    PlayerProjections.player_id == existing_player.id,
                    PlayerProjections.season == season,
                    PlayerProjections.week == week,
                    PlayerProjections.source == 'csv_rankings'
                ).first()
                
                if existing_projection:
                    # Update existing projection
                    existing_projection.projected_points = float(projected_points)
                    existing_projection.updated_at = datetime.now(timezone.utc)
                    results['projections_updated'] += 1
                    print(f"  ✏️ Updated: {player_name} -> {projected_points} pts")
                else:
                    # Create new projection
                    new_projection = PlayerProjections(
                        player_id=existing_player.id,
                        season=season,
                        week=week,
                        projected_points=float(projected_points),
                        source='csv_rankings',
                        scoring_format='ppr',
                        created_at=datetime.now(timezone.utc),
                        updated_at=datetime.now(timezone.utc)
                    )
                    self.session.add(new_projection)
                    results['projections_created'] += 1
                    print(f"  ✅ Added: {player_name} -> {projected_points} pts")
                    
            else:
                # Player not found - skip (LEFT JOIN logic)
                unmatched_players.append(f"{player_name} ({position})")
                results['unmatched_players'] += 1
                
        # Commit all changes
        print(f"\n💾 Committing {results['projections_created']} new and {results['projections_updated']} updated projections...")
        self.session.commit()
        
        # Report unmatched players
        if unmatched_players:
            print(f"\n⚠️ Unmatched players (skipped): {len(unmatched_players)}")
            for player in unmatched_players[:10]:  # Show first 10
                print(f"  - {player}")
            if len(unmatched_players) > 10:
                print(f"  ... and {len(unmatched_players) - 10} more")
                
        return results
    
    def _find_player(self, name: str, position: str) -> Optional[Player]:
        """Find player in database by name and position."""
        norm_name = self.normalize_name(name)
        
        # Try exact match first
        player = self.session.query(Player).filter(
            Player.name.ilike(name),
            Player.position == position
        ).first()
        
        if player:
            return player
            
        # Try fuzzy match by first and last name
        name_parts = norm_name.split()
        if len(name_parts) >= 2:
            first_name = name_parts[0]
            last_name = name_parts[-1]
            
            player = self.session.query(Player).filter(
                Player.name.ilike(f'%{first_name}%'),
                Player.name.ilike(f'%{last_name}%'),
                Player.position == position
            ).first()
            
            return player
            
        return None
    
    def verify_loading(self, season: int, week: int):
        """Verify that projections were loaded correctly."""
        print(f"\n🔍 Verifying projections for {season} season, week {week}...")
        
        # Count total projections loaded
        total_projections = self.session.query(PlayerProjections).filter(
            PlayerProjections.season == season,
            PlayerProjections.week == week,
            PlayerProjections.source == 'csv_rankings'
        ).count()
        
        print(f"Total CSV projections loaded: {total_projections}")
        
        # Show top 5 projected players
        top_players = self.session.query(
            Player.name,
            Player.position,
            Player.sleeper_id,
            Player.mfl_id,
            PlayerProjections.projected_points
        ).join(PlayerProjections).filter(
            PlayerProjections.season == season,
            PlayerProjections.week == week,
            PlayerProjections.source == 'csv_rankings'
        ).order_by(PlayerProjections.projected_points.desc()).limit(5).all()
        
        if top_players:
            print(f"\nTop 5 projected players:")
            for i, (name, pos, sleeper_id, mfl_id, points) in enumerate(top_players, 1):
                print(f"  {i}. {name} ({pos}) - {points} pts | Sleeper:{sleeper_id} | MFL:{mfl_id}")
        else:
            print("No projections found!")
            
        return total_projections


def main():
    parser = argparse.ArgumentParser(
        description='Load CSV projections into database with LEFT JOIN logic'
    )
    parser.add_argument('--csv-path', required=True,
                       help='Path to the CSV file')
    parser.add_argument('--season', type=int, default=2025,
                       help='NFL season year (default: 2025)')
    parser.add_argument('--week', type=int, default=1,
                       help='NFL week number (default: 1)')
    parser.add_argument('--verify', action='store_true',
                       help='Verify loading after completion')
    
    args = parser.parse_args()
    
    try:
        with SimpleCsvLoader() as loader:
            print(f"🚀 Loading CSV projections for {args.season} season, week {args.week}...")
            print(f"CSV file: {args.csv_path}")
            print(f"Using LEFT JOIN logic: Only existing database players get projections")
            print()
            
            # Load projections
            results = loader.load_csv_projections(args.csv_path, args.season, args.week)
            
            # Print results
            print("\n" + "="*50)
            print("LOADING RESULTS")
            print("="*50)
            print(f"CSV players processed: {results['csv_players']}")
            print(f"Players matched in DB: {results['matched_players']}")
            print(f"Projections created: {results['projections_created']}")
            print(f"Projections updated: {results['projections_updated']}")
            print(f"Unmatched players (skipped): {results['unmatched_players']}")
            
            match_rate = results['matched_players'] / results['csv_players'] * 100 if results['csv_players'] > 0 else 0
            print(f"Match rate: {match_rate:.1f}%")
            
            # Verify if requested
            if args.verify:
                loader.verify_loading(args.season, args.week)
                
        return 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())