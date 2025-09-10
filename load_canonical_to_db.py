#!/usr/bin/env python3
"""
Database Loader for Canonical Model Data

Loads the canonical model projection data into the existing SQLAlchemy database,
integrating with Player and PlayerProjections models.
"""

import argparse
import sys
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, List
from datetime import datetime, timezone

# Import existing infrastructure
from src.database import SessionLocal, Player, PlayerProjections
from src.utils.player_id_mapper import PlayerIDMapper
from src.config.config import get_config


class CanonicalDataLoader:
    def __init__(self):
        self.session = SessionLocal()
        self.player_id_mapper = PlayerIDMapper()
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()
        
    def load_canonical_data(self, canonical_dir: Path, season: int, week: int) -> Dict[str, int]:
        """Load canonical model data into the database."""
        results = {
            'players_loaded': 0,
            'players_updated': 0,
            'projections_loaded': 0,
            'projections_updated': 0,
            'errors': 0
        }
        
        # Load the canonical model outputs
        dim_player_path = canonical_dir / 'dim_player.parquet'
        fact_projections_path = canonical_dir / 'fact_projections.parquet'
        
        if not dim_player_path.exists():
            raise FileNotFoundError(f"Cannot find dim_player.parquet in {canonical_dir}")
        if not fact_projections_path.exists():
            raise FileNotFoundError(f"Cannot find fact_projections.parquet in {canonical_dir}")
            
        dim_player = pd.read_parquet(dim_player_path)
        fact_projections = pd.read_parquet(fact_projections_path)
        
        print(f"📊 Loading canonical data: {len(dim_player)} players, {len(fact_projections)} projections")
        
        # Step 1: Load/update players
        player_results = self._load_players(dim_player)
        results.update(player_results)
        
        # Step 2: Load projections
        proj_results = self._load_projections(fact_projections, dim_player, season, week)
        results.update(proj_results)
        
        # Commit changes
        self.session.commit()
        
        return results
    
    def _load_players(self, dim_player: pd.DataFrame) -> Dict[str, int]:
        """Load or update player records."""
        results = {'players_loaded': 0, 'players_updated': 0, 'player_errors': 0}
        
        print("👤 Processing player records...")
        
        for _, row in dim_player.iterrows():
            try:
                # Generate canonical NFL ID using PlayerIDMapper
                canonical_id = self.player_id_mapper.generate_canonical_id(
                    name=row.get('player_name'),
                    position=row.get('position'),
                    team=row.get('team')
                )
                
                if not canonical_id:
                    print(f"⚠️ Could not generate canonical ID for {row.get('player_name')}")
                    results['player_errors'] += 1
                    continue
                
                # Check if player already exists
                existing_player = self.session.query(Player).filter(
                    Player.nfl_id == canonical_id
                ).first()
                
                if existing_player:
                    # Update existing player
                    updated = self._update_player(existing_player, row)
                    if updated:
                        results['players_updated'] += 1
                else:
                    # Create new player
                    new_player = self._create_player(row, canonical_id)
                    if new_player:
                        results['players_loaded'] += 1
                        
            except Exception as e:
                print(f"❌ Error processing player {row.get('player_name', 'Unknown')}: {e}")
                results['player_errors'] += 1
                continue
                
        return results
    
    def _create_player(self, row: pd.Series, canonical_id: str) -> Optional[Player]:
        """Create a new player record."""
        try:
            # Check for conflicting platform IDs before creation
            sleeper_id = row.get('sleeper_id') if pd.notna(row.get('sleeper_id')) else None
            mfl_id = row.get('mfl_id') if pd.notna(row.get('mfl_id')) else None
            
            # If IDs conflict with existing players, skip them for this new player
            if sleeper_id:
                existing = self.session.query(Player).filter(Player.sleeper_id == sleeper_id).first()
                if existing:
                    sleeper_id = None  # Skip conflicting sleeper ID
                    
            if mfl_id:
                existing = self.session.query(Player).filter(Player.mfl_id == mfl_id).first()
                if existing:
                    mfl_id = None  # Skip conflicting MFL ID
            
            player = Player(
                nfl_id=canonical_id,
                sleeper_id=sleeper_id,
                mfl_id=mfl_id,
                name=row.get('player_name'),
                position=row.get('position', 'UNKNOWN'),
                team=row.get('team', 'UNK'),
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc)
            )
            
            self.session.add(player)
            return player
            
        except Exception as e:
            print(f"❌ Error creating player {row.get('player_name')}: {e}")
            return None
    
    def _update_player(self, player: Player, row: pd.Series) -> bool:
        """Update existing player with new information."""
        updated = False
        
        # Update platform IDs if we have new ones and they don't conflict
        if pd.notna(row.get('sleeper_id')) and not player.sleeper_id:
            # Check if this sleeper_id already exists for another player
            existing = self.session.query(Player).filter(
                Player.sleeper_id == row.get('sleeper_id'),
                Player.id != player.id
            ).first()
            if not existing:
                player.sleeper_id = row.get('sleeper_id')
                updated = True
            
        if pd.notna(row.get('mfl_id')) and not player.mfl_id:
            # Check if this mfl_id already exists for another player
            existing = self.session.query(Player).filter(
                Player.mfl_id == row.get('mfl_id'),
                Player.id != player.id
            ).first()
            if not existing:
                player.mfl_id = row.get('mfl_id')
                updated = True
            
        # Update team if changed
        if pd.notna(row.get('team')) and player.team != row.get('team'):
            player.team = row.get('team')
            updated = True
            
        if updated:
            player.updated_at = datetime.now(timezone.utc)
            
        return updated
    
    def _load_projections(self, fact_projections: pd.DataFrame, dim_player: pd.DataFrame, 
                         season: int, week: int) -> Dict[str, int]:
        """Load projection records."""
        results = {'projections_loaded': 0, 'projections_updated': 0, 'projection_errors': 0}
        
        print("📈 Processing projection records...")
        
        # Create player_sk to nfl_id mapping
        player_mapping = {}
        for _, row in dim_player.iterrows():
            canonical_id = self.player_id_mapper.generate_canonical_id(
                name=row.get('player_name'),
                position=row.get('position'),
                team=row.get('team')
            )
            if canonical_id:
                player_mapping[row['player_sk']] = canonical_id
        
        for _, row in fact_projections.iterrows():
            try:
                player_sk = row.get('player_sk')
                if pd.isna(player_sk) or player_sk not in player_mapping:
                    results['projection_errors'] += 1
                    continue
                    
                canonical_id = player_mapping[player_sk]
                
                # Find the player in our database
                player = self.session.query(Player).filter(
                    Player.nfl_id == canonical_id
                ).first()
                
                if not player:
                    print(f"⚠️ Player not found for canonical_id: {canonical_id}")
                    results['projection_errors'] += 1
                    continue
                
                # Check if projection already exists
                existing_proj = self.session.query(PlayerProjections).filter(
                    PlayerProjections.player_id == player.id,
                    PlayerProjections.week == week,
                    PlayerProjections.season == season,
                    PlayerProjections.source == row.get('source', 'csv_rankings')
                ).first()
                
                if existing_proj:
                    # Update existing projection
                    updated = self._update_projection(existing_proj, row)
                    if updated:
                        results['projections_updated'] += 1
                else:
                    # Create new projection
                    new_proj = self._create_projection(player.id, row, season, week)
                    if new_proj:
                        results['projections_loaded'] += 1
                        
            except Exception as e:
                print(f"❌ Error processing projection: {e}")
                results['projection_errors'] += 1
                continue
                
        return results
    
    def _create_projection(self, player_id: int, row: pd.Series, season: int, week: int) -> Optional[PlayerProjections]:
        """Create a new projection record."""
        try:
            projection = PlayerProjections(
                player_id=player_id,
                week=week,
                season=season,
                projected_points=row.get('proj_points'),
                source=row.get('source', 'csv_rankings'),
                scoring_format='ppr',
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc)
            )
            
            self.session.add(projection)
            return projection
            
        except Exception as e:
            print(f"❌ Error creating projection: {e}")
            return None
    
    def _update_projection(self, projection: PlayerProjections, row: pd.Series) -> bool:
        """Update existing projection with new data."""
        updated = False
        
        # Update projected points if different
        new_points = row.get('proj_points')
        if pd.notna(new_points) and projection.projected_points != new_points:
            projection.projected_points = new_points
            updated = True
            
        if updated:
            projection.updated_at = datetime.now(timezone.utc)
            
        return updated
    
    def validate_data_quality(self, canonical_dir: Path) -> Dict[str, float]:
        """Generate data quality metrics."""
        print("🔍 Validating data quality...")
        
        # Load data quality report from canonical model
        dq_path = canonical_dir / 'data_quality_report.parquet'
        if dq_path.exists():
            dq_report = pd.read_parquet(dq_path)
            metrics = dq_report.set_index('metric')['value'].to_dict()
            
            # Add database-specific metrics
            total_players = self.session.query(Player).count()
            total_projections = self.session.query(PlayerProjections).count()
            
            metrics.update({
                'db_total_players': total_players,
                'db_total_projections': total_projections
            })
            
            return metrics
        else:
            return {}


def main():
    parser = argparse.ArgumentParser(
        description='Load canonical model data into the database'
    )
    parser.add_argument('--canonical-dir', type=Path, default='out',
                       help='Directory containing canonical model outputs')
    parser.add_argument('--season', type=int, required=True,
                       help='NFL season year')
    parser.add_argument('--week', type=int, required=True,
                       help='NFL week number')
    parser.add_argument('--validate-only', action='store_true',
                       help='Only run validation, do not load data')
    
    args = parser.parse_args()
    
    if not args.canonical_dir.exists():
        print(f"❌ Canonical directory not found: {args.canonical_dir}")
        return 1
    
    try:
        with CanonicalDataLoader() as loader:
            if args.validate_only:
                # Just run validation
                metrics = loader.validate_data_quality(args.canonical_dir)
                print("📊 Data Quality Metrics:")
                for metric, value in metrics.items():
                    print(f"  {metric}: {value}")
            else:
                # Load data
                print(f"🚀 Loading canonical data for {args.season} season, week {args.week}...")
                results = loader.load_canonical_data(args.canonical_dir, args.season, args.week)
                
                # Print results
                print("\n" + "="*50)
                print("LOAD RESULTS")
                print("="*50)
                print(f"Players loaded: {results.get('players_loaded', 0)}")
                print(f"Players updated: {results.get('players_updated', 0)}")
                print(f"Projections loaded: {results.get('projections_loaded', 0)}")
                print(f"Projections updated: {results.get('projections_updated', 0)}")
                
                if results.get('player_errors', 0) > 0:
                    print(f"⚠️ Player errors: {results['player_errors']}")
                if results.get('projection_errors', 0) > 0:
                    print(f"⚠️ Projection errors: {results['projection_errors']}")
                
                # Run validation
                metrics = loader.validate_data_quality(args.canonical_dir)
                if metrics:
                    print("\n📊 Data Quality Metrics:")
                    for metric, value in metrics.items():
                        if isinstance(value, float):
                            print(f"  {metric}: {value:.2f}")
                        else:
                            print(f"  {metric}: {value}")
                
        return 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())