import logging
import pandas as pd
import nfl_data_py as nfl
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass

from src.config.config import get_config
from src.database import SessionLocal, Player, PlayerUsage
from src.utils.player_id_mapper import PlayerIDMapper

logger = logging.getLogger(__name__)

@dataclass
class NFLUsageData:
    """Container for NFL usage statistics"""
    player_id: int
    name: str
    position: str
    team: str
    week: int
    season: int
    snap_pct: Optional[float] = None
    targets: Optional[int] = None
    carries: Optional[int] = None
    receptions: Optional[int] = None
    receiving_yards: Optional[float] = None
    rushing_yards: Optional[float] = None
    touchdowns: Optional[int] = None
    target_share: Optional[float] = None
    carry_share: Optional[float] = None
    route_pct: Optional[float] = None
    rz_touches: Optional[int] = None
    ez_targets: Optional[int] = None

class NFLDataService:
    """Service for integrating with nfl-data-py package for real NFL statistics"""
    
    def __init__(self):
        self.config = get_config()
        self.player_mapper = PlayerIDMapper()
        self.current_season = 2025
        
    def fetch_snap_counts(self, years: List[int] = None, weeks: List[int] = None) -> pd.DataFrame:
        """
        Fetch snap count data from NFL
        
        Args:
            years: List of years to fetch (defaults to current season)
            weeks: List of weeks to fetch (optional)
        
        Returns:
            DataFrame with snap count data
        """
        try:
            if years is None:
                years = [self.current_season]
            
            logger.info(f"Fetching snap counts for years: {years}")
            
            # Get snap counts data
            snap_df = nfl.import_snap_counts(years)
            
            if snap_df.empty:
                logger.warning(f"No snap count data available for years: {years}")
                return pd.DataFrame()
            
            # Filter by weeks if specified
            if weeks:
                snap_df = snap_df[snap_df['week'].isin(weeks)]
            
            logger.info(f"✓ Fetched {len(snap_df)} snap count records")
            return snap_df
            
        except Exception as e:
            logger.error(f"Failed to fetch snap counts: {e}")
            return pd.DataFrame()
    
    def fetch_weekly_advanced_stats(self, stat_type: str, years: List[int] = None) -> pd.DataFrame:
        """
        Fetch weekly advanced statistics from Pro Football Reference
        
        Args:
            stat_type: Type of stats ('pass', 'rec', 'rush')
            years: List of years to fetch
        
        Returns:
            DataFrame with advanced weekly stats
        """
        try:
            if years is None:
                years = [self.current_season]
            
            if stat_type not in ['pass', 'rec', 'rush']:
                raise ValueError("stat_type must be 'pass', 'rec', or 'rush'")
            
            logger.info(f"Fetching weekly {stat_type} stats for years: {years}")
            
            # Get weekly advanced stats
            stats_df = nfl.import_weekly_pfr(stat_type, years)
            
            if stats_df.empty:
                logger.warning(f"No {stat_type} stats available for years: {years}")
                return pd.DataFrame()
            
            logger.info(f"✓ Fetched {len(stats_df)} {stat_type} stat records")
            return stats_df
            
        except Exception as e:
            logger.error(f"Failed to fetch {stat_type} stats: {e}")
            return pd.DataFrame()
    
    def build_usage_data_from_nfl_stats(self, week: int, season: int = None) -> List[NFLUsageData]:
        """
        Build comprehensive usage data by combining snap counts and advanced stats
        
        Args:
            week: NFL week to process
            season: NFL season (defaults to current)
        
        Returns:
            List of NFLUsageData objects
        """
        try:
            if season is None:
                season = self.current_season
            
            logger.info(f"Building usage data for week {week}, season {season}")
            
            # Fetch snap counts
            snap_df = self.fetch_snap_counts([season], [week])
            
            # Fetch receiving stats
            rec_df = self.fetch_weekly_advanced_stats('rec', [season])
            if not rec_df.empty:
                rec_df = rec_df[rec_df['week'] == week]
            
            # Fetch rushing stats  
            rush_df = self.fetch_weekly_advanced_stats('rush', [season])
            if not rush_df.empty:
                rush_df = rush_df[rush_df['week'] == week]
            
            if snap_df.empty:
                logger.warning(f"No snap count data available for week {week}")
                return []
            
            usage_data = []
            
            # Process each player in snap counts
            for _, snap_row in snap_df.iterrows():
                try:
                    # Get player info
                    player_name = snap_row.get('player', '')
                    position = snap_row.get('position', '')
                    team = snap_row.get('team', '')
                    
                    if not all([player_name, position, team]):
                        continue
                    
                    # Generate canonical player ID
                    canonical_id = self.player_mapper.generate_canonical_id(
                        player_name, position, team
                    )
                    
                    # Get or create player in database
                    player_id = self._get_or_create_player(
                        canonical_id, player_name, position, team
                    )
                    
                    if not player_id:
                        continue
                    
                    # Calculate snap percentage
                    offense_snaps = snap_row.get('offense_snaps', 0)
                    offense_pct = snap_row.get('offense_pct', 0.0)
                    
                    # Find matching receiving stats
                    rec_stats = None
                    if not rec_df.empty:
                        rec_match = rec_df[
                            (rec_df['pfr_player_name'] == player_name) |
                            (rec_df['team'] == team)  # Match by team as fallback
                        ]
                        if not rec_match.empty:
                            rec_stats = rec_match.iloc[0]
                    
                    # Find matching rushing stats
                    rush_stats = None  
                    if not rush_df.empty:
                        rush_match = rush_df[
                            (rush_df['pfr_player_name'] == player_name) |
                            (rush_df['team'] == team)  # Match by team as fallback
                        ]
                        if not rush_match.empty:
                            rush_stats = rush_match.iloc[0]
                    
                    # Build usage data
                    usage = NFLUsageData(
                        player_id=player_id,
                        name=player_name,
                        position=position,
                        team=team,
                        week=week,
                        season=season,
                        snap_pct=offense_pct / 100.0 if offense_pct else None,
                        targets=None,  # Not available in this dataset
                        carries=rush_stats.get('carries') if rush_stats is not None else None,
                        receptions=None,  # Not available in this dataset
                        receiving_yards=None,  # Not available in this dataset 
                        rushing_yards=None,  # Not available in this dataset
                        touchdowns=self._calculate_total_tds(rec_stats, rush_stats)
                    )
                    
                    # Calculate derived metrics
                    usage = self._calculate_usage_metrics(usage, snap_row, rec_stats, rush_stats)
                    
                    usage_data.append(usage)
                    
                except Exception as e:
                    logger.warning(f"Failed to process player {snap_row.get('player', 'unknown')}: {e}")
                    continue
            
            logger.info(f"✓ Built usage data for {len(usage_data)} players")
            return usage_data
            
        except Exception as e:
            logger.error(f"Failed to build usage data: {e}")
            return []
    
    def sync_usage_to_database(self, usage_data: List[NFLUsageData]) -> bool:
        """
        Sync usage data to the database
        
        Args:
            usage_data: List of usage data to sync
        
        Returns:
            Success boolean
        """
        try:
            if not usage_data:
                logger.warning("No usage data to sync")
                return False
            
            db = SessionLocal()
            synced_count = 0
            
            try:
                for usage in usage_data:
                    # Check if usage record already exists
                    existing = db.query(PlayerUsage).filter(
                        PlayerUsage.player_id == usage.player_id,
                        PlayerUsage.week == usage.week,
                        PlayerUsage.season == usage.season
                    ).first()
                    
                    if existing:
                        # Update existing record
                        existing.snap_pct = usage.snap_pct
                        existing.targets = usage.targets
                        existing.carries = usage.carries
                        existing.receptions = usage.receptions
                        existing.receiving_yards = usage.receiving_yards
                        existing.rushing_yards = usage.rushing_yards
                        existing.touchdowns = usage.touchdowns
                        existing.target_share = usage.target_share
                        existing.carry_share = usage.carry_share
                        existing.route_pct = usage.route_pct
                        existing.rz_touches = usage.rz_touches
                        existing.ez_targets = usage.ez_targets
                        existing.updated_at = datetime.utcnow()
                    else:
                        # Create new record
                        new_usage = PlayerUsage(
                            player_id=usage.player_id,
                            week=usage.week,
                            season=usage.season,
                            snap_pct=usage.snap_pct,
                            targets=usage.targets,
                            carries=usage.carries,
                            receptions=usage.receptions,
                            receiving_yards=usage.receiving_yards,
                            rushing_yards=usage.rushing_yards,
                            touchdowns=usage.touchdowns,
                            target_share=usage.target_share,
                            carry_share=usage.carry_share,
                            route_pct=usage.route_pct,
                            rz_touches=usage.rz_touches,
                            ez_targets=usage.ez_targets
                        )
                        db.add(new_usage)
                    
                    synced_count += 1
                
                db.commit()
                logger.info(f"✓ Synced {synced_count} usage records to database")
                return True
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to sync usage data to database: {e}")
            return False
    
    def daily_sync_job(self, week: Optional[int] = None, season: Optional[int] = None) -> Dict[str, Any]:
        """
        Daily sync job to update NFL usage statistics
        
        Args:
            week: Specific week to sync (defaults to current NFL week)
            season: Specific season to sync (defaults to current season)
        
        Returns:
            Dictionary with sync results
        """
        try:
            if season is None:
                season = self.current_season
                
            if week is None:
                week = self._get_current_nfl_week()
            
            logger.info(f"Starting daily NFL data sync for week {week}, season {season}")
            
            # Build usage data from NFL stats
            usage_data = self.build_usage_data_from_nfl_stats(week, season)
            
            if not usage_data:
                return {
                    'nfl_usage_sync': False,
                    'error': 'No usage data available',
                    'week': week,
                    'season': season
                }
            
            # Sync to database
            sync_success = self.sync_usage_to_database(usage_data)
            
            return {
                'nfl_usage_sync': sync_success,
                'records_processed': len(usage_data),
                'week': week,
                'season': season
            }
            
        except Exception as e:
            logger.error(f"Daily NFL sync job failed: {e}")
            return {
                'nfl_usage_sync': False,
                'error': str(e)
            }
    
    def _get_or_create_player(self, canonical_id: str, name: str, position: str, team: str) -> Optional[int]:
        """Get or create player in database and return player_id"""
        try:
            db = SessionLocal()
            try:
                # Look for existing player
                player = db.query(Player).filter(Player.nfl_id == canonical_id).first()
                
                if player:
                    return player.id
                
                # Create new player
                new_player = Player(
                    nfl_id=canonical_id,
                    name=name,
                    position=position,
                    team=team,
                    is_starter=position in ['QB', 'RB', 'WR', 'TE']  # Simple starter detection
                )
                
                db.add(new_player)
                db.commit()
                
                return new_player.id
                
            finally:
                db.close()
                
        except Exception as e:
            logger.warning(f"Failed to get/create player {name}: {e}")
            return None
    
    def _calculate_usage_metrics(self, usage: NFLUsageData, snap_row: Any, rec_stats: Any, rush_stats: Any) -> NFLUsageData:
        """Calculate derived usage metrics"""
        try:
            # Estimate target share (simplified - would need team totals for accuracy)
            if usage.targets and usage.targets > 0:
                usage.target_share = min(1.0, usage.targets / 35.0)  # Rough estimate
            
            # Estimate carry share (simplified)
            if usage.carries and usage.carries > 0:
                usage.carry_share = min(1.0, usage.carries / 25.0)  # Rough estimate
            
            # Estimate route percentage (based on position and snap percentage)
            if usage.position in ['WR', 'TE'] and usage.snap_pct:
                usage.route_pct = usage.snap_pct * 0.85  # Approximate route running rate
            
            return usage
            
        except Exception as e:
            logger.warning(f"Failed to calculate usage metrics for {usage.name}: {e}")
            return usage
    
    def _calculate_total_tds(self, rec_stats: Any, rush_stats: Any) -> Optional[int]:
        """Calculate total touchdowns from receiving and rushing stats"""
        # The current PFR weekly data doesn't include basic stats like TDs
        # This data is more about advanced metrics (broken tackles, etc.)
        # TDs would need to come from a different nfl-data-py function
        return None
    
    def _get_current_nfl_week(self) -> int:
        """Get current NFL week based on date"""
        now = datetime.now()
        if now.month >= 9:
            return min(max(now.isocalendar()[1] - 35, 1), 18)
        else:
            return 1

# Test function
def test_nfl_data_service():
    """Test the NFL data service"""
    print("Testing NFL Data Service...")
    print("=" * 60)
    
    service = NFLDataService()
    
    try:
        # Test snap counts fetch
        print("\n1. Testing snap counts fetch...")
        snap_df = service.fetch_snap_counts([2024], [1])  # Use 2024 data for testing
        print(f"   Fetched {len(snap_df)} snap count records")
        
        if not snap_df.empty:
            print(f"   Columns: {list(snap_df.columns)}")
            print(f"   Sample data:\n{snap_df.head(3)}")
        
        # Test advanced stats fetch
        print("\n2. Testing receiving stats fetch...")
        rec_df = service.fetch_weekly_advanced_stats('rec', [2024])
        print(f"   Fetched {len(rec_df)} receiving stat records")
        
        # Test building usage data
        print("\n3. Testing usage data building...")
        usage_data = service.build_usage_data_from_nfl_stats(1, 2024)  # Week 1, 2024
        print(f"   Built usage data for {len(usage_data)} players")
        
        if usage_data:
            sample = usage_data[0]
            print(f"   Sample usage: {sample.name} ({sample.position}, {sample.team})")
            print(f"     Snap %: {sample.snap_pct}")
            print(f"     Targets: {sample.targets}")
            print(f"     Carries: {sample.carries}")
        
        # Test database sync (with small subset)
        if usage_data:
            print("\n4. Testing database sync...")
            success = service.sync_usage_to_database(usage_data[:5])  # Sync first 5 players
            print(f"   Sync success: {success}")
        
        return True
        
    except Exception as e:
        print(f"   ✗ NFL data service test failed: {e}")
        return False

if __name__ == "__main__":
    test_nfl_data_service()