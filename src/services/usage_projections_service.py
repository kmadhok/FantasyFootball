import logging
import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass

from src.config.config import get_config
from src.database import SessionLocal, Player, PlayerUsage, PlayerProjections, RosterEntry
from src.services.espn_data_service import ESPNDataService, ESPNPlayerData
from src.services.mfl_projection_service import MFLProjectionService
from src.utils.player_id_mapper import PlayerIDMapper
from src.utils.retry_handler import handle_api_request

logger = logging.getLogger(__name__)

@dataclass
class WaiverCandidateData:
    """Data class for waiver candidate information"""
    player_id: int
    canonical_id: str
    name: str
    position: str
    team: str
    projected_points: Optional[float] = None
    snap_delta: Optional[float] = None
    route_delta: Optional[float] = None
    target_share: Optional[float] = None
    carry_share: Optional[float] = None
    rz_touches: Optional[int] = None
    ez_targets: Optional[int] = None
    trend_slope: Optional[float] = None
    is_rostered: bool = False
    available_on: List[str] = None

class UsageProjectionsService:
    """Service for processing player usage and projections data"""
    
    def __init__(self):
        self.config = get_config()
        self.espn_service = ESPNDataService()
        self.mfl_service = MFLProjectionService()
        self.player_mapper = PlayerIDMapper()
        self.current_season = 2025
        
    def daily_sync_job(self) -> Dict[str, bool]:
        """Daily sync of projections and available usage data"""
        logger.info("Starting daily usage and projections sync")
        
        results = {
            "espn_players_sync": False,
            "projections_sync": False,
            "usage_sync": False
        }
        
        try:
            # Sync ESPN player data first
            results["espn_players_sync"] = self.sync_espn_players()
            
            # Sync projections (prefer MFL; fallback to ESPN if MFL fails)
            proj_ok = self.sync_projections_from_mfl()
            if not proj_ok:
                proj_ok = self.sync_projections_from_espn()
            results["projections_sync"] = proj_ok
            
            # Sync available usage data (mock for now)
            results["usage_sync"] = self.sync_mock_usage_data()
            
            success_count = sum(results.values())
            logger.info(f"Daily sync completed: {success_count}/3 operations successful")
            
            return results
            
        except Exception as e:
            logger.error(f"Daily sync job failed: {e}")
            return results
    
    def sync_espn_players(self) -> bool:
        """Sync ESPN player data to database"""
        try:
            logger.info("Starting ESPN players sync")
            return self.espn_service.sync_espn_players_to_database()
        except Exception as e:
            logger.error(f"ESPN players sync failed: {e}")
            return False
    
    def sync_projections_from_espn(self, week: Optional[int] = None) -> bool:
        """Sync projections from ESPN"""
        try:
            logger.info(f"Starting projections sync for week {week or 'current'}")
            return self.espn_service.sync_projections_to_database(week)
        except Exception as e:
            logger.error(f"Projections sync failed: {e}")
            return False

    def sync_projections_from_mfl(self, week: Optional[int] = None) -> bool:
        """Sync projections from MFL (preferred source given league context)."""
        try:
            logger.info(f"Starting MFL projections sync for week {week or 'current'}")
            return self.mfl_service.sync_projections_to_database(week)
        except Exception as e:
            logger.error(f"MFL projections sync failed: {e}")
            return False
    
    def sync_mock_usage_data(self, week: Optional[int] = None) -> bool:
        """Sync mock usage data (temporary implementation)"""
        try:
            logger.info("Starting mock usage data sync")
            
            if week is None:
                week = self._get_current_nfl_week()
            
            db = SessionLocal()
            try:
                # Get all players that have projections but no usage data
                players = db.query(Player).join(PlayerProjections).filter(
                    PlayerProjections.week == week,
                    PlayerProjections.season == self.current_season
                ).all()
                
                usage_records = 0
                
                for player in players:
                    # Check if usage already exists
                    existing_usage = db.query(PlayerUsage).filter(
                        PlayerUsage.player_id == player.id,
                        PlayerUsage.week == week,
                        PlayerUsage.season == self.current_season
                    ).first()
                    
                    if existing_usage:
                        continue
                    
                    # Create mock usage data based on position
                    usage_data = self._generate_mock_usage_data(player)
                    
                    usage_record = PlayerUsage(
                        player_id=player.id,
                        week=week,
                        season=self.current_season,
                        snap_pct=usage_data.get('snap_pct'),
                        route_pct=usage_data.get('route_pct'),
                        target_share=usage_data.get('target_share'),
                        carry_share=usage_data.get('carry_share'),
                        rz_touches=usage_data.get('rz_touches'),
                        ez_targets=usage_data.get('ez_targets'),
                        targets=usage_data.get('targets'),
                        carries=usage_data.get('carries'),
                        receptions=usage_data.get('receptions'),
                        receiving_yards=usage_data.get('receiving_yards'),
                        rushing_yards=usage_data.get('rushing_yards'),
                        touchdowns=usage_data.get('touchdowns')
                    )
                    
                    db.add(usage_record)
                    usage_records += 1
                
                db.commit()
                logger.info(f"✓ Successfully created {usage_records} mock usage records")
                return True
                
            except Exception as e:
                db.rollback()
                logger.error(f"Database error during usage sync: {e}")
                raise
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Mock usage sync failed: {e}")
            return False
    
    def _generate_mock_usage_data(self, player: Player) -> Dict[str, Any]:
        """Generate mock usage data based on player position and starter status"""
        import random
        
        usage_data = {}
        
        if player.position == 'QB':
            if player.is_starter:
                usage_data = {
                    'snap_pct': random.uniform(0.85, 1.0),
                    'targets': 0,
                    'carries': random.randint(3, 8),
                    'receptions': 0,
                    'receiving_yards': 0,
                    'rushing_yards': random.uniform(15, 45),
                    'touchdowns': random.randint(0, 3)
                }
            else:
                usage_data = {
                    'snap_pct': random.uniform(0.0, 0.15),
                    'targets': 0,
                    'carries': random.randint(0, 2),
                    'receptions': 0,
                    'receiving_yards': 0,
                    'rushing_yards': random.uniform(0, 10),
                    'touchdowns': random.randint(0, 1)
                }
                
        elif player.position in ['RB']:
            if player.is_starter:
                usage_data = {
                    'snap_pct': random.uniform(0.6, 0.85),
                    'carry_share': random.uniform(0.6, 0.8),
                    'target_share': random.uniform(0.05, 0.15),
                    'rz_touches': random.randint(3, 8),
                    'targets': random.randint(2, 6),
                    'carries': random.randint(15, 25),
                    'receptions': random.randint(1, 4),
                    'receiving_yards': random.uniform(10, 35),
                    'rushing_yards': random.uniform(65, 120),
                    'touchdowns': random.randint(0, 2)
                }
            else:
                usage_data = {
                    'snap_pct': random.uniform(0.15, 0.45),
                    'carry_share': random.uniform(0.15, 0.35),
                    'target_share': random.uniform(0.02, 0.08),
                    'rz_touches': random.randint(0, 3),
                    'targets': random.randint(0, 3),
                    'carries': random.randint(3, 12),
                    'receptions': random.randint(0, 2),
                    'receiving_yards': random.uniform(0, 15),
                    'rushing_yards': random.uniform(15, 55),
                    'touchdowns': random.randint(0, 1)
                }
                
        elif player.position in ['WR', 'TE']:
            if player.is_starter:
                route_pct = random.uniform(0.7, 0.9) if player.position == 'WR' else random.uniform(0.6, 0.8)
                usage_data = {
                    'snap_pct': random.uniform(0.65, 0.9),
                    'route_pct': route_pct,
                    'target_share': random.uniform(0.15, 0.25),
                    'ez_targets': random.randint(1, 3),
                    'targets': random.randint(6, 12),
                    'carries': random.randint(0, 1),
                    'receptions': random.randint(4, 8),
                    'receiving_yards': random.uniform(45, 85),
                    'rushing_yards': random.uniform(0, 5),
                    'touchdowns': random.randint(0, 2)
                }
            else:
                usage_data = {
                    'snap_pct': random.uniform(0.25, 0.6),
                    'route_pct': random.uniform(0.3, 0.6),
                    'target_share': random.uniform(0.05, 0.12),
                    'ez_targets': random.randint(0, 1),
                    'targets': random.randint(2, 6),
                    'carries': 0,
                    'receptions': random.randint(1, 4),
                    'receiving_yards': random.uniform(15, 45),
                    'rushing_yards': 0,
                    'touchdowns': random.randint(0, 1)
                }
        else:
            # Default minimal stats
            usage_data = {
                'snap_pct': random.uniform(0.0, 0.3),
                'targets': 0,
                'carries': 0,
                'receptions': 0,
                'receiving_yards': 0,
                'rushing_yards': 0,
                'touchdowns': 0
            }
        
        return usage_data
    
    def calculate_usage_trends(self, player_id: int, weeks: int = 3) -> Dict[str, Any]:
        """Calculate usage trends for a player over specified weeks"""
        try:
            db = SessionLocal()
            try:
                current_week = self._get_current_nfl_week()
                start_week = max(1, current_week - weeks)
                
                usage_records = db.query(PlayerUsage).filter(
                    PlayerUsage.player_id == player_id,
                    PlayerUsage.week.between(start_week, current_week),
                    PlayerUsage.season == self.current_season
                ).order_by(PlayerUsage.week).all()
                
                if len(usage_records) < 2:
                    return {'error': 'Insufficient data for trend calculation'}
                
                # Calculate trends
                weeks_data = [r.week for r in usage_records]
                snap_pcts = [r.snap_pct or 0 for r in usage_records]
                target_shares = [r.target_share or 0 for r in usage_records]
                carry_shares = [r.carry_share or 0 for r in usage_records]
                
                trends = {
                    'snap_delta': self._calculate_trend_slope(weeks_data, snap_pcts),
                    'target_share_trend': self._calculate_trend_slope(weeks_data, target_shares),
                    'carry_share_trend': self._calculate_trend_slope(weeks_data, carry_shares),
                    'weeks_analyzed': len(usage_records),
                    'current_snap_pct': snap_pcts[-1] if snap_pcts else 0,
                    'current_target_share': target_shares[-1] if target_shares else 0
                }
                
                return trends
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to calculate usage trends for player {player_id}: {e}")
            return {'error': str(e)}
    
    def _calculate_trend_slope(self, weeks: List[int], values: List[float]) -> float:
        """Calculate linear trend slope"""
        if len(weeks) < 2 or len(values) < 2:
            return 0.0
        
        try:
            # Simple linear regression slope
            n = len(weeks)
            sum_x = sum(weeks)
            sum_y = sum(values)
            sum_xy = sum(x * y for x, y in zip(weeks, values))
            sum_x2 = sum(x * x for x in weeks)
            
            slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
            return slope
            
        except (ZeroDivisionError, TypeError):
            return 0.0
    
    def build_waiver_candidates_data(self, league_id: str, week: Optional[int] = None) -> List[WaiverCandidateData]:
        """Build waiver candidates data for a specific league and week"""
        try:
            logger.info(f"Building waiver candidates data for league {league_id}, week {week}")
            
            if week is None:
                week = self._get_current_nfl_week()
            
            db = SessionLocal()
            try:
                # Get all players with projections for this week
                players_query = db.query(Player).join(PlayerProjections).filter(
                    PlayerProjections.week == week,
                    PlayerProjections.season == self.current_season
                )
                
                # Exclude players that are rostered in this league
                rostered_player_ids = db.query(RosterEntry.player_id).filter(
                    RosterEntry.league_id == league_id,
                    RosterEntry.is_active == True
                ).distinct()
                
                available_players = players_query.filter(
                    ~Player.id.in_(rostered_player_ids)
                ).all()
                
                candidates = []
                
                for player in available_players:
                    # Get projections
                    projection = db.query(PlayerProjections).filter(
                        PlayerProjections.player_id == player.id,
                        PlayerProjections.week == week,
                        PlayerProjections.season == self.current_season
                    ).first()
                    
                    # Get usage data
                    usage = db.query(PlayerUsage).filter(
                        PlayerUsage.player_id == player.id,
                        PlayerUsage.week == week,
                        PlayerUsage.season == self.current_season
                    ).first()
                    
                    # Calculate trends
                    trends = self.calculate_usage_trends(player.id)
                    
                    # Check roster status across platforms
                    roster_entries = db.query(RosterEntry).filter(
                        RosterEntry.player_id == player.id,
                        RosterEntry.is_active == True
                    ).all()
                    
                    available_platforms = {'sleeper', 'mfl', 'espn'} - {entry.platform for entry in roster_entries}
                    
                    candidate = WaiverCandidateData(
                        player_id=player.id,
                        canonical_id=player.nfl_id,
                        name=player.name,
                        position=player.position,
                        team=player.team,
                        projected_points=projection.projected_points if projection else None,
                        snap_delta=trends.get('snap_delta'),
                        target_share=usage.target_share if usage else None,
                        carry_share=usage.carry_share if usage else None,
                        rz_touches=usage.rz_touches if usage else None,
                        ez_targets=usage.ez_targets if usage else None,
                        trend_slope=trends.get('snap_delta'),
                        is_rostered=len(roster_entries) > 0,
                        available_on=list(available_platforms)
                    )
                    
                    candidates.append(candidate)
                
                logger.info(f"✓ Built {len(candidates)} waiver candidates")
                return candidates
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to build waiver candidates data: {e}")
            return []
    
    def _get_current_nfl_week(self) -> int:
        """Get current NFL week (simplified calculation)"""
        now = datetime.now()
        if now.month >= 9:  # September or later
            return min(max(now.isocalendar()[1] - 35, 1), 18)  # Week 1-18
        else:  # Before September
            return 1
    
    def get_usage_projections_stats(self) -> Dict[str, Any]:
        """Get statistics about usage and projections data"""
        try:
            db = SessionLocal()
            try:
                current_week = self._get_current_nfl_week()
                
                usage_count = db.query(PlayerUsage).filter(
                    PlayerUsage.season == self.current_season
                ).count()
                
                projections_count = db.query(PlayerProjections).filter(
                    PlayerProjections.season == self.current_season
                ).count()
                
                current_week_projections = db.query(PlayerProjections).filter(
                    PlayerProjections.week == current_week,
                    PlayerProjections.season == self.current_season
                ).count()
                
                current_week_usage = db.query(PlayerUsage).filter(
                    PlayerUsage.week == current_week,
                    PlayerUsage.season == self.current_season
                ).count()
                
                return {
                    'total_usage_records': usage_count,
                    'total_projections': projections_count,
                    'current_week': current_week,
                    'current_week_projections': current_week_projections,
                    'current_week_usage': current_week_usage,
                    'last_updated': datetime.utcnow().isoformat()
                }
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to get usage projections stats: {e}")
            return {'error': str(e)}

# Test functions
def test_usage_projections_service():
    """Test the usage projections service"""
    print("Testing Usage Projections Service...")
    print("=" * 60)
    
    service = UsageProjectionsService()
    
    try:
        # Test statistics
        print("\n1. Testing service statistics...")
        stats = service.get_usage_projections_stats()
        if 'error' not in stats:
            print(f"   Total usage records: {stats['total_usage_records']}")
            print(f"   Total projections: {stats['total_projections']}")
            print(f"   Current week: {stats['current_week']}")
            print(f"   Current week projections: {stats['current_week_projections']}")
        else:
            print(f"   Error: {stats['error']}")
        
        # Test daily sync (will fail without ESPN auth but shows structure)
        print("\n2. Testing daily sync job...")
        sync_results = service.daily_sync_job()
        success_count = sum(sync_results.values())
        print(f"   Sync operations completed: {success_count}/3")
        for operation, success in sync_results.items():
            status = "SUCCESS" if success else "FAILED"
            print(f"     {operation}: {status}")
        
        return True
        
    except Exception as e:
        print(f"   ✗ Usage projections service test failed: {e}")
        return False

if __name__ == "__main__":
    test_usage_projections_service()
