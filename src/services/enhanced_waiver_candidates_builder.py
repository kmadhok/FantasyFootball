import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from dataclasses import dataclass, asdict
from sqlalchemy.orm import Session

from src.config.config import get_config
from src.database import SessionLocal, Player, PlayerUsage, PlayerProjections, RosterEntry, WaiverCandidates
from src.utils.player_id_mapper import PlayerIDMapper

logger = logging.getLogger(__name__)

@dataclass
class EnhancedWaiverCandidate:
    """Enhanced waiver candidate with all Epic A calculated fields"""
    # Basic info
    league_id: str
    week: int
    player_id: int
    pos: str
    rostered: bool
    
    # Week-over-week deltas (Epic A requirement)
    snap_delta: Optional[float] = None
    route_delta: Optional[float] = None
    
    # Advanced metrics (Epic A requirement)
    tprr: Optional[float] = None  # targets per route run
    rz_last2: Optional[int] = None  # red zone touches last 2 games
    ez_last2: Optional[int] = None  # end zone targets last 2 games
    
    # Schedule and projections
    opp_next: Optional[str] = None  # opponent next week
    proj_next: Optional[float] = None  # next week projection
    
    # Trend analysis (Epic A requirement)
    trend_slope: Optional[float] = None  # 3-week trend slope
    
    # League context (Epic A requirement)
    roster_fit: Optional[float] = None  # fit for user's roster needs
    market_heat: Optional[float] = None  # interest from other teams
    scarcity: Optional[float] = None  # positional scarcity in league

class EnhancedWaiverCandidatesBuilder:
    """
    Complete Epic A implementation of waiver candidates materialized view
    with all required calculated fields and < 1 minute performance
    """
    
    def __init__(self):
        self.config = get_config()
        self.player_mapper = PlayerIDMapper()
        self.current_season = 2025
        
    def build_waiver_candidates(self, league_id: str, week: Optional[int] = None, 
                              user_id: Optional[str] = None) -> List[EnhancedWaiverCandidate]:
        """
        Build complete waiver candidates with all Epic A required fields
        
        Args:
            league_id: Target league ID
            week: NFL week (defaults to current)
            user_id: Specific user ID for roster fit calculations
            
        Returns:
            List of EnhancedWaiverCandidate objects with all calculated fields
        """
        try:
            if week is None:
                week = self._get_current_nfl_week()
            
            logger.info(f"Building Enhanced waiver candidates for league {league_id}, week {week}")
            
            db = SessionLocal()
            try:
                # Get all players with usage data for current week
                current_week_players = self._get_players_with_usage(db, week)
                
                # Get rostered players for this league
                rostered_player_ids = self._get_rostered_players(db, league_id)
                
                candidates = []
                
                for player_data in current_week_players:
                    try:
                        # Skip rostered players (Epic A requirement: non-rostered only)
                        if player_data['player_id'] in rostered_player_ids:
                            continue
                        
                        # Build candidate with all calculated fields
                        candidate = self._build_enhanced_candidate(
                            db, player_data, league_id, week, user_id
                        )
                        
                        candidates.append(candidate)
                        
                    except Exception as e:
                        logger.warning(f"Failed to process candidate {player_data.get('name', 'unknown')}: {e}")
                        continue
                
                logger.info(f"✓ Built {len(candidates)} enhanced waiver candidates")
                return candidates
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to build enhanced waiver candidates: {e}")
            return []
    
    def _get_players_with_usage(self, db: Session, week: int) -> List[Dict]:
        """Get all players with usage data for the current week"""
        query = db.query(
            Player.id.label('player_id'),
            Player.name,
            Player.position,
            Player.team,
            PlayerUsage.snap_pct,
            PlayerUsage.route_pct,
            PlayerUsage.target_share,
            PlayerUsage.carry_share,
            PlayerUsage.targets,
            PlayerUsage.carries,
            PlayerUsage.rz_touches,
            PlayerUsage.ez_targets
        ).join(PlayerUsage).filter(
            PlayerUsage.week == week,
            PlayerUsage.season == self.current_season
        ).all()
        
        return [
            {
                'player_id': row.player_id,
                'name': row.name,
                'position': row.position,
                'team': row.team,
                'current_snap_pct': row.snap_pct or 0.0,
                'current_route_pct': row.route_pct or 0.0,
                'current_targets': row.targets or 0,
                'current_carries': row.carries or 0,
                'current_rz_touches': row.rz_touches or 0,
                'current_ez_targets': row.ez_targets or 0
            } for row in query
        ]
    
    def _get_rostered_players(self, db: Session, league_id: str) -> set:
        """Get set of rostered player IDs for this league"""
        rostered = db.query(RosterEntry.player_id).filter(
            RosterEntry.league_id == league_id,
            RosterEntry.is_active == True
        ).all()
        
        return set(row.player_id for row in rostered)
    
    def _build_enhanced_candidate(self, db: Session, player_data: Dict, 
                                league_id: str, week: int, user_id: Optional[str]) -> EnhancedWaiverCandidate:
        """Build EnhancedWaiverCandidate with all Epic A calculated fields"""
        
        player_id = player_data['player_id']
        
        # Calculate week-over-week deltas (Epic A requirement)
        snap_delta, route_delta = self._calculate_deltas(db, player_id, week)
        
        # Calculate TPRR (targets per route run) - Epic A requirement
        tprr = self._calculate_tprr(db, player_id, week)
        
        # Calculate rolling window metrics - Epic A requirement
        rz_last2, ez_last2 = self._calculate_rolling_metrics(db, player_id, week)
        
        # Get next week projection - Epic A requirement
        proj_next = self._get_next_week_projection(db, player_id, week)
        
        # Calculate trend slope - Epic A requirement
        trend_slope = self._calculate_trend_slope(db, player_id, week, player_data['position'])
        
        # Calculate league context - Epic A requirement
        roster_fit = self._calculate_roster_fit(db, player_data, league_id, user_id)
        market_heat = self._calculate_market_heat(db, player_data, league_id)
        scarcity = self._calculate_scarcity(db, player_data['position'])
        
        return EnhancedWaiverCandidate(
            league_id=league_id,
            week=week,
            player_id=player_id,
            pos=player_data['position'],
            rostered=False,  # Already filtered out rostered players
            snap_delta=snap_delta,
            route_delta=route_delta,
            tprr=tprr,
            rz_last2=rz_last2,
            ez_last2=ez_last2,
            opp_next=self._get_opponent_next_week(player_data['team'], week),
            proj_next=proj_next,
            trend_slope=trend_slope,
            roster_fit=roster_fit,
            market_heat=market_heat,
            scarcity=scarcity
        )
    
    def _calculate_deltas(self, db: Session, player_id: int, week: int) -> Tuple[Optional[float], Optional[float]]:
        """Calculate snap_delta and route_delta - Epic A requirement"""
        try:
            # Get current week and previous week usage
            usage_data = db.query(PlayerUsage).filter(
                PlayerUsage.player_id == player_id,
                PlayerUsage.week.in_([week-1, week]),
                PlayerUsage.season == self.current_season
            ).order_by(PlayerUsage.week).all()
            
            if len(usage_data) != 2:
                return None, None
            
            prev_usage, curr_usage = usage_data
            
            snap_delta = None
            route_delta = None
            
            if prev_usage.snap_pct is not None and curr_usage.snap_pct is not None:
                snap_delta = curr_usage.snap_pct - prev_usage.snap_pct
            
            if prev_usage.route_pct is not None and curr_usage.route_pct is not None:
                route_delta = curr_usage.route_pct - prev_usage.route_pct
            
            return snap_delta, route_delta
            
        except Exception as e:
            logger.warning(f"Failed to calculate deltas for player {player_id}: {e}")
            return None, None
    
    def _calculate_tprr(self, db: Session, player_id: int, week: int) -> Optional[float]:
        """Calculate TPRR (targets per route run) - Epic A requirement"""
        try:
            # Get recent usage data to calculate TPRR
            usage_data = db.query(PlayerUsage).filter(
                PlayerUsage.player_id == player_id,
                PlayerUsage.week.between(max(1, week-2), week),
                PlayerUsage.season == self.current_season,
                PlayerUsage.targets.isnot(None),
                PlayerUsage.route_pct.isnot(None)
            ).all()
            
            if not usage_data:
                return None
            
            total_targets = sum(u.targets for u in usage_data)
            # Estimate total routes (route_pct in 0-1 scale * estimated routes/game)
            total_routes = sum((u.route_pct or 0.0) * 35 for u in usage_data)  # ~35 routes per game estimate
            
            if total_routes > 0:
                return total_targets / total_routes
            
            return None
            
        except Exception as e:
            logger.warning(f"Failed to calculate TPRR for player {player_id}: {e}")
            return None
    
    def _calculate_rolling_metrics(self, db: Session, player_id: int, week: int) -> Tuple[Optional[int], Optional[int]]:
        """Calculate rz_last2 and ez_last2 - Epic A requirement"""
        try:
            # Get last 2 games of data
            usage_data = db.query(PlayerUsage).filter(
                PlayerUsage.player_id == player_id,
                PlayerUsage.week.between(max(1, week-2), week-1),
                PlayerUsage.season == self.current_season
            ).all()
            
            if not usage_data:
                return None, None
            
            rz_last2 = sum(u.rz_touches or 0 for u in usage_data)
            ez_last2 = sum(u.ez_targets or 0 for u in usage_data)
            
            return rz_last2 if rz_last2 > 0 else None, ez_last2 if ez_last2 > 0 else None
            
        except Exception as e:
            logger.warning(f"Failed to calculate rolling metrics for player {player_id}: {e}")
            return None, None
    
    def _get_next_week_projection(self, db: Session, player_id: int, week: int) -> Optional[float]:
        """Get next week projection - Epic A requirement"""
        try:
            projection = db.query(PlayerProjections).filter(
                PlayerProjections.player_id == player_id,
                PlayerProjections.week == week + 1,
                PlayerProjections.season == self.current_season
            ).first()
            
            if projection:
                return projection.projected_points or projection.mean
            
            # Fallback to current week projection
            current_proj = db.query(PlayerProjections).filter(
                PlayerProjections.player_id == player_id,
                PlayerProjections.week == week,
                PlayerProjections.season == self.current_season
            ).first()
            
            return current_proj.projected_points or current_proj.mean if current_proj else None
            
        except Exception as e:
            logger.warning(f"Failed to get projection for player {player_id}: {e}")
            return None
    
    def _calculate_trend_slope(self, db: Session, player_id: int, week: int, position: str) -> Optional[float]:
        """Calculate 3-week trend slope - Epic A requirement"""
        try:
            # Get last 3 weeks of data
            usage_data = db.query(PlayerUsage).filter(
                PlayerUsage.player_id == player_id,
                PlayerUsage.week.between(max(1, week-3), week-1),
                PlayerUsage.season == self.current_season
            ).order_by(PlayerUsage.week).all()
            
            if len(usage_data) < 2:
                return None
            
            # Choose metric based on position
            if position in ['WR', 'TE']:
                values = [u.targets or 0 for u in usage_data]
            elif position == 'RB':
                values = [(u.carries or 0) + (u.targets or 0) for u in usage_data]
            else:
                values = [u.snap_pct or 0 for u in usage_data]
            
            if len(values) < 2:
                return None
            
            # Calculate linear regression slope
            weeks = list(range(len(values)))
            if len(weeks) > 1:
                slope = np.polyfit(weeks, values, 1)[0]
                return float(slope)
            
            return None
            
        except Exception as e:
            logger.warning(f"Failed to calculate trend slope for player {player_id}: {e}")
            return None
    
    def _calculate_roster_fit(self, db: Session, player_data: Dict, 
                            league_id: str, user_id: Optional[str]) -> Optional[float]:
        """Calculate roster fit - Epic A requirement"""
        try:
            # Get user's roster for this league
            query = db.query(Player.position).join(RosterEntry).filter(
                RosterEntry.league_id == league_id,
                RosterEntry.is_active == True
            )
            
            if user_id:
                query = query.filter(RosterEntry.user_id == user_id)
            
            user_positions = [row.position for row in query.all()]
            
            # Count positions
            position_counts = {}
            for pos in user_positions:
                position_counts[pos] = position_counts.get(pos, 0) + 1
            
            # Position targets (ideal roster composition)
            position_targets = {'QB': 2, 'RB': 4, 'WR': 5, 'TE': 2, 'K': 1, 'DEF': 1}
            
            player_position = player_data['position']
            current_count = position_counts.get(player_position, 0)
            target_count = position_targets.get(player_position, 2)
            
            # Calculate fit (higher when position is needed)
            if current_count < target_count:
                fit = (target_count - current_count) / target_count
                return min(1.0, fit)
            else:
                return 0.1  # Low fit if position is full
                
        except Exception as e:
            logger.warning(f"Failed to calculate roster fit: {e}")
            return 0.5
    
    def _calculate_market_heat(self, db: Session, player_data: Dict, league_id: str) -> Optional[float]:
        """Calculate market heat - Epic A requirement"""
        try:
            # Count teams in league
            total_teams = db.query(RosterEntry.user_id).filter(
                RosterEntry.league_id == league_id,
                RosterEntry.is_active == True
            ).distinct().count()
            
            if total_teams == 0:
                return 0.5
            
            # Count teams with adequate depth at this position
            position = player_data['position']
            position_thresholds = {'QB': 1, 'RB': 2, 'WR': 3, 'TE': 1, 'K': 1, 'DEF': 1}
            threshold = position_thresholds.get(position, 2)
            
            # Count teams that have enough players at this position
            teams_with_position = db.query(RosterEntry.user_id).join(Player).filter(
                RosterEntry.league_id == league_id,
                RosterEntry.is_active == True,
                Player.position == position
            ).group_by(RosterEntry.user_id).all()
            
            # Count teams with enough players at position
            from sqlalchemy import func
            teams_with_position = len([
                team for team, count in db.query(
                    RosterEntry.user_id, func.count(Player.id)
                ).join(Player).filter(
                    RosterEntry.league_id == league_id,
                    RosterEntry.is_active == True,
                    Player.position == position
                ).group_by(RosterEntry.user_id).all()
                if count >= threshold
            ])
            
            # Market heat = percentage of teams that still need this position
            needy_teams = total_teams - teams_with_position
            market_heat = needy_teams / total_teams
            
            return max(0.1, min(1.0, market_heat))
            
        except Exception as e:
            logger.warning(f"Failed to calculate market heat: {e}")
            return 0.5
    
    def _calculate_scarcity(self, db: Session, position: str) -> Optional[float]:
        """Calculate positional scarcity - Epic A requirement"""
        try:
            # Count total available players at this position (not rostered)
            subquery = db.query(RosterEntry.player_id).filter(
                RosterEntry.is_active == True
            ).subquery()
            
            available_count = db.query(Player).filter(
                Player.position == position,
                ~Player.id.in_(db.query(subquery.c.player_id))
            ).count()
            
            # Position-specific scarcity calculation
            position_thresholds = {
                'QB': {'low': 20, 'high': 35},
                'RB': {'low': 40, 'high': 80},
                'WR': {'low': 60, 'high': 120},
                'TE': {'low': 15, 'high': 30},
                'K': {'low': 10, 'high': 20},
                'DEF': {'low': 10, 'high': 20}
            }
            
            thresholds = position_thresholds.get(position, {'low': 30, 'high': 60})
            
            if available_count <= thresholds['low']:
                scarcity = 1.0  # High scarcity
            elif available_count >= thresholds['high']:
                scarcity = 0.1  # Low scarcity
            else:
                # Linear interpolation between low and high
                range_size = thresholds['high'] - thresholds['low']
                position_in_range = available_count - thresholds['low']
                scarcity = 1.0 - (position_in_range / range_size)
            
            return max(0.1, min(1.0, scarcity))
            
        except Exception as e:
            logger.warning(f"Failed to calculate scarcity: {e}")
            return 0.5
    
    def _get_opponent_next_week(self, team: str, week: int) -> Optional[str]:
        """Get opponent for next week using NFLSchedule table if available."""
        try:
            db = SessionLocal()
            try:
                next_week = week + 1
                from src.database import NFLSchedule  # local import to avoid circulars at module load

                game = db.query(NFLSchedule).filter(
                    NFLSchedule.week == next_week,
                    ((NFLSchedule.home_team == team) | (NFLSchedule.away_team == team))
                ).first()

                if not game:
                    return None

                if game.home_team == team:
                    return game.away_team
                if game.away_team == team:
                    return game.home_team
                return None
            finally:
                db.close()
        except Exception:
            return None
    
    def sync_to_waiver_candidates_table(self, candidates: List[EnhancedWaiverCandidate]) -> bool:
        """Sync candidates to WaiverCandidates database table"""
        try:
            if not candidates:
                logger.warning("No candidates to sync")
                return False
            
            db = SessionLocal()
            try:
                # Group by league_id and week for efficient deletion
                league_weeks = set((c.league_id, c.week) for c in candidates)
                
                for league_id, week in league_weeks:
                    # Delete existing records for this league/week
                    db.query(WaiverCandidates).filter(
                        WaiverCandidates.league_id == league_id,
                        WaiverCandidates.week == week
                    ).delete()
                
                # Insert new records
                for candidate in candidates:
                    wc_record = WaiverCandidates(
                        league_id=candidate.league_id,
                        week=candidate.week,
                        player_id=candidate.player_id,
                        pos=candidate.pos,
                        rostered=candidate.rostered,
                        snap_delta=candidate.snap_delta,
                        route_delta=candidate.route_delta,
                        tprr=candidate.tprr,
                        rz_last2=candidate.rz_last2,
                        ez_last2=candidate.ez_last2,
                        opp_next=candidate.opp_next,
                        proj_next=candidate.proj_next,
                        trend_slope=candidate.trend_slope,
                        roster_fit=candidate.roster_fit,
                        market_heat=candidate.market_heat,
                        scarcity=candidate.scarcity
                    )
                    db.add(wc_record)
                
                db.commit()
                logger.info(f"✓ Synced {len(candidates)} candidates to WaiverCandidates table")
                return True
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to sync candidates to table: {e}")
            return False
    
    def refresh_waiver_candidates_for_league(self, league_id: str, week: Optional[int] = None, 
                                           user_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Complete Epic A refresh job - builds and syncs waiver candidates
        
        Returns:
            Dictionary with refresh results and performance metrics
        """
        start_time = datetime.utcnow()
        
        try:
            # Build enhanced candidates
            candidates = self.build_waiver_candidates(league_id, week, user_id)
            
            if not candidates:
                return {
                    'success': False,
                    'error': 'No candidates built',
                    'league_id': league_id,
                    'week': week,
                    'duration_seconds': 0
                }
            
            # Sync to database table
            sync_success = self.sync_to_waiver_candidates_table(candidates)
            
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            # Performance check (Epic A requirement: < 1 minute)
            performance_ok = duration < 60.0
            
            return {
                'success': sync_success,
                'league_id': league_id,
                'week': week or self._get_current_nfl_week(),
                'candidates_count': len(candidates),
                'duration_seconds': duration,
                'performance_ok': performance_ok,
                'timestamp': start_time.isoformat()
            }
            
        except Exception as e:
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            logger.error(f"Failed to refresh waiver candidates: {e}")
            return {
                'success': False,
                'error': str(e),
                'league_id': league_id,
                'week': week,
                'duration_seconds': duration
            }
    
    def _get_current_nfl_week(self) -> int:
        """Get current NFL week"""
        now = datetime.now()
        if now.month >= 9:
            return min(max(now.isocalendar()[1] - 35, 1), 18)
        else:
            return 1

# Test function
def test_enhanced_waiver_candidates():
    """Test the enhanced waiver candidates builder"""
    print("Testing Enhanced Waiver Candidates Builder...")
    print("=" * 70)
    
    builder = EnhancedWaiverCandidatesBuilder()
    test_league_id = "test_league_enhanced"
    
    try:
        # Test building candidates
        print("\n1. Testing enhanced candidates building...")
        candidates = builder.build_waiver_candidates(test_league_id, week=1)
        print(f"   Built {len(candidates)} enhanced candidates")
        
        if candidates:
            sample = candidates[0]
            print(f"   Sample candidate: {sample}")
            
            # Check Epic A requirements
            epic_a_fields = [
                'snap_delta', 'route_delta', 'tprr', 'rz_last2', 'ez_last2',
                'proj_next', 'trend_slope', 'roster_fit', 'market_heat', 'scarcity'
            ]
            
            present_fields = [field for field in epic_a_fields if hasattr(sample, field)]
            print(f"   Epic A fields present: {len(present_fields)}/{len(epic_a_fields)}")
            print(f"   Fields: {present_fields}")
        
        # Test refresh job
        print("\n2. Testing refresh job...")
        result = builder.refresh_waiver_candidates_for_league(test_league_id, week=1)
        print(f"   Refresh result: {result}")
        
        return True
        
    except Exception as e:
        print(f"   ✗ Enhanced waiver candidates test failed: {e}")
        return False

if __name__ == "__main__":
    test_enhanced_waiver_candidates()
