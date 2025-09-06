import logging
import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import asdict

from src.config.config import get_config
from src.database import SessionLocal, Player, PlayerUsage, PlayerProjections, RosterEntry
from src.services.usage_projections_service import UsageProjectionsService, WaiverCandidateData

logger = logging.getLogger(__name__)

class WaiverCandidatesBuilder:
    """Builder for waiver candidates materialized view"""
    
    def __init__(self):
        self.config = get_config()
        self.usage_service = UsageProjectionsService()
        self.current_season = 2025
        
    def build_waiver_candidates(self, league_id: str, week: Optional[int] = None) -> pd.DataFrame:
        """
        Build waiver candidates view with calculated fields:
        - snap_delta, route_delta (week-over-week changes)
        - tprr (targets per route run)
        - rz_last2, ez_last2 (red zone/end zone last 2 games)
        - proj_next (next week projection)
        - trend_slope (3-week trend analysis)
        - roster_fit, market_heat, scarcity (league context)
        """
        try:
            logger.info(f"Building waiver candidates for league {league_id}, week {week}")
            
            if week is None:
                week = self._get_current_nfl_week()
            
            # Get raw candidate data
            candidates_data = self.usage_service.build_waiver_candidates_data(league_id, week)
            
            if not candidates_data:
                logger.warning("No waiver candidates data available")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df_data = []
            for candidate in candidates_data:
                # Get additional calculated fields
                additional_fields = self._calculate_additional_fields(candidate, week, league_id)
                
                # Combine all data
                row_data = asdict(candidate)
                row_data.update(additional_fields)
                df_data.append(row_data)
            
            df = pd.DataFrame(df_data)
            
            # Add scoring and ranking
            df = self._add_waiver_scoring(df)
            
            # Sort by waiver score (highest first)
            df = df.sort_values('waiver_score', ascending=False).reset_index(drop=True)
            
            logger.info(f"✓ Built waiver candidates DataFrame with {len(df)} players")
            return df
            
        except Exception as e:
            logger.error(f"Failed to build waiver candidates: {e}")
            return pd.DataFrame()
    
    def _calculate_additional_fields(self, candidate: WaiverCandidateData, week: int, league_id: str) -> Dict[str, Any]:
        """Calculate additional fields for waiver candidate"""
        
        additional = {
            'week': week,
            'league_id': league_id,
            'route_delta': None,  # Will be calculated when we have route data
            'tprr': None,  # Targets per route run
            'rz_last2': None,  # Red zone touches last 2 games
            'ez_last2': None,  # End zone targets last 2 games
            'proj_next': candidate.projected_points,  # Next week projection (same as current for now)
            'roster_fit': self._calculate_roster_fit(candidate, league_id),
            'market_heat': self._calculate_market_heat(candidate, league_id),
            'scarcity': self._calculate_scarcity(candidate)
        }
        
        # Get historical data for more calculations
        try:
            db = SessionLocal()
            try:
                # Get last 2 weeks of usage for RZ/EZ calculations
                historical_usage = db.query(PlayerUsage).filter(
                    PlayerUsage.player_id == candidate.player_id,
                    PlayerUsage.week.between(max(1, week-2), week-1),
                    PlayerUsage.season == self.current_season
                ).all()
                
                if historical_usage:
                    # Calculate RZ touches and EZ targets over last 2 games
                    rz_touches = sum(u.rz_touches or 0 for u in historical_usage)
                    ez_targets = sum(u.ez_targets or 0 for u in historical_usage)
                    
                    additional['rz_last2'] = rz_touches
                    additional['ez_last2'] = ez_targets
                    
                    # Calculate TPRR if we have the data
                    total_targets = sum(u.targets or 0 for u in historical_usage)
                    total_routes = sum((u.route_pct or 0) * 35 for u in historical_usage)  # Estimate routes
                    
                    if total_routes > 0:
                        additional['tprr'] = total_targets / total_routes
                
            finally:
                db.close()
                
        except Exception as e:
            logger.warning(f"Failed to calculate additional fields for {candidate.name}: {e}")
        
        return additional
    
    def _calculate_roster_fit(self, candidate: WaiverCandidateData, league_id: str) -> float:
        """Calculate how well player fits roster needs (0-1 scale)"""
        try:
            db = SessionLocal()
            try:
                # Get user's roster for this league (simplified - assumes single user)
                user_roster = db.query(Player).join(RosterEntry).filter(
                    RosterEntry.league_id == league_id,
                    RosterEntry.is_active == True
                ).all()
                
                position_counts = {}
                for player in user_roster:
                    position_counts[player.position] = position_counts.get(player.position, 0) + 1
                
                # Simple scoring based on position scarcity in user's roster
                position_targets = {'QB': 2, 'RB': 4, 'WR': 5, 'TE': 2, 'K': 1, 'DEF': 1}
                current_count = position_counts.get(candidate.position, 0)
                target_count = position_targets.get(candidate.position, 2)
                
                if current_count < target_count:
                    return min(1.0, (target_count - current_count) / target_count)
                else:
                    return 0.1  # Low fit if position is full
                    
            finally:
                db.close()
                
        except Exception as e:
            logger.warning(f"Failed to calculate roster fit: {e}")
            return 0.5  # Default neutral fit
    
    def _calculate_market_heat(self, candidate: WaiverCandidateData, league_id: str) -> float:
        """Calculate market interest in player (0-1 scale)"""
        try:
            db = SessionLocal()
            try:
                # Count how many teams in league need this position
                teams_in_league = db.query(RosterEntry.user_id).filter(
                    RosterEntry.league_id == league_id,
                    RosterEntry.is_active == True
                ).distinct().count()
                
                teams_with_position = db.query(RosterEntry.user_id).join(Player).filter(
                    RosterEntry.league_id == league_id,
                    RosterEntry.is_active == True,
                    Player.position == candidate.position
                ).distinct().count()
                
                if teams_in_league > 0:
                    position_scarcity = 1.0 - (teams_with_position / teams_in_league)
                    return max(0.1, position_scarcity)
                else:
                    return 0.5
                    
            finally:
                db.close()
                
        except Exception as e:
            logger.warning(f"Failed to calculate market heat: {e}")
            return 0.5
    
    def _calculate_scarcity(self, candidate: WaiverCandidateData) -> float:
        """Calculate position scarcity in free agency (0-1 scale)"""
        try:
            db = SessionLocal()
            try:
                # Count available players at this position
                total_available = db.query(Player).filter(
                    Player.position == candidate.position,
                    ~Player.id.in_(
                        db.query(RosterEntry.player_id).filter(RosterEntry.is_active == True)
                    )
                ).count()
                
                # Rough scarcity calculation (fewer available = higher scarcity)
                if candidate.position in ['QB', 'TE', 'K', 'DEF']:
                    scarcity_threshold = 15  # Fewer available at these positions
                else:
                    scarcity_threshold = 30  # More available RBs/WRs
                
                scarcity = max(0.1, min(1.0, 1.0 - (total_available / scarcity_threshold)))
                return scarcity
                
            finally:
                db.close()
                
        except Exception as e:
            logger.warning(f"Failed to calculate scarcity: {e}")
            return 0.5
    
    def _add_waiver_scoring(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add waiver scoring to the DataFrame"""
        if df.empty:
            return df
        
        # Simple waiver scoring formula (can be enhanced)
        df['waiver_score'] = 0.0
        
        # Base score from projections
        if 'projected_points' in df.columns:
            df['proj_score'] = df['projected_points'].fillna(0) / 20  # Normalize to 0-1
        else:
            df['proj_score'] = 0.0
        
        # Usage score from snap percentage
        if 'snap_delta' in df.columns:
            df['usage_score'] = df['snap_delta'].fillna(0).clip(-1, 1) * 0.5 + 0.5  # Convert to 0-1
        else:
            df['usage_score'] = 0.5
        
        # Trend score
        if 'trend_slope' in df.columns:
            df['trend_score'] = df['trend_slope'].fillna(0).clip(-1, 1) * 0.5 + 0.5
        else:
            df['trend_score'] = 0.5
        
        # Position-based scoring adjustments
        position_multipliers = {'QB': 0.8, 'RB': 1.2, 'WR': 1.0, 'TE': 1.1, 'K': 0.5, 'DEF': 0.6}
        df['position_multiplier'] = df['position'].map(position_multipliers).fillna(1.0)
        
        # Calculate final waiver score
        df['waiver_score'] = (
            0.35 * df['proj_score'] +
            0.25 * df['usage_score'] +
            0.15 * df['trend_score'] +
            0.15 * df['roster_fit'].fillna(0.5) +
            0.10 * df['market_heat'].fillna(0.5)
        ) * df['position_multiplier']
        
        # Add tier classification
        df['tier'] = 'C'
        df.loc[df['waiver_score'] >= 0.7, 'tier'] = 'A'
        df.loc[(df['waiver_score'] >= 0.45) & (df['waiver_score'] < 0.7), 'tier'] = 'B'
        
        return df
    
    def refresh_waiver_candidates_table(self, league_id: str, week: Optional[int] = None) -> bool:
        """Refresh the materialized waiver candidates table"""
        try:
            logger.info(f"Refreshing waiver candidates table for league {league_id}")
            
            # Build the candidates DataFrame
            df = self.build_waiver_candidates(league_id, week)
            
            if df.empty:
                logger.warning("No data to refresh waiver candidates table")
                return False
            
            # For now, just log the results (in production, would write to database table)
            logger.info(f"Waiver candidates summary:")
            logger.info(f"  Total candidates: {len(df)}")
            logger.info(f"  Tier A: {len(df[df['tier'] == 'A'])}")
            logger.info(f"  Tier B: {len(df[df['tier'] == 'B'])}")
            logger.info(f"  Tier C: {len(df[df['tier'] == 'C'])}")
            
            if len(df) > 0:
                top_candidates = df.head(5)
                logger.info("Top 5 candidates:")
                for idx, row in top_candidates.iterrows():
                    logger.info(f"    {row['name']} ({row['position']}, {row['team']}) - "
                              f"Score: {row['waiver_score']:.2f}, Tier: {row['tier']}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to refresh waiver candidates table: {e}")
            return False
    
    def get_waiver_candidates_for_league(self, league_id: str, week: Optional[int] = None, 
                                       tier: Optional[str] = None, limit: int = 20) -> Dict[str, Any]:
        """Get waiver candidates for a specific league"""
        try:
            df = self.build_waiver_candidates(league_id, week)
            
            if df.empty:
                return {
                    'candidates': [],
                    'total_count': 0,
                    'week': week,
                    'league_id': league_id
                }
            
            # Filter by tier if specified
            if tier:
                df = df[df['tier'] == tier.upper()]
            
            # Limit results
            df = df.head(limit)
            
            # Convert to dict format
            candidates = []
            for idx, row in df.iterrows():
                candidate = {
                    'player_id': int(row['player_id']),
                    'name': row['name'],
                    'position': row['position'],
                    'team': row['team'],
                    'projected_points': row.get('projected_points'),
                    'waiver_score': round(row['waiver_score'], 3),
                    'tier': row['tier'],
                    'roster_fit': round(row.get('roster_fit', 0), 3),
                    'market_heat': round(row.get('market_heat', 0), 3),
                    'available_on': row.get('available_on', [])
                }
                candidates.append(candidate)
            
            return {
                'candidates': candidates,
                'total_count': len(candidates),
                'week': week or self._get_current_nfl_week(),
                'league_id': league_id,
                'generated_at': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get waiver candidates for league: {e}")
            return {'error': str(e)}
    
    def _get_current_nfl_week(self) -> int:
        """Get current NFL week"""
        now = datetime.now()
        if now.month >= 9:
            return min(max(now.isocalendar()[1] - 35, 1), 18)
        else:
            return 1

# Test functions
def test_waiver_candidates_builder():
    """Test the waiver candidates builder"""
    print("Testing Waiver Candidates Builder...")
    print("=" * 60)
    
    builder = WaiverCandidatesBuilder()
    test_league_id = "test_league_123"
    
    try:
        # Test building candidates (will be empty without data)
        print("\n1. Testing waiver candidates building...")
        df = builder.build_waiver_candidates(test_league_id, week=1)
        print(f"   Built DataFrame with {len(df)} candidates")
        
        if not df.empty:
            print(f"   Columns: {list(df.columns)}")
            print(f"   Sample data:")
            print(df.head(3).to_string(index=False))
        
        # Test getting candidates for league
        print("\n2. Testing league candidates retrieval...")
        candidates = builder.get_waiver_candidates_for_league(test_league_id, week=1, limit=5)
        
        if 'error' not in candidates:
            print(f"   Total candidates: {candidates['total_count']}")
            print(f"   Week: {candidates['week']}")
            
            if candidates['candidates']:
                print("   Sample candidates:")
                for candidate in candidates['candidates'][:3]:
                    print(f"     {candidate['name']} ({candidate['position']}) - "
                         f"Score: {candidate['waiver_score']}, Tier: {candidate['tier']}")
        else:
            print(f"   Error: {candidates['error']}")
        
        # Test table refresh
        print("\n3. Testing table refresh...")
        success = builder.refresh_waiver_candidates_table(test_league_id, week=1)
        print(f"   Refresh success: {success}")
        
        return True
        
    except Exception as e:
        print(f"   ✗ Waiver candidates builder test failed: {e}")
        return False

if __name__ == "__main__":
    test_waiver_candidates_builder()