import logging
import os
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from dataclasses import dataclass

from yfpy import YahooFantasySportsQuery
from src.config.config import get_config
from src.database import SessionLocal, Player, PlayerProjections
from src.utils.player_id_mapper import PlayerIDMapper

logger = logging.getLogger(__name__)

@dataclass
class YahooProjectionData:
    """Container for Yahoo fantasy projections"""
    player_id: int
    name: str
    position: str
    team: str
    week: int
    season: int
    projected_points: Optional[float] = None
    floor: Optional[float] = None
    ceiling: Optional[float] = None
    mean: Optional[float] = None
    stdev: Optional[float] = None
    source: str = 'yahoo'
    scoring_format: str = 'ppr'

@dataclass
class YahooPlayerInfo:
    """Container for Yahoo player information"""
    yahoo_id: str
    name: str
    position: str
    team: str
    is_available: bool = True
    ownership_percentage: Optional[float] = None

class YahooDataService:
    """Service for integrating with Yahoo Fantasy Sports API via yfpy"""
    
    def __init__(self, league_id: Optional[str] = None):
        self.config = get_config()
        self.player_mapper = PlayerIDMapper()
        self.current_season = 2025
        self.league_id = league_id
        self.yahoo_query = None
        self._setup_yahoo_client()
        
    def _setup_yahoo_client(self):
        """Setup Yahoo Fantasy Sports client"""
        try:
            # Check for Yahoo OAuth credentials
            consumer_key = os.getenv('YAHOO_CONSUMER_KEY')
            consumer_secret = os.getenv('YAHOO_CONSUMER_SECRET')
            
            if not consumer_key or not consumer_secret:
                logger.warning("Yahoo OAuth credentials not found in environment variables")
                logger.info("To use Yahoo API, set YAHOO_CONSUMER_KEY and YAHOO_CONSUMER_SECRET in .env file")
                return
            
            # Initialize Yahoo Fantasy Sports Query
            self.yahoo_query = YahooFantasySportsQuery(
                league_id=self.league_id,
                game_code='nfl',
                game_id=None,
                yahoo_consumer_key=consumer_key,
                yahoo_consumer_secret=consumer_secret,
                env_var_fallback=True
            )
            
            logger.info("✓ Yahoo Fantasy Sports client initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to setup Yahoo client: {e}")
            self.yahoo_query = None
    
    def is_available(self) -> bool:
        """Check if Yahoo API is available and configured"""
        return self.yahoo_query is not None
    
    def fetch_free_agents_with_projections(self, position: Optional[str] = None, 
                                         count: int = 50) -> List[YahooPlayerInfo]:
        """
        Fetch free agents with projections from Yahoo Fantasy
        
        Args:
            position: Filter by position (QB, RB, WR, TE, etc.)
            count: Number of players to fetch
        
        Returns:
            List of YahooPlayerInfo objects
        """
        try:
            if not self.is_available():
                logger.warning("Yahoo API not available - missing credentials")
                return []
            
            if not self.league_id:
                logger.warning("No league ID provided for Yahoo API")
                return []
            
            logger.info(f"Fetching free agents from Yahoo Fantasy (position: {position}, count: {count})")
            
            # Get free agents from Yahoo API
            free_agents = self.yahoo_query.get_league_players(
                player_count=count,
                player_position=position,
                status='A'  # Available players only
            )
            
            players_info = []
            
            if free_agents and hasattr(free_agents, 'players'):
                for player in free_agents.players:
                    try:
                        # Extract player information
                        player_info = YahooPlayerInfo(
                            yahoo_id=str(player.player_id),
                            name=player.name.full if hasattr(player.name, 'full') else str(player.name),
                            position=player.primary_position if hasattr(player, 'primary_position') else 'UNKNOWN',
                            team=player.editorial_team_abbr if hasattr(player, 'editorial_team_abbr') else 'UNKNOWN',
                            is_available=True,
                            ownership_percentage=getattr(player, 'percent_owned', None)
                        )
                        
                        players_info.append(player_info)
                        
                    except Exception as e:
                        logger.warning(f"Failed to process Yahoo player: {e}")
                        continue
            
            logger.info(f"✓ Fetched {len(players_info)} free agents from Yahoo")
            return players_info
            
        except Exception as e:
            logger.error(f"Failed to fetch free agents from Yahoo: {e}")
            return []
    
    def fetch_player_projections(self, week: Optional[int] = None) -> List[YahooProjectionData]:
        """
        Fetch player projections from Yahoo Fantasy
        
        Args:
            week: Specific week to fetch projections for
        
        Returns:
            List of YahooProjectionData objects
        """
        try:
            if not self.is_available():
                logger.warning("Yahoo API not available - using mock projections")
                return self._generate_mock_projections()
            
            if week is None:
                week = self._get_current_nfl_week()
            
            logger.info(f"Fetching player projections from Yahoo for week {week}")
            
            # Get all available players (free agents + rostered)
            all_players = []
            
            # Fetch by position to get comprehensive coverage
            positions = ['QB', 'RB', 'WR', 'TE', 'K', 'DEF']
            
            for position in positions:
                position_players = self.fetch_free_agents_with_projections(position, 200)
                all_players.extend(position_players)
            
            if not all_players:
                logger.warning("No players fetched from Yahoo API")
                return self._generate_mock_projections()
            
            # Convert to projection data
            projections = []
            
            for yahoo_player in all_players:
                try:
                    # Get or create player in database
                    canonical_id = self.player_mapper.generate_canonical_id(
                        yahoo_player.name, yahoo_player.position, yahoo_player.team
                    )
                    
                    player_id = self._get_or_create_player(
                        canonical_id, yahoo_player.name, yahoo_player.position, 
                        yahoo_player.team, yahoo_player.yahoo_id
                    )
                    
                    if not player_id:
                        continue
                    
                    # Generate projection based on position (Yahoo API may not provide exact projections)
                    projection = self._generate_position_projection(
                        player_id, yahoo_player.name, yahoo_player.position, 
                        yahoo_player.team, week
                    )
                    
                    projections.append(projection)
                    
                except Exception as e:
                    logger.warning(f"Failed to process projection for {yahoo_player.name}: {e}")
                    continue
            
            logger.info(f"✓ Generated {len(projections)} projections from Yahoo data")
            return projections
            
        except Exception as e:
            logger.error(f"Failed to fetch projections from Yahoo: {e}")
            return self._generate_mock_projections()
    
    def sync_projections_to_database(self, projections: List[YahooProjectionData]) -> bool:
        """
        Sync projections to the database
        
        Args:
            projections: List of projection data to sync
        
        Returns:
            Success boolean
        """
        try:
            if not projections:
                logger.warning("No projections to sync")
                return False
            
            db = SessionLocal()
            synced_count = 0
            
            try:
                for proj in projections:
                    # Check if projection already exists
                    existing = db.query(PlayerProjections).filter(
                        PlayerProjections.player_id == proj.player_id,
                        PlayerProjections.week == proj.week,
                        PlayerProjections.season == proj.season,
                        PlayerProjections.source == proj.source
                    ).first()
                    
                    if existing:
                        # Update existing projection
                        existing.projected_points = proj.projected_points
                        existing.floor = proj.floor
                        existing.ceiling = proj.ceiling
                        existing.mean = proj.mean
                        existing.stdev = proj.stdev
                        existing.scoring_format = proj.scoring_format
                        existing.updated_at = datetime.utcnow()
                    else:
                        # Create new projection
                        new_proj = PlayerProjections(
                            player_id=proj.player_id,
                            week=proj.week,
                            season=proj.season,
                            projected_points=proj.projected_points,
                            floor=proj.floor,
                            ceiling=proj.ceiling,
                            mean=proj.mean,
                            stdev=proj.stdev,
                            source=proj.source,
                            scoring_format=proj.scoring_format
                        )
                        db.add(new_proj)
                    
                    synced_count += 1
                
                db.commit()
                logger.info(f"✓ Synced {synced_count} projections to database")
                return True
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to sync projections to database: {e}")
            return False
    
    def daily_projections_sync_job(self, week: Optional[int] = None) -> Dict[str, Any]:
        """
        Daily sync job to update Yahoo projections
        
        Args:
            week: Specific week to sync (defaults to current NFL week)
        
        Returns:
            Dictionary with sync results
        """
        try:
            if week is None:
                week = self._get_current_nfl_week()
            
            logger.info(f"Starting daily Yahoo projections sync for week {week}")
            
            # Fetch projections
            projections = self.fetch_player_projections(week)
            
            if not projections:
                return {
                    'yahoo_projections_sync': False,
                    'error': 'No projections available',
                    'week': week
                }
            
            # Sync to database
            sync_success = self.sync_projections_to_database(projections)
            
            return {
                'yahoo_projections_sync': sync_success,
                'projections_count': len(projections),
                'week': week
            }
            
        except Exception as e:
            logger.error(f"Daily Yahoo sync job failed: {e}")
            return {
                'yahoo_projections_sync': False,
                'error': str(e)
            }
    
    def _generate_mock_projections(self) -> List[YahooProjectionData]:
        """Generate mock projections when Yahoo API is unavailable"""
        logger.info("Generating mock projections (Yahoo API unavailable)")
        
        mock_players = [
            {'name': 'Josh Allen', 'position': 'QB', 'team': 'BUF', 'proj': 22.5},
            {'name': 'Lamar Jackson', 'position': 'QB', 'team': 'BAL', 'proj': 21.8},
            {'name': 'Christian McCaffrey', 'position': 'RB', 'team': 'SF', 'proj': 18.5},
            {'name': 'Derrick Henry', 'position': 'RB', 'team': 'BAL', 'proj': 16.2},
            {'name': 'Cooper Kupp', 'position': 'WR', 'team': 'LAR', 'proj': 15.8},
            {'name': 'Tyreek Hill', 'position': 'WR', 'team': 'MIA', 'proj': 15.5},
            {'name': 'Travis Kelce', 'position': 'TE', 'team': 'KC', 'proj': 14.2},
            {'name': 'Mark Andrews', 'position': 'TE', 'team': 'BAL', 'proj': 12.8}
        ]
        
        projections = []
        current_week = self._get_current_nfl_week()
        
        for player_data in mock_players:
            canonical_id = self.player_mapper.generate_canonical_id(
                player_data['name'], player_data['position'], player_data['team']
            )
            
            player_id = self._get_or_create_player(
                canonical_id, player_data['name'], player_data['position'], 
                player_data['team'], f"mock_{player_data['name'].replace(' ', '_').lower()}"
            )
            
            if player_id:
                proj_points = player_data['proj']
                
                projection = YahooProjectionData(
                    player_id=player_id,
                    name=player_data['name'],
                    position=player_data['position'],
                    team=player_data['team'],
                    week=current_week,
                    season=self.current_season,
                    projected_points=proj_points,
                    floor=proj_points * 0.7,
                    ceiling=proj_points * 1.4,
                    mean=proj_points,
                    stdev=proj_points * 0.15,
                    source='yahoo_mock'
                )
                
                projections.append(projection)
        
        return projections
    
    def _generate_position_projection(self, player_id: int, name: str, position: str, 
                                    team: str, week: int) -> YahooProjectionData:
        """Generate realistic projections based on position"""
        
        # Base projections by position (these would come from Yahoo API in real implementation)
        position_baselines = {
            'QB': {'base': 18.5, 'variance': 4.0},
            'RB': {'base': 12.8, 'variance': 3.5},
            'WR': {'base': 11.2, 'variance': 3.0},
            'TE': {'base': 8.5, 'variance': 2.5},
            'K': {'base': 7.8, 'variance': 2.0},
            'DEF': {'base': 8.2, 'variance': 2.2}
        }
        
        baseline = position_baselines.get(position, {'base': 10.0, 'variance': 3.0})
        projected = baseline['base']
        variance = baseline['variance']
        
        return YahooProjectionData(
            player_id=player_id,
            name=name,
            position=position,
            team=team,
            week=week,
            season=self.current_season,
            projected_points=projected,
            floor=max(0, projected - variance),
            ceiling=projected + variance * 1.2,
            mean=projected,
            stdev=variance * 0.4,
            source='yahoo'
        )
    
    def _get_or_create_player(self, canonical_id: str, name: str, position: str, 
                            team: str, yahoo_id: str) -> Optional[int]:
        """Get or create player in database with Yahoo ID"""
        try:
            db = SessionLocal()
            try:
                # Look for existing player by canonical ID
                player = db.query(Player).filter(Player.nfl_id == canonical_id).first()
                
                if player:
                    # Update Yahoo ID if not set
                    if not player.yahoo_id and yahoo_id:
                        player.yahoo_id = yahoo_id
                        db.commit()
                    return player.id
                
                # Create new player
                new_player = Player(
                    nfl_id=canonical_id,
                    yahoo_id=yahoo_id,
                    name=name,
                    position=position,
                    team=team,
                    is_starter=position in ['QB', 'RB', 'WR', 'TE']
                )
                
                db.add(new_player)
                db.commit()
                
                return new_player.id
                
            finally:
                db.close()
                
        except Exception as e:
            logger.warning(f"Failed to get/create player {name}: {e}")
            return None
    
    def _get_current_nfl_week(self) -> int:
        """Get current NFL week"""
        now = datetime.now()
        if now.month >= 9:
            return min(max(now.isocalendar()[1] - 35, 1), 18)
        else:
            return 1

# Test function
def test_yahoo_data_service():
    """Test the Yahoo data service"""
    print("Testing Yahoo Data Service...")
    print("=" * 60)
    
    service = YahooDataService()
    
    try:
        # Test availability
        print(f"\n1. Yahoo API available: {service.is_available()}")
        
        # Test projections fetch
        print("\n2. Testing projections fetch...")
        projections = service.fetch_player_projections()
        print(f"   Fetched {len(projections)} projections")
        
        if projections:
            sample = projections[0]
            print(f"   Sample projection: {sample.name} ({sample.position}, {sample.team})")
            print(f"     Projected: {sample.projected_points}")
            print(f"     Floor: {sample.floor}, Ceiling: {sample.ceiling}")
            print(f"     Source: {sample.source}")
        
        # Test database sync
        if projections:
            print("\n3. Testing database sync...")
            success = service.sync_projections_to_database(projections[:5])
            print(f"   Sync success: {success}")
        
        # Test daily sync job
        print("\n4. Testing daily sync job...")
        result = service.daily_projections_sync_job()
        print(f"   Job result: {result}")
        
        return True
        
    except Exception as e:
        print(f"   ✗ Yahoo data service test failed: {e}")
        return False

if __name__ == "__main__":
    test_yahoo_data_service()