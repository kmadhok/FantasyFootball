import logging
import requests
import os
from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import dataclass

from src.config.config import get_config
from src.database import SessionLocal, Player, PlayerProjections
from src.utils.player_id_mapper import PlayerIDMapper

logger = logging.getLogger(__name__)

@dataclass
class MFLProjectionData:
    """Container for MFL fantasy projections"""
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
    source: str = 'mfl'
    scoring_format: str = 'ppr'

@dataclass
class MFLPlayerInfo:
    """Container for MFL player information"""
    mfl_id: str
    name: str
    position: str
    team: str
    projected_score: Optional[float] = None

class MFLProjectionService:
    """Service for integrating with MyFantasyLeague API for player projections"""
    
    def __init__(self):
        self.config = get_config()
        self.player_mapper = PlayerIDMapper()
        self.current_season = 2025
        self.mfl_league_id = os.getenv('MFL_LEAGUE_ID')
        self.mfl_api_key = os.getenv('MFL_LEAGUE_API_KEY')
        self.base_url = "https://api.myfantasyleague.com"
        
    def is_available(self) -> bool:
        """Check if MFL API is available and configured"""
        return bool(self.mfl_league_id and self.mfl_api_key)
    
    def fetch_players_data(self) -> Dict[str, Dict[str, Any]]:
        """
        Fetch all players data from MFL to create ID->details mapping
        
        Returns:
            Dictionary mapping player IDs to player details
        """
        try:
            logger.info("Fetching MFL players data for ID mapping")
            
            url = f"{self.base_url}/{self.current_season}/export"
            params = {
                'TYPE': 'players',
                'L': self.mfl_league_id,
                'APIKEY': self.mfl_api_key,
                'DETAILS': '1',
                'JSON': '1'
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            players_map = {}
            
            if 'players' in data and 'player' in data['players']:
                players = data['players']['player']
                
                if not isinstance(players, list):
                    players = [players]
                
                for player in players:
                    player_id = player.get('id', '')
                    if player_id:
                        players_map[player_id] = {
                            'name': player.get('name', ''),
                            'position': player.get('position', ''),
                            'team': player.get('team', ''),
                            'jersey': player.get('jersey', ''),
                            'college': player.get('college', '')
                        }
            
            logger.info(f"✓ Loaded {len(players_map)} player details from MFL")
            return players_map
            
        except Exception as e:
            logger.error(f"Failed to fetch MFL players data: {e}")
            return {}
    
    def fetch_projected_scores(self, week: Optional[int] = None, 
                             position: Optional[str] = None,
                             count: int = 200) -> List[MFLPlayerInfo]:
        """
        Fetch projected scores from MFL API using FantasySharks projections
        
        Args:
            week: Specific week to get projections for
            position: Filter by position (QB, RB, WR, TE, etc.)
            count: Number of players to return
        
        Returns:
            List of MFLPlayerInfo objects with projections
        """
        try:
            if not self.is_available():
                logger.warning("MFL API not available - missing league ID or API key")
                return []
            
            if week is None:
                week = self._get_current_nfl_week()
            
            logger.info(f"Fetching MFL projected scores for week {week}, position: {position}")
            
            # First get players data for ID mapping
            players_map = self.fetch_players_data()
            if not players_map:
                logger.warning("No players data available for mapping")
                return []
            
            # Build MFL API request for projections
            url = f"{self.base_url}/{self.current_season}/export"
            params = {
                'TYPE': 'projectedScores',
                'L': self.mfl_league_id,
                'APIKEY': self.mfl_api_key,
                'W': str(week),
                'JSON': '1'  # Request JSON format
            }
            
            if position:
                params['POSITION'] = position
            if count:
                params['COUNT'] = str(count)
            
            # Make API request
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Parse projectedScores response
            players_info = []
            
            if 'projectedScores' in data and 'playerScore' in data['projectedScores']:
                player_scores = data['projectedScores']['playerScore']
                
                # Handle both single player and multiple players response
                if not isinstance(player_scores, list):
                    player_scores = [player_scores]
                
                for player_score in player_scores:
                    try:
                        player_id = player_score.get('id', '')
                        projected_score = float(player_score.get('score', 0))
                        
                        # Look up player details using ID
                        player_details = players_map.get(player_id, {})
                        
                        if not player_details:
                            logger.warning(f"No player details found for ID {player_id}")
                            continue
                        
                        # Create player info with details
                        player_info = MFLPlayerInfo(
                            mfl_id=player_id,
                            name=player_details.get('name', ''),
                            position=player_details.get('position', ''),
                            team=player_details.get('team', ''),
                            projected_score=projected_score
                        )
                        
                        players_info.append(player_info)
                        
                    except (ValueError, KeyError) as e:
                        logger.warning(f"Failed to parse MFL player score: {e}")
                        continue
            
            logger.info(f"✓ Fetched {len(players_info)} projected scores from MFL")
            return players_info
            
        except requests.RequestException as e:
            logger.error(f"MFL API request failed: {e}")
            return []
        except Exception as e:
            logger.error(f"Failed to fetch MFL projected scores: {e}")
            return []
    
    def convert_to_projection_data(self, mfl_players: List[MFLPlayerInfo], 
                                 week: int) -> List[MFLProjectionData]:
        """
        Convert MFL player info to projection data for Epic A
        
        Args:
            mfl_players: List of MFL player info
            week: NFL week
        
        Returns:
            List of MFLProjectionData objects
        """
        try:
            logger.info(f"Converting {len(mfl_players)} MFL players to projection data")
            
            projections = []
            
            for mfl_player in mfl_players:
                try:
                    # Skip players without valid data
                    if not all([mfl_player.name, mfl_player.position, mfl_player.projected_score]):
                        continue
                    
                    # Get or create player in database
                    canonical_id = self.player_mapper.generate_canonical_id(
                        mfl_player.name, mfl_player.position, mfl_player.team or 'FA'
                    )
                    
                    player_id = self._get_or_create_player(
                        canonical_id, mfl_player.name, mfl_player.position, 
                        mfl_player.team or 'FA', mfl_player.mfl_id
                    )
                    
                    if not player_id:
                        continue
                    
                    # Calculate floor and ceiling from projected score
                    projected = mfl_player.projected_score
                    
                    # Epic A projection calculations
                    # Floor: ~75% of projection (25th percentile)
                    # Ceiling: ~125% of projection (75th percentile)
                    floor = projected * 0.75
                    ceiling = projected * 1.25
                    mean = projected
                    stdev = projected * 0.15  # Estimated standard deviation
                    
                    projection = MFLProjectionData(
                        player_id=player_id,
                        name=mfl_player.name,
                        position=mfl_player.position,
                        team=mfl_player.team or 'FA',
                        week=week,
                        season=self.current_season,
                        projected_points=projected,
                        floor=floor,
                        ceiling=ceiling,
                        mean=mean,
                        stdev=stdev,
                        source='mfl'
                    )
                    
                    projections.append(projection)
                    
                except Exception as e:
                    logger.warning(f"Failed to convert player {mfl_player.name}: {e}")
                    continue
            
            logger.info(f"✓ Converted to {len(projections)} projection records")
            return projections
            
        except Exception as e:
            logger.error(f"Failed to convert MFL players to projections: {e}")
            return []
    
    def fetch_player_projections(self, week: Optional[int] = None) -> List[MFLProjectionData]:
        """
        Fetch comprehensive player projections from MFL API
        
        Args:
            week: Specific week to fetch projections for
        
        Returns:
            List of MFLProjectionData objects
        """
        try:
            if week is None:
                week = self._get_current_nfl_week()
            
            logger.info(f"Fetching player projections from MFL for week {week}")
            
            # Get projections for all positions
            all_mfl_players = []
            positions = ['QB', 'RB', 'WR', 'TE', 'K', 'Def']
            
            for position in positions:
                try:
                    position_players = self.fetch_projected_scores(week, position, 50)
                    all_mfl_players.extend(position_players)
                except Exception as e:
                    logger.warning(f"Failed to fetch {position} projections: {e}")
                    continue
            
            if not all_mfl_players:
                logger.warning("No MFL projections fetched - using mock data")
                return self._generate_mock_projections(week)
            
            # Convert to projection data
            projections = self.convert_to_projection_data(all_mfl_players, week)
            
            logger.info(f"✓ Generated {len(projections)} projections from MFL data")
            return projections
            
        except Exception as e:
            logger.error(f"Failed to fetch projections from MFL: {e}")
            return self._generate_mock_projections(week)
    
    def sync_projections_to_database(self, projections: List[MFLProjectionData]) -> bool:
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
                logger.info(f"✓ Synced {synced_count} MFL projections to database")
                return True
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to sync MFL projections to database: {e}")
            return False
    
    def daily_projections_sync_job(self, week: Optional[int] = None) -> Dict[str, Any]:
        """
        Daily sync job to update MFL projections
        
        Args:
            week: Specific week to sync (defaults to current NFL week)
        
        Returns:
            Dictionary with sync results
        """
        try:
            if week is None:
                week = self._get_current_nfl_week()
            
            logger.info(f"Starting daily MFL projections sync for week {week}")
            
            # Fetch projections
            projections = self.fetch_player_projections(week)
            
            if not projections:
                return {
                    'mfl_projections_sync': False,
                    'error': 'No projections available',
                    'week': week
                }
            
            # Sync to database
            sync_success = self.sync_projections_to_database(projections)
            
            return {
                'mfl_projections_sync': sync_success,
                'projections_count': len(projections),
                'week': week,
                'source': 'mfl'
            }
            
        except Exception as e:
            logger.error(f"Daily MFL sync job failed: {e}")
            return {
                'mfl_projections_sync': False,
                'error': str(e)
            }
    
    def _generate_mock_projections(self, week: int) -> List[MFLProjectionData]:
        """Generate mock projections when MFL API is unavailable"""
        logger.info("Generating mock MFL projections (API unavailable)")
        
        mock_players = [
            {'name': 'Josh Allen', 'position': 'QB', 'team': 'BUF', 'proj': 22.8},
            {'name': 'Lamar Jackson', 'position': 'QB', 'team': 'BAL', 'proj': 21.5},
            {'name': 'Christian McCaffrey', 'position': 'RB', 'team': 'SF', 'proj': 18.2},
            {'name': 'Derrick Henry', 'position': 'RB', 'team': 'BAL', 'proj': 16.5},
            {'name': 'Cooper Kupp', 'position': 'WR', 'team': 'LAR', 'proj': 15.9},
            {'name': 'Tyreek Hill', 'position': 'WR', 'team': 'MIA', 'proj': 15.3},
            {'name': 'Travis Kelce', 'position': 'TE', 'team': 'KC', 'proj': 14.1},
            {'name': 'Mark Andrews', 'position': 'TE', 'team': 'BAL', 'proj': 12.6}
        ]
        
        projections = []
        
        for player_data in mock_players:
            canonical_id = self.player_mapper.generate_canonical_id(
                player_data['name'], player_data['position'], player_data['team']
            )
            
            player_id = self._get_or_create_player(
                canonical_id, player_data['name'], player_data['position'], 
                player_data['team'], f"mock_mfl_{player_data['name'].replace(' ', '_').lower()}"
            )
            
            if player_id:
                proj_points = player_data['proj']
                
                projection = MFLProjectionData(
                    player_id=player_id,
                    name=player_data['name'],
                    position=player_data['position'],
                    team=player_data['team'],
                    week=week,
                    season=self.current_season,
                    projected_points=proj_points,
                    floor=proj_points * 0.75,
                    ceiling=proj_points * 1.25,
                    mean=proj_points,
                    stdev=proj_points * 0.15,
                    source='mfl_mock'
                )
                
                projections.append(projection)
        
        return projections
    
    def _get_or_create_player(self, canonical_id: str, name: str, position: str, 
                            team: str, mfl_id: str) -> Optional[int]:
        """Get or create player in database with MFL ID"""
        try:
            db = SessionLocal()
            try:
                # Look for existing player by canonical ID
                player = db.query(Player).filter(Player.nfl_id == canonical_id).first()
                
                if player:
                    # Update MFL ID if not set
                    if not player.mfl_id and mfl_id:
                        player.mfl_id = mfl_id
                        db.commit()
                    return player.id
                
                # Create new player
                new_player = Player(
                    nfl_id=canonical_id,
                    mfl_id=mfl_id,
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
def test_mfl_projection_service():
    """Test the MFL projection service"""
    print("Testing MFL Projection Service...")
    print("=" * 60)
    
    service = MFLProjectionService()
    
    try:
        # Test availability
        print(f"\n1. MFL API available: {service.is_available()}")
        print(f"   League ID: {service.mfl_league_id}")
        print(f"   API Key: {service.mfl_api_key[:10] + '...' if service.mfl_api_key else 'None'}")
        
        # Test projected scores fetch
        print("\n2. Testing projected scores fetch...")
        mfl_players = service.fetch_projected_scores(week=1, position='QB', count=10)
        print(f"   Fetched {len(mfl_players)} QB projections")
        
        if mfl_players:
            sample = mfl_players[0]
            print(f"   Sample: {sample.name} ({sample.position}, {sample.team})")
            print(f"     MFL ID: {sample.mfl_id}")
            print(f"     Projected: {sample.projected_score}")
        
        # Test full projections
        print("\n3. Testing full projections fetch...")
        projections = service.fetch_player_projections()
        print(f"   Generated {len(projections)} total projections")
        
        if projections:
            sample = projections[0]
            print(f"   Sample projection: {sample.name}")
            print(f"     Projected: {sample.projected_points}")
            print(f"     Floor: {sample.floor}, Ceiling: {sample.ceiling}")
            print(f"     Source: {sample.source}")
        
        # Test database sync
        if projections:
            print("\n4. Testing database sync...")
            success = service.sync_projections_to_database(projections[:5])
            print(f"   Sync success: {success}")
        
        # Test daily sync job
        print("\n5. Testing daily sync job...")
        result = service.daily_projections_sync_job()
        print(f"   Job result: {result}")
        
        return True
        
    except Exception as e:
        print(f"   ✗ MFL projection service test failed: {e}")
        return False

if __name__ == "__main__":
    test_mfl_projection_service()