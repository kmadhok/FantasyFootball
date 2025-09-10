import logging
import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime
from espn_api.football import League
from dataclasses import dataclass

from src.config.config import get_config
from src.database import SessionLocal, Player, PlayerUsage, PlayerProjections
from src.utils.player_id_mapper import PlayerIDMapper
from src.utils.retry_handler import handle_api_request, APIError

logger = logging.getLogger(__name__)

@dataclass
class ESPNPlayerData:
    """Data class for ESPN player information"""
    espn_id: str
    name: str
    position: str
    team: str
    projected_points: Optional[float] = None
    avg_points: Optional[float] = None
    total_points: Optional[float] = None
    stats: Optional[Dict[str, Any]] = None
    injury_status: Optional[str] = None

class ESPNDataService:
    """Service for fetching data from ESPN Fantasy API"""
    
    def __init__(self):
        self.config = get_config()
        self.league_id = int(self.config.ESPN_LEAGUE_ID)
        self.espn_s2 = self.config.ESPN_S2
        self.swid = self.config.SWID
        self.league = None
        self.player_mapper = PlayerIDMapper()
        self.current_year = self.config.get_current_season_year()
        
    def authenticate_league(self) -> bool:
        """Authenticate with ESPN Fantasy API"""
        try:
            logger.info(f"Authenticating with ESPN league {self.league_id}")
            
            # Try to connect to the league
            # First try with authentication cookies if available
            if self.espn_s2 and self.swid:
                logger.info("Using ESPN S2/SWID authentication")
                self.league = League(
                    league_id=self.league_id,
                    year=self.current_year,
                    espn_s2=self.espn_s2,
                    swid=self.swid
                )
            else:
                # Try public access
                logger.info("Trying public league access")
                self.league = League(
                    league_id=self.league_id,
                    year=self.current_year
                )
            
            # Test the connection by accessing basic league info
            if self.league is None:
                logger.error("League object is None after initialization")
                return False
                
            try:
                league_name = getattr(self.league, 'name', 'Unknown')
                logger.info(f"✓ Successfully connected to ESPN league: {league_name}")
                return True
            except Exception as e:
                logger.error(f"Failed to access league properties: {e}")
                # The league might be private - let's try to see what we get
                logger.info(f"League object type: {type(self.league)}")
                logger.info(f"League object attributes: {dir(self.league) if self.league else 'None'}")
                return False
            
        except Exception as e:
            logger.error(f"Failed to authenticate with ESPN: {e}")
            self.league = None
            return False
    
    @handle_api_request
    def fetch_free_agents_projections(self, week: Optional[int] = None) -> List[ESPNPlayerData]:
        """Fetch projections from ESPN free agents"""
        if not self.league and not self.authenticate_league():
            raise APIError("Failed to authenticate with ESPN", platform="espn")
            
        try:
            logger.info("Fetching free agents projections from ESPN")
            
            # Get all free agents
            free_agents = self.league.free_agents()
            
            espn_players = []
            
            for player in free_agents:
                try:
                    # Extract player information
                    espn_player = ESPNPlayerData(
                        espn_id=str(player.playerId),
                        name=player.name,
                        position=player.position,
                        team=getattr(player, 'proTeam', 'FA'),
                        projected_points=getattr(player, 'projected_points', None),
                        avg_points=getattr(player, 'avg_points', None),
                        total_points=getattr(player, 'points', None),
                        injury_status=getattr(player, 'injuryStatus', None)
                    )
                    
                    # Try to get stats if available
                    if hasattr(player, 'stats'):
                        espn_player.stats = player.stats
                    
                    espn_players.append(espn_player)
                    
                except Exception as e:
                    logger.warning(f"Error processing player {getattr(player, 'name', 'Unknown')}: {e}")
                    continue
            
            logger.info(f"✓ Successfully fetched {len(espn_players)} free agent projections from ESPN")
            return espn_players
            
        except Exception as e:
            logger.error(f"Failed to fetch ESPN free agents projections: {e}")
            raise APIError(f"ESPN free agents request failed: {e}", platform="espn")
    
    @handle_api_request
    def fetch_all_players_data(self) -> List[ESPNPlayerData]:
        """Fetch data for all players (free agents + rostered)"""
        if not self.league and not self.authenticate_league():
            raise APIError("Failed to authenticate with ESPN", platform="espn")
            
        try:
            logger.info("Fetching all players data from ESPN")
            
            all_players = []
            
            # Get free agents
            try:
                free_agents = self.fetch_free_agents_projections()
                all_players.extend(free_agents)
                logger.info(f"Added {len(free_agents)} free agents")
            except Exception as e:
                logger.warning(f"Failed to fetch free agents: {e}")
            
            # Get rostered players from all teams
            try:
                for team in self.league.teams:
                    for player in team.roster:
                        try:
                            espn_player = ESPNPlayerData(
                                espn_id=str(player.playerId),
                                name=player.name,
                                position=player.position,
                                team=getattr(player, 'proTeam', 'FA'),
                                projected_points=getattr(player, 'projected_points', None),
                                avg_points=getattr(player, 'avg_points', None),
                                total_points=getattr(player, 'points', None),
                                injury_status=getattr(player, 'injuryStatus', None)
                            )
                            
                            if hasattr(player, 'stats'):
                                espn_player.stats = player.stats
                            
                            all_players.append(espn_player)
                            
                        except Exception as e:
                            logger.warning(f"Error processing rostered player {getattr(player, 'name', 'Unknown')}: {e}")
                            continue
                            
                logger.info(f"Added {len(self.league.teams)} teams' roster players")
                
            except Exception as e:
                logger.warning(f"Failed to fetch rostered players: {e}")
            
            # Remove duplicates based on ESPN ID
            unique_players = {}
            for player in all_players:
                if player.espn_id not in unique_players:
                    unique_players[player.espn_id] = player
            
            final_players = list(unique_players.values())
            logger.info(f"✓ Successfully fetched {len(final_players)} total players from ESPN")
            return final_players
            
        except Exception as e:
            logger.error(f"Failed to fetch ESPN players data: {e}")
            raise APIError(f"ESPN players data request failed: {e}", platform="espn")
    
    def sync_espn_players_to_database(self) -> bool:
        """Sync ESPN player data to database"""
        try:
            logger.info("Starting ESPN player sync to database")
            
            # Fetch all ESPN players
            espn_players = self.fetch_all_players_data()
            
            if not espn_players:
                logger.warning("No ESPN players data to sync")
                return False
            
            db = SessionLocal()
            try:
                players_synced = 0
                
                for espn_player in espn_players:
                    # Map to canonical player or create new
                    canonical_id = self.player_mapper.get_canonical_id(
                        espn_id=espn_player.espn_id,
                        name=espn_player.name,
                        position=espn_player.position,
                        team=espn_player.team
                    )
                    
                    if not canonical_id:
                        # Generate canonical ID
                        canonical_id = self.player_mapper.generate_canonical_id(
                            espn_player.name, espn_player.position, espn_player.team
                        )
                    
                    # Find or create player record
                    player = db.query(Player).filter(Player.espn_id == espn_player.espn_id).first()
                    
                    if not player:
                        # Check if player exists by canonical ID
                        player = db.query(Player).filter(Player.nfl_id == canonical_id).first()
                        
                        if player:
                            # Update existing player with ESPN ID
                            player.espn_id = espn_player.espn_id
                        else:
                            # Create new player
                            player = Player(
                                nfl_id=canonical_id,
                                espn_id=espn_player.espn_id,
                                name=espn_player.name,
                                position=espn_player.position,
                                team=espn_player.team,
                                is_starter=self.player_mapper._is_starter_position(espn_player.position)
                            )
                            db.add(player)
                    else:
                        # Update existing player info
                        player.name = espn_player.name
                        player.position = espn_player.position
                        player.team = espn_player.team
                    
                    db.flush()  # Get the player ID
                    players_synced += 1
                
                db.commit()
                logger.info(f"✓ Successfully synced {players_synced} ESPN players to database")
                return True
                
            except Exception as e:
                db.rollback()
                logger.error(f"Database error during ESPN player sync: {e}")
                raise
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to sync ESPN players to database: {e}")
            return False
    
    def sync_projections_to_database(self, week: Optional[int] = None) -> bool:
        """Sync ESPN projections to database"""
        try:
            logger.info(f"Starting ESPN projections sync to database for week {week or 'current'}")
            
            # Fetch projections
            espn_players = self.fetch_free_agents_projections(week)
            
            if not espn_players:
                logger.warning("No ESPN projections data to sync")
                return False
            
            # Use current week if not specified
            if week is None:
                week = self._get_current_nfl_week()
            
            db = SessionLocal()
            try:
                projections_synced = 0
                
                for espn_player in espn_players:
                    # Find player in database
                    player = db.query(Player).filter(Player.espn_id == espn_player.espn_id).first()
                    
                    if not player:
                        logger.debug(f"Player {espn_player.name} not found in database, skipping projection")
                        continue
                    
                    # Create or update projection
                    projection = db.query(PlayerProjections).filter(
                        PlayerProjections.player_id == player.id,
                        PlayerProjections.week == week,
                        PlayerProjections.season == self.current_year,
                        PlayerProjections.source == 'espn'
                    ).first()
                    
                    if projection:
                        # Update existing projection
                        projection.projected_points = espn_player.projected_points
                        projection.mean = espn_player.projected_points
                        projection.updated_at = datetime.utcnow()
                    else:
                        # Create new projection
                        projection = PlayerProjections(
                            player_id=player.id,
                            week=week,
                            season=self.current_year,
                            projected_points=espn_player.projected_points,
                            mean=espn_player.projected_points,
                            source='espn',
                            scoring_format='ppr'
                        )
                        db.add(projection)
                    
                    projections_synced += 1
                
                db.commit()
                logger.info(f"✓ Successfully synced {projections_synced} ESPN projections to database")
                return True
                
            except Exception as e:
                db.rollback()
                logger.error(f"Database error during ESPN projections sync: {e}")
                raise
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to sync ESPN projections to database: {e}")
            return False
    
    def _get_current_nfl_week(self) -> int:
        """Estimate current NFL week (simplified)"""
        # This is a simplified calculation - in production you'd want more accurate week detection
        now = datetime.now()
        if now.month >= 9:  # September or later
            return min(max(now.isocalendar()[1] - 35, 1), 18)  # Week 1-18
        else:  # Before September
            return 1
    
    def get_league_info(self) -> Dict[str, Any]:
        """Get ESPN league information"""
        if not self.league and not self.authenticate_league():
            return {}
        
        try:
            return {
                'name': getattr(self.league, 'name', 'Unknown'),
                'size': len(getattr(self.league, 'teams', [])),
                'year': self.current_year,
                'current_week': getattr(self.league, 'current_week', 1),
                'scoring_format': 'PPR'  # Assuming PPR based on user request
            }
        except Exception as e:
            logger.error(f"Failed to get ESPN league info: {e}")
            return {}

# Test functions
def test_espn_connection():
    """Test ESPN API connection"""
    print("Testing ESPN API Connection...")
    print("=" * 50)
    
    service = ESPNDataService()
    
    try:
        # Test authentication
        print("1. Testing authentication...")
        auth_success = service.authenticate_league()
        print(f"   Authentication: {'SUCCESS' if auth_success else 'FAILED'}")
        
        if auth_success:
            # Test league info
            print("\n2. Testing league info...")
            league_info = service.get_league_info()
            print(f"   League: {league_info.get('name', 'Unknown')}")
            print(f"   Size: {league_info.get('size', 0)} teams")
            print(f"   Current Week: {league_info.get('current_week', 'Unknown')}")
            
            # Test free agents fetch (limit to first 5 for testing)
            print("\n3. Testing free agents fetch...")
            free_agents = service.fetch_free_agents_projections()
            print(f"   Found {len(free_agents)} free agents")
            
            print("\n   Sample players:")
            for i, player in enumerate(free_agents[:5]):
                proj = player.projected_points or 0
                print(f"     {i+1}. {player.name} ({player.position}, {player.team}) - {proj:.1f} pts")
        
        return auth_success
        
    except Exception as e:
        print(f"   ✗ ESPN connection test failed: {e}")
        return False

if __name__ == "__main__":
    test_espn_connection()
