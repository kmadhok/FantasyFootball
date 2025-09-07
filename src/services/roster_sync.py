import requests
import logging
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime
#from .config import get_config
from src.config.config import get_config

#from ..database import SessionLocal, Player, RosterEntry
from src.database import SessionLocal, Player, RosterEntry, get_storage_service
from src.utils.player_id_mapper import PlayerIDMapper
from src.utils.retry_handler import handle_api_request, safe_api_call, APIError

logger = logging.getLogger(__name__)

class SleeperAPIClient:
    """Client for Sleeper API interactions"""
    
    def __init__(self):
        self.config = get_config()
        self.base_url = "https://api.sleeper.app/v1"
        self.league_id = self.config.SLEEPER_LEAGUE_ID
        self.timeout = 10
    
    @handle_api_request
    def get_league_info(self, platform: str = "sleeper") -> Dict[str, Any]:
        """Get league information with retry logic"""
        url = f"{self.base_url}/league/{self.league_id}"
        
        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch Sleeper league info: {e}")
            raise APIError(f"Sleeper league info request failed: {e}", 
                         status_code=getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None,
                         platform="sleeper")
    
    @handle_api_request
    def get_rosters(self, platform: str = "sleeper") -> List[Dict[str, Any]]:
        """Fetch all rosters for the league with retry logic"""
        url = f"{self.base_url}/league/{self.league_id}/rosters"
        
        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            rosters = response.json()
            
            logger.info(f"Successfully fetched {len(rosters)} rosters from Sleeper")
            return rosters
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch Sleeper rosters: {e}")
            raise APIError(f"Sleeper rosters request failed: {e}", 
                         status_code=getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None,
                         platform="sleeper")
    
    @handle_api_request
    def get_users(self, platform: str = "sleeper") -> List[Dict[str, Any]]:
        """Get all users in the league with retry logic"""
        url = f"{self.base_url}/league/{self.league_id}/users"
        
        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            users = response.json()
            
            logger.info(f"Successfully fetched {len(users)} users from Sleeper")
            return users
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch Sleeper users: {e}")
            raise APIError(f"Sleeper users request failed: {e}", 
                         status_code=getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None,
                         platform="sleeper")
    
    @handle_api_request
    def get_players(self, platform: str = "sleeper") -> Dict[str, Dict[str, Any]]:
        """Get all NFL players data from Sleeper with retry logic"""
        url = f"{self.base_url}/players/nfl"
        
        try:
            response = requests.get(url, timeout=30)  # Longer timeout for large dataset
            response.raise_for_status()
            players = response.json()
            
            logger.info(f"Successfully fetched {len(players)} players from Sleeper")
            return players
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch Sleeper players: {e}")
            raise APIError(f"Sleeper players request failed: {e}", 
                         status_code=getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None,
                         platform="sleeper")

class MFLAPIClient:
    """Client for MyFantasyLeague API interactions"""
    
    def __init__(self):
        self.config = get_config()
        self.season = 2025  # Current season
        self.league_id = self.config.MFL_LEAGUE_ID
        self.api_key = self.config.MFL_LEAGUE_API_KEY
        self.base_url = f"https://api.myfantasyleague.com/{self.season}/export"
        self.timeout = 10
    
    @handle_api_request
    def get_league_info(self, platform: str = "mfl") -> Dict[str, Any]:
        """Get league information with retry logic"""
        params = {
            "TYPE": "league",
            "L": self.league_id,
            "JSON": "1"
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            return data.get("league", {})
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch MFL league info: {e}")
            raise APIError(f"MFL league info request failed: {e}", 
                         status_code=getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None,
                         platform="mfl")
    
    @handle_api_request
    def get_rosters(self, platform: str = "mfl") -> List[Dict[str, Any]]:
        """Fetch all rosters for the league using export?TYPE=rosters with retry logic"""
        params = {
            "TYPE": "rosters",
            "L": self.league_id,
            "JSON": "1"
        }
        
        try:
            logger.info(f"Fetching MFL rosters for league {self.league_id}")
            response = requests.get(self.base_url, params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            
            # MFL response structure: {"rosters": {"franchise": [...]}}
            rosters_data = data.get("rosters", {})
            
            if "franchise" in rosters_data:
                rosters = rosters_data["franchise"]
                if not isinstance(rosters, list):
                    rosters = [rosters]  # Handle single franchise case
                
                logger.info(f"Successfully fetched {len(rosters)} rosters from MFL")
                logger.debug(f"MFL rosters data structure: {rosters_data}")
                return rosters
            else:
                logger.warning("No franchise data found in MFL rosters response")
                logger.debug(f"MFL response data: {data}")
                return []
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch MFL rosters: {e}")
            raise APIError(f"MFL rosters request failed: {e}", 
                         status_code=getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None,
                         platform="mfl")
    
    @handle_api_request
    def get_detailed_roster(self, franchise_id: str, platform: str = "mfl") -> Dict[str, Any]:
        """Get detailed roster information for a specific franchise with retry logic"""
        params = {
            "TYPE": "rosters",
            "L": self.league_id,
            "FRANCHISE": franchise_id,
            "JSON": "1"
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            
            return data.get("rosters", {})
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch detailed MFL roster for franchise {franchise_id}: {e}")
            raise APIError(f"MFL detailed roster request failed: {e}", 
                         status_code=getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None,
                         platform="mfl")
    
    @handle_api_request
    def get_players(self, platform: str = "mfl") -> List[Dict[str, Any]]:
        """Get all NFL players data from MFL with retry logic"""
        params = {
            "TYPE": "players",
            "L": self.league_id,
            "JSON": "1"
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            players_data = data.get("players", {})
            
            if "player" in players_data:
                players = players_data["player"]
                if not isinstance(players, list):
                    players = [players]  # Handle single player case
                
                logger.info(f"Successfully fetched {len(players)} players from MFL")
                return players
            else:
                logger.warning("No player data found in MFL players response")
                return []
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch MFL players: {e}")
            raise APIError(f"MFL players request failed: {e}", 
                         status_code=getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None,
                         platform="mfl")

class RosterSyncService:
    """Service to synchronize roster data from both platforms"""
    
    def __init__(self):
        self.sleeper_client = SleeperAPIClient()
        self.mfl_client = MFLAPIClient()
        self.player_mapper = PlayerIDMapper()
        self.storage_service = get_storage_service()
        self.current_season = 2025
        
    def sync_sleeper_rosters(self) -> bool:
        """Sync rosters from Sleeper platform with comprehensive error handling"""
        try:
            logger.info("Starting Sleeper roster sync with error handling...")
            
            # Get rosters and users with fallback handling
            try:
                rosters = self.sleeper_client.get_rosters()
                if not rosters:
                    logger.warning("No rosters returned from Sleeper API")
                    return False
            except APIError as e:
                logger.error(f"Failed to fetch Sleeper rosters: {e}")
                return False
            
            try:
                users = self.sleeper_client.get_users()
                if not users:
                    logger.warning("No users returned from Sleeper API, continuing without user info")
                    users = []
            except APIError as e:
                logger.warning(f"Failed to fetch Sleeper users: {e}, continuing without user info")
                users = []
            
            # Create user lookup dictionary
            user_lookup = {user["user_id"]: user for user in users}
            
            db = SessionLocal()
            try:
                # Clear existing Sleeper roster entries
                db.query(RosterEntry).filter(RosterEntry.platform == "sleeper").delete()
                
                for roster in rosters:
                    owner_id = roster.get("owner_id")
                    roster_id = roster.get("roster_id")
                    players = roster.get("players", [])
                    
                    # Get user info
                    user_info = user_lookup.get(owner_id, {})
                    username = user_info.get("username", f"user_{owner_id}")
                    
                    logger.info(f"Processing roster for {username} with {len(players)} players")
                    
                    # Process each player on the roster
                    for player_id in players:
                        if player_id:  # Skip null player IDs
                            # Get canonical ID using mapper
                            canonical_id = self.player_mapper.get_canonical_id(sleeper_id=player_id)
                            
                            # Find or create player record
                            player = db.query(Player).filter(Player.sleeper_id == player_id).first()
                            if not player:
                                if canonical_id:
                                    # Get player info from mapper
                                    player_info = self.player_mapper.get_player_info(canonical_id)
                                    if player_info:
                                        player = Player(
                                            sleeper_id=player_id,
                                            nfl_id=canonical_id,
                                            name=player_info.name,
                                            position=player_info.position,
                                            team=player_info.team,
                                            is_starter=self.player_mapper._is_starter_position(player_info.position)
                                        )
                                    else:
                                        # Fallback to placeholder
                                        player = Player(
                                            sleeper_id=player_id,
                                            nfl_id=f"sleeper_{player_id}",
                                            name=f"Player_{player_id}",
                                            position="UNKNOWN",
                                            team="UNKNOWN"
                                        )
                                else:
                                    # Create placeholder player record
                                    player = Player(
                                        sleeper_id=player_id,
                                        nfl_id=f"sleeper_{player_id}",
                                        name=f"Player_{player_id}",
                                        position="UNKNOWN",
                                        team="UNKNOWN"
                                    )
                                db.add(player)
                                db.flush()  # Get the ID
                            
                            # Create roster entry
                            roster_entry = RosterEntry(
                                player_id=player.id,
                                platform="sleeper",
                                league_id=self.sleeper_client.league_id,
                                user_id=owner_id,
                                roster_slot="active",
                                is_active=True
                            )
                            db.add(roster_entry)

                            # Snapshot this roster state (idempotent)
                            try:
                                self.storage_service.upsert_roster_snapshot(
                                    platform="sleeper",
                                    league_id=self.sleeper_client.league_id,
                                    team_id=str(owner_id or roster_id),
                                    player_id=player.id,
                                    week=self._get_current_nfl_week(),
                                    season=self.current_season,
                                    slot="active",
                                )
                            except Exception as e:
                                logger.warning(f"Failed to snapshot Sleeper roster: {e}")
                
                db.commit()
                logger.info("Sleeper roster sync completed successfully")
                return True
                
            except Exception as e:
                db.rollback()
                logger.error(f"Database error during Sleeper roster sync: {e}")
                raise
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to sync Sleeper rosters: {e}")
            return False
    
    def sync_mfl_rosters(self) -> bool:
        """Sync rosters from MFL platform with comprehensive error handling"""
        try:
            logger.info("Starting MFL roster sync with error handling...")
            
            try:
                rosters = self.mfl_client.get_rosters()
                if not rosters:
                    logger.warning("No rosters returned from MFL API")
                    return False
            except APIError as e:
                logger.error(f"Failed to fetch MFL rosters: {e}")
                return False
            
            db = SessionLocal()
            try:
                # Clear existing MFL roster entries
                db.query(RosterEntry).filter(RosterEntry.platform == "mfl").delete()
                
                for franchise in rosters:
                    franchise_id = franchise.get("id")
                    franchise_name = franchise.get("name", f"Franchise_{franchise_id}")
                    
                    # Get player data from franchise
                    player_data = franchise.get("player", [])
                    if not isinstance(player_data, list):
                        player_data = [player_data] if player_data else []
                    
                    logger.info(f"Processing roster for {franchise_name} with {len(player_data)} players")
                    
                    # Process each player on the roster
                    for player_info in player_data:
                        player_id = player_info.get("id")
                        if player_id:
                            # Get canonical ID using mapper
                            canonical_id = self.player_mapper.get_canonical_id(mfl_id=player_id)
                            
                            # Find or create player record
                            player = db.query(Player).filter(Player.mfl_id == player_id).first()
                            if not player:
                                if canonical_id:
                                    # Get player info from mapper
                                    player_info_mapped = self.player_mapper.get_player_info(canonical_id)
                                    if player_info_mapped:
                                        player = Player(
                                            mfl_id=player_id,
                                            nfl_id=canonical_id,
                                            name=player_info_mapped.name,
                                            position=player_info_mapped.position,
                                            team=player_info_mapped.team,
                                            is_starter=self.player_mapper._is_starter_position(player_info_mapped.position)
                                        )
                                    else:
                                        # Fallback to placeholder
                                        player = Player(
                                            mfl_id=player_id,
                                            nfl_id=f"mfl_{player_id}",
                                            name=f"Player_{player_id}",
                                            position="UNKNOWN",
                                            team="UNKNOWN"
                                        )
                                else:
                                    # Create placeholder player record
                                    player = Player(
                                        mfl_id=player_id,
                                        nfl_id=f"mfl_{player_id}",
                                        name=f"Player_{player_id}",
                                        position="UNKNOWN",
                                        team="UNKNOWN"
                                    )
                                db.add(player)
                                db.flush()  # Get the ID
                            
                            # Create roster entry
                            roster_entry = RosterEntry(
                                player_id=player.id,
                                platform="mfl",
                                league_id=self.mfl_client.league_id,
                                user_id=franchise_id,
                                roster_slot=player_info.get("status", "active"),
                                is_active=True
                            )
                            db.add(roster_entry)

                            # Snapshot this roster state (idempotent)
                            try:
                                self.storage_service.upsert_roster_snapshot(
                                    platform="mfl",
                                    league_id=self.mfl_client.league_id,
                                    team_id=str(franchise_id),
                                    player_id=player.id,
                                    week=self._get_current_nfl_week(),
                                    season=self.current_season,
                                    slot=player_info.get("status", "active"),
                                )
                            except Exception as e:
                                logger.warning(f"Failed to snapshot MFL roster: {e}")
                
                db.commit()
                logger.info("MFL roster sync completed successfully")
                return True
                
            except Exception as e:
                db.rollback()
                logger.error(f"Database error during MFL roster sync: {e}")
                raise
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to sync MFL rosters: {e}")
            return False

    def _get_current_nfl_week(self) -> int:
        now = datetime.now()
        if now.month >= 9:
            return min(max(now.isocalendar()[1] - 35, 1), 18)
        else:
            return 1
    
    async def sync_all_rosters(self) -> Dict[str, bool]:
        """Sync rosters from both platforms"""
        logger.info("Starting full roster sync from both platforms...")
        
        results = {
            "sleeper": False,
            "mfl": False
        }
        
        # Sync Sleeper rosters
        try:
            results["sleeper"] = self.sync_sleeper_rosters()
        except Exception as e:
            logger.error(f"Sleeper roster sync failed: {e}")
        
        # Sync MFL rosters
        try:
            results["mfl"] = self.sync_mfl_rosters()
        except Exception as e:
            logger.error(f"MFL roster sync failed: {e}")
        
        success_count = sum(results.values())
        logger.info(f"Roster sync completed: {success_count}/2 platforms successful")
        
        return results
    
    def get_sync_statistics(self) -> Dict[str, Any]:
        """Get comprehensive sync statistics"""
        try:
            # Get roster statistics from storage service
            roster_stats = self.storage_service.get_roster_statistics()
            
            # Get player mapping statistics
            mapping_stats = self.player_mapper.get_mapping_stats()
            
            # Get recent changes
            recent_changes = self.storage_service.get_roster_changes(hours_back=24)
            
            # Get waiver states
            sleeper_waivers = self.storage_service.get_waiver_states(platform='sleeper')
            mfl_waivers = self.storage_service.get_waiver_states(platform='mfl')
            
            # Combine all statistics
            sync_stats = {
                'roster_data': roster_stats,
                'player_mapping': mapping_stats,
                'recent_changes': {
                    'count': len(recent_changes),
                    'last_24h': recent_changes[:10]  # Show last 10 changes
                },
                'waiver_states': {
                    'sleeper_count': len(sleeper_waivers),
                    'mfl_count': len(mfl_waivers)
                },
                'last_updated': datetime.utcnow().isoformat()
            }
            
            return sync_stats
            
        except Exception as e:
            logger.error(f"Failed to get sync statistics: {e}")
            return {'error': str(e)}
    
    def validate_sync_data(self) -> Dict[str, Any]:
        """Validate the integrity of synced data"""
        try:
            # Use storage service to validate data integrity
            integrity_report = self.storage_service.validate_data_integrity()
            
            # Add additional validations specific to sync
            db = SessionLocal()
            try:
                # Check for players without any roster entries
                players_without_rosters = db.query(Player).filter(
                    ~Player.id.in_(
                        db.query(RosterEntry.player_id).filter(RosterEntry.is_active == True)
                    )
                ).count()
                
                if players_without_rosters > 0:
                    integrity_report['issues'].append(
                        f"Found {players_without_rosters} players without any roster entries"
                    )
                    integrity_report['issues_found'] += 1
                
                # Check for missing platform IDs in active rosters
                active_players = db.query(Player).join(RosterEntry).filter(
                    RosterEntry.is_active == True
                ).distinct()
                
                missing_sleeper_ids = active_players.filter(
                    Player.sleeper_id.is_(None)
                ).count()
                
                missing_mfl_ids = active_players.filter(
                    Player.mfl_id.is_(None)
                ).count()
                
                if missing_sleeper_ids > 0:
                    integrity_report['issues'].append(
                        f"Found {missing_sleeper_ids} active players without Sleeper IDs"
                    )
                    integrity_report['issues_found'] += 1
                
                if missing_mfl_ids > 0:
                    integrity_report['issues'].append(
                        f"Found {missing_mfl_ids} active players without MFL IDs"
                    )
                    integrity_report['issues_found'] += 1
                
                return integrity_report
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to validate sync data: {e}")
            return {'error': str(e)}
    
    def cleanup_sync_data(self, days_old: int = 30) -> Dict[str, Any]:
        """Clean up old sync data"""
        try:
            # Use storage service to clean up old data
            cleanup_stats = self.storage_service.cleanup_old_data(days_old)
            
            # Log cleanup results
            logger.info(f"Cleanup completed: {cleanup_stats}")
            
            return cleanup_stats
            
        except Exception as e:
            logger.error(f"Failed to cleanup sync data: {e}")
            return {'error': str(e)}

# Convenience function for testing
def test_api_connections():
    """Test API connections to both platforms"""
    sleeper = SleeperAPIClient()
    mfl = MFLAPIClient()
    
    print("Testing Sleeper API connection...")
    try:
        league_info = sleeper.get_league_info()
        print(f"✓ Sleeper connection successful. League: {league_info.get('name', 'Unknown')}")
    except Exception as e:
        print(f"✗ Sleeper connection failed: {e}")
    
    print("\nTesting MFL API connection...")
    try:
        league_info = mfl.get_league_info()
        print(f"✓ MFL connection successful. League: {league_info.get('name', 'Unknown')}")
    except Exception as e:
        print(f"✗ MFL connection failed: {e}")

def test_mfl_roster_fetching():
    """Test MFL roster fetching functionality specifically"""
    print("Testing MFL roster fetching (export?TYPE=rosters)...")
    
    mfl = MFLAPIClient()
    
    try:
        # Test basic roster fetching
        rosters = mfl.get_rosters()
        print(f"✓ Successfully fetched {len(rosters)} rosters from MFL")
        
        # Test detailed roster info for each franchise
        for roster in rosters:
            franchise_id = roster.get("id")
            franchise_name = roster.get("name", f"Franchise_{franchise_id}")
            player_count = len(roster.get("player", []))
            print(f"  - {franchise_name}: {player_count} players")
            
        return True
    except Exception as e:
        print(f"✗ MFL roster fetching failed: {e}")
        return False

def test_roster_fetching():
    """Test roster fetching for both Sleeper and MFL platforms"""
    print("Testing roster fetching for both platforms...")
    print("=" * 60)
    
    # Test Sleeper roster fetching
    print("\n1. SLEEPER ROSTER FETCHING:")
    print("-" * 30)
    try:
        sleeper = SleeperAPIClient()
        rosters = sleeper.get_rosters()
        users = sleeper.get_users()
        
        # Create user lookup for better display
        user_lookup = {user["user_id"]: user for user in users}
        
        print(f"✓ Successfully fetched {len(rosters)} rosters from Sleeper")
        
        for roster in rosters:
            owner_id = roster.get("owner_id")
            user_info = user_lookup.get(owner_id, {})
            username = user_info.get("username", f"User_{owner_id}")
            player_count = len(roster.get("players", []))
            
            print(f"  - {username}: {player_count} players")
            
    except Exception as e:
        print(f"✗ Sleeper roster fetching failed: {e}")
    
    # Test MFL roster fetching
    print("\n2. MFL ROSTER FETCHING:")
    print("-" * 30)
    try:
        mfl = MFLAPIClient()
        rosters = mfl.get_rosters()
        
        print(f"✓ Successfully fetched {len(rosters)} rosters from MFL")
        
        for roster in rosters:
            franchise_id = roster.get("id")
            franchise_name = roster.get("name", f"Franchise_{franchise_id}")
            
            # Handle player data structure
            player_data = roster.get("player", [])
            if not isinstance(player_data, list):
                player_data = [player_data] if player_data else []
            
            player_count = len(player_data)
            print(f"  - {franchise_name}: {player_count} players")
            
    except Exception as e:
        print(f"✗ MFL roster fetching failed: {e}")

def test_storage_integration():
    """Test storage service integration"""
    print("Testing Storage Service Integration...")
    print("=" * 60)
    
    sync_service = RosterSyncService()
    
    try:
        # Test getting sync statistics
        print("\n1. Testing sync statistics...")
        stats = sync_service.get_sync_statistics()
        if 'error' not in stats:
            print(f"   ✓ Total roster entries: {stats['roster_data'].get('total_roster_entries', 0)}")
            print(f"   ✓ Recent changes: {stats['recent_changes']['count']}")
            print(f"   ✓ Player mappings: {stats['player_mapping'].get('total_players', 0)}")
        else:
            print(f"   ✗ Error getting statistics: {stats['error']}")
        
        # Test data validation
        print("\n2. Testing data validation...")
        validation = sync_service.validate_sync_data()
        if 'error' not in validation:
            print(f"   ✓ Data integrity issues: {validation.get('issues_found', 0)}")
            for issue in validation.get('issues', []):
                print(f"     - {issue}")
        else:
            print(f"   ✗ Error validating data: {validation['error']}")
        
        # Test storage service directly
        print("\n3. Testing storage service...")
        storage_stats = sync_service.storage_service.get_roster_statistics()
        print(f"   ✓ Storage service working: {storage_stats.get('total_roster_entries', 0)} entries")
        
        return True
        
    except Exception as e:
        print(f"   ✗ Storage integration test failed: {e}")
        return False

def find_mfl_roster_by_team_name(team_name: str) -> Dict[str, Any]:
    """Find a specific MFL roster by team name"""
    print(f"Searching for MFL roster: '{team_name}'...")
    print("=" * 60)
    
    mfl_client = MFLAPIClient()
    
    try:
        # Get all rosters
        rosters = mfl_client.get_rosters()
        print(f"Retrieved {len(rosters)} rosters from MFL")
        
        # Search for the team
        target_roster = None
        
        print(f"\nSearching through rosters:")
        for roster in rosters:
            franchise_id = roster.get("id")
            franchise_name = roster.get("name", f"Franchise_{franchise_id}")
            
            print(f"  - Checking: {franchise_name} (ID: {franchise_id})")
            
            # Case-insensitive search for team name
            if team_name.lower() in franchise_name.lower():
                target_roster = roster
                print(f"  ✅ FOUND MATCH: {franchise_name}")
                break
        
        if not target_roster:
            print(f"\n❌ Team '{team_name}' not found in MFL league")
            print("Available teams:")
            for roster in rosters:
                franchise_name = roster.get("name", f"Franchise_{roster.get('id', 'Unknown')}")
                print(f"  - {franchise_name}")
            return {}
        
        # Extract roster details
        franchise_id = target_roster.get("id")
        franchise_name = target_roster.get("name")
        player_data = target_roster.get("player", [])
        
        # Handle player data structure
        if not isinstance(player_data, list):
            player_data = [player_data] if player_data else []
        
        print(f"\n🏈 ROSTER DETAILS FOR {franchise_name.upper()}")
        print("=" * 60)
        print(f"Franchise ID: {franchise_id}")
        print(f"Team Name: {franchise_name}")
        print(f"Total Players: {len(player_data)}")
        
        if player_data:
            print(f"\nPlayer IDs:")
            for i, player_info in enumerate(player_data, 1):
                player_id = player_info.get("id", "Unknown")
                player_status = player_info.get("status", "active")
                print(f"  {i:2d}. Player ID: {player_id} (Status: {player_status})")
        else:
            print("  No players found in roster")
        
        return {
            "franchise_id": franchise_id,
            "franchise_name": franchise_name,
            "player_count": len(player_data),
            "players": player_data,
            "raw_roster": target_roster
        }
        
    except Exception as e:
        print(f"❌ Error finding MFL roster: {e}")
        return {}

def get_mfl_player_details(player_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """Get detailed player information from MFL for given player IDs"""
    print(f"\n🏈 Getting player details for {len(player_ids)} players from MFL...")
    
    mfl_client = MFLAPIClient()
    
    try:
        # Get all players from MFL
        players = mfl_client.get_players()
        print(f"Retrieved {len(players)} total players from MFL database")
        
        # Create player lookup dictionary
        player_lookup = {}
        for player in players:
            player_id = player.get("id")
            if player_id:
                player_lookup[player_id] = player
        
        # Get details for requested players
        player_details = {}
        found_count = 0
        
        print(f"\nPlayer Details:")
        print("-" * 50)
        
        for i, player_id in enumerate(player_ids, 1):
            if player_id in player_lookup:
                player_data = player_lookup[player_id]
                name = player_data.get("name", f"Player_{player_id}")
                position = player_data.get("position", "UNK")
                team = player_data.get("team", "FA")
                
                player_details[player_id] = {
                    "name": name,
                    "position": position,
                    "team": team,
                    "mfl_data": player_data
                }
                
                print(f"  {i:2d}. {position:3s}: {name:25s} ({team}) [ID: {player_id}]")
                found_count += 1
            else:
                player_details[player_id] = {
                    "name": f"Unknown_Player_{player_id}",
                    "position": "UNK",
                    "team": "FA",
                    "mfl_data": {}
                }
                print(f"  {i:2d}. UNK: Unknown_Player_{player_id:12s} (FA) [ID: {player_id}] - NOT FOUND")
        
        print(f"\nSummary: Found details for {found_count}/{len(player_ids)} players")
        return player_details
        
    except Exception as e:
        print(f"❌ Error getting MFL player details: {e}")
        return {}

def get_puntersfordays_roster():
    """Convenience function to get PUNTERSFORDAYS roster specifically"""
    print("🏈 GETTING PUNTERSFORDAYS MFL ROSTER")
    print("=" * 60)
    
    # Find the roster
    roster_info = find_mfl_roster_by_team_name("PUNTERSFORDAYS")
    
    if not roster_info:
        return None
    
    # Get player details
    player_data = roster_info.get("players", [])
    if player_data:
        player_ids = [p.get("id") for p in player_data if p.get("id")]
        if player_ids:
            player_details = get_mfl_player_details(player_ids)
            roster_info["player_details"] = player_details
    
    return roster_info

def test_error_handling():
    """Test error handling and retry logic"""
    print("Testing Error Handling and Retry Logic...")
    print("=" * 60)
    
    try:
        # Import retry handler to test
        from src.utils.retry_handler import get_retry_statistics
        
        # Test getting retry statistics
        print("\n1. Testing retry statistics...")
        stats = get_retry_statistics()
        
        for platform, platform_stats in stats.items():
            print(f"   {platform.upper()}:")
            print(f"     Total attempts: {platform_stats.get('total_attempts', 0)}")
            print(f"     Successful retries: {platform_stats.get('successful_retries', 0)}")
            print(f"     Failed retries: {platform_stats.get('failed_retries', 0)}")
        
        # Test API clients with retry logic
        print("\n2. Testing API clients with retry logic...")
        sleeper_client = SleeperAPIClient()
        mfl_client = MFLAPIClient()
        
        # Test Sleeper API (should work with retry logic)
        try:
            league_info = sleeper_client.get_league_info()
            print("   ✓ Sleeper API call with retry logic successful")
        except Exception as e:
            print(f"   ✗ Sleeper API call failed: {e}")
        
        # Test MFL API (should work with retry logic)
        try:
            league_info = mfl_client.get_league_info()
            print("   ✓ MFL API call with retry logic successful")
        except Exception as e:
            print(f"   ✗ MFL API call failed: {e}")
        
        # Test RosterSyncService with error handling
        print("\n3. Testing RosterSyncService with error handling...")
        sync_service = RosterSyncService()
        
        try:
            # Test sync with error handling
            sleeper_result = sync_service.sync_sleeper_rosters()
            mfl_result = sync_service.sync_mfl_rosters()
            
            print(f"   Sleeper sync: {'SUCCESS' if sleeper_result else 'FAILED'}")
            print(f"   MFL sync: {'SUCCESS' if mfl_result else 'FAILED'}")
            
        except Exception as e:
            print(f"   ✗ Sync service test failed: {e}")
        
        return True
        
    except Exception as e:
        print(f"   ✗ Error handling test failed: {e}")
        return False

if __name__ == "__main__":
    # Test API connections
    test_api_connections()
    
    print("\n" + "="*60)
    
    # Test roster fetching for both platforms
    test_roster_fetching()
    
    print("\n" + "="*60)
    
    # Test PUNTERSFORDAYS specific roster
    get_puntersfordays_roster()
    
    print("\n" + "="*60)
    
    # Test storage integration
    test_storage_integration()
    
    print("\n" + "="*60)
    
    # Test error handling
    test_error_handling()
