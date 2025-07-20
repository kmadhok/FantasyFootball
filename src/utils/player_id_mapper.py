import logging
import hashlib
from typing import Dict, Optional, Any, List
from dataclasses import dataclass
from ..services.roster_sync import SleeperAPIClient, MFLAPIClient
from ..database import SessionLocal, Player

logger = logging.getLogger(__name__)

@dataclass
class PlayerInfo:
    """Unified player information across platforms"""
    canonical_id: str
    name: str
    position: str
    team: str
    sleeper_id: Optional[str] = None
    mfl_id: Optional[str] = None
    active: bool = True

class PlayerIDMapper:
    """Maps platform-specific player IDs to canonical NFL IDs"""
    
    def __init__(self):
        self.sleeper_client = SleeperAPIClient()
        self.mfl_client = MFLAPIClient()
        self._player_cache: Dict[str, PlayerInfo] = {}
        
    def generate_canonical_id(self, name: str, position: str, team: str) -> str:
        """Generate a canonical NFL ID based on player attributes"""
        # Normalize inputs
        name_norm = name.strip().upper().replace(".", "").replace("'", "")
        position_norm = position.strip().upper()
        team_norm = team.strip().upper()
        
        # Create unique identifier
        identifier = f"{name_norm}_{position_norm}_{team_norm}"
        
        # Generate hash-based ID for consistency
        hash_obj = hashlib.md5(identifier.encode())
        canonical_id = f"NFL_{hash_obj.hexdigest()[:8].upper()}"
        
        return canonical_id
    
    def normalize_player_name(self, name: str) -> str:
        """Normalize player name for matching"""
        if not name:
            return ""
        
        # Remove common suffixes and normalize
        name = name.strip()
        suffixes = [" Jr.", " Sr.", " III", " II", " IV"]
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
                break
        
        # Handle common name variations
        name_mappings = {
            "DJ": "D.J.",
            "AJ": "A.J.",
            "TJ": "T.J.",
            "CJ": "C.J.",
            "JJ": "J.J.",
        }
        
        for old, new in name_mappings.items():
            if name.startswith(old + " "):
                name = name.replace(old + " ", new + " ", 1)
        
        return name.strip()
    
    def normalize_position(self, position: str) -> str:
        """Normalize position for consistency"""
        if not position:
            return "UNKNOWN"
        
        position = position.strip().upper()
        
        # Position mappings
        position_mappings = {
            "QB": "QB",
            "RB": "RB", 
            "WR": "WR",
            "TE": "TE",
            "K": "K",
            "DEF": "DEF",
            "D/ST": "DEF",
            "DST": "DEF",
            "FLEX": "FLEX",
            "BN": "BN",
            "IR": "IR"
        }
        
        return position_mappings.get(position, position)
    
    def normalize_team(self, team: str) -> str:
        """Normalize team name for consistency"""
        if not team:
            return "UNKNOWN"
        
        team = team.strip().upper()
        
        # Team mappings for consistency
        team_mappings = {
            "JAX": "JAC",
            "LV": "LAS",
            "WSH": "WAS",
        }
        
        return team_mappings.get(team, team)
    
    def fetch_sleeper_players(self) -> Dict[str, Dict[str, Any]]:
        """Fetch all players from Sleeper API"""
        try:
            logger.info("Fetching player data from Sleeper...")
            players = self.sleeper_client.get_players()
            logger.info(f"Successfully fetched {len(players)} players from Sleeper")
            return players
        except Exception as e:
            logger.error(f"Failed to fetch Sleeper players: {e}")
            return {}
    
    def fetch_mfl_players(self) -> List[Dict[str, Any]]:
        """Fetch all players from MFL API"""
        try:
            logger.info("Fetching player data from MFL...")
            players = self.mfl_client.get_players()
            logger.info(f"Successfully fetched {len(players)} players from MFL")
            return players
        except Exception as e:
            logger.error(f"Failed to fetch MFL players: {e}")
            return []
    
    def create_player_mapping(self) -> Dict[str, PlayerInfo]:
        """Create comprehensive player ID mapping"""
        logger.info("Creating player ID mapping...")
        
        # Fetch players from both platforms
        sleeper_players = self.fetch_sleeper_players()
        mfl_players = self.fetch_mfl_players()
        
        player_mapping = {}
        
        # Process Sleeper players
        for sleeper_id, player_data in sleeper_players.items():
            if not player_data:
                continue
                
            name = self.normalize_player_name(player_data.get("full_name", ""))
            position = self.normalize_position(player_data.get("position", ""))
            team = self.normalize_team(player_data.get("team", ""))
            active = player_data.get("active", True)
            
            if name and position and team:
                canonical_id = self.generate_canonical_id(name, position, team)
                
                player_info = PlayerInfo(
                    canonical_id=canonical_id,
                    name=name,
                    position=position,
                    team=team,
                    sleeper_id=sleeper_id,
                    active=active
                )
                
                player_mapping[canonical_id] = player_info
        
        # Process MFL players and match with existing
        for mfl_player in mfl_players:
            name = self.normalize_player_name(mfl_player.get("name", ""))
            position = self.normalize_position(mfl_player.get("position", ""))
            team = self.normalize_team(mfl_player.get("team", ""))
            mfl_id = mfl_player.get("id")
            
            if name and position and team and mfl_id:
                canonical_id = self.generate_canonical_id(name, position, team)
                
                if canonical_id in player_mapping:
                    # Update existing player with MFL ID
                    player_mapping[canonical_id].mfl_id = mfl_id
                else:
                    # Create new player entry
                    player_info = PlayerInfo(
                        canonical_id=canonical_id,
                        name=name,
                        position=position,
                        team=team,
                        mfl_id=mfl_id,
                        active=True
                    )
                    player_mapping[canonical_id] = player_info
        
        logger.info(f"Created mapping for {len(player_mapping)} players")
        self._player_cache = player_mapping
        return player_mapping
    
    def get_canonical_id(self, sleeper_id: str = None, mfl_id: str = None) -> Optional[str]:
        """Get canonical ID for a player given platform-specific ID"""
        if not self._player_cache:
            self.create_player_mapping()
        
        for canonical_id, player_info in self._player_cache.items():
            if sleeper_id and player_info.sleeper_id == sleeper_id:
                return canonical_id
            if mfl_id and player_info.mfl_id == mfl_id:
                return canonical_id
        
        return None
    
    def get_player_info(self, canonical_id: str) -> Optional[PlayerInfo]:
        """Get player information by canonical ID"""
        if not self._player_cache:
            self.create_player_mapping()
        
        return self._player_cache.get(canonical_id)
    
    def sync_players_to_database(self) -> bool:
        """Sync player mappings to database"""
        try:
            logger.info("Syncing player mappings to database...")
            
            if not self._player_cache:
                self.create_player_mapping()
            
            db = SessionLocal()
            try:
                # Clear existing players
                db.query(Player).delete()
                
                # Add all mapped players
                for player_info in self._player_cache.values():
                    player = Player(
                        nfl_id=player_info.canonical_id,
                        sleeper_id=player_info.sleeper_id,
                        mfl_id=player_info.mfl_id,
                        name=player_info.name,
                        position=player_info.position,
                        team=player_info.team,
                        is_starter=self._is_starter_position(player_info.position)
                    )
                    db.add(player)
                
                db.commit()
                logger.info(f"Successfully synced {len(self._player_cache)} players to database")
                return True
                
            except Exception as e:
                db.rollback()
                logger.error(f"Database error during player sync: {e}")
                raise
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to sync players to database: {e}")
            return False
    
    def _is_starter_position(self, position: str) -> bool:
        """Determine if position is typically a starter"""
        starter_positions = {"QB", "RB", "WR", "TE", "K", "DEF"}
        return position in starter_positions
    
    def find_player_by_name(self, name: str, position: str = None, team: str = None) -> Optional[PlayerInfo]:
        """Find player by name with optional position and team filters"""
        if not self._player_cache:
            self.create_player_mapping()
        
        normalized_name = self.normalize_player_name(name)
        
        for player_info in self._player_cache.values():
            if player_info.name == normalized_name:
                if position and player_info.position != self.normalize_position(position):
                    continue
                if team and player_info.team != self.normalize_team(team):
                    continue
                return player_info
        
        return None
    
    def get_mapping_stats(self) -> Dict[str, int]:
        """Get statistics about the player mapping"""
        if not self._player_cache:
            self.create_player_mapping()
        
        stats = {
            "total_players": len(self._player_cache),
            "sleeper_only": 0,
            "mfl_only": 0,
            "both_platforms": 0,
            "active_players": 0
        }
        
        for player_info in self._player_cache.values():
            if player_info.sleeper_id and player_info.mfl_id:
                stats["both_platforms"] += 1
            elif player_info.sleeper_id:
                stats["sleeper_only"] += 1
            elif player_info.mfl_id:
                stats["mfl_only"] += 1
            
            if player_info.active:
                stats["active_players"] += 1
        
        return stats

# Convenience functions for easy usage
def create_player_mapping() -> Dict[str, PlayerInfo]:
    """Create and return player ID mapping"""
    mapper = PlayerIDMapper()
    return mapper.create_player_mapping()

def get_canonical_id(sleeper_id: str = None, mfl_id: str = None) -> Optional[str]:
    """Get canonical ID for a player"""
    mapper = PlayerIDMapper()
    return mapper.get_canonical_id(sleeper_id=sleeper_id, mfl_id=mfl_id)

def sync_players_to_database() -> bool:
    """Sync all player mappings to database"""
    mapper = PlayerIDMapper()
    return mapper.sync_players_to_database()

# Testing function
def test_player_mapping():
    """Test the player ID mapping system"""
    print("Testing Player ID Mapping System...")
    print("=" * 50)
    
    mapper = PlayerIDMapper()
    
    # Create mapping
    print("\n1. Creating player mapping...")
    mapping = mapper.create_player_mapping()
    
    # Get statistics
    stats = mapper.get_mapping_stats()
    print(f"\n2. Mapping Statistics:")
    print(f"   Total players: {stats['total_players']}")
    print(f"   Both platforms: {stats['both_platforms']}")
    print(f"   Sleeper only: {stats['sleeper_only']}")
    print(f"   MFL only: {stats['mfl_only']}")
    print(f"   Active players: {stats['active_players']}")
    
    # Test some examples
    print(f"\n3. Sample mappings:")
    count = 0
    for canonical_id, player_info in mapping.items():
        if count >= 5:  # Show first 5 examples
            break
        print(f"   {player_info.name} ({player_info.position}, {player_info.team})")
        print(f"     Canonical ID: {canonical_id}")
        print(f"     Sleeper ID: {player_info.sleeper_id}")
        print(f"     MFL ID: {player_info.mfl_id}")
        print()
        count += 1
    
    # Test database sync
    print("4. Testing database sync...")
    try:
        success = mapper.sync_players_to_database()
        if success:
            print("   ✓ Database sync successful")
        else:
            print("   ✗ Database sync failed")
    except Exception as e:
        print(f"   ✗ Database sync error: {e}")

if __name__ == "__main__":
    test_player_mapping()