import logging
import hashlib
from typing import Dict, Optional, Any, List
from dataclasses import dataclass
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
    espn_id: Optional[str] = None
    active: bool = True

class PlayerIDMapper:
    """Maps platform-specific player IDs to canonical NFL IDs"""
    
    def __init__(self):
        self._player_cache: Dict[str, PlayerInfo] = {}
        self.id_mappings: Dict[str, PlayerInfo] = {}
        self.sleeper_to_canonical: Dict[str, str] = {}
        self.mfl_to_canonical: Dict[str, str] = {}
        self.espn_to_canonical: Dict[str, str] = {}
        
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
    
    
    def create_player_mapping(self, sleeper_players: Dict[str, Dict[str, Any]] = None, 
                            mfl_players: List[Dict[str, Any]] = None) -> Dict[str, PlayerInfo]:
        """Create comprehensive player ID mapping from provided data"""
        logger.info("Creating player ID mapping...")
        
        # Use provided data or empty defaults
        sleeper_players = sleeper_players or {}
        mfl_players = mfl_players or []
        
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
    
    def get_canonical_id(self, sleeper_id: str = None, mfl_id: str = None, espn_id: str = None,
                       name: str = None, position: str = None, team: str = None) -> Optional[str]:
        """Get canonical ID for a player given platform-specific ID or attributes"""
        # First try to find from internal mappings
        if sleeper_id and sleeper_id in self.sleeper_to_canonical:
            return self.sleeper_to_canonical[sleeper_id]
        
        if mfl_id and mfl_id in self.mfl_to_canonical:
            return self.mfl_to_canonical[mfl_id]
        
        if espn_id and espn_id in self.espn_to_canonical:
            return self.espn_to_canonical[espn_id]
        
        # Try to find from database
        try:
            db = SessionLocal()
            try:
                if sleeper_id:
                    player = db.query(Player).filter(Player.sleeper_id == sleeper_id).first()
                    if player:
                        self.sleeper_to_canonical[sleeper_id] = player.nfl_id
                        return player.nfl_id
                
                if mfl_id:
                    player = db.query(Player).filter(Player.mfl_id == mfl_id).first()
                    if player:
                        self.mfl_to_canonical[mfl_id] = player.nfl_id
                        return player.nfl_id
                
                if espn_id:
                    player = db.query(Player).filter(Player.espn_id == espn_id).first()
                    if player:
                        self.espn_to_canonical[espn_id] = player.nfl_id
                        return player.nfl_id
                
                # If name, position, team provided, generate canonical ID
                if name and position and team:
                    return self.generate_canonical_id(name, position, team)
                    
            finally:
                db.close()
        except Exception as e:
            logger.error(f"Database error in get_canonical_id: {e}")
        
        return None
    
    def get_player_info(self, canonical_id: str) -> Optional[PlayerInfo]:
        """Get player information by canonical ID"""
        # Check cache first
        if canonical_id in self.id_mappings:
            return self.id_mappings[canonical_id]
        
        # Try database
        try:
            db = SessionLocal()
            try:
                player = db.query(Player).filter(Player.nfl_id == canonical_id).first()
                if player:
                    player_info = PlayerInfo(
                        canonical_id=player.nfl_id,
                        name=player.name,
                        position=player.position,
                        team=player.team,
                        sleeper_id=player.sleeper_id,
                        mfl_id=player.mfl_id,
                        espn_id=player.espn_id
                    )
                    self.id_mappings[canonical_id] = player_info
                    return player_info
            finally:
                db.close()
        except Exception as e:
            logger.error(f"Database error in get_player_info: {e}")
        
        return None
    
    def add_player_mapping(self, player_info: PlayerInfo) -> Optional[str]:
        """Add a player mapping to the database"""
        try:
            # Generate canonical ID if not provided
            canonical_id = self.generate_canonical_id(
                player_info.name, 
                player_info.position, 
                player_info.team
            )
            
            db = SessionLocal()
            try:
                # Check if player already exists
                existing_player = db.query(Player).filter(
                    Player.nfl_id == canonical_id
                ).first()
                
                if existing_player:
                    # Update existing player
                    if player_info.sleeper_id:
                        existing_player.sleeper_id = player_info.sleeper_id
                    if player_info.mfl_id:
                        existing_player.mfl_id = player_info.mfl_id
                    if player_info.espn_id:
                        existing_player.espn_id = player_info.espn_id
                    db.commit()
                    logger.debug(f"Updated existing player: {player_info.name}")
                else:
                    # Create new player
                    player = Player(
                        nfl_id=canonical_id,
                        sleeper_id=player_info.sleeper_id,
                        mfl_id=player_info.mfl_id,
                        espn_id=player_info.espn_id,
                        name=player_info.name,
                        position=player_info.position,
                        team=player_info.team,
                        is_starter=self._is_starter_position(player_info.position)
                    )
                    db.add(player)
                    db.commit()
                    logger.debug(f"Added new player: {player_info.name}")
                
                # Update internal mappings
                self.id_mappings[canonical_id] = player_info
                if player_info.sleeper_id:
                    self.sleeper_to_canonical[player_info.sleeper_id] = canonical_id
                if player_info.mfl_id:
                    self.mfl_to_canonical[player_info.mfl_id] = canonical_id
                if player_info.espn_id:
                    self.espn_to_canonical[player_info.espn_id] = canonical_id
                
                return canonical_id
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to add player mapping: {e}")
            return None
    
    def load_from_database(self):
        """Load all player mappings from database"""
        try:
            db = SessionLocal()
            try:
                players = db.query(Player).all()
                
                for player in players:
                    player_info = PlayerInfo(
                        canonical_id=player.nfl_id,
                        name=player.name,
                        position=player.position,
                        team=player.team,
                        sleeper_id=player.sleeper_id,
                        mfl_id=player.mfl_id,
                        espn_id=player.espn_id
                    )
                    
                    self.id_mappings[player.nfl_id] = player_info
                    if player.sleeper_id:
                        self.sleeper_to_canonical[player.sleeper_id] = player.nfl_id
                    if player.mfl_id:
                        self.mfl_to_canonical[player.mfl_id] = player.nfl_id
                    if player.espn_id:
                        self.espn_to_canonical[player.espn_id] = player.nfl_id
                
                logger.info(f"Loaded {len(players)} players from database")
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to load players from database: {e}")
    
    def get_mapping_stats(self) -> Dict[str, Any]:
        """Get statistics about current mappings"""
        return {
            "total_players": len(self.id_mappings),
            "sleeper_mappings": len(self.sleeper_to_canonical),
            "mfl_mappings": len(self.mfl_to_canonical),
            "espn_mappings": len(self.espn_to_canonical),
            "cross_platform_mappings": len([
                pid for pid, info in self.id_mappings.items() 
                if sum([bool(info.sleeper_id), bool(info.mfl_id), bool(info.espn_id)]) >= 2
            ])
        }
    
    def sync_players_to_database(self, sleeper_players: Dict[str, Dict[str, Any]] = None, 
                               mfl_players: List[Dict[str, Any]] = None) -> bool:
        """Sync player mappings to database"""
        try:
            logger.info("Syncing player mappings to database...")
            
            # Create mapping if data provided, otherwise use existing cache
            if sleeper_players or mfl_players:
                self.create_player_mapping(sleeper_players, mfl_players)
            elif not self._player_cache:
                logger.warning("No player data available for database sync")
                return False
            
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
                        espn_id=player_info.espn_id,
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
def create_player_mapping(sleeper_players: Dict[str, Dict[str, Any]] = None, 
                        mfl_players: List[Dict[str, Any]] = None) -> Dict[str, PlayerInfo]:
    """Create and return player ID mapping"""
    mapper = PlayerIDMapper()
    return mapper.create_player_mapping(sleeper_players, mfl_players)

def get_canonical_id(sleeper_id: str = None, mfl_id: str = None, espn_id: str = None,
                   name: str = None, position: str = None, team: str = None) -> Optional[str]:
    """Get canonical ID for a player"""
    mapper = PlayerIDMapper()
    return mapper.get_canonical_id(sleeper_id=sleeper_id, mfl_id=mfl_id, espn_id=espn_id,
                                 name=name, position=position, team=team)

def sync_players_to_database(sleeper_players: Dict[str, Dict[str, Any]] = None, 
                           mfl_players: List[Dict[str, Any]] = None) -> int:
    """Sync player mappings to database with provided data"""
    mapper = PlayerIDMapper()
    
    # If no data provided, we can't sync
    if not sleeper_players and not mfl_players:
        mapper.load_from_database()  # Load existing data
        return len(mapper.id_mappings)
    
    success = mapper.sync_players_to_database(sleeper_players, mfl_players)
    return len(mapper.id_mappings) if success else 0

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