import logging
import hashlib
from typing import Dict, Optional, Any, List
from dataclasses import dataclass
import pandas as pd
import nfl_data_py as nfl
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
    # Additional professional IDs from nfl_data_py
    pfr_id: Optional[str] = None      # Pro Football Reference
    gsis_id: Optional[str] = None     # NFL GSIS system
    yahoo_id: Optional[str] = None    # Yahoo Fantasy
    # Enhanced metadata
    birthdate: Optional[str] = None
    merge_name: Optional[str] = None  # Normalized name for matching
    draft_year: Optional[int] = None
    # Status flags
    active: bool = True
    from_nfl_data_py: bool = False    # Indicates if seeded from crosswalk

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
        """Normalize player name for matching across platforms"""
        if not name:
            return ""
        
        name = name.strip()
        
        # Handle Last, First format conversion (MFL → Sleeper format)
        if ", " in name and not name.startswith("Team") and not name.startswith("Bills"):
            # Handle cases like "Smith Jr., John" or "Van Der Berg, Kyle"
            parts = name.split(", ", 1)
            if len(parts) == 2:
                last_name = parts[0].strip()
                first_part = parts[1].strip()
                
                # Check if last name has suffix that should stay with last name
                last_name_suffixes = ["Jr", "Sr", "III", "II", "IV", "V"]
                for suffix in last_name_suffixes:
                    if last_name.endswith(" " + suffix):
                        # Move suffix to end: "Smith Jr., John" → "John Smith Jr."
                        last_name_base = last_name.replace(" " + suffix, "")
                        name = f"{first_part} {last_name_base} {suffix}"
                        break
                else:
                    # Standard case: "Allen, Josh" → "Josh Allen"
                    name = f"{first_part} {last_name}"
        
        # Remove common suffixes temporarily for consistent processing
        suffix_removed = ""
        suffixes = [" Jr.", " Sr.", " III", " II", " IV", " V", " Jr", " Sr"]
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
                suffix_removed = suffix.replace(" Jr", " Jr.").replace(" Sr", " Sr.")  # Normalize dots
                break
        
        # Handle common name variations and abbreviations
        name_mappings = {
            "DJ": "D.J.",
            "AJ": "A.J.", 
            "TJ": "T.J.",
            "CJ": "C.J.",
            "JJ": "J.J.",
            "RJ": "R.J.",
            "BJ": "B.J.",
            "PJ": "P.J.",
            "MJ": "M.J.",
        }
        
        # Apply name mappings at the start of names
        for old, new in name_mappings.items():
            if name.startswith(old + " "):
                name = name.replace(old + " ", new + " ", 1)
                break
        
        # Clean up extra spaces and punctuation  
        name = " ".join(name.split())  # Remove extra whitespace
        name = name.replace(".  ", ".").replace("  ", " ")  # Normalize periods without adding extra spaces
        
        # Add suffix back
        if suffix_removed:
            name = name + suffix_removed
            
        return name.strip()
    
    def normalize_position(self, position: str) -> str:
        """Normalize position for consistency across platforms"""
        if not position:
            return "UNKNOWN"
        
        position = position.strip().upper()
        
        # Comprehensive position mappings including MFL-specific codes
        position_mappings = {
            # Standard fantasy positions
            "QB": "QB",
            "RB": "RB", 
            "WR": "WR",
            "TE": "TE",
            "K": "K",
            "PK": "K",     # Place kicker
            "DEF": "DEF",
            "D/ST": "DEF",
            "DST": "DEF",
            "FLEX": "FLEX",
            "BN": "BN",
            "IR": "IR",
            
            # MFL-specific position codes
            "TMWR": "DEF",     # Team Defense/Special Teams (MFL)
            "TEAM": "DEF",     # Team Defense
            "DT": "DL",        # Defensive Tackle → Defensive Line
            "DE": "DL",        # Defensive End → Defensive Line  
            "DL": "DL",        # Defensive Line
            "LB": "LB",        # Linebacker
            "ILB": "LB",       # Inside Linebacker
            "OLB": "LB",       # Outside Linebacker
            "CB": "DB",        # Cornerback → Defensive Back
            "S": "DB",         # Safety → Defensive Back
            "FS": "DB",        # Free Safety → Defensive Back
            "SS": "DB",        # Strong Safety → Defensive Back
            "DB": "DB",        # Defensive Back
            
            # Offensive line positions (usually not fantasy relevant)
            "C": "OL",         # Center → Offensive Line
            "G": "OL",         # Guard → Offensive Line
            "T": "OL",         # Tackle → Offensive Line
            "OL": "OL",        # Offensive Line
            "OT": "OL",        # Offensive Tackle → Offensive Line
            "OG": "OL",        # Offensive Guard → Offensive Line
            
            # Special teams and other positions
            "P": "P",          # Punter
            "LS": "LS",        # Long Snapper
            "FB": "FB",        # Fullback
            "H": "P",          # Holder (usually punter)
            "NT": "DL",        # Nose Tackle → Defensive Line
            
            # Position groups for IDP (Individual Defensive Players)
            "DL": "DL",
            "LB": "LB", 
            "DB": "DB",
            
            # Special roster designations
            "BENCH": "BN",
            "RESERVE": "IR",
            "INJURED": "IR",
            "PRACTICE": "PRACTICE",
            "TAXI": "TAXI"
        }
        
        # Handle compound positions like "RB/WR" or "QB/RB" 
        if "/" in position:
            # Take the first position for primary classification
            primary_position = position.split("/")[0].strip()
            mapped_position = position_mappings.get(primary_position, primary_position)
            return mapped_position
        
        return position_mappings.get(position, position)
    
    def normalize_team(self, team: str) -> str:
        """Normalize team name for consistency across platforms"""
        if not team:
            return "UNKNOWN"
        
        team = team.strip().upper()
        
        # Handle full team names to abbreviations
        full_name_mappings = {
            "ARIZONA CARDINALS": "ARI",
            "ATLANTA FALCONS": "ATL", 
            "BALTIMORE RAVENS": "BAL",
            "BUFFALO BILLS": "BUF",
            "CAROLINA PANTHERS": "CAR",
            "CHICAGO BEARS": "CHI",
            "CINCINNATI BENGALS": "CIN",
            "CLEVELAND BROWNS": "CLE",
            "DALLAS COWBOYS": "DAL",
            "DENVER BRONCOS": "DEN",
            "DETROIT LIONS": "DET",
            "GREEN BAY PACKERS": "GB",
            "HOUSTON TEXANS": "HOU",
            "INDIANAPOLIS COLTS": "IND",
            "JACKSONVILLE JAGUARS": "JAC",
            "KANSAS CITY CHIEFS": "KC",
            "LAS VEGAS RAIDERS": "LV",
            "LOS ANGELES CHARGERS": "LAC",
            "LOS ANGELES RAMS": "LAR",
            "MIAMI DOLPHINS": "MIA",
            "MINNESOTA VIKINGS": "MIN",
            "NEW ENGLAND PATRIOTS": "NE",
            "NEW ORLEANS SAINTS": "NO",
            "NEW YORK GIANTS": "NYG",
            "NEW YORK JETS": "NYJ",
            "PHILADELPHIA EAGLES": "PHI",
            "PITTSBURGH STEELERS": "PIT",
            "SAN FRANCISCO 49ERS": "SF",
            "SEATTLE SEAHAWKS": "SEA",
            "TAMPA BAY BUCCANEERS": "TB",
            "TENNESSEE TITANS": "TEN",
            "WASHINGTON COMMANDERS": "WAS"
        }
        
        # Check full names first
        if team in full_name_mappings:
            return full_name_mappings[team]
        
        # Team abbreviation mappings for consistency between platforms
        team_mappings = {
            # Standard variations
            "JAX": "JAC",  # Jacksonville  
            "LV": "LAS",   # Las Vegas (some platforms use LAS)
            "LAS": "LV",   # Normalize to LV
            "WSH": "WAS",  # Washington
            "GBP": "GB",   # Green Bay
            "KCC": "KC",   # Kansas City
            "LAR": "LAR",  # Los Angeles Rams
            "LAC": "LAC",  # Los Angeles Chargers
            "NEP": "NE",   # New England
            "NOS": "NO",   # New Orleans  
            "SFO": "SF",   # San Francisco
            "TBB": "TB",   # Tampa Bay
            "LVR": "LV",   # Las Vegas Raiders
            # Common alternate forms
            "JAGUARS": "JAC",
            "RAIDERS": "LV",
            "CHIEFS": "KC",
            "PACKERS": "GB",
            "PATRIOTS": "NE",
            "SAINTS": "NO",
            "49ERS": "SF",
            "BUCS": "TB",
            "BUCCANEERS": "TB",
            "COMMANDERS": "WAS"
        }
        
        return team_mappings.get(team, team)
    
    def load_nfl_data_py_crosswalk(self) -> pd.DataFrame:
        """
        Load NFL player ID crosswalk from nfl_data_py
        
        This provides battle-tested cross-platform ID mapping used by major fantasy sites.
        Returns DataFrame with sleeper_id, mfl_id, espn_id, pfr_id, gsis_id, etc.
        """
        try:
            logger.info("Loading NFL player ID crosswalk from nfl_data_py...")
            
            # Import the comprehensive player ID crosswalk
            ids_df = nfl.import_ids()
            
            logger.info(f"✓ Loaded {len(ids_df):,} players from nfl_data_py crosswalk")
            
            # Log coverage statistics
            key_platforms = ['sleeper_id', 'mfl_id', 'espn_id', 'pfr_id', 'gsis_id']
            for platform in key_platforms:
                if platform in ids_df.columns:
                    coverage = ids_df[platform].notna().sum()
                    pct = (coverage / len(ids_df)) * 100
                    logger.info(f"  {platform}: {coverage:,} players ({pct:.1f}% coverage)")
            
            # Count cross-platform matches
            cross_platform = ids_df[
                (ids_df['sleeper_id'].notna()) & 
                (ids_df['mfl_id'].notna())
            ]
            logger.info(f"  Cross-platform (Sleeper+MFL): {len(cross_platform):,} pre-matched players")
            
            return ids_df
            
        except Exception as e:
            logger.error(f"Failed to load nfl_data_py crosswalk: {e}")
            # Return empty DataFrame with expected columns if import fails
            return pd.DataFrame(columns=['name', 'position', 'team', 'sleeper_id', 'mfl_id', 'espn_id', 'pfr_id', 'gsis_id', 'yahoo_id', 'birthdate', 'merge_name'])
    
    def create_player_from_crosswalk(self, row: pd.Series) -> PlayerInfo:
        """Convert nfl_data_py crosswalk row to PlayerInfo"""
        try:
            # Use merge_name if available, fallback to name
            display_name = row.get('merge_name', row.get('name', ''))
            if pd.isna(display_name) or not display_name:
                display_name = row.get('name', '')
            
            # Handle team normalization  
            team = self.normalize_team(str(row.get('team', '')))
            position = self.normalize_position(str(row.get('position', '')))
            
            # Generate canonical ID
            canonical_id = self.generate_canonical_id(display_name, position, team)
            
            # Extract platform IDs (handle NaN values)
            def safe_str(val):
                return str(int(val)) if pd.notna(val) and val != '' else None
            
            player_info = PlayerInfo(
                canonical_id=canonical_id,
                name=display_name,
                position=position,
                team=team,
                sleeper_id=safe_str(row.get('sleeper_id')),
                mfl_id=str(row.get('mfl_id', '')) if pd.notna(row.get('mfl_id')) else None,
                espn_id=safe_str(row.get('espn_id')),
                pfr_id=str(row.get('pfr_id', '')) if pd.notna(row.get('pfr_id')) else None,
                gsis_id=str(row.get('gsis_id', '')) if pd.notna(row.get('gsis_id')) else None,
                yahoo_id=safe_str(row.get('yahoo_id')),
                birthdate=str(row.get('birthdate', '')) if pd.notna(row.get('birthdate')) else None,
                merge_name=str(row.get('merge_name', '')) if pd.notna(row.get('merge_name')) else None,
                draft_year=int(row.get('draft_year', 0)) if pd.notna(row.get('draft_year')) and row.get('draft_year', 0) > 0 else None,
                active=True,
                from_nfl_data_py=True
            )
            
            return player_info
            
        except Exception as e:
            logger.warning(f"Failed to create PlayerInfo from crosswalk row: {e}")
            return None
    
    def _is_team_defense_entry(self, name: str, position: str) -> bool:
        """Filter out team defense and organizational entries"""
        if not name:
            return True
            
        name_upper = name.upper()
        
        # Team defense patterns
        team_defense_patterns = [
            "BILLS, BUFFALO",
            "PATRIOTS, NEW ENGLAND", 
            "DOLPHINS, MIAMI",
            "JETS, NEW YORK",
            "RAVENS, BALTIMORE",
            "BROWNS, CLEVELAND",
            "STEELERS, PITTSBURGH",
            "BENGALS, CINCINNATI",
            "TEXANS, HOUSTON",
            "COLTS, INDIANAPOLIS",
            "TITANS, TENNESSEE",
            "JAGUARS, JACKSONVILLE",
            "CHIEFS, KANSAS CITY",
            "RAIDERS, LAS VEGAS",
            "CHARGERS, LOS ANGELES",
            "BRONCOS, DENVER"
        ]
        
        # Check for specific team defense names
        for pattern in team_defense_patterns:
            if pattern in name_upper:
                return True
        
        # Check for general team patterns
        if (name_upper.startswith("TEAM ") or 
            ", " in name and any(city in name_upper for city in ["BUFFALO", "NEW ENGLAND", "MIAMI", "BALTIMORE", "CLEVELAND", "PITTSBURGH", "CINCINNATI", "HOUSTON", "INDIANAPOLIS", "TENNESSEE", "JACKSONVILLE", "KANSAS CITY", "LAS VEGAS", "LOS ANGELES", "DENVER", "CHICAGO", "DETROIT", "GREEN BAY", "MINNESOTA", "ATLANTA", "CAROLINA", "NEW ORLEANS", "TAMPA BAY", "ARIZONA", "LOS ANGELES", "SAN FRANCISCO", "SEATTLE", "DALLAS", "NEW YORK", "PHILADELPHIA", "WASHINGTON"])):
            return True
        
        # Check position-based filtering
        if position in ["TMWR", "TEAM", "DEF"] and not any(typical_name in name_upper for typical_name in ["SMITH", "JOHNSON", "WILLIAMS", "BROWN", "JONES", "GARCIA", "MILLER", "DAVIS", "RODRIGUEZ", "MARTINEZ"]):
            return True
            
        return False
    
    def create_player_mapping(self, sleeper_players: Dict[str, Dict[str, Any]] = None, 
                            mfl_players: List[Dict[str, Any]] = None, 
                            use_nfl_data_py: bool = True) -> Dict[str, PlayerInfo]:
        """
        Create comprehensive player ID mapping with layered data strategy
        
        Layer 1: nfl_data_py crosswalk (authoritative for stable cross-platform IDs)
        Layer 2: Smart matching algorithm (handles new/missing players)  
        Layer 3: Live API data (current team/status/position)
        """
        logger.info("Creating enhanced player ID mapping with layered data strategy...")
        
        # Use provided data or empty defaults
        sleeper_players = sleeper_players or {}
        mfl_players = mfl_players or []
        
        player_mapping = {}
        # Track alternative mappings for fallback matching
        name_position_mapping = {}  # For when team is UNKNOWN
        
        # LAYER 1: Seed from nfl_data_py crosswalk (if enabled)
        crosswalk_seeded = 0
        if use_nfl_data_py:
            try:
                logger.info("LAYER 1: Seeding from nfl_data_py crosswalk...")
                crosswalk_df = self.load_nfl_data_py_crosswalk()
                
                for _, row in crosswalk_df.iterrows():
                    player_info = self.create_player_from_crosswalk(row)
                    if player_info and player_info.name:
                        player_mapping[player_info.canonical_id] = player_info
                        
                        # Track for fallback matching
                        name_pos_key = f"{player_info.name}|{player_info.position}"
                        name_position_mapping[name_pos_key] = player_info
                        crosswalk_seeded += 1
                
                logger.info(f"✓ Layer 1 complete: {crosswalk_seeded:,} players seeded from crosswalk")
                
                # Count pre-matched cross-platform players
                pre_matched = sum(1 for p in player_mapping.values() if p.sleeper_id and p.mfl_id)
                logger.info(f"  Pre-matched cross-platform players: {pre_matched:,}")
                
            except Exception as e:
                logger.warning(f"Layer 1 nfl_data_py seeding failed, continuing with smart matching: {e}")
        
        # LAYER 2 & 3: Smart matching with live API data (enhanced from original)
        logger.info("LAYER 2-3: Processing live API data with smart matching...")
        
        # Process Sleeper players with crosswalk integration
        logger.info(f"Processing {len(sleeper_players)} Sleeper players...")
        sleeper_processed = 0
        sleeper_updated = 0
        sleeper_skipped = 0
        
        for sleeper_id, player_data in sleeper_players.items():
            if not player_data:
                continue
                
            name = self.normalize_player_name(player_data.get("full_name", ""))
            position = self.normalize_position(player_data.get("position", ""))
            team = self.normalize_team(player_data.get("team", ""))
            active = player_data.get("active", True)
            
            # Skip if essential data is missing
            if not name or not position:
                sleeper_skipped += 1
                continue
                
            # Filter out obvious non-player entries
            if self._is_team_defense_entry(name, position):
                sleeper_skipped += 1
                continue
            
            # Strategy 1: Try to match with existing crosswalk player by Sleeper ID
            existing_player = None
            for player in player_mapping.values():
                if player.sleeper_id == sleeper_id:
                    existing_player = player
                    break
            
            if existing_player:
                # Update existing crosswalk player with live Sleeper data
                if team != "UNKNOWN" and existing_player.team == "UNKNOWN":
                    existing_player.team = team
                if position != "UNKNOWN":
                    existing_player.position = position
                existing_player.active = active
                sleeper_updated += 1
            else:
                # Strategy 2: Try name+position matching with crosswalk
                name_pos_key = f"{name}|{position}"
                if name_pos_key in name_position_mapping:
                    existing_player = name_position_mapping[name_pos_key]
                    if not existing_player.sleeper_id:  # Only if not already assigned
                        existing_player.sleeper_id = sleeper_id
                        existing_player.active = active
                        if team != "UNKNOWN":
                            existing_player.team = team
                        sleeper_updated += 1
                    else:
                        sleeper_skipped += 1  # Already has different Sleeper ID
                else:
                    # Strategy 3: Create new player (not in crosswalk)
                    canonical_id = self.generate_canonical_id(name, position, team or "UNKNOWN")
                    
                    player_info = PlayerInfo(
                        canonical_id=canonical_id,
                        name=name,
                        position=position,
                        team=team,
                        sleeper_id=sleeper_id,
                        active=active,
                        from_nfl_data_py=False  # This is a new player not in crosswalk
                    )
                    
                    player_mapping[canonical_id] = player_info
                    name_position_mapping[name_pos_key] = player_info
                    sleeper_processed += 1
        
        logger.info(f"Sleeper integration: {sleeper_processed} new, {sleeper_updated} updated, {sleeper_skipped} skipped")
        
        # Process MFL players with enhanced crosswalk integration  
        logger.info(f"Processing {len(mfl_players)} MFL players...")
        mfl_processed = 0
        mfl_updated = 0
        mfl_skipped = 0
        
        for mfl_player in mfl_players:
            name = self.normalize_player_name(mfl_player.get("name", ""))
            position = self.normalize_position(mfl_player.get("position", ""))
            team = self.normalize_team(mfl_player.get("team", ""))
            mfl_id = mfl_player.get("id")
            
            # Skip if essential data is missing
            if not name or not position or not mfl_id:
                mfl_skipped += 1
                continue
                
            # Filter out team defense entries and non-player data
            if self._is_team_defense_entry(name, position):
                mfl_skipped += 1
                continue
            
            # Strategy 1: Try to match with existing crosswalk player by MFL ID
            existing_player = None
            for player in player_mapping.values():
                if player.mfl_id == mfl_id:
                    existing_player = player
                    break
            
            if existing_player:
                # Update existing crosswalk player with live MFL data
                if team != "UNKNOWN" and existing_player.team == "UNKNOWN":
                    existing_player.team = team
                if position != "UNKNOWN":
                    existing_player.position = position
                existing_player.active = True
                mfl_updated += 1
            else:
                # Strategy 2: Try name+position matching with crosswalk
                name_pos_key = f"{name}|{position}"
                if name_pos_key in name_position_mapping:
                    existing_player = name_position_mapping[name_pos_key]
                    if not existing_player.mfl_id:  # Only if not already assigned
                        existing_player.mfl_id = mfl_id
                        existing_player.active = True
                        if team != "UNKNOWN":
                            existing_player.team = team
                        mfl_updated += 1
                    else:
                        mfl_skipped += 1  # Already has different MFL ID
                else:
                    # Strategy 3: Create new player (not in crosswalk)
                    canonical_id = self.generate_canonical_id(name, position, team or "UNKNOWN")
                    
                    player_info = PlayerInfo(
                        canonical_id=canonical_id,
                        name=name,
                        position=position,
                        team=team or "UNKNOWN",
                        mfl_id=mfl_id,
                        active=True,
                        from_nfl_data_py=False  # This is a new player not in crosswalk
                    )
                    
                    player_mapping[canonical_id] = player_info
                    name_position_mapping[name_pos_key] = player_info
                    mfl_processed += 1
        
        logger.info(f"MFL integration: {mfl_processed} new, {mfl_updated} updated, {mfl_skipped} skipped")
        
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
                
                # Add all mapped players with duplicate handling
                seen_espn_ids = set()
                seen_sleeper_ids = set() 
                seen_mfl_ids = set()
                added_count = 0
                
                for player_info in self._player_cache.values():
                    # Handle duplicate ESPN IDs
                    espn_id = player_info.espn_id
                    if espn_id and espn_id in seen_espn_ids:
                        espn_id = None  # Clear duplicate ESPN ID
                    elif espn_id:
                        seen_espn_ids.add(espn_id)
                    
                    # Handle duplicate Sleeper IDs  
                    sleeper_id = player_info.sleeper_id
                    if sleeper_id and sleeper_id in seen_sleeper_ids:
                        sleeper_id = None
                    elif sleeper_id:
                        seen_sleeper_ids.add(sleeper_id)
                    
                    # Handle duplicate MFL IDs
                    mfl_id = player_info.mfl_id
                    if mfl_id and mfl_id in seen_mfl_ids:
                        mfl_id = None
                    elif mfl_id:
                        seen_mfl_ids.add(mfl_id)
                    
                    try:
                        player = Player(
                            nfl_id=player_info.canonical_id,
                            sleeper_id=sleeper_id,
                            mfl_id=mfl_id,
                            espn_id=espn_id,
                            name=player_info.name,
                            position=player_info.position,
                            team=player_info.team,
                            is_starter=self._is_starter_position(player_info.position)
                        )
                        db.add(player)
                        added_count += 1
                    except Exception as e:
                        logger.warning(f"Failed to add player {player_info.name}: {e}")
                        continue
                
                db.commit()
                logger.info(f"Successfully synced {added_count} players to database (from {len(self._player_cache)} total)")
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