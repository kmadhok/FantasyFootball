"""
Data models for manual testing of Fantasy Football APIs
Based on official Sleeper and MFL API documentation
"""
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
from datetime import datetime


@dataclass
class LeagueInfo:
    """League information from either platform"""
    league_id: str
    name: str
    season: str
    total_rosters: int
    platform: str
    settings: Dict[str, Any] = None
    
    def __str__(self) -> str:
        return f"{self.platform.upper()} League: {self.name} ({self.season}) - {self.total_rosters} teams"


@dataclass
class TeamInfo:
    """Team/franchise information"""
    team_id: str
    owner_name: str
    display_name: Optional[str]
    platform: str
    roster_id: Optional[str] = None
    avatar_url: Optional[str] = None
    
    def __str__(self) -> str:
        display = self.display_name or self.owner_name
        return f"{self.platform.upper()} Team: {display} (ID: {self.team_id})"


@dataclass
class PlayerInfo:
    """Player information"""
    player_id: str
    name: Optional[str]
    position: Optional[str]
    team: Optional[str]
    platform: str
    
    def __str__(self) -> str:
        if self.name:
            return f"{self.name} ({self.position}, {self.team})"
        return f"Player ID: {self.player_id}"


@dataclass
class RosterInfo:
    """Roster information for a team"""
    team_id: str
    owner_name: str
    starters: List[str]
    bench: List[str]
    platform: str
    roster_id: Optional[str] = None
    total_players: int = 0
    
    def __post_init__(self):
        self.total_players = len(self.starters) + len(self.bench)
    
    def __str__(self) -> str:
        return f"{self.platform.upper()} Roster - {self.owner_name}: {len(self.starters)} starters, {len(self.bench)} bench ({self.total_players} total)"


@dataclass
class WaiverInfo:
    """Waiver/transaction information"""
    transaction_id: str
    player_id: Optional[str]
    claiming_team: Optional[str]
    platform: str
    transaction_type: str = "waiver"
    bid_amount: Optional[int] = None
    status: str = "unknown"
    timestamp: Optional[datetime] = None
    
    def __str__(self) -> str:
        bid_str = f" (${self.bid_amount})" if self.bid_amount else ""
        return f"{self.platform.upper()} {self.transaction_type}: Player {self.player_id} -> {self.claiming_team}{bid_str} [{self.status}]"


@dataclass
class APIResponse:
    """Generic API response wrapper"""
    platform: str
    endpoint: str
    success: bool
    data: Any = None
    error_message: Optional[str] = None
    status_code: Optional[int] = None
    
    def __str__(self) -> str:
        status = "SUCCESS" if self.success else f"ERROR ({self.status_code})"
        return f"{self.platform.upper()} API [{self.endpoint}]: {status}"


@dataclass
class TestResults:
    """Container for all test results"""
    league_info: Optional[LeagueInfo] = None
    teams: List[TeamInfo] = None
    rosters: List[RosterInfo] = None
    waivers: List[WaiverInfo] = None
    errors: List[str] = None
    
    def __post_init__(self):
        if self.teams is None:
            self.teams = []
        if self.rosters is None:
            self.rosters = []
        if self.waivers is None:
            self.waivers = []
        if self.errors is None:
            self.errors = []
    
    def add_error(self, error: str):
        """Add an error message"""
        self.errors.append(error)
    
    def has_errors(self) -> bool:
        """Check if there are any errors"""
        return len(self.errors) > 0
    
    def summary(self) -> str:
        """Get a summary of test results"""
        summary_lines = []
        
        if self.league_info:
            summary_lines.append(f"League: {self.league_info}")
        
        if self.teams:
            summary_lines.append(f"Teams: {len(self.teams)} found")
            
        if self.rosters:
            summary_lines.append(f"Rosters: {len(self.rosters)} found")
            
        if self.waivers:
            summary_lines.append(f"Waiver Transactions: {len(self.waivers)} found")
            
        if self.errors:
            summary_lines.append(f"Errors: {len(self.errors)} encountered")
            
        return "\n".join(summary_lines)