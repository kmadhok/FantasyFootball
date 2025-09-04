"""
MFL API Client for Manual Testing
Based on official documentation: https://api.myfantasyleague.com/2022/api_info?STATE=details
"""
import requests
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from data_models import LeagueInfo, TeamInfo, RosterInfo, WaiverInfo, APIResponse

logger = logging.getLogger(__name__)

class MFLManualTestClient:
    """MFL API client using only official documented endpoints"""
    
    def __init__(self, league_id: str, season: str = "2025"):
        self.base_url = f"https://api.myfantasyleague.com/{season}/export"
        self.league_id = league_id
        self.season = season
        self.timeout = 10
        
    def _make_request(self, params: Dict[str, str]) -> APIResponse:
        """Make API request and return standardized response"""
        # Always add JSON format and league ID
        request_params = {
            "L": self.league_id,
            "JSON": "1",
            **params
        }
        
        try:
            logger.info(f"Making MFL request with params: {request_params}")
            response = requests.get(self.base_url, params=request_params, timeout=self.timeout)
            response.raise_for_status()
            
            endpoint = f"?{'+'.join([f'{k}={v}' for k, v in request_params.items()])}"
            
            return APIResponse(
                platform="mfl",
                endpoint=endpoint,
                success=True,
                data=response.json(),
                status_code=response.status_code
            )
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Request failed: {str(e)}"
            status_code = getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
            
            logger.error(f"MFL API request failed: {error_msg}")
            
            return APIResponse(
                platform="mfl",
                endpoint=f"?{'+'.join([f'{k}={v}' for k, v in request_params.items()])}",
                success=False,
                error_message=error_msg,
                status_code=status_code
            )
    
    def get_league_info(self) -> APIResponse:
        """
        Get league information
        Official endpoint: export?TYPE=league&L={league_id}&JSON=1
        """
        params = {"TYPE": "league"}
        response = self._make_request(params)
        
        if response.success:
            league_data = response.data.get("league", {})
            
            # Transform to our data model
            # Handle franchises count - it could be a dict or int
            franchises = league_data.get("franchises", 0)
            if isinstance(franchises, dict):
                # If franchises is a dict, try to count franchise entries
                franchise_count = len(franchises.get("franchise", []))
            else:
                franchise_count = int(franchises) if franchises else 0
                
            league_info = LeagueInfo(
                league_id=self.league_id,
                name=league_data.get("name", "Unknown League"),
                season=self.season,
                total_rosters=franchise_count,
                platform="mfl",
                settings=league_data
            )
            
            response.data = league_info
            
        return response
    
    def get_franchises(self) -> APIResponse:
        """
        Get all franchises (teams) in the league
        Official endpoint: export?TYPE=franchises&L={league_id}&JSON=1
        """
        params = {"TYPE": "franchises"}
        response = self._make_request(params)
        
        if response.success:
            franchises_data = response.data.get("franchises", {})
            franchise_list = franchises_data.get("franchise", [])
            
            # Handle single franchise case
            if not isinstance(franchise_list, list):
                franchise_list = [franchise_list] if franchise_list else []
            
            # Transform to our data model
            teams = []
            for franchise in franchise_list:
                team = TeamInfo(
                    team_id=franchise.get("id", "unknown"),
                    owner_name=franchise.get("owner_name", franchise.get("name", "Unknown")),
                    display_name=franchise.get("name"),
                    platform="mfl"
                )
                teams.append(team)
            
            response.data = teams
            
        return response
    
    def get_rosters(self) -> APIResponse:
        """
        Get all rosters in the league
        Official endpoint: export?TYPE=rosters&L={league_id}&JSON=1
        """
        params = {"TYPE": "rosters"}
        response = self._make_request(params)
        
        if response.success:
            rosters_data = response.data.get("rosters", {})
            franchise_list = rosters_data.get("franchise", [])
            
            # Handle single franchise case
            if not isinstance(franchise_list, list):
                franchise_list = [franchise_list] if franchise_list else []
            
            # Get franchise info for owner names
            franchises_response = self.get_franchises()
            franchise_lookup = {}
            if franchises_response.success:
                for team in franchises_response.data:
                    franchise_lookup[team.team_id] = team.owner_name
            
            # Transform to our data model
            rosters = []
            for franchise in franchise_list:
                franchise_id = franchise.get("id", "unknown")
                owner_name = franchise_lookup.get(franchise_id, f"Franchise_{franchise_id}")
                
                # Get player data
                player_data = franchise.get("player", [])
                if not isinstance(player_data, list):
                    player_data = [player_data] if player_data else []
                
                # MFL doesn't clearly separate starters/bench in basic roster call
                # For simplicity, we'll put all players in starters
                all_players = [p.get("id", "") for p in player_data if p.get("id")]
                
                roster_info = RosterInfo(
                    team_id=franchise_id,
                    owner_name=owner_name,
                    starters=all_players,  # MFL requires separate call for lineup
                    bench=[],
                    platform="mfl"
                )
                rosters.append(roster_info)
            
            response.data = rosters
            
        return response
    
    def get_transactions(self, week: str = "*") -> APIResponse:
        """
        Get transactions for the league
        Official endpoint: export?TYPE=transactions&L={league_id}&W={week}&JSON=1
        """
        params = {
            "TYPE": "transactions",
            "W": week  # "*" for all weeks, or specific week number
        }
        response = self._make_request(params)
        
        if response.success:
            transactions_data = response.data.get("transactions", {})
            transaction_list = transactions_data.get("transaction", [])
            
            # Handle single transaction case
            if not isinstance(transaction_list, list):
                transaction_list = [transaction_list] if transaction_list else []
            
            # Get franchise info for owner names
            franchises_response = self.get_franchises()
            franchise_lookup = {}
            if franchises_response.success:
                for team in franchises_response.data:
                    franchise_lookup[team.team_id] = team.owner_name
            
            # Transform to our data model
            waivers = []
            for transaction in transaction_list:
                transaction_type = transaction.get("type", "unknown")
                
                # Focus on waiver-related transactions
                if "WAIVER" in transaction_type or "BLIND_BID" in transaction_type:
                    franchise_id = transaction.get("franchise", "")
                    owner_name = franchise_lookup.get(franchise_id, f"Franchise_{franchise_id}")
                    
                    # Extract player from transaction
                    player_id = ""
                    if "player" in transaction:
                        player_id = transaction["player"]
                    
                    waiver_info = WaiverInfo(
                        transaction_id=transaction.get("transaction", "unknown"),
                        player_id=player_id,
                        claiming_team=owner_name,
                        platform="mfl",
                        transaction_type=transaction_type.lower(),
                        bid_amount=int(transaction.get("amount", 0)) if transaction.get("amount") else None,
                        status="processed",  # MFL shows completed transactions
                        timestamp=datetime.fromtimestamp(int(transaction.get("timestamp", 0))) if transaction.get("timestamp") else None
                    )
                    waivers.append(waiver_info)
            
            response.data = waivers
            
        return response
    
    def get_faab_balances(self) -> APIResponse:
        """
        Get FAAB balances for all franchises
        Official endpoint: export?TYPE=blindBidSummary&L={league_id}&JSON=1
        """
        params = {"TYPE": "blindBidSummary"}
        response = self._make_request(params)
        
        if response.success:
            faab_data = response.data.get("blindBidSummary", {})
            franchise_list = faab_data.get("franchise", [])
            
            # Handle single franchise case
            if not isinstance(franchise_list, list):
                franchise_list = [franchise_list] if franchise_list else []
            
            # Get franchise info for owner names
            franchises_response = self.get_franchises()
            franchise_lookup = {}
            if franchises_response.success:
                for team in franchises_response.data:
                    franchise_lookup[team.team_id] = team.owner_name
            
            # Transform to our data model - using WaiverInfo to store FAAB data
            faab_info = []
            for franchise in franchise_list:
                franchise_id = franchise.get("id", "")
                owner_name = franchise_lookup.get(franchise_id, f"Franchise_{franchise_id}")
                balance = franchise.get("balance", franchise.get("faabBalance", 0))
                
                # Convert to int if it's a string
                if isinstance(balance, str):
                    try:
                        balance = int(balance)
                    except ValueError:
                        balance = 0
                
                waiver_info = WaiverInfo(
                    transaction_id=f"faab_{franchise_id}",
                    player_id="",
                    claiming_team=owner_name,
                    platform="mfl",
                    transaction_type="faab_balance",
                    bid_amount=balance,
                    status="current"
                )
                faab_info.append(waiver_info)
            
            response.data = faab_info
            
        return response
    
    def get_players(self) -> APIResponse:
        """
        Get all NFL players
        Official endpoint: export?TYPE=players&L={league_id}&JSON=1
        """
        params = {"TYPE": "players"}
        response = self._make_request(params)
        
        if response.success:
            players_data = response.data.get("players", {})
            player_list = players_data.get("player", [])
            
            # Handle single player case
            if not isinstance(player_list, list):
                player_list = [player_list] if player_list else []
            
            # Transform to our data model (sample of players to avoid overwhelming output)
            from data_models import PlayerInfo
            
            players = []
            count = 0
            for player_data in player_list:
                if count >= 100:  # Limit to first 100 for demo
                    break
                    
                player = PlayerInfo(
                    player_id=player_data.get("id", ""),
                    name=player_data.get("name", "Unknown"),
                    position=player_data.get("position"),
                    team=player_data.get("team"),
                    platform="mfl"
                )
                players.append(player)
                count += 1
            
            response.data = players
            
        return response
    
    def get_current_week(self) -> APIResponse:
        """
        Get current NFL week
        Official endpoint: export?TYPE=nflSchedule&L={league_id}&JSON=1
        """
        params = {"TYPE": "nflSchedule"}
        response = self._make_request(params)
        
        if response.success:
            schedule_data = response.data.get("nflSchedule", {})
            # Extract current week information
            current_week = schedule_data.get("week", "1")
            response.data = {"current_week": current_week, "full_schedule": schedule_data}
            
        return response

def test_mfl_client():
    """Test function for MFL client"""
    # Use league ID from environment or default
    import os
    from pathlib import Path
    
    # Load environment variables
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        from dotenv import load_dotenv
        load_dotenv(env_path)
    
    league_id = os.getenv("MFL_LEAGUE_ID", "73756")
    
    print(f"Testing MFL API Client with League ID: {league_id}")
    print("=" * 60)
    
    client = MFLManualTestClient(league_id)
    
    # Test league info
    print("\n1. Testing League Info:")
    league_response = client.get_league_info()
    print(f"   Status: {league_response}")
    if league_response.success:
        print(f"   League: {league_response.data}")
    
    # Test franchises/teams
    print("\n2. Testing Franchises/Teams:")
    franchises_response = client.get_franchises()
    print(f"   Status: {franchises_response}")
    if franchises_response.success:
        print(f"   Found {len(franchises_response.data)} franchises:")
        for team in franchises_response.data[:5]:  # Show first 5
            print(f"     - {team}")
    
    # Test rosters
    print("\n3. Testing Rosters:")
    rosters_response = client.get_rosters()
    print(f"   Status: {rosters_response}")
    if rosters_response.success:
        print(f"   Found {len(rosters_response.data)} rosters:")
        for roster in rosters_response.data[:3]:  # Show first 3
            print(f"     - {roster}")
    
    # Test FAAB balances
    print("\n4. Testing FAAB Balances:")
    faab_response = client.get_faab_balances()
    print(f"   Status: {faab_response}")
    if faab_response.success:
        print(f"   Found {len(faab_response.data)} FAAB balances:")
        for faab in faab_response.data[:5]:  # Show first 5
            print(f"     - {faab}")
    
    # Test transactions
    print("\n5. Testing Transactions:")
    transactions_response = client.get_transactions()
    print(f"   Status: {transactions_response}")
    if transactions_response.success:
        print(f"   Found {len(transactions_response.data)} transactions:")
        for transaction in transactions_response.data[:3]:  # Show first 3
            print(f"     - {transaction}")
    
    # Test current week
    print("\n6. Testing Current Week:")
    week_response = client.get_current_week()
    print(f"   Status: {week_response}")
    if week_response.success:
        print(f"   Current Week: {week_response.data.get('current_week', 'Unknown')}")

if __name__ == "__main__":
    test_mfl_client()