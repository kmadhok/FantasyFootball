"""
Sleeper API Client for Manual Testing
Based on official documentation: https://docs.sleeper.com/
"""
import requests
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from data_models import LeagueInfo, TeamInfo, RosterInfo, WaiverInfo, APIResponse

logger = logging.getLogger(__name__)

class SleeperManualTestClient:
    """Sleeper API client using only official documented endpoints"""
    
    def __init__(self, league_id: str):
        self.base_url = "https://api.sleeper.app/v1"
        self.league_id = league_id
        self.timeout = 10
        
    def _make_request(self, endpoint: str) -> APIResponse:
        """Make API request and return standardized response"""
        url = f"{self.base_url}{endpoint}"
        
        try:
            logger.info(f"Making request to: {url}")
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            return APIResponse(
                platform="sleeper",
                endpoint=endpoint,
                success=True,
                data=response.json(),
                status_code=response.status_code
            )
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Request failed: {str(e)}"
            status_code = getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
            
            logger.error(f"API request failed for {endpoint}: {error_msg}")
            
            return APIResponse(
                platform="sleeper",
                endpoint=endpoint,
                success=False,
                error_message=error_msg,
                status_code=status_code
            )
    
    def get_league_info(self) -> APIResponse:
        """
        Get league information
        Official endpoint: GET https://api.sleeper.app/v1/league/{league_id}
        """
        endpoint = f"/league/{self.league_id}"
        response = self._make_request(endpoint)
        
        if response.success:
            league_data = response.data
            
            # Transform to our data model
            league_info = LeagueInfo(
                league_id=self.league_id,
                name=league_data.get("name", "Unknown League"),
                season=league_data.get("season", "Unknown"),
                total_rosters=league_data.get("total_rosters", 0),
                platform="sleeper",
                settings=league_data.get("settings", {})
            )
            
            response.data = league_info
            
        return response
    
    def get_users(self) -> APIResponse:
        """
        Get all users in the league
        Official endpoint: GET https://api.sleeper.app/v1/league/{league_id}/users
        """
        endpoint = f"/league/{self.league_id}/users"
        response = self._make_request(endpoint)
        
        if response.success:
            users_data = response.data
            
            # Transform to our data model
            teams = []
            for user in users_data:
                team = TeamInfo(
                    team_id=user.get("user_id", "unknown"),
                    owner_name=user.get("display_name", user.get("username", "Unknown")),
                    display_name=user.get("display_name"),
                    platform="sleeper",
                    avatar_url=user.get("avatar")
                )
                teams.append(team)
            
            response.data = teams
            
        return response
    
    def get_rosters(self) -> APIResponse:
        """
        Get all rosters in the league
        Official endpoint: GET https://api.sleeper.app/v1/league/{league_id}/rosters
        """
        endpoint = f"/league/{self.league_id}/rosters"
        response = self._make_request(endpoint)
        
        if response.success:
            rosters_data = response.data
            
            # Get users for owner lookup
            users_response = self.get_users()
            user_lookup = {}
            if users_response.success:
                for team in users_response.data:
                    user_lookup[team.team_id] = team.owner_name
            
            # Transform to our data model
            rosters = []
            for roster in rosters_data:
                owner_id = roster.get("owner_id", "unknown")
                owner_name = user_lookup.get(owner_id, f"User_{owner_id}")
                
                # Get starters and bench
                starters = roster.get("starters", [])
                all_players = roster.get("players", [])
                bench = [p for p in all_players if p not in starters]
                
                roster_info = RosterInfo(
                    team_id=owner_id,
                    owner_name=owner_name,
                    starters=starters,
                    bench=bench,
                    platform="sleeper",
                    roster_id=str(roster.get("roster_id", ""))
                )
                rosters.append(roster_info)
            
            response.data = rosters
            
        return response
    
    def get_transactions(self, week: int = None) -> APIResponse:
        """
        Get transactions (including waivers) for the league
        Official endpoint: GET https://api.sleeper.app/v1/league/{league_id}/transactions/{round}
        """
        # If no week specified, get current week (rough estimate)
        if week is None:
            # Simple calculation - you could enhance this
            import datetime
            start_of_season = datetime.date(2024, 9, 5)  # Approximate NFL season start
            current_date = datetime.date.today()
            days_elapsed = (current_date - start_of_season).days
            week = max(1, min(18, (days_elapsed // 7) + 1))
        
        endpoint = f"/league/{self.league_id}/transactions/{week}"
        response = self._make_request(endpoint)
        
        if response.success:
            transactions_data = response.data
            
            # Get users for owner lookup
            users_response = self.get_users()
            user_lookup = {}
            if users_response.success:
                for team in users_response.data:
                    user_lookup[team.team_id] = team.owner_name
            
            # Transform to our data model
            waivers = []
            for transaction in transactions_data:
                if transaction.get("type") == "waiver":
                    # Extract waiver information
                    adds = transaction.get("adds", {})
                    drops = transaction.get("drops", {})
                    roster_ids = transaction.get("roster_ids", [])
                    
                    for player_id, roster_id in adds.items():
                        if roster_id:  # Valid roster ID
                            owner_name = user_lookup.get(str(roster_id), f"Team_{roster_id}")
                            
                            waiver_info = WaiverInfo(
                                transaction_id=transaction.get("transaction_id", "unknown"),
                                player_id=player_id,
                                claiming_team=owner_name,
                                platform="sleeper",
                                transaction_type="waiver_add",
                                bid_amount=transaction.get("settings", {}).get("waiver_bid"),
                                status=transaction.get("status", "unknown"),
                                timestamp=datetime.fromtimestamp(transaction.get("created", 0) / 1000)
                            )
                            waivers.append(waiver_info)
            
            response.data = waivers
            
        return response
    
    def get_players(self) -> APIResponse:
        """
        Get all NFL players
        Official endpoint: GET https://api.sleeper.app/v1/players/nfl
        """
        endpoint = "/players/nfl"
        response = self._make_request(endpoint)
        
        if response.success:
            players_data = response.data
            
            # Transform to our data model (sample of players to avoid overwhelming output)
            from data_models import PlayerInfo
            
            players = []
            count = 0
            for player_id, player_data in players_data.items():
                if count >= 100:  # Limit to first 100 for demo
                    break
                    
                player = PlayerInfo(
                    player_id=player_id,
                    name=f"{player_data.get('first_name', '')} {player_data.get('last_name', '')}".strip(),
                    position=player_data.get("position"),
                    team=player_data.get("team"),
                    platform="sleeper"
                )
                players.append(player)
                count += 1
            
            response.data = players
            
        return response
    
    def get_waiver_orders(self) -> APIResponse:
        """
        Get current waiver orders
        Official endpoint: GET https://api.sleeper.app/v1/league/{league_id}/waivers
        """
        endpoint = f"/league/{self.league_id}/waivers"
        response = self._make_request(endpoint)
        
        if response.success:
            waivers_data = response.data
            
            # Get users for owner lookup
            users_response = self.get_users()
            user_lookup = {}
            if users_response.success:
                for team in users_response.data:
                    user_lookup[team.team_id] = team.owner_name
            
            # Transform to our data model
            waivers = []
            for waiver in waivers_data:
                roster_id = waiver.get("roster_id")
                owner_name = user_lookup.get(str(roster_id), f"Team_{roster_id}")
                
                waiver_info = WaiverInfo(
                    transaction_id=str(waiver.get("waiver_id", "unknown")),
                    player_id=str(waiver.get("player_id", "")),
                    claiming_team=owner_name,
                    platform="sleeper",
                    transaction_type="waiver_claim",
                    bid_amount=waiver.get("waiver_bid"),
                    status=waiver.get("status", "pending"),
                    timestamp=datetime.fromtimestamp(waiver.get("created", 0) / 1000) if waiver.get("created") else None
                )
                waivers.append(waiver_info)
            
            response.data = waivers
            
        return response

def test_sleeper_client():
    """Test function for Sleeper client"""
    # Use league ID from environment or default
    import os
    from pathlib import Path
    
    # Load environment variables
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        from dotenv import load_dotenv
        load_dotenv(env_path)
    
    league_id = os.getenv("SLEEPER_LEAGUE_ID", "1124820373402046464")
    
    print(f"Testing Sleeper API Client with League ID: {league_id}")
    print("=" * 60)
    
    client = SleeperManualTestClient(league_id)
    
    # Test league info
    print("\n1. Testing League Info:")
    league_response = client.get_league_info()
    print(f"   Status: {league_response}")
    if league_response.success:
        print(f"   League: {league_response.data}")
    
    # Test users/teams
    print("\n2. Testing Users/Teams:")
    users_response = client.get_users()
    print(f"   Status: {users_response}")
    if users_response.success:
        print(f"   Found {len(users_response.data)} teams:")
        for team in users_response.data[:5]:  # Show first 5
            print(f"     - {team}")
    
    # Test rosters
    print("\n3. Testing Rosters:")
    rosters_response = client.get_rosters()
    print(f"   Status: {rosters_response}")
    if rosters_response.success:
        print(f"   Found {len(rosters_response.data)} rosters:")
        for roster in rosters_response.data[:3]:  # Show first 3
            print(f"     - {roster}")
    
    # Test waiver orders
    print("\n4. Testing Waiver Orders:")
    waivers_response = client.get_waiver_orders()
    print(f"   Status: {waivers_response}")
    if waivers_response.success:
        print(f"   Found {len(waivers_response.data)} waiver claims:")
        for waiver in waivers_response.data[:3]:  # Show first 3
            print(f"     - {waiver}")
    
    # Test transactions
    print("\n5. Testing Transactions:")
    transactions_response = client.get_transactions()
    print(f"   Status: {transactions_response}")
    if transactions_response.success:
        print(f"   Found {len(transactions_response.data)} transactions:")
        for transaction in transactions_response.data[:3]:  # Show first 3
            print(f"     - {transaction}")

if __name__ == "__main__":
    test_sleeper_client()