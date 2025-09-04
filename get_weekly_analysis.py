#!/usr/bin/env python3
"""
Comprehensive Weekly Fantasy Football Analysis
Pulls your roster, opponent roster, waiver wire players, and waiver status from both leagues
"""
import os
import sys
import requests
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, List, Any, Optional, Tuple
import json
import time
from urllib3.exceptions import NameResolutionError
from requests.exceptions import ConnectionError, Timeout, RequestException

# Load environment
project_root = Path(__file__).parent
load_dotenv(project_root / ".env")

def diagnose_network_connectivity():
    """Diagnose network connectivity issues"""
    print("🔧 Running network diagnostics...")
    
    # Test 1: Basic internet
    try:
        response = requests.get("https://www.google.com", timeout=5)
        print("   ✅ Basic internet connectivity: OK")
    except Exception as e:
        print(f"   ❌ Basic internet connectivity: FAILED ({e})")
        return False
    
    # Test 2: DNS resolution for fantasy APIs
    import socket
    try:
        socket.gethostbyname("api.sleeper.app")
        print("   ✅ Sleeper API DNS resolution: OK")
    except Exception as e:
        print(f"   ❌ Sleeper API DNS resolution: FAILED ({e})")
    
    try:
        socket.gethostbyname("api.myfantasyleague.com")
        print("   ✅ MFL API DNS resolution: OK")
    except Exception as e:
        print(f"   ❌ MFL API DNS resolution: FAILED ({e})")
    
    # Test 3: API endpoints
    try:
        response = requests.get("https://api.sleeper.app", timeout=10)
        print("   ✅ Sleeper API endpoint: Reachable")
    except Exception as e:
        print(f"   ❌ Sleeper API endpoint: FAILED ({e})")
    
    try:
        response = requests.get("https://api.myfantasyleague.com", timeout=10)
        print("   ✅ MFL API endpoint: Reachable") 
    except Exception as e:
        print(f"   ❌ MFL API endpoint: FAILED ({e})")
    
    return True

def make_request_with_retry(url: str, params: Optional[Dict] = None, max_retries: int = 3, timeout: int = 10) -> requests.Response:
    """Make HTTP request with retry logic and better error handling"""
    last_error = None
    
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"   Retrying in {wait_time} seconds... (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
            
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response
            
        except (ConnectionError, NameResolutionError) as e:
            last_error = f"Network error: {e}"
            if attempt == 0:
                print(f"   ⚠️  Network connectivity issue detected")
        except Timeout as e:
            last_error = f"Request timeout: {e}"
            if attempt == 0:
                print(f"   ⚠️  Request timeout, retrying...")
        except RequestException as e:
            last_error = f"Request failed: {e}"
            if attempt == 0:
                print(f"   ⚠️  Request failed, retrying...")
        except Exception as e:
            last_error = f"Unexpected error: {e}"
            break  # Don't retry on unexpected errors
    
    # All retries failed
    raise Exception(f"Failed after {max_retries} attempts. Last error: {last_error}")

class FantasyAnalyzer:
    """Main analyzer for comprehensive fantasy football data"""
    
    def __init__(self, week: int = 1):
        self.week = week
        self.sleeper_league_id = os.getenv('SLEEPER_LEAGUE_ID')
        self.mfl_league_id = os.getenv('MFL_LEAGUE_ID')
        
        # User IDs for both platforms
        self.sleeper_user_id = None  # Will find dynamically
        self.mfl_franchise_id = "0002"  # PuntersForDays franchise ID
        
        # API base URLs
        self.sleeper_base = "https://api.sleeper.app/v1"
        self.mfl_base = "https://api.myfantasyleague.com/2025/export"
        
        print(f"🏈 WEEKLY FANTASY ANALYSIS - WEEK {week}")
        print("=" * 60)
        print(f"Sleeper League: {self.sleeper_league_id}")
        print(f"MFL League: {self.mfl_league_id}")
        print("=" * 60)
    
    def find_sleeper_user_id(self) -> str:
        """Find Kanum's user ID in Sleeper league"""
        print("🔍 Finding your Sleeper user ID...")
        
        try:
            url = f"{self.sleeper_base}/league/{self.sleeper_league_id}/users"
            response = make_request_with_retry(url, timeout=10)
            users = response.json()
            
            for user in users:
                username = user.get("username", "").lower()
                display_name = user.get("display_name", "").lower()
                
                if "kanum" in username or "kanum" in display_name:
                    self.sleeper_user_id = user["user_id"]
                    name = user.get("display_name") or user.get("username") or "Unknown"
                    print(f"✅ Found Sleeper user: {name} (ID: {self.sleeper_user_id})")
                    return self.sleeper_user_id
            
            print("❌ Could not find 'Kanum' in Sleeper users")
            print("Available users:")
            for user in users:
                name = user.get("display_name") or user.get("username") or "Unknown"
                print(f"  - {name}")
            return None
            
        except Exception as e:
            print(f"❌ Error finding Sleeper user: {e}")
            
            # Run network diagnostics if it's a connectivity issue
            if any(term in str(e).lower() for term in ["connection", "resolve", "network", "dns"]):
                print("\n🔧 Network issue detected. Running diagnostics...")
                diagnose_network_connectivity()
                print("\n💡 Troubleshooting suggestions:")
                print("   1. Check your internet connection")
                print("   2. Try running the script again in a few minutes")
                print("   3. If using VPN, try disconnecting temporarily")
                print("   4. Check if your firewall is blocking API access")
            
            return None
    
    def get_sleeper_matchup(self, week: int) -> Tuple[Dict, Dict]:
        """Get Week matchup data from Sleeper"""
        print(f"\n📅 Getting Sleeper Week {week} matchup...")
        
        try:
            # Get matchups for the week
            url = f"{self.sleeper_base}/league/{self.sleeper_league_id}/matchups/{week}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            matchups = response.json()
            
            # Find your matchup
            your_matchup = None
            opponent_matchup = None
            
            # First, get rosters to map roster_id to user_id
            rosters_url = f"{self.sleeper_base}/league/{self.sleeper_league_id}/rosters"
            rosters_response = requests.get(rosters_url, timeout=10)
            rosters_response.raise_for_status()
            rosters = rosters_response.json()
            
            # Find your roster_id
            your_roster_id = None
            for roster in rosters:
                if roster.get("owner_id") == self.sleeper_user_id:
                    your_roster_id = roster.get("roster_id")
                    break
            
            if not your_roster_id:
                print("❌ Could not find your roster in Sleeper")
                return {}, {}
            
            # Find your matchup and opponent
            for matchup in matchups:
                if matchup.get("roster_id") == your_roster_id:
                    your_matchup = matchup
                    matchup_id = matchup.get("matchup_id")
                    
                    # Find opponent with same matchup_id
                    for opp_matchup in matchups:
                        if (opp_matchup.get("matchup_id") == matchup_id and 
                            opp_matchup.get("roster_id") != your_roster_id):
                            opponent_matchup = opp_matchup
                            break
                    break
            
            if your_matchup and opponent_matchup:
                # Get opponent info
                opp_roster_id = opponent_matchup.get("roster_id")
                opp_roster = next((r for r in rosters if r.get("roster_id") == opp_roster_id), {})
                opp_owner_id = opp_roster.get("owner_id")
                
                # Get users for names
                users_url = f"{self.sleeper_base}/league/{self.sleeper_league_id}/users"
                users_response = requests.get(users_url, timeout=10)
                users_response.raise_for_status()
                users = users_response.json()
                
                opp_user = next((u for u in users if u.get("user_id") == opp_owner_id), {})
                opp_name = opp_user.get("display_name") or opp_user.get("username") or f"User_{opp_owner_id}"
                
                print(f"✅ Found matchup vs {opp_name}")
                print(f"   Your projected: {your_matchup.get('points', 0):.1f}")
                print(f"   Opponent projected: {opponent_matchup.get('points', 0):.1f}")
                
                return your_matchup, opponent_matchup
            else:
                print("❌ Could not find your Week 1 matchup")
                return {}, {}
                
        except Exception as e:
            print(f"❌ Error getting Sleeper matchup: {e}")
            return {}, {}
    
    def get_mfl_matchup(self, week: int) -> Tuple[Dict, str]:
        """Get Week matchup data from MFL"""
        print(f"\n📅 Getting MFL Week {week} matchup...")
        
        try:
            # Get schedule/matchups
            params = {
                "TYPE": "schedule",
                "L": self.mfl_league_id,
                "W": str(week),
                "JSON": "1"
            }
            
            response = requests.get(self.mfl_base, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            schedule_data = data.get("schedule", {})
            matchups = schedule_data.get("matchup", [])
            
            if not isinstance(matchups, list):
                matchups = [matchups] if matchups else []
            
            # Find your matchup
            your_matchup = None
            opponent_id = None
            
            for matchup in matchups:
                franchises = matchup.get("franchise", [])
                if not isinstance(franchises, list):
                    franchises = [franchises] if franchises else []
                
                franchise_ids = [f.get("id") for f in franchises]
                
                if self.mfl_franchise_id in franchise_ids:
                    your_matchup = matchup
                    # Find opponent
                    for fid in franchise_ids:
                        if fid != self.mfl_franchise_id:
                            opponent_id = fid
                            break
                    break
            
            if your_matchup and opponent_id:
                # Get franchise names
                league_params = {
                    "TYPE": "league",
                    "L": self.mfl_league_id,
                    "JSON": "1"
                }
                
                league_response = requests.get(self.mfl_base, params=league_params, timeout=10)
                league_response.raise_for_status()
                league_data = league_response.json()
                
                franchises_data = league_data.get("league", {}).get("franchises", {}).get("franchise", [])
                if not isinstance(franchises_data, list):
                    franchises_data = [franchises_data] if franchises_data else []
                
                opponent_name = "Unknown"
                for franchise in franchises_data:
                    if franchise.get("id") == opponent_id:
                        opponent_name = franchise.get("name", f"Franchise_{opponent_id}")
                        break
                
                print(f"✅ Found MFL matchup vs {opponent_name} (ID: {opponent_id})")
                return your_matchup, opponent_id
            else:
                print("❌ Could not find your MFL Week 1 matchup")
                return {}, None
                
        except Exception as e:
            print(f"❌ Error getting MFL matchup: {e}")
            return {}, None
    
    def get_player_details(self, player_ids: List[str], platform: str) -> Dict[str, Dict]:
        """Get detailed player information for given IDs"""
        print(f"\n🏈 Getting {len(player_ids)} player details from {platform.upper()}...")
        
        if platform == "sleeper":
            return self.get_sleeper_player_details(player_ids)
        else:
            return self.get_mfl_player_details(player_ids)
    
    def get_sleeper_player_details(self, player_ids: List[str]) -> Dict[str, Dict]:
        """Get Sleeper player details"""
        try:
            url = f"{self.sleeper_base}/players/nfl"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            all_players = response.json()
            
            player_details = {}
            found_count = 0
            
            for player_id in player_ids:
                if player_id in all_players:
                    player = all_players[player_id]
                    player_details[player_id] = {
                        "name": f"{player.get('first_name', '')} {player.get('last_name', '')}".strip(),
                        "position": player.get("position", "UNK"),
                        "team": player.get("team", "FA"),
                        "status": player.get("status", "Active")
                    }
                    found_count += 1
                else:
                    player_details[player_id] = {
                        "name": f"Player_{player_id}",
                        "position": "UNK",
                        "team": "FA",
                        "status": "Unknown"
                    }
            
            print(f"✅ Found details for {found_count}/{len(player_ids)} Sleeper players")
            return player_details
            
        except Exception as e:
            print(f"❌ Error getting Sleeper player details: {e}")
            return {}
    
    def get_mfl_player_details(self, player_ids: List[str]) -> Dict[str, Dict]:
        """Get MFL player details"""
        try:
            params = {
                "TYPE": "players",
                "L": self.mfl_league_id,
                "JSON": "1"
            }
            
            response = requests.get(self.mfl_base, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            players_data = data.get("players", {}).get("player", [])
            if not isinstance(players_data, list):
                players_data = [players_data] if players_data else []
            
            # Create lookup
            player_lookup = {}
            for player in players_data:
                pid = player.get("id")
                if pid:
                    player_lookup[pid] = player
            
            player_details = {}
            found_count = 0
            
            for player_id in player_ids:
                if player_id in player_lookup:
                    player = player_lookup[player_id]
                    player_details[player_id] = {
                        "name": player.get("name", f"Player_{player_id}"),
                        "position": player.get("position", "UNK"),
                        "team": player.get("team", "FA"),
                        "status": "Active"
                    }
                    found_count += 1
                else:
                    player_details[player_id] = {
                        "name": f"Player_{player_id}",
                        "position": "UNK", 
                        "team": "FA",
                        "status": "Unknown"
                    }
            
            print(f"✅ Found details for {found_count}/{len(player_ids)} MFL players")
            return player_details
            
        except Exception as e:
            print(f"❌ Error getting MFL player details: {e}")
            return {}
    
    def get_available_players(self, platform: str, limit: int = 10) -> List[Dict]:
        """Get top available players on waiver wire"""
        print(f"\n🔄 Getting top {limit} available players from {platform.upper()}...")
        
        try:
            if platform == "sleeper":
                return self.get_sleeper_available_players(limit)
            else:
                return self.get_mfl_available_players(limit)
                
        except Exception as e:
            print(f"❌ Error getting available players from {platform}: {e}")
            return []
    
    def get_sleeper_available_players(self, limit: int) -> List[Dict]:
        """Get available players from Sleeper"""
        try:
            # Get all players
            players_url = f"{self.sleeper_base}/players/nfl"
            players_response = requests.get(players_url, timeout=30)
            players_response.raise_for_status()
            all_players = players_response.json()
            
            # Get all rostered players
            rosters_url = f"{self.sleeper_base}/league/{self.sleeper_league_id}/rosters"
            rosters_response = requests.get(rosters_url, timeout=10)
            rosters_response.raise_for_status()
            rosters = rosters_response.json()
            
            rostered_players = set()
            for roster in rosters:
                players = roster.get("players", [])
                rostered_players.update(players)
            
            # Find available players
            available_players = []
            
            for player_id, player_data in all_players.items():
                if player_id not in rostered_players:
                    # Filter for relevant players (active NFL players)
                    if (player_data.get("team") and 
                        player_data.get("position") in ["QB", "RB", "WR", "TE", "K", "DEF"]):
                        
                        available_players.append({
                            "id": player_id,
                            "name": f"{player_data.get('first_name', '')} {player_data.get('last_name', '')}".strip(),
                            "position": player_data.get("position", "UNK"),
                            "team": player_data.get("team", "FA"),
                            "status": player_data.get("status", "Active")
                        })
            
            # Sort by position priority (QB, RB, WR, TE, K, DEF)
            position_priority = {"QB": 1, "RB": 2, "WR": 3, "TE": 4, "K": 5, "DEF": 6}
            available_players.sort(key=lambda x: (position_priority.get(x["position"], 7), x["name"]))
            
            print(f"✅ Found {len(available_players)} available players, showing top {limit}")
            return available_players[:limit]
            
        except Exception as e:
            print(f"❌ Error getting Sleeper available players: {e}")
            return []
    
    def get_mfl_available_players(self, limit: int) -> List[Dict]:
        """Get available players from MFL"""
        try:
            # Get all players
            players_params = {
                "TYPE": "players",
                "L": self.mfl_league_id,
                "JSON": "1"
            }
            
            players_response = requests.get(self.mfl_base, params=players_params, timeout=30)
            players_response.raise_for_status()
            players_data = players_response.json()
            
            all_players = players_data.get("players", {}).get("player", [])
            if not isinstance(all_players, list):
                all_players = [all_players] if all_players else []
            
            # Get all rostered players
            rosters_params = {
                "TYPE": "rosters",
                "L": self.mfl_league_id,
                "JSON": "1"
            }
            
            rosters_response = requests.get(self.mfl_base, params=rosters_params, timeout=10)
            rosters_response.raise_for_status()
            rosters_data = rosters_response.json()
            
            rosters = rosters_data.get("rosters", {}).get("franchise", [])
            if not isinstance(rosters, list):
                rosters = [rosters] if rosters else []
            
            rostered_players = set()
            for roster in rosters:
                players = roster.get("player", [])
                if not isinstance(players, list):
                    players = [players] if players else []
                
                for player in players:
                    player_id = player.get("id")
                    if player_id:
                        rostered_players.add(player_id)
            
            # Find available players
            available_players = []
            
            for player in all_players:
                player_id = player.get("id")
                if player_id and player_id not in rostered_players:
                    # Filter for relevant players
                    position = player.get("position", "")
                    team = player.get("team", "")
                    
                    if team and position in ["QB", "RB", "WR", "TE", "PK", "Def"]:
                        available_players.append({
                            "id": player_id,
                            "name": player.get("name", f"Player_{player_id}"),
                            "position": position,
                            "team": team,
                            "status": "Active"
                        })
            
            # Sort by position priority
            position_priority = {"QB": 1, "RB": 2, "WR": 3, "TE": 4, "PK": 5, "Def": 6}
            available_players.sort(key=lambda x: (position_priority.get(x["position"], 7), x["name"]))
            
            print(f"✅ Found {len(available_players)} available players, showing top {limit}")
            return available_players[:limit]
            
        except Exception as e:
            print(f"❌ Error getting MFL available players: {e}")
            return []
    
    def get_waiver_status(self) -> Tuple[Dict, Dict]:
        """Get current waiver status from both platforms"""
        print(f"\n📊 Getting waiver status...")
        
        sleeper_status = self.get_sleeper_waiver_status()
        mfl_status = self.get_mfl_faab_status()
        
        return sleeper_status, mfl_status
    
    def get_sleeper_waiver_status(self) -> Dict:
        """Get Sleeper waiver order status"""
        try:
            # Get league settings for waiver type
            league_url = f"{self.sleeper_base}/league/{self.sleeper_league_id}"
            league_response = requests.get(league_url, timeout=10)
            league_response.raise_for_status()
            league_data = league_response.json()
            
            settings = league_data.get("settings", {})
            waiver_type = settings.get("waiver_type", 0)
            
            # Get rosters for waiver priority
            rosters_url = f"{self.sleeper_base}/league/{self.sleeper_league_id}/rosters"
            rosters_response = requests.get(rosters_url, timeout=10)
            rosters_response.raise_for_status()
            rosters = rosters_response.json()
            
            # Get users for names
            users_url = f"{self.sleeper_base}/league/{self.sleeper_league_id}/users"
            users_response = requests.get(users_url, timeout=10)
            users_response.raise_for_status()
            users = users_response.json()
            
            user_lookup = {u["user_id"]: u.get("display_name") or u.get("username") or f"User_{u['user_id']}" for u in users}
            
            waiver_info = []
            your_waiver_order = None
            
            for roster in rosters:
                owner_id = roster.get("owner_id")
                waiver_order = roster.get("waiver_budget_used", roster.get("settings", {}).get("waiver_budget_used", 0))
                
                username = user_lookup.get(owner_id, f"User_{owner_id}")
                
                if owner_id == self.sleeper_user_id:
                    your_waiver_order = len(waiver_info) + 1  # Approximate
                
                waiver_info.append({
                    "username": username,
                    "waiver_order": len(waiver_info) + 1,
                    "waiver_budget_used": waiver_order
                })
            
            return {
                "platform": "sleeper",
                "waiver_type": waiver_type,
                "your_position": your_waiver_order,
                "total_teams": len(waiver_info),
                "details": waiver_info[:5]  # Show top 5
            }
            
        except Exception as e:
            print(f"❌ Error getting Sleeper waiver status: {e}")
            return {"platform": "sleeper", "error": str(e)}
    
    def get_mfl_faab_status(self) -> Dict:
        """Get MFL FAAB balance status"""
        try:
            # Get league info with franchises (includes FAAB balances)
            params = {
                "TYPE": "league",
                "L": self.mfl_league_id,
                "JSON": "1"
            }
            
            response = requests.get(self.mfl_base, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            franchises_data = data.get("league", {}).get("franchises", {}).get("franchise", [])
            if not isinstance(franchises_data, list):
                franchises_data = [franchises_data] if franchises_data else []
            
            faab_info = []
            your_faab = None
            
            for franchise in franchises_data:
                franchise_id = franchise.get("id")
                franchise_name = franchise.get("name", f"Franchise_{franchise_id}")
                faab_balance = float(franchise.get("bbidAvailableBalance", "0"))
                
                if franchise_id == self.mfl_franchise_id:
                    your_faab = faab_balance
                
                faab_info.append({
                    "franchise_name": franchise_name,
                    "franchise_id": franchise_id,
                    "faab_balance": faab_balance
                })
            
            # Sort by FAAB balance descending
            faab_info.sort(key=lambda x: x["faab_balance"], reverse=True)
            
            return {
                "platform": "mfl",
                "your_faab": your_faab,
                "total_faab": sum(f["faab_balance"] for f in faab_info),
                "details": faab_info[:5]  # Show top 5
            }
            
        except Exception as e:
            print(f"❌ Error getting MFL FAAB status: {e}")
            return {"platform": "mfl", "error": str(e)}
    
    def display_roster_comparison(self, your_roster: List[str], opponent_roster: List[str], 
                                platform: str, opponent_name: str):
        """Display roster comparison"""
        print(f"\n🆚 {platform.upper()} ROSTER COMPARISON vs {opponent_name}")
        print("=" * 70)
        
        # Get player details
        all_player_ids = list(set(your_roster + opponent_roster))
        player_details = self.get_player_details(all_player_ids, platform)
        
        print(f"\n{'YOUR ROSTER':<35} | {'OPPONENT ROSTER':<35}")
        print("-" * 35 + " | " + "-" * 35)
        
        max_length = max(len(your_roster), len(opponent_roster))
        
        for i in range(max_length):
            your_player = ""
            opp_player = ""
            
            if i < len(your_roster):
                pid = your_roster[i]
                player = player_details.get(pid, {})
                name = player.get("name", f"Player_{pid}")
                pos = player.get("position", "UNK")
                team = player.get("team", "FA")
                your_player = f"{pos:3s}: {name:<20s} ({team})"
            
            if i < len(opponent_roster):
                pid = opponent_roster[i]
                player = player_details.get(pid, {})
                name = player.get("name", f"Player_{pid}")
                pos = player.get("position", "UNK")
                team = player.get("team", "FA")
                opp_player = f"{pos:3s}: {name:<20s} ({team})"
            
            print(f"{your_player:<35} | {opp_player:<35}")
    
    def display_available_players(self, available_players: List[Dict], platform: str):
        """Display available players"""
        print(f"\n🔄 TOP AVAILABLE PLAYERS - {platform.upper()}")
        print("=" * 60)
        
        for i, player in enumerate(available_players, 1):
            name = player["name"]
            position = player["position"]
            team = player["team"]
            print(f"{i:2d}. {position:3s}: {name:<25s} ({team})")
    
    def display_waiver_status(self, sleeper_status: Dict, mfl_status: Dict):
        """Display waiver status information"""
        print(f"\n📊 WAIVER STATUS")
        print("=" * 60)
        
        # Sleeper Status
        if "error" not in sleeper_status:
            print(f"\n🟢 SLEEPER WAIVER ORDER:")
            print(f"   Your Position: #{sleeper_status.get('your_position', 'Unknown')}")
            print(f"   Total Teams: {sleeper_status.get('total_teams', 'Unknown')}")
        else:
            print(f"\n🔴 SLEEPER: {sleeper_status['error']}")
        
        # MFL Status
        if "error" not in mfl_status:
            print(f"\n💰 MFL FAAB BALANCE:")
            print(f"   Your Balance: ${mfl_status.get('your_faab', 0):.2f}")
            print(f"   League Total: ${mfl_status.get('total_faab', 0):.2f}")
            
            print(f"\n   Top 5 FAAB Balances:")
            for i, franchise in enumerate(mfl_status.get('details', []), 1):
                name = franchise['franchise_name']
                balance = franchise['faab_balance']
                marker = " ← YOU" if franchise['franchise_id'] == self.mfl_franchise_id else ""
                print(f"   {i}. {name:<20s} ${balance:>6.2f}{marker}")
        else:
            print(f"\n🔴 MFL: {mfl_status['error']}")
    
    def run_analysis(self):
        """Run the complete weekly analysis"""
        # Step 1: Find your user IDs
        if not self.find_sleeper_user_id():
            print("❌ Cannot proceed without Sleeper user ID")
            return
        
        print(f"\n🎯 ANALYSIS TARGET: Week {self.week}")
        print(f"   Sleeper User: {self.sleeper_user_id}")
        print(f"   MFL Franchise: {self.mfl_franchise_id} (PuntersForDays)")
        
        # Step 2: Get matchups
        sleeper_matchup, sleeper_opponent = self.get_sleeper_matchup(self.week)
        mfl_matchup, mfl_opponent_id = self.get_mfl_matchup(self.week)
        
        # Step 3: Display roster comparisons
        if sleeper_matchup and sleeper_opponent:
            your_starters = sleeper_matchup.get("starters", [])
            opponent_starters = sleeper_opponent.get("starters", [])
            
            # Get opponent info for display
            rosters_url = f"{self.sleeper_base}/league/{self.sleeper_league_id}/rosters"
            rosters_response = requests.get(rosters_url, timeout=10)
            rosters = rosters_response.json()
            
            users_url = f"{self.sleeper_base}/league/{self.sleeper_league_id}/users"
            users_response = requests.get(users_url, timeout=10)
            users = users_response.json()
            
            opp_roster_id = sleeper_opponent.get("roster_id")
            opp_roster = next((r for r in rosters if r.get("roster_id") == opp_roster_id), {})
            opp_owner_id = opp_roster.get("owner_id")
            opp_user = next((u for u in users if u.get("user_id") == opp_owner_id), {})
            opp_name = opp_user.get("display_name") or opp_user.get("username") or "Unknown"
            
            self.display_roster_comparison(your_starters, opponent_starters, "sleeper", opp_name)
        
        # Step 4: Get and display MFL matchup
        if mfl_matchup and mfl_opponent_id:
            # Get your roster
            your_roster_params = {
                "TYPE": "rosters",
                "L": self.mfl_league_id,
                "FRANCHISE": self.mfl_franchise_id,
                "JSON": "1"
            }
            
            your_roster_response = requests.get(self.mfl_base, params=your_roster_params, timeout=10)
            your_roster_data = your_roster_response.json()
            your_roster_franchise = your_roster_data.get("rosters", {}).get("franchise", {})
            if isinstance(your_roster_franchise, list):
                your_roster_franchise = your_roster_franchise[0] if your_roster_franchise else {}
            
            your_players = your_roster_franchise.get("player", [])
            if not isinstance(your_players, list):
                your_players = [your_players] if your_players else []
            your_player_ids = [p.get("id") for p in your_players if p.get("id")]
            
            # Get opponent roster
            opp_roster_params = {
                "TYPE": "rosters",
                "L": self.mfl_league_id,
                "FRANCHISE": mfl_opponent_id,
                "JSON": "1"
            }
            
            opp_roster_response = requests.get(self.mfl_base, params=opp_roster_params, timeout=10)
            opp_roster_data = opp_roster_response.json()
            opp_roster_franchise = opp_roster_data.get("rosters", {}).get("franchise", {})
            if isinstance(opp_roster_franchise, list):
                opp_roster_franchise = opp_roster_franchise[0] if opp_roster_franchise else {}
            
            opp_players = opp_roster_franchise.get("player", [])
            if not isinstance(opp_players, list):
                opp_players = [opp_players] if opp_players else []
            opp_player_ids = [p.get("id") for p in opp_players if p.get("id")]
            
            # Get opponent name
            league_params = {
                "TYPE": "league",
                "L": self.mfl_league_id,
                "JSON": "1"
            }
            
            league_response = requests.get(self.mfl_base, params=league_params, timeout=10)
            league_data = league_response.json()
            franchises = league_data.get("league", {}).get("franchises", {}).get("franchise", [])
            
            opp_name = "Unknown"
            for franchise in franchises:
                if franchise.get("id") == mfl_opponent_id:
                    opp_name = franchise.get("name", f"Franchise_{mfl_opponent_id}")
                    break
            
            self.display_roster_comparison(your_player_ids, opp_player_ids, "mfl", opp_name)
        
        # Step 5: Display available players
        sleeper_available = self.get_available_players("sleeper", 10)
        mfl_available = self.get_available_players("mfl", 10)
        
        self.display_available_players(sleeper_available, "sleeper")
        self.display_available_players(mfl_available, "mfl")
        
        # Step 6: Display waiver status
        sleeper_waiver, mfl_faab = self.get_waiver_status()
        self.display_waiver_status(sleeper_waiver, mfl_faab)
        
        print(f"\n🎉 ANALYSIS COMPLETE!")
        print("=" * 60)

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Fantasy Football Weekly Analysis")
    parser.add_argument("--week", type=int, default=1, help="Week number to analyze (default: 1)")
    parser.add_argument("--diagnose", action="store_true", help="Run network diagnostics only")
    args = parser.parse_args()
    
    if args.diagnose:
        print("🏈 NETWORK DIAGNOSTICS")
        print("=" * 60)
        diagnose_network_connectivity()
        return
    
    analyzer = FantasyAnalyzer(week=args.week)
    analyzer.run_analysis()

if __name__ == "__main__":
    main()