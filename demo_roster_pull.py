#!/usr/bin/env python3
"""
Simple demonstration of pulling roster information from both platforms
"""
import os
import sys
import json
import requests
from pathlib import Path
from dotenv import load_dotenv

# Load environment
project_root = Path(__file__).parent
load_dotenv(project_root / ".env")

def get_sleeper_rosters():
    """Get Sleeper roster information"""
    league_id = os.getenv('SLEEPER_LEAGUE_ID')
    print(f"🏈 SLEEPER LEAGUE ROSTERS (League: {league_id})")
    print("=" * 60)
    
    # Get league info
    league_url = f"https://api.sleeper.app/v1/league/{league_id}"
    league_response = requests.get(league_url)
    league_data = league_response.json()
    print(f"League Name: {league_data.get('name', 'Unknown')}")
    print(f"Total Rosters: {league_data.get('total_rosters', 0)}")
    
    # Get rosters
    rosters_url = f"https://api.sleeper.app/v1/league/{league_id}/rosters"
    rosters_response = requests.get(rosters_url)
    rosters = rosters_response.json()
    
    # Get users for name mapping
    users_url = f"https://api.sleeper.app/v1/league/{league_id}/users"
    users_response = requests.get(users_url)
    users = users_response.json()
    
    # Create user lookup
    user_lookup = {user["user_id"]: user.get("username", f"User_{user['user_id']}") for user in users}
    
    print(f"\nRoster Details:")
    for i, roster in enumerate(rosters[:5], 1):  # Show first 5
        owner_name = user_lookup.get(roster.get("owner_id"), "Unknown")
        player_count = len(roster.get("players", []))
        print(f"{i}. {owner_name}: {player_count} players")
    
    return rosters

def get_mfl_rosters():
    """Get MFL roster information"""
    league_id = os.getenv('MFL_LEAGUE_ID')
    print(f"\n🏈 MFL LEAGUE ROSTERS (League: {league_id})")
    print("=" * 60)
    
    base_url = "https://api.myfantasyleague.com/2025/export"
    
    # Get league info
    league_params = {"TYPE": "league", "L": league_id, "JSON": "1"}
    league_response = requests.get(base_url, params=league_params)
    league_data = league_response.json()
    league_info = league_data.get("league", {})
    print(f"League Name: {league_info.get('name', 'Unknown')}")
    
    # Get rosters
    roster_params = {"TYPE": "rosters", "L": league_id, "JSON": "1"}
    rosters_response = requests.get(base_url, params=roster_params)
    rosters_data = rosters_response.json()
    
    rosters = rosters_data.get("rosters", {}).get("franchise", [])
    if not isinstance(rosters, list):
        rosters = [rosters] if rosters else []
    
    print(f"\nRoster Details:")
    for i, roster in enumerate(rosters[:5], 1):  # Show first 5
        franchise_name = roster.get("name", f"Franchise_{roster.get('id', 'Unknown')}")
        
        # Handle player data structure
        player_data = roster.get("player", [])
        if not isinstance(player_data, list):
            player_data = [player_data] if player_data else []
        
        player_count = len(player_data)
        print(f"{i}. {franchise_name}: {player_count} players")
    
    return rosters

def compare_platforms():
    """Compare data from both platforms"""
    print(f"\n📊 PLATFORM COMPARISON")
    print("=" * 60)
    
    sleeper_rosters = get_sleeper_rosters()
    mfl_rosters = get_mfl_rosters()
    
    print(f"\nSummary:")
    print(f"Sleeper Rosters: {len(sleeper_rosters)}")
    print(f"MFL Rosters: {len(mfl_rosters)}")
    
    # Show data structure differences
    print(f"\nData Structure Examples:")
    print(f"Sleeper roster keys: {list(sleeper_rosters[0].keys()) if sleeper_rosters else 'No data'}")
    print(f"MFL roster keys: {list(mfl_rosters[0].keys()) if mfl_rosters else 'No data'}")

if __name__ == "__main__":
    compare_platforms()