#!/usr/bin/env python3
"""
Get PUNTERSFORDAYS MFL Roster with Real Team Names
Retrieves actual team names from MFL franchises endpoint and finds your roster
"""
import os
import sys
import requests
from pathlib import Path
from dotenv import load_dotenv

# Load environment
project_root = Path(__file__).parent
load_dotenv(project_root / ".env")

def get_mfl_config():
    """Get MFL configuration from environment"""
    league_id = os.getenv('MFL_LEAGUE_ID')
    if not league_id:
        print("❌ Error: MFL_LEAGUE_ID not found in .env file")
        sys.exit(1)
    return league_id

def get_mfl_franchises(league_id):
    """Get all franchises with actual team names from league info"""
    print("🏈 Getting MFL franchise names...")
    
    base_url = "https://api.myfantasyleague.com/2025/export"
    params = {
        "TYPE": "league",
        "L": league_id,
        "JSON": "1"
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Get franchises from league data
        league_data = data.get("league", {})
        franchises_data = league_data.get("franchises", {})
        franchises = franchises_data.get("franchise", [])
        
        # Handle single franchise case
        if not isinstance(franchises, list):
            franchises = [franchises] if franchises else []
        
        print(f"✅ Retrieved {len(franchises)} franchises")
        
        # Create franchise lookup
        franchise_lookup = {}
        print(f"\nAvailable teams:")
        for franchise in franchises:
            franchise_id = franchise.get("id")
            franchise_name = franchise.get("name", f"Franchise_{franchise_id}")
            franchise_lookup[franchise_id] = franchise_name
            print(f"  - {franchise_name} (ID: {franchise_id})")
        
        return franchise_lookup
        
    except Exception as e:
        print(f"❌ Error getting MFL franchises: {e}")
        return {}

def find_puntersfordays_franchise(franchise_lookup, team_name="PuntersForDays"):
    """Find PUNTERSFORDAYS franchise ID"""
    print(f"\n🔍 Searching for team: '{team_name}'...")
    
    target_franchise_id = None
    target_franchise_name = None
    
    for franchise_id, franchise_name in franchise_lookup.items():
        if team_name.lower() in franchise_name.lower():
            target_franchise_id = franchise_id
            target_franchise_name = franchise_name
            print(f"  ✅ FOUND: {franchise_name} (ID: {franchise_id})")
            break
    
    if not target_franchise_id:
        print(f"  ❌ Team '{team_name}' not found in MFL league")
        return None, None
    
    return target_franchise_id, target_franchise_name

def get_mfl_roster(league_id, franchise_id):
    """Get roster for specific franchise"""
    print(f"\n📋 Getting roster for franchise {franchise_id}...")
    
    base_url = "https://api.myfantasyleague.com/2025/export"
    params = {
        "TYPE": "rosters",
        "L": league_id,
        "FRANCHISE": franchise_id,
        "JSON": "1"
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        rosters_data = data.get("rosters", {})
        franchise_data = rosters_data.get("franchise", {})
        
        # Handle the franchise data structure
        if isinstance(franchise_data, list):
            franchise_data = franchise_data[0] if franchise_data else {}
        
        player_data = franchise_data.get("player", [])
        
        # Handle player data structure
        if not isinstance(player_data, list):
            player_data = [player_data] if player_data else []
        
        print(f"✅ Found {len(player_data)} players in roster")
        
        return player_data
        
    except Exception as e:
        print(f"❌ Error getting MFL roster: {e}")
        return []

def get_mfl_players(league_id):
    """Get all NFL players from MFL with their details"""
    print(f"\n🏈 Getting NFL players database from MFL...")
    
    base_url = "https://api.myfantasyleague.com/2025/export"
    params = {
        "TYPE": "players",
        "L": league_id,
        "JSON": "1"
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        players_data = data.get("players", {})
        players = players_data.get("player", [])
        
        # Handle single player case
        if not isinstance(players, list):
            players = [players] if players else []
        
        print(f"✅ Retrieved {len(players)} NFL players")
        
        # Create player lookup dictionary
        player_lookup = {}
        for player in players:
            player_id = player.get("id")
            if player_id:
                player_lookup[player_id] = player
        
        return player_lookup
        
    except Exception as e:
        print(f"❌ Error getting MFL players: {e}")
        return {}

def display_puntersfordays_roster(franchise_name, player_data, player_lookup):
    """Display PUNTERSFORDAYS roster with player details"""
    print(f"\n" + "=" * 60)
    print(f"🏈 {franchise_name.upper()}'S MFL ROSTER")
    print("=" * 60)
    
    if not player_data:
        print("❌ No players found in roster")
        return
    
    print(f"Total Players: {len(player_data)}")
    print("-" * 60)
    
    found_players = 0
    missing_players = 0
    
    for i, player_info in enumerate(player_data, 1):
        player_id = player_info.get("id", "Unknown")
        player_status = player_info.get("status", "active")
        
        if player_id in player_lookup:
            player_details = player_lookup[player_id]
            name = player_details.get("name", f"Player_{player_id}")
            position = player_details.get("position", "UNK")
            team = player_details.get("team", "FA")
            
            print(f"{i:2d}. {position:3s}: {name:25s} ({team}) - {player_status}")
            found_players += 1
        else:
            print(f"{i:2d}. UNK: Player_{player_id:12s} (FA) - {player_status} [ID: {player_id}] - NOT FOUND")
            missing_players += 1
    
    print("-" * 60)
    print(f"📊 SUMMARY:")
    print(f"   Total Players: {len(player_data)}")
    print(f"   Found Details: {found_players}")
    print(f"   Missing Details: {missing_players}")
    print("=" * 60)

def main():
    """Main function to get and display PuntersForDays roster"""
    print("🏈 GETTING PUNTERSFORDAYS MFL ROSTER")
    print("=" * 60)
    
    # Get MFL configuration
    league_id = get_mfl_config()
    print(f"📱 Using MFL League ID: {league_id}")
    
    # Get franchise names
    franchise_lookup = get_mfl_franchises(league_id)
    if not franchise_lookup:
        print("❌ Could not retrieve franchise information")
        sys.exit(1)
    
    # Find PUNTERSFORDAYS franchise
    franchise_id, franchise_name = find_puntersfordays_franchise(franchise_lookup)
    if not franchise_id:
        sys.exit(1)
    
    # Get roster for PUNTERSFORDAYS
    player_data = get_mfl_roster(league_id, franchise_id)
    if not player_data:
        print("❌ Could not retrieve roster information")
        sys.exit(1)
    
    # Get player details
    player_lookup = get_mfl_players(league_id)
    
    # Display the roster
    display_puntersfordays_roster(franchise_name, player_data, player_lookup)

if __name__ == "__main__":
    main()