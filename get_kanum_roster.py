#!/usr/bin/env python3
"""
Get Kanum's Specific Roster from Sleeper
Shows actual player names, positions, and NFL teams
"""
import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "manual_test"))

from manual_test.sleeper_client import SleeperManualTestClient

def load_environment():
    """Load environment variables"""
    env_path = project_root / ".env"
    if env_path.exists():
        from dotenv import load_dotenv
        load_dotenv(env_path)
    
    sleeper_league_id = os.getenv("SLEEPER_LEAGUE_ID")
    if not sleeper_league_id:
        print("❌ Error: SLEEPER_LEAGUE_ID not found in .env file")
        sys.exit(1)
    
    return sleeper_league_id

def find_kanum_team(teams):
    """Find Kanum's team from the user list"""
    kanum_user_id = None
    kanum_team = None
    
    print("🔍 Looking for team 'Kanum'...")
    print("Available teams:")
    
    for team in teams:
        print(f"  - {team.owner_name} (ID: {team.team_id})")
        
        # Look for "Kanum" in display name or username
        if "kanum" in team.owner_name.lower():
            kanum_user_id = team.team_id
            kanum_team = team
            print(f"  ✅ Found Kanum's team: {team.owner_name}")
    
    if not kanum_user_id:
        print("❌ Team 'Kanum' not found in league!")
        print("Please check the team name or league ID")
        return None, None
    
    return kanum_user_id, kanum_team

def get_kanum_roster(client, kanum_user_id):
    """Get Kanum's specific roster"""
    print(f"\n📋 Getting roster for team ID: {kanum_user_id}...")
    
    # Get all rosters
    rosters_response = client.get_rosters()
    if not rosters_response.success:
        print(f"❌ Failed to get rosters: {rosters_response.error_message}")
        return None
    
    # Find Kanum's roster
    kanum_roster = None
    for roster in rosters_response.data:
        if roster.team_id == kanum_user_id:
            kanum_roster = roster
            break
    
    if not kanum_roster:
        print(f"❌ Roster not found for team ID: {kanum_user_id}")
        return None
    
    print(f"✅ Found roster: {kanum_roster.total_players} total players")
    print(f"   - Starters: {len(kanum_roster.starters)}")
    print(f"   - Bench: {len(kanum_roster.bench)}")
    
    return kanum_roster

def get_player_details(client, player_ids):
    """Get actual player names and details from NFL database"""
    print(f"\n🏈 Getting player details for {len(player_ids)} players...")
    
    # Get all NFL players
    players_response = client.get_players()
    if not players_response.success:
        print(f"❌ Failed to get player details: {players_response.error_message}")
        return {}
    
    # Note: The client limits to 100 players for demo, but we can get the full database
    # Let's make a direct call to get all players
    endpoint = "/players/nfl"
    full_players_response = client._make_request(endpoint)
    
    if not full_players_response.success:
        print(f"❌ Failed to get full player database: {full_players_response.error_message}")
        return {}
    
    all_players = full_players_response.data
    print(f"✅ Retrieved {len(all_players)} NFL players from database")
    
    # Create player details lookup
    player_details = {}
    for player_id in player_ids:
        if player_id in all_players:
            player_data = all_players[player_id]
            first_name = player_data.get('first_name', '')
            last_name = player_data.get('last_name', '')
            name = f"{first_name} {last_name}".strip()
            
            player_details[player_id] = {
                'name': name if name else f"Player_{player_id}",
                'position': player_data.get('position', 'UNK'),
                'team': player_data.get('team', 'FA'),
                'status': player_data.get('status', 'Active')
            }
        else:
            # Fallback for missing players
            player_details[player_id] = {
                'name': f"Unknown_Player_{player_id}",
                'position': 'UNK',
                'team': 'FA',
                'status': 'Unknown'
            }
    
    return player_details

def display_kanum_roster(kanum_roster, player_details):
    """Display Kanum's roster in a nice format"""
    print("\n" + "=" * 60)
    print(f"🏈 {kanum_roster.owner_name.upper()}'S SLEEPER ROSTER")
    print("=" * 60)
    
    # Display starters
    print(f"\n🟢 STARTERS ({len(kanum_roster.starters)}):")
    print("-" * 40)
    for i, player_id in enumerate(kanum_roster.starters, 1):
        player = player_details.get(player_id, {})
        name = player.get('name', f'Player_{player_id}')
        position = player.get('position', 'UNK')
        team = player.get('team', 'FA')
        print(f"{i:2d}. {position:3s}: {name:25s} ({team})")
    
    # Display bench
    if kanum_roster.bench:
        print(f"\n🔶 BENCH ({len(kanum_roster.bench)}):")
        print("-" * 40)
        for i, player_id in enumerate(kanum_roster.bench, 1):
            player = player_details.get(player_id, {})
            name = player.get('name', f'Player_{player_id}')
            position = player.get('position', 'UNK')
            team = player.get('team', 'FA')
            print(f"{i:2d}. {position:3s}: {name:25s} ({team})")
    
    # Summary
    print("\n" + "=" * 60)
    print(f"📊 ROSTER SUMMARY:")
    print(f"   Total Players: {kanum_roster.total_players}")
    print(f"   Starters: {len(kanum_roster.starters)}")
    print(f"   Bench: {len(kanum_roster.bench)}")
    print("=" * 60)

def main():
    """Main function to get and display Kanum's roster"""
    print("🏈 GETTING KANUM'S SLEEPER ROSTER")
    print("=" * 60)
    
    # Load environment
    sleeper_league_id = load_environment()
    print(f"📱 Using Sleeper League ID: {sleeper_league_id}")
    
    # Initialize client
    client = SleeperManualTestClient(sleeper_league_id)
    
    # Get users/teams
    print("\n👥 Getting league teams...")
    users_response = client.get_users()
    if not users_response.success:
        print(f"❌ Failed to get teams: {users_response.error_message}")
        sys.exit(1)
    
    teams = users_response.data
    print(f"✅ Found {len(teams)} teams in league")
    
    # Find Kanum's team
    kanum_user_id, kanum_team = find_kanum_team(teams)
    if not kanum_user_id:
        sys.exit(1)
    
    # Get Kanum's roster
    kanum_roster = get_kanum_roster(client, kanum_user_id)
    if not kanum_roster:
        sys.exit(1)
    
    # Get all player IDs from roster
    all_player_ids = kanum_roster.starters + kanum_roster.bench
    if not all_player_ids:
        print("❌ No players found in roster!")
        sys.exit(1)
    
    # Get player details
    player_details = get_player_details(client, all_player_ids)
    
    # Display the roster
    display_kanum_roster(kanum_roster, player_details)

if __name__ == "__main__":
    main()