#!/usr/bin/env python3
"""
Quick investigation script for player ID linking issues
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.database import SessionLocal, Player
from src.utils.player_id_mapper import PlayerIDMapper

def main():
    db = SessionLocal()
    player_mapper = PlayerIDMapper()
    
    try:
        print("=== PLAYER ID INVESTIGATION ===")
        
        # Check overall player ID distribution
        sleeper_count = db.query(Player).filter(Player.sleeper_id.isnot(None)).count()
        mfl_count = db.query(Player).filter(Player.mfl_id.isnot(None)).count()
        nfl_count = db.query(Player).filter(Player.nfl_id.isnot(None)).count()
        total_players = db.query(Player).count()
        
        print(f"\nPlayer ID Distribution:")
        print(f"  Total players: {total_players}")
        print(f"  Players with Sleeper ID: {sleeper_count}")
        print(f"  Players with MFL ID: {mfl_count}")
        print(f"  Players with NFL ID: {nfl_count}")
        
        # Check specific test players
        print(f"\nTest Players Analysis:")
        test_players = [
            {'name': 'CeeDee Lamb', 'position': 'WR', 'team': 'DAL'},
            {'name': 'Josh Allen', 'position': 'QB', 'team': 'BUF'},
            {'name': 'Christian McCaffrey', 'position': 'RB', 'team': 'SF'}
        ]
        
        for player_info in test_players:
            # Try to find by name
            player = db.query(Player).filter(
                Player.name.contains(player_info['name'].split()[-1]),  # Last name
                Player.position == player_info['position']
            ).first()
            
            if player:
                print(f"  {player_info['name']}:")
                print(f"    Found: {player.name} (ID: {player.id})")
                print(f"    Sleeper ID: {player.sleeper_id}")
                print(f"    MFL ID: {player.mfl_id}")
                print(f"    NFL ID: {player.nfl_id}")
                
                # Test canonical ID generation
                canonical_id = player_mapper.generate_canonical_id(
                    player_info['name'], player_info['position'], player_info['team']
                )
                print(f"    Generated canonical: {canonical_id}")
                print(f"    Matches stored NFL ID: {canonical_id == player.nfl_id}")
                
            else:
                print(f"  {player_info['name']}: NOT FOUND")
            print()
        
        # Check for any players with multiple platform IDs
        cross_platform = db.query(Player).filter(
            Player.sleeper_id.isnot(None),
            Player.mfl_id.isnot(None)
        ).limit(5).all()
        
        print(f"Players with both Sleeper and MFL IDs: {len(cross_platform)}")
        for player in cross_platform:
            print(f"  {player.name}: sleeper={player.sleeper_id}, mfl={player.mfl_id}")
        
        # Check roster entries to see where platform linking might happen
        print(f"\nRoster Entry Analysis:")
        from src.database import RosterEntry
        
        sleeper_rosters = db.query(RosterEntry).filter(RosterEntry.platform == 'sleeper').count()
        mfl_rosters = db.query(RosterEntry).filter(RosterEntry.platform == 'mfl').count()
        
        print(f"  Sleeper roster entries: {sleeper_rosters}")
        print(f"  MFL roster entries: {mfl_rosters}")
        
        # Sample roster entries to see if they link to players correctly
        sample_sleeper = db.query(RosterEntry).filter(RosterEntry.platform == 'sleeper').first()
        if sample_sleeper:
            linked_player = db.query(Player).filter(Player.id == sample_sleeper.player_id).first()
            print(f"  Sample Sleeper roster -> Player: {linked_player.name if linked_player else 'None'}")
            if linked_player:
                print(f"    Player has sleeper_id: {linked_player.sleeper_id}")
        
        sample_mfl = db.query(RosterEntry).filter(RosterEntry.platform == 'mfl').first()
        if sample_mfl:
            linked_player = db.query(Player).filter(Player.id == sample_mfl.player_id).first()
            print(f"  Sample MFL roster -> Player: {linked_player.name if linked_player else 'None'}")
            if linked_player:
                print(f"    Player has mfl_id: {linked_player.mfl_id}")
        
    finally:
        db.close()

if __name__ == "__main__":
    main()