#!/usr/bin/env python3
"""
CSV Lookup Validation Script
Tests enhanced player unification system against weekly_player_metrics.csv data
"""

import sys
import os
import pandas as pd
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.database import SessionLocal, Player
from src.utils.player_id_mapper import PlayerIDMapper

def main():
    print("=== CSV LOOKUP VALIDATION ===")
    print("Testing enhanced player unification against weekly_player_metrics.csv")
    print("=" * 70)
    
    # Load CSV data
    try:
        csv_file = "weekly_player_metrics.csv"
        if not os.path.exists(csv_file):
            print(f"❌ CSV file {csv_file} not found")
            return False
        
        df = pd.read_csv(csv_file)
        print(f"✓ Loaded {len(df)} records from {csv_file}")
        
    except Exception as e:
        print(f"❌ Failed to load CSV: {e}")
        return False
    
    # Initialize components
    db = SessionLocal()
    player_mapper = PlayerIDMapper()
    
    try:
        # Get unique players from CSV
        unique_players = df[['player_id', 'player_name', 'position']].drop_duplicates()
        print(f"✓ Found {len(unique_players)} unique players in CSV")
        print()
        
        # Test lookup statistics
        lookups_successful = 0
        cross_platform_found = 0
        canonical_matches = 0
        
        print("Testing individual player lookups:")
        print("-" * 50)
        
        # Test first 15 players for detailed analysis
        test_sample = unique_players.head(15)
        
        for _, row in test_sample.iterrows():
            csv_player_id = row['player_id']
            csv_name = row['player_name']
            csv_position = row['position']
            
            print(f"🔍 Testing: {csv_name} ({csv_position})")
            print(f"   CSV Player ID: {csv_player_id}")
            
            # Method 1: Direct name search in database
            db_matches = db.query(Player).filter(
                Player.name.ilike(f"%{csv_name.split()[-1]}%"),  # Last name
                Player.position == csv_position
            ).all()
            
            if db_matches:
                print(f"   ✓ Found {len(db_matches)} database match(es)")
                
                for match in db_matches:
                    print(f"     - {match.name} (ID: {match.id})")
                    print(f"       Sleeper: {match.sleeper_id}, MFL: {match.mfl_id}")
                    print(f"       NFL ID: {match.nfl_id}")
                    
                    # Check if cross-platform unified
                    if match.sleeper_id and match.mfl_id:
                        cross_platform_found += 1
                        print(f"       ✓ Cross-platform unified!")
                    
                    # Test canonical ID generation
                    try:
                        # Try with different team possibilities since CSV doesn't have team
                        potential_teams = ['BUF', 'SF', 'DAL', 'KC', 'UNKNOWN']
                        canonical_generated = None
                        
                        for team in potential_teams:
                            test_canonical = player_mapper.generate_canonical_id(
                                csv_name, csv_position, team
                            )
                            if test_canonical == match.nfl_id:
                                canonical_generated = test_canonical
                                canonical_matches += 1
                                print(f"       ✓ Canonical ID matches (team: {team})")
                                break
                        
                        if not canonical_generated:
                            print(f"       ⚠️  Canonical ID generation needs team context")
                    
                    except Exception as e:
                        print(f"       ⚠️  Canonical generation error: {e}")
                
                lookups_successful += 1
            else:
                print(f"   ❌ No database matches found")
                
                # Try fuzzy matching on name parts
                name_parts = csv_name.split()
                if len(name_parts) >= 2:
                    first_name = name_parts[0]
                    last_name = name_parts[-1]
                    
                    fuzzy_matches = db.query(Player).filter(
                        Player.name.ilike(f"%{first_name}%"),
                        Player.name.ilike(f"%{last_name}%"),
                        Player.position == csv_position
                    ).all()
                    
                    if fuzzy_matches:
                        print(f"   📝 Fuzzy matches found: {[p.name for p in fuzzy_matches[:3]]}")
            
            print()  # Blank line between players
        
        print("=" * 70)
        print("VALIDATION SUMMARY")
        print("=" * 70)
        
        # Overall statistics
        total_tested = len(test_sample)
        lookup_success_rate = (lookups_successful / total_tested) * 100
        cross_platform_rate = (cross_platform_found / total_tested) * 100
        canonical_match_rate = (canonical_matches / total_tested) * 100
        
        print(f"📊 Test Results:")
        print(f"   Players tested: {total_tested}")
        print(f"   Successful lookups: {lookups_successful} ({lookup_success_rate:.1f}%)")
        print(f"   Cross-platform unified: {cross_platform_found} ({cross_platform_rate:.1f}%)")
        print(f"   Canonical ID matches: {canonical_matches} ({canonical_match_rate:.1f}%)")
        print()
        
        # Database statistics
        total_db_players = db.query(Player).count()
        total_unified = db.query(Player).filter(
            Player.sleeper_id.isnot(None),
            Player.mfl_id.isnot(None)
        ).count()
        
        print(f"📈 Database Statistics:")
        print(f"   Total players in database: {total_db_players:,}")
        print(f"   Cross-platform unified: {total_unified:,}")
        print(f"   Overall unification rate: {(total_unified/total_db_players)*100:.1f}%")
        print()
        
        # Success criteria
        success = lookup_success_rate >= 60 and cross_platform_rate >= 20
        
        if success:
            print("✅ CSV VALIDATION PASSED")
            print("   Enhanced unification system successfully handles CSV data")
        else:
            print("⚠️  CSV VALIDATION NEEDS IMPROVEMENT") 
            print("   Consider adding team data to CSV for better matching")
        
        return success
        
    finally:
        db.close()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)