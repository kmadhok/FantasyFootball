#!/usr/bin/env python3
"""
Quick investigation script for usage data quality issues
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.database import SessionLocal, Player, PlayerUsage
from sqlalchemy import func

def main():
    db = SessionLocal()
    
    try:
        print("=== USAGE DATA INVESTIGATION ===")
        
        # Check overall usage data
        total_usage = db.query(PlayerUsage).count()
        current_season_usage = db.query(PlayerUsage).filter(PlayerUsage.season == 2025).count()
        recent_usage = db.query(PlayerUsage).filter(PlayerUsage.season == 2024).count()
        
        print(f"\nUsage Data Overview:")
        print(f"  Total usage records: {total_usage}")
        print(f"  2025 season records: {current_season_usage}")
        print(f"  2024 season records: {recent_usage}")
        
        # Check field completeness for Epic A required fields
        print(f"\nEpic A Field Completeness Analysis:")
        epic_fields = [
            ('week', PlayerUsage.week),
            ('snap_pct', PlayerUsage.snap_pct),
            ('route_pct', PlayerUsage.route_pct),
            ('target_share', PlayerUsage.target_share),
            ('carry_share', PlayerUsage.carry_share),
            ('rz_touches', PlayerUsage.rz_touches),
            ('ez_targets', PlayerUsage.ez_targets)
        ]
        
        for field_name, field_attr in epic_fields:
            non_null_count = db.query(PlayerUsage).filter(
                PlayerUsage.season == 2024,  # Use 2024 data since it's more complete
                field_attr.isnot(None)
            ).count()
            
            non_zero_count = db.query(PlayerUsage).filter(
                PlayerUsage.season == 2024,
                field_attr.isnot(None),
                field_attr > 0
            ).count() if field_name != 'week' else non_null_count
            
            print(f"  {field_name}: {non_null_count} non-null, {non_zero_count} non-zero")
        
        # Check specific player usage data
        print(f"\nSpecific Player Usage Analysis:")
        test_players = ['CeeDee Lamb', 'Josh Allen', 'Christian McCaffrey']
        
        for player_name in test_players:
            # Find player
            player = db.query(Player).filter(
                Player.name.contains(player_name.split()[-1])
            ).first()
            
            if player:
                usage_records = db.query(PlayerUsage).filter(
                    PlayerUsage.player_id == player.id,
                    PlayerUsage.season == 2024
                ).all()
                
                print(f"  {player_name} ({player.id}):")
                print(f"    Usage records: {len(usage_records)}")
                
                if usage_records:
                    # Show sample record
                    sample = usage_records[0]
                    print(f"    Sample week {sample.week}:")
                    print(f"      snap_pct: {sample.snap_pct}")
                    print(f"      route_pct: {sample.route_pct}")
                    print(f"      target_share: {sample.target_share}")
                    print(f"      carry_share: {sample.carry_share}")
                    print(f"      rz_touches: {sample.rz_touches}")
                    print(f"      ez_targets: {sample.ez_targets}")
                    
                    # Check if values are realistic
                    realistic_snap = sample.snap_pct and sample.snap_pct > 5 if player.position in ['QB', 'RB', 'WR', 'TE'] else True
                    print(f"    Realistic data: {'Yes' if realistic_snap else 'No'}")
            else:
                print(f"  {player_name}: NOT FOUND")
        
        # Check data source/creation patterns
        print(f"\nUsage Data Patterns:")
        
        # Check if usage data is linked to any platform
        usage_with_platform_players = db.query(PlayerUsage).join(Player).filter(
            Player.sleeper_id.isnot(None)
        ).count()
        
        usage_with_nfl_players = db.query(PlayerUsage).join(Player).filter(
            Player.nfl_id.isnot(None)
        ).count()
        
        print(f"  Usage records linked to Sleeper players: {usage_with_platform_players}")
        print(f"  Usage records linked to NFL players: {usage_with_nfl_players}")
        
        # Check week distribution
        week_distribution = db.query(
            PlayerUsage.week,
            func.count(PlayerUsage.id).label('count')
        ).filter(
            PlayerUsage.season == 2024
        ).group_by(PlayerUsage.week).order_by(PlayerUsage.week).all()
        
        print(f"  Week distribution (2024):")
        for week, count in week_distribution:
            print(f"    Week {week}: {count} records")
        
        # Sample usage records to understand data structure
        print(f"\nSample Usage Records:")
        sample_usage = db.query(PlayerUsage).filter(
            PlayerUsage.season == 2024,
            PlayerUsage.snap_pct.isnot(None),
            PlayerUsage.snap_pct > 0
        ).limit(5).all()
        
        for usage in sample_usage:
            player = db.query(Player).filter(Player.id == usage.player_id).first()
            print(f"  Week {usage.week}, {player.name if player else 'Unknown'} ({player.position if player else 'Unknown'}):")
            print(f"    snap_pct: {usage.snap_pct}, route_pct: {usage.route_pct}, target_share: {usage.target_share}")
    
    finally:
        db.close()

if __name__ == "__main__":
    main()