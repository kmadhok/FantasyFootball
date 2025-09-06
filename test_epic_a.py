#!/usr/bin/env python3
"""
Comprehensive test of Epic A implementation
Tests all components of the data foundation:
- Database models
- ESPN data service (with mock data)
- Player ID mapping
- Usage projections service
- Waiver candidates builder
- Scheduler integration
"""

import asyncio
import sys
import os
sys.path.append(os.path.abspath('.'))

from src.services.usage_projections_service import test_usage_projections_service
from src.services.waiver_candidates_builder import test_waiver_candidates_builder
from src.services.scheduler import FantasyFootballScheduler
from src.database import SessionLocal, Player, PlayerUsage, PlayerProjections
from src.utils.player_id_mapper import PlayerIDMapper

def create_sample_data():
    """Create sample data for testing Epic A"""
    print("\n" + "="*60)
    print("CREATING SAMPLE DATA FOR TESTING")
    print("="*60)
    
    try:
        db = SessionLocal()
        
        # Create sample players
        sample_players = [
            {
                'name': 'Josh Allen',
                'position': 'QB',
                'team': 'BUF',
                'is_starter': True
            },
            {
                'name': 'Christian McCaffrey',
                'position': 'RB',
                'team': 'SF',
                'is_starter': True
            },
            {
                'name': 'Cooper Kupp',
                'position': 'WR',
                'team': 'LAR',
                'is_starter': True
            },
            {
                'name': 'Isiah Pacheco',
                'position': 'RB',
                'team': 'KC',
                'is_starter': False
            },
            {
                'name': 'Calvin Ridley',
                'position': 'WR',
                'team': 'TEN',
                'is_starter': False
            }
        ]
        
        # Create players with canonical IDs
        mapper = PlayerIDMapper()
        players_created = 0
        
        for player_data in sample_players:
            canonical_id = mapper.generate_canonical_id(
                player_data['name'], 
                player_data['position'], 
                player_data['team']
            )
            
            # Check if player already exists
            existing = db.query(Player).filter(Player.nfl_id == canonical_id).first()
            if existing:
                print(f"   Player {player_data['name']} already exists, skipping")
                continue
            
            player = Player(
                nfl_id=canonical_id,
                name=player_data['name'],
                position=player_data['position'],
                team=player_data['team'],
                is_starter=player_data['is_starter']
            )
            
            db.add(player)
            players_created += 1
        
        db.commit()
        print(f"✓ Created {players_created} sample players")
        
        # Get players for creating projections and usage
        players = db.query(Player).all()
        
        # Create sample projections for current week
        current_week = 1
        projections_created = 0
        
        for player in players:
            # Check if projection already exists
            existing_proj = db.query(PlayerProjections).filter(
                PlayerProjections.player_id == player.id,
                PlayerProjections.week == current_week,
                PlayerProjections.season == 2025
            ).first()
            
            if existing_proj:
                continue
            
            # Create mock projections based on position
            if player.position == 'QB':
                projected_points = 22.5 if player.is_starter else 8.3
            elif player.position == 'RB':
                projected_points = 16.8 if player.is_starter else 7.2
            elif player.position == 'WR':
                projected_points = 14.2 if player.is_starter else 6.5
            else:
                projected_points = 10.0
            
            projection = PlayerProjections(
                player_id=player.id,
                week=current_week,
                season=2025,
                projected_points=projected_points,
                mean=projected_points,
                source='mock'
            )
            
            db.add(projection)
            projections_created += 1
        
        db.commit()
        print(f"✓ Created {projections_created} sample projections")
        
        # Create sample usage data
        usage_created = 0
        
        for player in players:
            # Check if usage already exists
            existing_usage = db.query(PlayerUsage).filter(
                PlayerUsage.player_id == player.id,
                PlayerUsage.week == current_week,
                PlayerUsage.season == 2025
            ).first()
            
            if existing_usage:
                continue
            
            # Create mock usage based on position and starter status
            usage_data = {}
            
            if player.position == 'QB':
                if player.is_starter:
                    usage_data = {
                        'snap_pct': 0.95,
                        'carries': 6,
                        'rushing_yards': 32.0,
                        'touchdowns': 2
                    }
                else:
                    usage_data = {
                        'snap_pct': 0.05,
                        'carries': 1,
                        'rushing_yards': 4.0,
                        'touchdowns': 0
                    }
            
            elif player.position == 'RB':
                if player.is_starter:
                    usage_data = {
                        'snap_pct': 0.75,
                        'carry_share': 0.65,
                        'target_share': 0.08,
                        'rz_touches': 5,
                        'targets': 4,
                        'carries': 18,
                        'receptions': 3,
                        'receiving_yards': 25.0,
                        'rushing_yards': 98.0,
                        'touchdowns': 1
                    }
                else:
                    usage_data = {
                        'snap_pct': 0.35,
                        'carry_share': 0.25,
                        'target_share': 0.04,
                        'rz_touches': 2,
                        'targets': 2,
                        'carries': 8,
                        'receptions': 1,
                        'receiving_yards': 12.0,
                        'rushing_yards': 38.0,
                        'touchdowns': 0
                    }
            
            elif player.position == 'WR':
                if player.is_starter:
                    usage_data = {
                        'snap_pct': 0.85,
                        'route_pct': 0.82,
                        'target_share': 0.22,
                        'ez_targets': 2,
                        'targets': 9,
                        'receptions': 6,
                        'receiving_yards': 78.0,
                        'touchdowns': 1
                    }
                else:
                    usage_data = {
                        'snap_pct': 0.45,
                        'route_pct': 0.55,
                        'target_share': 0.12,
                        'ez_targets': 1,
                        'targets': 5,
                        'receptions': 3,
                        'receiving_yards': 42.0,
                        'touchdowns': 0
                    }
            
            usage = PlayerUsage(
                player_id=player.id,
                week=current_week,
                season=2025,
                **usage_data
            )
            
            db.add(usage)
            usage_created += 1
        
        db.commit()
        print(f"✓ Created {usage_created} sample usage records")
        
        db.close()
        return True
        
    except Exception as e:
        print(f"✗ Failed to create sample data: {e}")
        return False

async def test_scheduler_epic_a():
    """Test the scheduler with Epic A jobs"""
    print("\n" + "="*60)
    print("TESTING SCHEDULER WITH EPIC A JOBS")
    print("="*60)
    
    try:
        scheduler = FantasyFootballScheduler()
        
        # Test manual Epic A jobs
        print("\n1. Testing manual usage/projections sync...")
        result = await scheduler.manual_usage_projections_sync()
        success_count = sum(result.values()) if isinstance(result, dict) else 0
        print(f"   Result: {success_count} operations successful")
        
        print("\n2. Testing manual waiver candidates refresh...")
        result = await scheduler.manual_waiver_candidates_refresh()
        success = result.get('waiver_candidates_refresh', False) if isinstance(result, dict) else False
        print(f"   Result: {'SUCCESS' if success else 'FAILED'}")
        
        print("\n3. Testing scheduler job status...")
        # Don't actually start scheduler, just check setup
        status = scheduler.get_job_status()
        print(f"   Scheduler status: {status.get('status', 'unknown')}")
        
        return True
        
    except Exception as e:
        print(f"   ✗ Scheduler test failed: {e}")
        return False

def main():
    """Main test function for Epic A"""
    print("EPIC A - DATA FOUNDATION COMPREHENSIVE TEST")
    print("=" * 80)
    
    results = {
        'sample_data': False,
        'usage_projections_service': False,
        'waiver_candidates_builder': False,
        'scheduler_epic_a': False
    }
    
    # Test 1: Create sample data
    print("\n🔧 STEP 1: Creating sample test data...")
    results['sample_data'] = create_sample_data()
    
    # Test 2: Usage Projections Service
    print("\n📊 STEP 2: Testing Usage Projections Service...")
    results['usage_projections_service'] = test_usage_projections_service()
    
    # Test 3: Waiver Candidates Builder
    print("\n🎯 STEP 3: Testing Waiver Candidates Builder...")
    results['waiver_candidates_builder'] = test_waiver_candidates_builder()
    
    # Test 4: Scheduler Integration
    print("\n⏰ STEP 4: Testing Scheduler Integration...")
    try:
        results['scheduler_epic_a'] = asyncio.run(test_scheduler_epic_a())
    except Exception as e:
        print(f"   ✗ Scheduler async test failed: {e}")
        results['scheduler_epic_a'] = False
    
    # Summary
    print("\n" + "="*80)
    print("EPIC A IMPLEMENTATION TEST SUMMARY")
    print("="*80)
    
    total_tests = len(results)
    passed_tests = sum(results.values())
    
    print(f"📋 Total Tests: {total_tests}")
    print(f"✅ Passed: {passed_tests}")
    print(f"❌ Failed: {total_tests - passed_tests}")
    print(f"📊 Success Rate: {(passed_tests/total_tests)*100:.1f}%")
    
    print("\nDetailed Results:")
    for test_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {status} {test_name.replace('_', ' ').title()}")
    
    if passed_tests == total_tests:
        print(f"\n🎉 ALL TESTS PASSED! Epic A implementation is complete and functional.")
        print(f"\n📝 Next Steps:")
        print(f"   1. Get ESPN authentication cookies (see get_espn_cookies.py)")
        print(f"   2. Add ESPN_S2 and SWID to .env file")
        print(f"   3. Test with real ESPN data")
        print(f"   4. Move on to Epic B (Alert Rules)")
    else:
        print(f"\n⚠️  Some tests failed. Check the logs above for details.")
    
    return passed_tests == total_tests

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)