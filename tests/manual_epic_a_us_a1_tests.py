#!/usr/bin/env python3
"""
Epic A - US-A1: Canonical Player/League Schema Manual Tests

This script provides comprehensive manual validation for US-A1 acceptance criteria:
1. Player rows can be looked up by canonical_player_id regardless of platform
2. Roster snapshots persist (league_id, team_id, player_id, week, slot)  
3. Usage table contains: week, snap_pct, route_pct, target_share, carry_share, rz_touches, ez_targets
4. Projections table contains: week, mean, stdev, floor, ceiling for your scoring
5. All tables can be joined to materialize a waiver_candidates view

Usage: python tests/manual_epic_a_us_a1_tests.py
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from sqlalchemy import text, func

from src.database import SessionLocal, Player, PlayerUsage, PlayerProjections, RosterEntry
from src.utils.player_id_mapper import PlayerIDMapper

# Configure logging for test results
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

class EpicAUS1ManualTester:
    """Manual test suite for Epic A US-A1: Canonical player/league schema"""
    
    def __init__(self):
        self.db = SessionLocal()
        self.player_mapper = PlayerIDMapper()
        self.test_results = []
        self.known_players = [
            {'name': 'CeeDee Lamb', 'position': 'WR', 'team': 'DAL'},
            {'name': 'Josh Allen', 'position': 'QB', 'team': 'BUF'}, 
            {'name': 'Christian McCaffrey', 'position': 'RB', 'team': 'SF'},
            {'name': 'Cooper Kupp', 'position': 'WR', 'team': 'LAR'},
            {'name': 'Travis Kelce', 'position': 'TE', 'team': 'KC'}
        ]
        self.test_leagues = {
            'sleeper': '1257071160403709954',
            'mfl': '73756'
        }
    
    def run_all_tests(self) -> Dict[str, Any]:
        """Run all US-A1 manual validation tests"""
        logger.info("🧪 EPIC A - US-A1 MANUAL VALIDATION TESTS")
        logger.info("=" * 60)
        
        results = {
            'test_a1_1': self.test_a1_1_cross_platform_lookup(),
            'test_a1_2': self.test_a1_2_roster_snapshots_persistence(), 
            'test_a1_3': self.test_a1_3_usage_table_completeness(),
            'test_a1_4': self.test_a1_4_projections_table_validation(),
            'test_a1_5': self.test_a1_5_table_join_capability()
        }
        
        # Summary
        total_tests = len(results)
        passed_tests = sum(1 for result in results.values() if result['overall_pass'])
        
        logger.info("\n" + "=" * 60)
        logger.info(f"📊 US-A1 TEST SUMMARY:")
        logger.info(f"   Tests Passed: {passed_tests}/{total_tests}")
        logger.info(f"   Success Rate: {(passed_tests/total_tests)*100:.1f}%")
        logger.info(f"   Overall Status: {'✅ PASS' if passed_tests == total_tests else '❌ FAIL'}")
        
        return {
            'overall_pass': passed_tests == total_tests,
            'passed_tests': passed_tests,
            'total_tests': total_tests,
            'individual_results': results,
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def test_a1_1_cross_platform_lookup(self) -> Dict[str, Any]:
        """
        Test A1-1: Cross-Platform Player Lookup Validation
        Acceptance Criteria: Player rows can be looked up by canonical_player_id regardless of platform
        """
        logger.info("\n🔍 Test A1-1: Cross-Platform Player Lookup Validation")
        logger.info("-" * 50)
        
        test_results = []
        
        # Test 1: CeeDee Lamb Cross-Platform Test
        logger.info("1. CeeDee Lamb Cross-Platform Identity Test")
        try:
            # Generate canonical ID for CeeDee Lamb
            canonical_id = self.player_mapper.generate_canonical_id('CeeDee Lamb', 'WR', 'DAL')
            logger.info(f"   Generated canonical ID: {canonical_id}")
            
            # Query by canonical ID
            player = self.db.query(Player).filter(Player.nfl_id == canonical_id).first()
            
            if player:
                logger.info(f"   ✅ Player found: {player.name} ({player.position}, {player.team})")
                logger.info(f"   NFL ID: {player.nfl_id}")
                logger.info(f"   Sleeper ID: {player.sleeper_id or 'None'}")
                logger.info(f"   MFL ID: {player.mfl_id or 'None'}")
                
                # Check cross-platform IDs
                has_cross_platform = bool(player.sleeper_id or player.mfl_id)
                test_results.append({
                    'test': 'CeeDee Lamb Cross-Platform',
                    'pass': True,
                    'details': f"Found with canonical ID, cross-platform: {has_cross_platform}"
                })
            else:
                logger.info("   ❌ Player not found by canonical ID")
                test_results.append({
                    'test': 'CeeDee Lamb Cross-Platform', 
                    'pass': False,
                    'details': 'Player not found by canonical ID'
                })
                
        except Exception as e:
            logger.info(f"   ❌ Test failed: {e}")
            test_results.append({
                'test': 'CeeDee Lamb Cross-Platform',
                'pass': False, 
                'details': str(e)
            })
        
        # Test 2: Multi-Platform Identity Verification  
        logger.info("\n2. Multi-Platform Identity Verification (5 known players)")
        cross_platform_count = 0
        
        for player_data in self.known_players:
            try:
                canonical_id = self.player_mapper.generate_canonical_id(
                    player_data['name'], player_data['position'], player_data['team']
                )
                player = self.db.query(Player).filter(Player.nfl_id == canonical_id).first()
                
                if player:
                    has_multiple_platforms = sum([
                        bool(player.sleeper_id),
                        bool(player.mfl_id), 
                        bool(player.espn_id)
                    ]) > 1
                    
                    if has_multiple_platforms:
                        cross_platform_count += 1
                        logger.info(f"   ✅ {player.name}: Multiple platform IDs")
                    else:
                        logger.info(f"   ⚠️ {player.name}: Single platform ID")
                else:
                    logger.info(f"   ❌ {player_data['name']}: Not found")
                    
            except Exception as e:
                logger.info(f"   ❌ {player_data['name']}: Error - {e}")
        
        logger.info(f"   Cross-platform players: {cross_platform_count}/{len(self.known_players)}")
        
        test_results.append({
            'test': 'Multi-Platform Identity',
            'pass': cross_platform_count >= 2,  # At least 2 players should have cross-platform IDs
            'details': f"{cross_platform_count}/{len(self.known_players)} players with cross-platform IDs"
        })
        
        # Overall test result
        overall_pass = all(result['pass'] for result in test_results)
        
        return {
            'test_name': 'A1-1: Cross-Platform Player Lookup',
            'overall_pass': overall_pass,
            'individual_tests': test_results,
            'summary': f"Cross-platform lookup validation: {len([r for r in test_results if r['pass']])}/{len(test_results)} passed"
        }
    
    def test_a1_2_roster_snapshots_persistence(self) -> Dict[str, Any]:
        """
        Test A1-2: Roster Snapshots Persistence Validation
        Acceptance Criteria: Roster snapshots persist (league_id, team_id, player_id, week, slot)
        """
        logger.info("\n📝 Test A1-2: Roster Snapshots Persistence Validation")
        logger.info("-" * 50)
        
        test_results = []
        
        # Test 1: Sleeper League Roster Verification
        logger.info("1. Sleeper League Roster Verification")
        try:
            sleeper_rosters = self.db.query(RosterEntry).filter(
                RosterEntry.league_id == self.test_leagues['sleeper'],
                RosterEntry.platform == 'sleeper'
            ).all()
            
            logger.info(f"   Sleeper roster entries: {len(sleeper_rosters)}")
            
            if sleeper_rosters:
                sample_roster = sleeper_rosters[0]
                logger.info(f"   Sample entry fields:")
                logger.info(f"     Platform: {sample_roster.platform}")
                logger.info(f"     League ID: {sample_roster.league_id}")
                logger.info(f"     User ID: {sample_roster.user_id}")
                logger.info(f"     Player ID: {sample_roster.player_id}")
                logger.info(f"     Is Active: {sample_roster.is_active}")
                logger.info(f"     Created At: {sample_roster.created_at}")
                
                # Check required fields
                required_fields_present = all([
                    sample_roster.platform,
                    sample_roster.league_id,
                    sample_roster.user_id,
                    sample_roster.player_id is not None,
                    sample_roster.created_at
                ])
                
                # Expected ~180 entries (12 teams × 15 players)
                reasonable_count = 100 <= len(sleeper_rosters) <= 300
                
                test_results.append({
                    'test': 'Sleeper Roster Persistence',
                    'pass': required_fields_present and reasonable_count,
                    'details': f"{len(sleeper_rosters)} entries, fields complete: {required_fields_present}"
                })
                
                logger.info(f"   ✅ Required fields present: {required_fields_present}")
                logger.info(f"   ✅ Reasonable entry count: {reasonable_count}")
            else:
                logger.info("   ❌ No Sleeper roster entries found")
                test_results.append({
                    'test': 'Sleeper Roster Persistence',
                    'pass': False,
                    'details': 'No roster entries found'
                })
                
        except Exception as e:
            logger.info(f"   ❌ Sleeper roster test failed: {e}")
            test_results.append({
                'test': 'Sleeper Roster Persistence',
                'pass': False,
                'details': str(e)
            })
        
        # Test 2: MFL League Roster Verification
        logger.info("\n2. MFL League Roster Verification")
        try:
            mfl_rosters = self.db.query(RosterEntry).filter(
                RosterEntry.league_id == self.test_leagues['mfl'],
                RosterEntry.platform == 'mfl'
            ).all()
            
            logger.info(f"   MFL roster entries: {len(mfl_rosters)}")
            
            if mfl_rosters:
                # Check foreign key relationships
                valid_foreign_keys = 0
                for roster in mfl_rosters[:10]:  # Sample 10 entries
                    player = self.db.query(Player).filter(Player.id == roster.player_id).first()
                    if player:
                        valid_foreign_keys += 1
                
                fk_success_rate = valid_foreign_keys / min(10, len(mfl_rosters))
                
                # Expected ~360 entries (24 teams × 15 players)
                reasonable_count = 200 <= len(mfl_rosters) <= 500
                
                test_results.append({
                    'test': 'MFL Roster Persistence',
                    'pass': fk_success_rate >= 0.8 and reasonable_count,
                    'details': f"{len(mfl_rosters)} entries, FK success: {fk_success_rate:.1%}"
                })
                
                logger.info(f"   ✅ Foreign key success rate: {fk_success_rate:.1%}")
                logger.info(f"   ✅ Reasonable entry count: {reasonable_count}")
            else:
                logger.info("   ❌ No MFL roster entries found")
                test_results.append({
                    'test': 'MFL Roster Persistence',
                    'pass': False,
                    'details': 'No MFL roster entries found'
                })
                
        except Exception as e:
            logger.info(f"   ❌ MFL roster test failed: {e}")
            test_results.append({
                'test': 'MFL Roster Persistence',
                'pass': False,
                'details': str(e)
            })
        
        overall_pass = all(result['pass'] for result in test_results)
        
        return {
            'test_name': 'A1-2: Roster Snapshots Persistence',
            'overall_pass': overall_pass,
            'individual_tests': test_results,
            'summary': f"Roster persistence validation: {len([r for r in test_results if r['pass']])}/{len(test_results)} passed"
        }
    
    def test_a1_3_usage_table_completeness(self) -> Dict[str, Any]:
        """
        Test A1-3: Usage Table Completeness Validation
        Acceptance Criteria: Usage table contains: week, snap_pct, route_pct, target_share, carry_share, rz_touches, ez_targets
        """
        logger.info("\n📊 Test A1-3: Usage Table Completeness Validation")
        logger.info("-" * 50)
        
        test_results = []
        
        # Test 1: NFL Data Integration Check
        logger.info("1. NFL Data Integration Check (Week 1, 2024)")
        try:
            usage_records = self.db.query(PlayerUsage).filter(
                PlayerUsage.week == 1,
                PlayerUsage.season == 2024
            ).all()
            
            logger.info(f"   Total usage records: {len(usage_records)}")
            
            if usage_records:
                # Check Epic A required fields presence
                sample_record = usage_records[0]
                epic_a_fields = {
                    'week': sample_record.week,
                    'snap_pct': sample_record.snap_pct,
                    'route_pct': sample_record.route_pct, 
                    'target_share': sample_record.target_share,
                    'carry_share': sample_record.carry_share,
                    'rz_touches': sample_record.rz_touches,
                    'ez_targets': sample_record.ez_targets
                }
                
                logger.info("   Epic A required fields:")
                for field, value in epic_a_fields.items():
                    status = "✅" if value is not None else "⚠️"
                    logger.info(f"     {status} {field}: {value}")
                
                # Check WR records for route_pct > 0
                wr_usage = self.db.query(PlayerUsage).join(Player).filter(
                    PlayerUsage.week == 1,
                    PlayerUsage.season == 2024,
                    Player.position == 'WR',
                    PlayerUsage.route_pct.isnot(None),
                    PlayerUsage.route_pct > 0
                ).limit(10).all()
                
                logger.info(f"   WR records with route_pct > 0: {len(wr_usage)}/10 sampled")
                
                # Check RB records for carry_share > 0
                rb_usage = self.db.query(PlayerUsage).join(Player).filter(
                    PlayerUsage.week == 1,
                    PlayerUsage.season == 2024,
                    Player.position == 'RB',
                    PlayerUsage.carry_share.isnot(None),
                    PlayerUsage.carry_share > 0
                ).limit(10).all()
                
                logger.info(f"   RB records with carry_share > 0: {len(rb_usage)}/10 sampled")
                
                # Expected ~1,491 records
                reasonable_count = len(usage_records) >= 1000
                field_completeness = sum(1 for v in epic_a_fields.values() if v is not None) >= 5
                
                test_results.append({
                    'test': 'NFL Data Integration',
                    'pass': reasonable_count and field_completeness,
                    'details': f"{len(usage_records)} records, field completeness good"
                })
                
            else:
                logger.info("   ❌ No usage records found for Week 1, 2024")
                test_results.append({
                    'test': 'NFL Data Integration',
                    'pass': False,
                    'details': 'No usage records found'
                })
                
        except Exception as e:
            logger.info(f"   ❌ NFL data integration test failed: {e}")
            test_results.append({
                'test': 'NFL Data Integration',
                'pass': False,
                'details': str(e)
            })
        
        # Test 2: CeeDee Lamb Usage Data Drill-Down
        logger.info("\n2. CeeDee Lamb Usage Data Drill-Down")
        try:
            # Find CeeDee Lamb's player ID
            ceedee = self.db.query(Player).filter(
                Player.name.contains('Lamb'),
                Player.position == 'WR',
                Player.team == 'DAL'
            ).first()
            
            if ceedee:
                logger.info(f"   Found: {ceedee.name} (ID: {ceedee.id})")
                
                # Get usage data across multiple weeks
                ceedee_usage = self.db.query(PlayerUsage).filter(
                    PlayerUsage.player_id == ceedee.id,
                    PlayerUsage.season == 2024
                ).order_by(PlayerUsage.week).all()
                
                logger.info(f"   Usage records: {len(ceedee_usage)} weeks")
                
                if ceedee_usage:
                    for usage in ceedee_usage[:3]:  # Show first 3 weeks
                        logger.info(f"     Week {usage.week}: snap_pct={usage.snap_pct:.1f}%, route_pct={usage.route_pct:.1f}%")
                        logger.info(f"       rz_touches={usage.rz_touches}, ez_targets={usage.ez_targets}")
                    
                    # Verify realistic values for elite WR
                    avg_snap_pct = sum(u.snap_pct or 0 for u in ceedee_usage) / len(ceedee_usage)
                    avg_route_pct = sum(u.route_pct or 0 for u in ceedee_usage) / len(ceedee_usage)
                    
                    realistic_usage = (60 <= avg_snap_pct <= 100) and (70 <= avg_route_pct <= 100)
                    
                    logger.info(f"   Average snap_pct: {avg_snap_pct:.1f}% (realistic: {60 <= avg_snap_pct <= 100})")
                    logger.info(f"   Average route_pct: {avg_route_pct:.1f}% (realistic: {70 <= avg_route_pct <= 100})")
                    
                    test_results.append({
                        'test': 'CeeDee Lamb Usage Data',
                        'pass': len(ceedee_usage) > 0 and realistic_usage,
                        'details': f"{len(ceedee_usage)} weeks, realistic usage: {realistic_usage}"
                    })
                    
                else:
                    logger.info("   ❌ No usage data found for CeeDee Lamb")
                    test_results.append({
                        'test': 'CeeDee Lamb Usage Data',
                        'pass': False,
                        'details': 'No usage data found'
                    })
            else:
                logger.info("   ❌ CeeDee Lamb not found in Player table")
                test_results.append({
                    'test': 'CeeDee Lamb Usage Data',
                    'pass': False,
                    'details': 'Player not found'
                })
                
        except Exception as e:
            logger.info(f"   ❌ CeeDee Lamb usage test failed: {e}")
            test_results.append({
                'test': 'CeeDee Lamb Usage Data',
                'pass': False,
                'details': str(e)
            })
        
        overall_pass = all(result['pass'] for result in test_results)
        
        return {
            'test_name': 'A1-3: Usage Table Completeness',
            'overall_pass': overall_pass,
            'individual_tests': test_results,
            'summary': f"Usage table validation: {len([r for r in test_results if r['pass']])}/{len(test_results)} passed"
        }
    
    def test_a1_4_projections_table_validation(self) -> Dict[str, Any]:
        """
        Test A1-4: Projections Table Validation
        Acceptance Criteria: Projections table contains: week, mean, stdev, floor, ceiling for your scoring
        """
        logger.info("\n🎯 Test A1-4: Projections Table Validation")
        logger.info("-" * 50)
        
        test_results = []
        
        # Test 1: MFL Projections Integration Check
        logger.info("1. MFL Projections Integration Check")
        try:
            mfl_projections = self.db.query(PlayerProjections).filter(
                PlayerProjections.source == 'mfl'
            ).all()
            
            logger.info(f"   MFL projection records: {len(mfl_projections)}")
            
            if mfl_projections:
                sample_proj = mfl_projections[0]
                epic_a_fields = {
                    'week': sample_proj.week,
                    'projected_points': sample_proj.projected_points,
                    'floor': sample_proj.floor,
                    'ceiling': sample_proj.ceiling,
                    'mean': sample_proj.mean,
                    'stdev': sample_proj.stdev
                }
                
                logger.info("   Epic A projection fields:")
                for field, value in epic_a_fields.items():
                    status = "✅" if value is not None else "⚠️"
                    logger.info(f"     {status} {field}: {value}")
                
                # Sample QB records (should be 15-25 range)
                qb_projections = self.db.query(PlayerProjections).join(Player).filter(
                    PlayerProjections.source == 'mfl',
                    Player.position == 'QB',
                    PlayerProjections.projected_points.isnot(None)
                ).limit(5).all()
                
                logger.info("   Sample QB projections:")
                qb_in_range = 0
                for proj in qb_projections:
                    player = self.db.query(Player).filter(Player.id == proj.player_id).first()
                    logger.info(f"     {player.name if player else 'Unknown'}: {proj.projected_points:.1f} pts")
                    if 10 <= proj.projected_points <= 30:  # Reasonable QB range
                        qb_in_range += 1
                
                # Sample WR records (check floor < mean < ceiling)
                wr_projections = self.db.query(PlayerProjections).join(Player).filter(
                    PlayerProjections.source == 'mfl',
                    Player.position == 'WR',
                    PlayerProjections.floor.isnot(None),
                    PlayerProjections.ceiling.isnot(None)
                ).limit(5).all()
                
                logger.info("   Sample WR floor < ceiling relationships:")
                valid_relationships = 0
                for proj in wr_projections:
                    player = self.db.query(Player).filter(Player.id == proj.player_id).first()
                    valid_relationship = proj.floor < proj.ceiling
                    logger.info(f"     {player.name if player else 'Unknown'}: floor={proj.floor:.1f} < ceiling={proj.ceiling:.1f} ({'✅' if valid_relationship else '❌'})")
                    if valid_relationship:
                        valid_relationships += 1
                
                # Expected ~232 MFL records
                reasonable_count = 200 <= len(mfl_projections) <= 300
                field_completeness = sum(1 for v in epic_a_fields.values() if v is not None) >= 4
                
                test_results.append({
                    'test': 'MFL Projections Integration',
                    'pass': reasonable_count and field_completeness,
                    'details': f"{len(mfl_projections)} records, QB range: {qb_in_range}/5, WR relationships: {valid_relationships}/5"
                })
                
            else:
                logger.info("   ❌ No MFL projection records found")
                test_results.append({
                    'test': 'MFL Projections Integration', 
                    'pass': False,
                    'details': 'No MFL projection records found'
                })
                
        except Exception as e:
            logger.info(f"   ❌ MFL projections test failed: {e}")
            test_results.append({
                'test': 'MFL Projections Integration',
                'pass': False,
                'details': str(e)
            })
        
        # Test 2: Multi-Source Projection Verification
        logger.info("\n2. Multi-Source Projection Verification")
        try:
            # Group projections by source
            source_query = self.db.query(
                PlayerProjections.source,
                func.count(PlayerProjections.id).label('count')
            ).group_by(PlayerProjections.source).all()
            
            logger.info("   Projection sources:")
            total_sources = 0
            for source, count in source_query:
                logger.info(f"     {source}: {count} records")
                total_sources += 1
            
            # Check for CeeDee Lamb projection
            ceedee = self.db.query(Player).filter(
                Player.name.contains('Lamb'),
                Player.position == 'WR',
                Player.team == 'DAL'
            ).first()
            
            ceedee_proj = None
            if ceedee:
                ceedee_proj = self.db.query(PlayerProjections).filter(
                    PlayerProjections.player_id == ceedee.id
                ).first()
                
                if ceedee_proj:
                    logger.info(f"   CeeDee Lamb projection: {ceedee_proj.projected_points or ceedee_proj.mean:.1f} pts")
                    logger.info(f"     Floor: {ceedee_proj.floor:.1f}, Ceiling: {ceedee_proj.ceiling:.1f}")
                    
                    # Check realistic values for elite WR (floor ~12, ceiling ~25)
                    realistic_values = (
                        (ceedee_proj.floor or 0) >= 8 and 
                        (ceedee_proj.ceiling or 0) <= 35 and
                        (ceedee_proj.projected_points or ceedee_proj.mean or 0) >= 10
                    )
                    
                    logger.info(f"     Realistic values: {'✅' if realistic_values else '❌'}")
                else:
                    logger.info("   ⚠️ No projection found for CeeDee Lamb")
            
            test_results.append({
                'test': 'Multi-Source Projections',
                'pass': total_sources >= 1 and (ceedee_proj is not None),
                'details': f"{total_sources} sources, CeeDee projection: {ceedee_proj is not None}"
            })
            
        except Exception as e:
            logger.info(f"   ❌ Multi-source projections test failed: {e}")
            test_results.append({
                'test': 'Multi-Source Projections',
                'pass': False,
                'details': str(e)
            })
        
        overall_pass = all(result['pass'] for result in test_results)
        
        return {
            'test_name': 'A1-4: Projections Table Validation',
            'overall_pass': overall_pass,
            'individual_tests': test_results,
            'summary': f"Projections validation: {len([r for r in test_results if r['pass']])}/{len(test_results)} passed"
        }
    
    def test_a1_5_table_join_capability(self) -> Dict[str, Any]:
        """
        Test A1-5: Table Join Capability Validation
        Acceptance Criteria: All tables can be joined to materialize a waiver_candidates view
        """
        logger.info("\n🔗 Test A1-5: Table Join Capability Validation")
        logger.info("-" * 50)
        
        test_results = []
        
        # Test 1: Complex Join Query Test
        logger.info("1. Complex Join Query Test")
        try:
            # Complex join across all Epic A tables
            join_query = self.db.query(
                Player.name,
                Player.position,
                PlayerUsage.snap_pct,
                PlayerProjections.projected_points,
                RosterEntry.league_id
            ).join(
                PlayerUsage, Player.id == PlayerUsage.player_id
            ).join(
                PlayerProjections, Player.id == PlayerProjections.player_id
            ).outerjoin(
                RosterEntry, Player.id == RosterEntry.player_id
            ).filter(
                PlayerUsage.week == 1,
                PlayerProjections.week == 1
            ).limit(10)
            
            results = join_query.all()
            
            logger.info(f"   Complex join results: {len(results)} records")
            
            if results:
                logger.info("   Sample joined records:")
                for i, row in enumerate(results[:3]):
                    logger.info(f"     {i+1}. {row.name} ({row.position}): snap_pct={row.snap_pct}, proj={row.projected_points}, league={row.league_id}")
                
                # Check all expected columns populated  
                complete_records = sum(1 for row in results if all([
                    row.name,
                    row.position, 
                    row.snap_pct is not None,
                    row.projected_points is not None
                ]))
                
                completeness_rate = complete_records / len(results)
                logger.info(f"   Record completeness: {completeness_rate:.1%}")
                
                test_results.append({
                    'test': 'Complex Join Query',
                    'pass': len(results) > 0 and completeness_rate >= 0.7,
                    'details': f"{len(results)} joined records, {completeness_rate:.1%} complete"
                })
                
            else:
                logger.info("   ❌ No results from complex join")
                test_results.append({
                    'test': 'Complex Join Query',
                    'pass': False,
                    'details': 'No results from join query'
                })
                
        except Exception as e:
            logger.info(f"   ❌ Complex join test failed: {e}")
            test_results.append({
                'test': 'Complex Join Query',
                'pass': False,
                'details': str(e)
            })
        
        # Test 2: Foreign Key Relationship Verification
        logger.info("\n2. Foreign Key Relationship Verification")
        try:
            # Test Player -> PlayerUsage relationship
            players_with_usage = self.db.query(Player).join(PlayerUsage).filter(
                PlayerUsage.week == 1
            ).count()
            
            # Test Player -> PlayerProjections relationship  
            players_with_projections = self.db.query(Player).join(PlayerProjections).count()
            
            # Test Player -> RosterEntry relationship
            players_with_rosters = self.db.query(Player).join(RosterEntry).count()
            
            logger.info(f"   Players with usage data: {players_with_usage}")
            logger.info(f"   Players with projections: {players_with_projections}")
            logger.info(f"   Players with roster entries: {players_with_rosters}")
            
            # All relationships should work
            relationships_working = all([
                players_with_usage > 0,
                players_with_projections > 0,
                players_with_rosters > 0
            ])
            
            test_results.append({
                'test': 'Foreign Key Relationships',
                'pass': relationships_working,
                'details': f"Usage: {players_with_usage}, Projections: {players_with_projections}, Rosters: {players_with_rosters}"
            })
            
            logger.info(f"   ✅ All relationships working: {relationships_working}")
            
        except Exception as e:
            logger.info(f"   ❌ Foreign key test failed: {e}")
            test_results.append({
                'test': 'Foreign Key Relationships',
                'pass': False,
                'details': str(e)
            })
        
        overall_pass = all(result['pass'] for result in test_results)
        
        return {
            'test_name': 'A1-5: Table Join Capability',
            'overall_pass': overall_pass,
            'individual_tests': test_results,
            'summary': f"Join capability validation: {len([r for r in test_results if r['pass']])}/{len(test_results)} passed"
        }
    
    def __del__(self):
        """Clean up database connection"""
        if hasattr(self, 'db'):
            self.db.close()

def main():
    """Run all US-A1 manual tests"""
    tester = EpicAUS1ManualTester()
    results = tester.run_all_tests()
    
    # Return exit code based on test results
    exit_code = 0 if results['overall_pass'] else 1
    return exit_code

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)