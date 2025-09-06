#!/usr/bin/env python3
"""
Manual Test Suite for Epic A - US-A2: Waiver Candidates Materialized View

User Story US-A2: 
"As a manager, I want a materialized view that computes all features per player/week 
so alerts run fast and predictably."

Acceptance Criteria:
1. View waiver_candidates(league_id, week, player_id, pos, rostered?, snap_delta, 
   route_delta, tprr, rz_last2, ez_last2, opp_next, proj_next, trend_slope, 
   roster_fit, market_heat, scarcity) is queryable
2. Refresh job populates for current week in < 1 minute  
3. Non-rostered players only (relative to your team)

This script provides comprehensive manual validation of all US-A2 acceptance criteria
with specific success metrics and real data testing.
"""

import os
import sys
import time
import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import asdict

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.config import get_config
from src.database import SessionLocal, Player, PlayerUsage, PlayerProjections, RosterEntry, WaiverCandidates
from src.services.enhanced_waiver_candidates_builder import EnhancedWaiverCandidatesBuilder, EnhancedWaiverCandidate
from src.utils.player_id_mapper import PlayerIDMapper

class EpicAUS2ManualTests:
    """
    Comprehensive manual test suite for US-A2 acceptance criteria validation
    """
    
    def __init__(self):
        self.config = get_config()
        self.db = SessionLocal()
        self.builder = EnhancedWaiverCandidatesBuilder()
        self.player_mapper = PlayerIDMapper()
        
        # Test configuration with real league data
        self.test_league_id = "1257071160403709954"  # Real Sleeper league
        self.test_week = 4
        self.current_season = 2025
        
        print("=" * 80)
        print("EPIC A - US-A2 MANUAL TEST SUITE")
        print("Waiver Candidates Materialized View Validation")
        print("=" * 80)
    
    def run_all_tests(self) -> Dict[str, Any]:
        """
        Execute all US-A2 acceptance criteria tests
        
        Returns comprehensive test results with success metrics
        """
        test_results = {
            'us_a2_overall': True,
            'timestamp': datetime.utcnow().isoformat(),
            'league_id': self.test_league_id,
            'test_week': self.test_week,
            'tests': {}
        }
        
        try:
            print(f"\n🏈 Testing Epic A US-A2 for League: {self.test_league_id}, Week: {self.test_week}")
            print(f"Database: {self.config.DATABASE_URL}")
            print("-" * 60)
            
            # Test A2.1: View Structure and Queryability
            print("\n📊 TEST A2.1: View Structure and Queryability")
            result_a2_1 = self.test_a2_1_view_queryability()
            test_results['tests']['a2_1_view_queryability'] = result_a2_1
            if not result_a2_1.get('success', False):
                test_results['us_a2_overall'] = False
            
            # Test A2.2: Performance Requirement (< 1 minute)
            print("\n⏱️  TEST A2.2: Performance Requirement")
            result_a2_2 = self.test_a2_2_performance_requirement()
            test_results['tests']['a2_2_performance'] = result_a2_2
            if not result_a2_2.get('success', False):
                test_results['us_a2_overall'] = False
            
            # Test A2.3: Non-Rostered Players Only
            print("\n🚫 TEST A2.3: Non-Rostered Players Only")
            result_a2_3 = self.test_a2_3_non_rostered_only()
            test_results['tests']['a2_3_non_rostered'] = result_a2_3
            if not result_a2_3.get('success', False):
                test_results['us_a2_overall'] = False
            
            # Test A2.4: All Required Fields Validation
            print("\n🔍 TEST A2.4: All Required Fields Validation")
            result_a2_4 = self.test_a2_4_required_fields()
            test_results['tests']['a2_4_required_fields'] = result_a2_4
            if not result_a2_4.get('success', False):
                test_results['us_a2_overall'] = False
            
            # Test A2.5: Calculated Fields Accuracy
            print("\n🧮 TEST A2.5: Calculated Fields Accuracy")
            result_a2_5 = self.test_a2_5_calculated_fields()
            test_results['tests']['a2_5_calculated_fields'] = result_a2_5
            if not result_a2_5.get('success', False):
                test_results['us_a2_overall'] = False
            
            # Test A2.6: Database Persistence
            print("\n💾 TEST A2.6: Database Persistence")
            result_a2_6 = self.test_a2_6_database_persistence()
            test_results['tests']['a2_6_persistence'] = result_a2_6
            if not result_a2_6.get('success', False):
                test_results['us_a2_overall'] = False
            
            # Final Summary
            self._print_test_summary(test_results)
            
            return test_results
            
        except Exception as e:
            print(f"❌ CRITICAL ERROR in US-A2 tests: {e}")
            traceback.print_exc()
            test_results['us_a2_overall'] = False
            test_results['error'] = str(e)
            return test_results
        
        finally:
            self.db.close()
    
    def test_a2_1_view_queryability(self) -> Dict[str, Any]:
        """
        Test A2.1: View waiver_candidates with all required fields is queryable
        
        Success Criteria:
        - Can build waiver candidates with all Epic A fields
        - All 11 required fields are present and accessible
        - View is queryable without errors
        """
        try:
            print("   Testing waiver candidates view queryability...")
            
            # Build waiver candidates
            candidates = self.builder.build_waiver_candidates(
                league_id=self.test_league_id,
                week=self.test_week
            )
            
            if not candidates:
                return {
                    'success': False,
                    'error': 'No waiver candidates built',
                    'candidates_count': 0
                }
            
            # Check all required Epic A fields are present
            required_fields = [
                'league_id', 'week', 'player_id', 'pos', 'rostered',
                'snap_delta', 'route_delta', 'tprr', 'rz_last2', 'ez_last2',
                'opp_next', 'proj_next', 'trend_slope', 'roster_fit',
                'market_heat', 'scarcity'
            ]
            
            sample_candidate = candidates[0]
            present_fields = []
            missing_fields = []
            
            for field in required_fields:
                if hasattr(sample_candidate, field):
                    present_fields.append(field)
                else:
                    missing_fields.append(field)
            
            field_coverage = len(present_fields) / len(required_fields)
            success = field_coverage >= 1.0  # All fields must be present
            
            print(f"   ✓ Built {len(candidates)} candidates")
            print(f"   ✓ Field coverage: {len(present_fields)}/{len(required_fields)} ({field_coverage:.1%})")
            
            if missing_fields:
                print(f"   ⚠️  Missing fields: {missing_fields}")
            
            return {
                'success': success,
                'candidates_count': len(candidates),
                'required_fields': len(required_fields),
                'present_fields': len(present_fields),
                'field_coverage': field_coverage,
                'missing_fields': missing_fields,
                'sample_candidate': {
                    'player_id': sample_candidate.player_id,
                    'pos': sample_candidate.pos,
                    'has_snap_delta': sample_candidate.snap_delta is not None,
                    'has_tprr': sample_candidate.tprr is not None,
                    'has_roster_fit': sample_candidate.roster_fit is not None
                }
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'View queryability test failed: {e}'
            }
    
    def test_a2_2_performance_requirement(self) -> Dict[str, Any]:
        """
        Test A2.2: Refresh job populates for current week in < 1 minute
        
        Success Criteria:
        - Complete refresh operation completes in < 60 seconds
        - All candidates are processed and stored
        - Performance metrics are tracked
        """
        try:
            print("   Testing performance requirement (< 1 minute)...")
            
            start_time = time.time()
            
            # Execute full refresh job
            refresh_result = self.builder.refresh_waiver_candidates_for_league(
                league_id=self.test_league_id,
                week=self.test_week
            )
            
            end_time = time.time()
            duration = end_time - start_time
            
            success = refresh_result.get('success', False) and duration < 60.0
            
            print(f"   ✓ Refresh duration: {duration:.2f} seconds")
            print(f"   ✓ Performance target: < 60 seconds")
            print(f"   ✓ Candidates processed: {refresh_result.get('candidates_count', 0)}")
            
            if duration >= 60.0:
                print(f"   ❌ PERFORMANCE FAILURE: {duration:.2f}s exceeds 60s limit")
            
            return {
                'success': success,
                'duration_seconds': duration,
                'performance_ok': duration < 60.0,
                'target_seconds': 60.0,
                'candidates_processed': refresh_result.get('candidates_count', 0),
                'refresh_success': refresh_result.get('success', False)
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Performance test failed: {e}'
            }
    
    def test_a2_3_non_rostered_only(self) -> Dict[str, Any]:
        """
        Test A2.3: Non-rostered players only (relative to your team)
        
        Success Criteria:
        - All returned candidates are not on any roster in the league
        - Rostered field is False for all candidates
        - No rostered players appear in results
        """
        try:
            print("   Testing non-rostered players filter...")
            
            # Get rostered players for this league
            rostered_players = set(
                row.player_id for row in self.db.query(RosterEntry.player_id).filter(
                    RosterEntry.league_id == self.test_league_id,
                    RosterEntry.is_active == True
                ).all()
            )
            
            # Build waiver candidates
            candidates = self.builder.build_waiver_candidates(
                league_id=self.test_league_id,
                week=self.test_week
            )
            
            if not candidates:
                return {
                    'success': False,
                    'error': 'No candidates to test filtering',
                    'candidates_count': 0
                }
            
            # Check filtering
            rostered_in_candidates = 0
            non_rostered_count = 0
            rostered_field_errors = 0
            
            for candidate in candidates:
                if candidate.player_id in rostered_players:
                    rostered_in_candidates += 1
                else:
                    non_rostered_count += 1
                
                # Check rostered field accuracy
                if candidate.rostered != (candidate.player_id in rostered_players):
                    rostered_field_errors += 1
            
            filter_accuracy = non_rostered_count / len(candidates) if candidates else 0
            success = (rostered_in_candidates == 0 and 
                      rostered_field_errors == 0 and 
                      filter_accuracy >= 1.0)
            
            print(f"   ✓ Total candidates: {len(candidates)}")
            print(f"   ✓ Non-rostered: {non_rostered_count}")
            print(f"   ✓ Rostered (should be 0): {rostered_in_candidates}")
            print(f"   ✓ Filter accuracy: {filter_accuracy:.1%}")
            
            if rostered_in_candidates > 0:
                print(f"   ❌ FILTER FAILURE: {rostered_in_candidates} rostered players in results")
            
            return {
                'success': success,
                'total_candidates': len(candidates),
                'non_rostered_count': non_rostered_count,
                'rostered_in_candidates': rostered_in_candidates,
                'filter_accuracy': filter_accuracy,
                'rostered_field_errors': rostered_field_errors,
                'league_rostered_players': len(rostered_players)
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Non-rostered filter test failed: {e}'
            }
    
    def test_a2_4_required_fields(self) -> Dict[str, Any]:
        """
        Test A2.4: All required Epic A fields are present and properly typed
        
        Success Criteria:
        - All 16 Epic A fields are present
        - Fields have correct data types
        - Optional fields handle None values properly
        """
        try:
            print("   Testing required fields presence and typing...")
            
            candidates = self.builder.build_waiver_candidates(
                league_id=self.test_league_id,
                week=self.test_week
            )
            
            if not candidates:
                return {
                    'success': False,
                    'error': 'No candidates for field testing'
                }
            
            # Field type validation
            field_validations = {
                'league_id': str,
                'week': int,
                'player_id': int,
                'pos': str,
                'rostered': bool,
                'snap_delta': (float, type(None)),
                'route_delta': (float, type(None)),
                'tprr': (float, type(None)),
                'rz_last2': (int, type(None)),
                'ez_last2': (int, type(None)),
                'opp_next': (str, type(None)),
                'proj_next': (float, type(None)),
                'trend_slope': (float, type(None)),
                'roster_fit': (float, type(None)),
                'market_heat': (float, type(None)),
                'scarcity': (float, type(None))
            }
            
            type_errors = []
            field_coverage = {}
            
            for candidate in candidates[:5]:  # Check first 5 candidates
                for field_name, expected_types in field_validations.items():
                    if hasattr(candidate, field_name):
                        field_value = getattr(candidate, field_name)
                        
                        if isinstance(expected_types, tuple):
                            type_ok = isinstance(field_value, expected_types)
                        else:
                            type_ok = isinstance(field_value, expected_types)
                        
                        if not type_ok:
                            type_errors.append(f"{field_name}: {type(field_value)} (expected {expected_types})")
                        
                        # Track field coverage
                        if field_name not in field_coverage:
                            field_coverage[field_name] = {'present': 0, 'with_data': 0}
                        
                        field_coverage[field_name]['present'] += 1
                        if field_value is not None:
                            field_coverage[field_name]['with_data'] += 1
            
            total_fields = len(field_validations)
            present_fields = len(field_coverage)
            success = len(type_errors) == 0 and present_fields == total_fields
            
            print(f"   ✓ Field presence: {present_fields}/{total_fields}")
            print(f"   ✓ Type validation errors: {len(type_errors)}")
            
            # Show data availability for key calculated fields
            key_fields = ['snap_delta', 'tprr', 'roster_fit', 'market_heat']
            for field in key_fields:
                if field in field_coverage:
                    coverage = field_coverage[field]
                    data_rate = coverage['with_data'] / coverage['present'] if coverage['present'] > 0 else 0
                    print(f"   ✓ {field} data rate: {data_rate:.1%}")
            
            return {
                'success': success,
                'total_fields_expected': total_fields,
                'fields_present': present_fields,
                'type_errors': type_errors,
                'field_coverage': field_coverage
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Required fields test failed: {e}'
            }
    
    def test_a2_5_calculated_fields(self) -> Dict[str, Any]:
        """
        Test A2.5: Calculated fields accuracy and logic validation
        
        Success Criteria:
        - snap_delta and route_delta show reasonable week-over-week changes
        - TPRR calculations are within expected ranges (0.1-0.5 typical)
        - Roster fit, market heat, scarcity are in 0-1 range
        """
        try:
            print("   Testing calculated fields accuracy...")
            
            candidates = self.builder.build_waiver_candidates(
                league_id=self.test_league_id,
                week=self.test_week
            )
            
            if not candidates:
                return {
                    'success': False,
                    'error': 'No candidates for calculation testing'
                }
            
            calculation_stats = {
                'snap_delta': {'valid': 0, 'total': 0, 'range_ok': 0},
                'route_delta': {'valid': 0, 'total': 0, 'range_ok': 0},
                'tprr': {'valid': 0, 'total': 0, 'range_ok': 0},
                'roster_fit': {'valid': 0, 'total': 0, 'range_ok': 0},
                'market_heat': {'valid': 0, 'total': 0, 'range_ok': 0},
                'scarcity': {'valid': 0, 'total': 0, 'range_ok': 0}
            }
            
            for candidate in candidates:
                # Check snap_delta (-100 to +100 reasonable)
                if candidate.snap_delta is not None:
                    calculation_stats['snap_delta']['valid'] += 1
                    if -100 <= candidate.snap_delta <= 100:
                        calculation_stats['snap_delta']['range_ok'] += 1
                calculation_stats['snap_delta']['total'] += 1
                
                # Check route_delta (-100 to +100 reasonable)  
                if candidate.route_delta is not None:
                    calculation_stats['route_delta']['valid'] += 1
                    if -100 <= candidate.route_delta <= 100:
                        calculation_stats['route_delta']['range_ok'] += 1
                calculation_stats['route_delta']['total'] += 1
                
                # Check TPRR (0.05 to 0.6 reasonable)
                if candidate.tprr is not None:
                    calculation_stats['tprr']['valid'] += 1
                    if 0.05 <= candidate.tprr <= 0.6:
                        calculation_stats['tprr']['range_ok'] += 1
                calculation_stats['tprr']['total'] += 1
                
                # Check roster_fit (0 to 1)
                if candidate.roster_fit is not None:
                    calculation_stats['roster_fit']['valid'] += 1
                    if 0.0 <= candidate.roster_fit <= 1.0:
                        calculation_stats['roster_fit']['range_ok'] += 1
                calculation_stats['roster_fit']['total'] += 1
                
                # Check market_heat (0 to 1)
                if candidate.market_heat is not None:
                    calculation_stats['market_heat']['valid'] += 1
                    if 0.0 <= candidate.market_heat <= 1.0:
                        calculation_stats['market_heat']['range_ok'] += 1
                calculation_stats['market_heat']['total'] += 1
                
                # Check scarcity (0 to 1)
                if candidate.scarcity is not None:
                    calculation_stats['scarcity']['valid'] += 1
                    if 0.0 <= candidate.scarcity <= 1.0:
                        calculation_stats['scarcity']['range_ok'] += 1
                calculation_stats['scarcity']['total'] += 1
            
            # Calculate success metrics
            total_validations = 0
            passed_validations = 0
            
            for field, stats in calculation_stats.items():
                if stats['total'] > 0:
                    data_rate = stats['valid'] / stats['total']
                    range_accuracy = stats['range_ok'] / stats['valid'] if stats['valid'] > 0 else 0
                    
                    print(f"   ✓ {field}: {data_rate:.1%} data, {range_accuracy:.1%} range valid")
                    
                    total_validations += 2  # Data presence + range validation
                    if data_rate > 0:
                        passed_validations += 1
                    if range_accuracy >= 0.8:  # 80% should be in valid range
                        passed_validations += 1
            
            success = passed_validations / total_validations >= 0.7 if total_validations > 0 else False
            
            return {
                'success': success,
                'calculation_stats': calculation_stats,
                'validation_score': passed_validations / total_validations if total_validations > 0 else 0,
                'total_candidates_tested': len(candidates)
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Calculated fields test failed: {e}'
            }
    
    def test_a2_6_database_persistence(self) -> Dict[str, Any]:
        """
        Test A2.6: Database persistence of waiver candidates table
        
        Success Criteria:
        - Candidates are successfully written to WaiverCandidates table
        - Table can be queried directly
        - Data integrity is maintained
        """
        try:
            print("   Testing database persistence...")
            
            # Build and persist candidates
            candidates = self.builder.build_waiver_candidates(
                league_id=self.test_league_id,
                week=self.test_week
            )
            
            if not candidates:
                return {
                    'success': False,
                    'error': 'No candidates to persist'
                }
            
            # Sync to database
            sync_success = self.builder.sync_to_waiver_candidates_table(candidates)
            
            if not sync_success:
                return {
                    'success': False,
                    'error': 'Failed to sync candidates to database'
                }
            
            # Verify database query
            db_candidates = self.db.query(WaiverCandidates).filter(
                WaiverCandidates.league_id == self.test_league_id,
                WaiverCandidates.week == self.test_week
            ).all()
            
            # Check data integrity
            original_count = len(candidates)
            db_count = len(db_candidates)
            data_integrity = db_count == original_count
            
            # Verify field persistence for sample record
            field_persistence = True
            if db_candidates:
                sample_db = db_candidates[0]
                required_db_fields = [
                    'league_id', 'week', 'player_id', 'pos', 'rostered',
                    'snap_delta', 'route_delta', 'tprr', 'rz_last2', 'ez_last2',
                    'opp_next', 'proj_next', 'trend_slope', 'roster_fit',
                    'market_heat', 'scarcity'
                ]
                
                for field in required_db_fields:
                    if not hasattr(sample_db, field):
                        field_persistence = False
                        break
            
            success = sync_success and data_integrity and field_persistence
            
            print(f"   ✓ Sync to database: {'SUCCESS' if sync_success else 'FAILED'}")
            print(f"   ✓ Records persisted: {db_count}/{original_count}")
            print(f"   ✓ Data integrity: {'OK' if data_integrity else 'FAILED'}")
            print(f"   ✓ Field persistence: {'OK' if field_persistence else 'FAILED'}")
            
            return {
                'success': success,
                'sync_success': sync_success,
                'original_count': original_count,
                'persisted_count': db_count,
                'data_integrity': data_integrity,
                'field_persistence': field_persistence
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Database persistence test failed: {e}'
            }
    
    def _print_test_summary(self, results: Dict[str, Any]):
        """Print comprehensive test summary"""
        print("\n" + "=" * 80)
        print("EPIC A - US-A2 TEST RESULTS SUMMARY")
        print("=" * 80)
        
        overall_success = results['us_a2_overall']
        status_icon = "✅" if overall_success else "❌"
        
        print(f"\n{status_icon} OVERALL US-A2 STATUS: {'PASS' if overall_success else 'FAIL'}")
        print(f"📅 Test Date: {results['timestamp']}")
        print(f"🏈 League: {results['league_id']}")
        print(f"📅 Week: {results['test_week']}")
        
        print(f"\n📊 Individual Test Results:")
        for test_name, test_result in results.get('tests', {}).items():
            success = test_result.get('success', False)
            icon = "✅" if success else "❌"
            print(f"  {icon} {test_name}: {'PASS' if success else 'FAIL'}")
            
            if not success and 'error' in test_result:
                print(f"      Error: {test_result['error']}")
        
        # Key metrics summary
        print(f"\n🎯 Key Epic A US-A2 Metrics:")
        
        if 'a2_1_view_queryability' in results.get('tests', {}):
            q_test = results['tests']['a2_1_view_queryability']
            print(f"  • Candidates Built: {q_test.get('candidates_count', 0)}")
            print(f"  • Field Coverage: {q_test.get('field_coverage', 0):.1%}")
        
        if 'a2_2_performance' in results.get('tests', {}):
            p_test = results['tests']['a2_2_performance']
            duration = p_test.get('duration_seconds', 0)
            print(f"  • Refresh Time: {duration:.2f}s (< 60s required)")
            print(f"  • Performance: {'✅ PASS' if duration < 60 else '❌ FAIL'}")
        
        if 'a2_3_non_rostered' in results.get('tests', {}):
            f_test = results['tests']['a2_3_non_rostered']
            accuracy = f_test.get('filter_accuracy', 0)
            print(f"  • Filter Accuracy: {accuracy:.1%}")
        
        if 'a2_6_persistence' in results.get('tests', {}):
            db_test = results['tests']['a2_6_persistence']
            integrity = db_test.get('data_integrity', False)
            print(f"  • Database Persistence: {'✅ OK' if integrity else '❌ FAIL'}")
        
        print(f"\n{'🎉 Epic A US-A2 is FULLY VALIDATED!' if overall_success else '⚠️  Epic A US-A2 needs attention'}")
        print("=" * 80)

def main():
    """Run Epic A US-A2 manual tests"""
    print("Starting Epic A US-A2 Manual Test Suite...")
    
    try:
        test_suite = EpicAUS2ManualTests()
        results = test_suite.run_all_tests()
        
        # Return appropriate exit code
        exit_code = 0 if results['us_a2_overall'] else 1
        print(f"\nExiting with code {exit_code}")
        return exit_code
        
    except Exception as e:
        print(f"❌ CRITICAL ERROR: {e}")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)