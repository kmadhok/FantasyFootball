#!/usr/bin/env python3
"""
Performance Requirement Validation Tests

Epic A US-A2 Performance Requirement:
"Refresh job populates for current week in < 1 minute."

This script provides comprehensive validation of performance requirements
for Epic A data operations, with focus on the critical < 1 minute refresh requirement.
"""

import os
import sys
import traceback
import time
import threading
import concurrent.futures
import psutil
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from collections import defaultdict

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.config import get_config
from src.database import SessionLocal, Player, PlayerUsage, PlayerProjections, RosterEntry
from src.services.enhanced_waiver_candidates_builder import EnhancedWaiverCandidatesBuilder
from src.services.waiver_candidates_builder import WaiverCandidatesBuilder
from src.utils.player_id_mapper import PlayerIDMapper
from sqlalchemy import func, distinct, and_, text

class PerformanceRequirementValidator:
    """
    Comprehensive validation of Epic A performance requirements
    """
    
    def __init__(self):
        self.config = get_config()
        self.db = SessionLocal()
        self.enhanced_builder = EnhancedWaiverCandidatesBuilder()
        self.waiver_builder = WaiverCandidatesBuilder()
        self.player_mapper = PlayerIDMapper()
        
        self.current_season = 2025
        self.test_week = 4
        self.test_league_id = "1257071160403709954"  # Real Sleeper league
        
        # Performance thresholds
        self.performance_thresholds = {
            'waiver_refresh_max_seconds': 60.0,  # Epic A US-A2 requirement
            'query_response_max_seconds': 5.0,
            'join_query_max_seconds': 10.0,
            'bulk_operation_max_seconds': 30.0,
            'memory_usage_max_mb': 500,
            'concurrent_operations_max_seconds': 120.0
        }
        
        print("=" * 80)
        print("PERFORMANCE REQUIREMENT VALIDATION")
        print("Epic A US-A2: Performance Requirements Testing")
        print("=" * 80)
    
    def run_all_validations(self) -> Dict[str, Any]:
        """
        Execute all performance requirement validation tests
        
        Returns comprehensive validation results
        """
        validation_results = {
            'overall_success': True,
            'timestamp': datetime.utcnow().isoformat(),
            'database': self.config.DATABASE_URL,
            'season': self.current_season,
            'test_week': self.test_week,
            'performance_thresholds': self.performance_thresholds,
            'validations': {}
        }
        
        try:
            print(f"\n⚡ Running Performance Requirement Validations")
            print(f"Database: {self.config.DATABASE_URL}")
            print(f"Season: {self.current_season}, Week: {self.test_week}")
            print(f"League: {self.test_league_id}")
            print("-" * 60)
            
            # Validation 1: Waiver Refresh Performance (Critical Epic A US-A2)
            print("\n🎯 VALIDATION 1: Waiver Refresh Performance (< 1 minute)")
            result_1 = self.validate_waiver_refresh_performance()
            validation_results['validations']['waiver_refresh_performance'] = result_1
            if not result_1.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 2: Database Query Performance
            print("\n💾 VALIDATION 2: Database Query Performance")
            result_2 = self.validate_database_query_performance()
            validation_results['validations']['database_query_performance'] = result_2
            if not result_2.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 3: Join Operation Performance
            print("\n🔗 VALIDATION 3: Join Operation Performance")
            result_3 = self.validate_join_operation_performance()
            validation_results['validations']['join_operation_performance'] = result_3
            if not result_3.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 4: Bulk Data Operations Performance
            print("\n📊 VALIDATION 4: Bulk Data Operations Performance")
            result_4 = self.validate_bulk_operations_performance()
            validation_results['validations']['bulk_operations_performance'] = result_4
            if not result_4.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 5: Memory and Resource Usage
            print("\n🧠 VALIDATION 5: Memory and Resource Usage")
            result_5 = self.validate_memory_resource_usage()
            validation_results['validations']['memory_resource_usage'] = result_5
            if not result_5.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 6: Concurrent Operations Performance
            print("\n🔄 VALIDATION 6: Concurrent Operations Performance")
            result_6 = self.validate_concurrent_operations_performance()
            validation_results['validations']['concurrent_operations_performance'] = result_6
            if not result_6.get('success', False):
                validation_results['overall_success'] = False
            
            # Final Summary
            self._print_validation_summary(validation_results)
            
            return validation_results
            
        except Exception as e:
            print(f"❌ CRITICAL ERROR in performance requirement validation: {e}")
            traceback.print_exc()
            validation_results['overall_success'] = False
            validation_results['error'] = str(e)
            return validation_results
        
        finally:
            self.db.close()
    
    def validate_waiver_refresh_performance(self) -> Dict[str, Any]:
        """
        Validate the critical Epic A US-A2 performance requirement
        
        Success Criteria:
        - Waiver candidates refresh completes in < 1 minute
        - Enhanced waiver builder meets performance requirements
        - Multiple refresh operations are consistently fast
        """
        try:
            print("   Testing waiver refresh performance...")
            
            refresh_tests = []
            performance_target = self.performance_thresholds['waiver_refresh_max_seconds']
            
            # Test enhanced waiver candidates builder (primary Epic A implementation)
            print("   Testing Enhanced Waiver Candidates Builder...")
            for test_run in range(3):  # Run 3 times for consistency
                try:
                    start_time = time.time()
                    
                    # Execute the full Epic A refresh operation
                    refresh_result = self.enhanced_builder.refresh_waiver_candidates_for_league(
                        league_id=self.test_league_id,
                        week=self.test_week
                    )
                    
                    end_time = time.time()
                    duration = end_time - start_time
                    
                    refresh_tests.append({
                        'test_run': test_run + 1,
                        'builder': 'enhanced',
                        'duration': duration,
                        'success': refresh_result.get('success', False),
                        'candidates_count': refresh_result.get('candidates_count', 0),
                        'performance_ok': duration < performance_target,
                        'details': refresh_result
                    })
                    
                    print(f"     Run {test_run + 1}: {duration:.2f}s ({'✓' if duration < performance_target else '❌'})")
                    
                except Exception as e:
                    refresh_tests.append({
                        'test_run': test_run + 1,
                        'builder': 'enhanced',
                        'error': str(e),
                        'success': False,
                        'performance_ok': False
                    })
                    print(f"     Run {test_run + 1}: ERROR - {e}")
            
            # Test standard waiver candidates builder for comparison
            print("   Testing Standard Waiver Candidates Builder...")
            for test_run in range(2):  # Run 2 times
                try:
                    start_time = time.time()
                    
                    # Execute standard refresh operation
                    success = self.waiver_builder.refresh_waiver_candidates_table(
                        league_id=self.test_league_id,
                        week=self.test_week
                    )
                    
                    end_time = time.time()
                    duration = end_time - start_time
                    
                    refresh_tests.append({
                        'test_run': test_run + 1,
                        'builder': 'standard',
                        'duration': duration,
                        'success': success,
                        'performance_ok': duration < performance_target
                    })
                    
                    print(f"     Standard Run {test_run + 1}: {duration:.2f}s ({'✓' if duration < performance_target else '❌'})")
                    
                except Exception as e:
                    refresh_tests.append({
                        'test_run': test_run + 1,
                        'builder': 'standard',
                        'error': str(e),
                        'success': False,
                        'performance_ok': False
                    })
                    print(f"     Standard Run {test_run + 1}: ERROR - {e}")
            
            # Analyze results
            successful_tests = [t for t in refresh_tests if t.get('success', False)]
            performance_compliant_tests = [t for t in refresh_tests if t.get('performance_ok', False)]
            
            if successful_tests:
                durations = [t['duration'] for t in successful_tests if 'duration' in t]
                avg_duration = sum(durations) / len(durations) if durations else float('inf')
                max_duration = max(durations) if durations else float('inf')
                min_duration = min(durations) if durations else float('inf')
            else:
                avg_duration = max_duration = min_duration = float('inf')
            
            # Success criteria (Epic A US-A2 requirement)
            success = (
                len(successful_tests) >= 3 and  # At least 3 successful refreshes
                len(performance_compliant_tests) >= 3 and  # At least 3 meet < 1 minute requirement
                avg_duration < performance_target and  # Average meets requirement
                max_duration < performance_target * 1.2  # Max within 20% buffer
            )
            
            print(f"   ✓ Successful tests: {len(successful_tests)}/{len(refresh_tests)}")
            print(f"   ✓ Performance compliant: {len(performance_compliant_tests)}/{len(refresh_tests)}")
            print(f"   ✓ Average duration: {avg_duration:.2f}s (target: <{performance_target}s)")
            print(f"   ✓ Duration range: {min_duration:.2f}s - {max_duration:.2f}s")
            
            # Show Epic A US-A2 compliance status
            epic_compliance = avg_duration < performance_target and max_duration < performance_target
            print(f"   {'✅' if epic_compliance else '❌'} Epic A US-A2 Compliance: {'PASS' if epic_compliance else 'FAIL'}")
            
            return {
                'success': success,
                'total_tests': len(refresh_tests),
                'successful_tests': len(successful_tests),
                'performance_compliant_tests': len(performance_compliant_tests),
                'avg_duration': avg_duration,
                'max_duration': max_duration,
                'min_duration': min_duration,
                'performance_target': performance_target,
                'epic_a_us_a2_compliant': epic_compliance,
                'test_results': refresh_tests
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Waiver refresh performance validation failed: {e}'
            }
    
    def validate_database_query_performance(self) -> Dict[str, Any]:
        """
        Validate database query performance for Epic A operations
        
        Success Criteria:
        - All Epic A queries execute in reasonable time
        - Query performance is consistent
        - No query bottlenecks
        """
        try:
            print("   Testing database query performance...")
            
            query_tests = [
                {
                    'name': 'player_lookup_by_canonical_id',
                    'description': 'Player lookup by canonical ID',
                    'query': lambda: self.db.query(Player).filter(
                        Player.nfl_id == self.player_mapper.generate_canonical_id('Josh Allen', 'QB', 'BUF')
                    ).first()
                },
                {
                    'name': 'usage_data_weekly',
                    'description': 'Usage data for current week',
                    'query': lambda: self.db.query(PlayerUsage).filter(
                        PlayerUsage.season == self.current_season,
                        PlayerUsage.week == self.test_week
                    ).limit(100).all()
                },
                {
                    'name': 'projections_weekly',
                    'description': 'Projections for current week',
                    'query': lambda: self.db.query(PlayerProjections).filter(
                        PlayerProjections.season == self.current_season,
                        PlayerProjections.week == self.test_week
                    ).limit(100).all()
                },
                {
                    'name': 'roster_by_league',
                    'description': 'Roster entries by league',
                    'query': lambda: self.db.query(RosterEntry).filter(
                        RosterEntry.league_id == self.test_league_id,
                        RosterEntry.is_active == True
                    ).all()
                },
                {
                    'name': 'players_by_position',
                    'description': 'Players by position',
                    'query': lambda: self.db.query(Player).filter(
                        Player.position.in_(['QB', 'RB', 'WR', 'TE'])
                    ).limit(200).all()
                },
                {
                    'name': 'usage_aggregation',
                    'description': 'Usage aggregation by position',
                    'query': lambda: self.db.query(
                        Player.position,
                        func.avg(PlayerUsage.snap_pct).label('avg_snap_pct'),
                        func.count(PlayerUsage.id).label('usage_count')
                    ).join(PlayerUsage).filter(
                        PlayerUsage.season == self.current_season,
                        PlayerUsage.week == self.test_week
                    ).group_by(Player.position).all()
                }
            ]
            
            query_performance = {
                'queries_tested': len(query_tests),
                'successful_queries': 0,
                'performance_results': {},
                'query_errors': []
            }
            
            performance_threshold = self.performance_thresholds['query_response_max_seconds']
            
            for query_test in query_tests:
                try:
                    # Run query multiple times for consistency
                    execution_times = []
                    
                    for run in range(3):
                        start_time = time.time()
                        result = query_test['query']()
                        end_time = time.time()
                        
                        execution_times.append(end_time - start_time)
                    
                    avg_time = sum(execution_times) / len(execution_times)
                    max_time = max(execution_times)
                    result_count = len(result) if isinstance(result, list) else (1 if result else 0)
                    
                    query_performance['successful_queries'] += 1
                    query_performance['performance_results'][query_test['name']] = {
                        'avg_execution_time': avg_time,
                        'max_execution_time': max_time,
                        'result_count': result_count,
                        'description': query_test['description'],
                        'performance_ok': avg_time < performance_threshold
                    }
                    
                    performance_ok = "✓" if avg_time < performance_threshold else "❌"
                    print(f"     {performance_ok} {query_test['name']}: {avg_time:.3f}s avg, {result_count} results")
                    
                except Exception as e:
                    query_performance['query_errors'].append({
                        'query': query_test['name'],
                        'error': str(e)
                    })
                    print(f"     ❌ {query_test['name']}: ERROR - {e}")
            
            # Calculate overall metrics
            successful_queries = list(query_performance['performance_results'].values())
            
            if successful_queries:
                avg_query_time = sum(q['avg_execution_time'] for q in successful_queries) / len(successful_queries)
                max_query_time = max(q['max_execution_time'] for q in successful_queries)
                fast_queries = sum(1 for q in successful_queries if q['performance_ok'])
                fast_query_rate = fast_queries / len(successful_queries)
            else:
                avg_query_time = max_query_time = float('inf')
                fast_query_rate = 0
            
            success = (
                query_performance['successful_queries'] >= len(query_tests) * 0.8 and  # 80% success rate
                avg_query_time < performance_threshold and  # Average meets threshold
                fast_query_rate >= 0.8 and  # 80% of queries meet performance threshold
                len(query_performance['query_errors']) <= 1  # Few query errors
            )
            
            print(f"   ✓ Successful queries: {query_performance['successful_queries']}/{query_performance['queries_tested']}")
            print(f"   ✓ Average query time: {avg_query_time:.3f}s")
            print(f"   ✓ Max query time: {max_query_time:.3f}s")
            print(f"   ✓ Fast queries rate: {fast_query_rate:.1%}")
            
            return {
                'success': success,
                'queries_tested': query_performance['queries_tested'],
                'successful_queries': query_performance['successful_queries'],
                'avg_query_time': avg_query_time,
                'max_query_time': max_query_time,
                'fast_query_rate': fast_query_rate,
                'performance_threshold': performance_threshold,
                'performance_results': query_performance['performance_results']
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Database query performance validation failed: {e}'
            }
    
    def validate_join_operation_performance(self) -> Dict[str, Any]:
        """
        Validate performance of join operations critical to Epic A
        
        Success Criteria:
        - Multi-table joins execute in reasonable time
        - Complex waiver candidates joins perform adequately
        - Join performance scales appropriately
        """
        try:
            print("   Testing join operation performance...")
            
            join_tests = [
                {
                    'name': 'player_usage_join',
                    'description': 'Player + Usage join',
                    'query': lambda: self.db.query(
                        Player.name,
                        Player.position,
                        PlayerUsage.snap_pct,
                        PlayerUsage.target_share
                    ).join(PlayerUsage).filter(
                        PlayerUsage.season == self.current_season,
                        PlayerUsage.week == self.test_week
                    ).limit(200).all()
                },
                {
                    'name': 'player_projections_join',
                    'description': 'Player + Projections join',
                    'query': lambda: self.db.query(
                        Player.name,
                        Player.position,
                        PlayerProjections.mean,
                        PlayerProjections.floor,
                        PlayerProjections.ceiling
                    ).join(PlayerProjections).filter(
                        PlayerProjections.season == self.current_season,
                        PlayerProjections.week == self.test_week
                    ).limit(200).all()
                },
                {
                    'name': 'three_table_join',
                    'description': 'Player + Usage + Projections join',
                    'query': lambda: self.db.query(
                        Player.name,
                        Player.position,
                        PlayerUsage.snap_pct,
                        PlayerProjections.mean
                    ).join(PlayerUsage).join(PlayerProjections).filter(
                        PlayerUsage.season == self.current_season,
                        PlayerUsage.week == self.test_week,
                        PlayerProjections.season == self.current_season,
                        PlayerProjections.week == self.test_week
                    ).limit(100).all()
                },
                {
                    'name': 'waiver_candidates_join',
                    'description': 'Full waiver candidates join (Epic A critical)',
                    'query': lambda: self.db.query(
                        Player.name,
                        Player.position,
                        PlayerUsage.snap_pct,
                        PlayerProjections.mean,
                        func.case(
                            (RosterEntry.id.isnot(None), True),
                            else_=False
                        ).label('rostered')
                    ).outerjoin(PlayerUsage, and_(
                        Player.id == PlayerUsage.player_id,
                        PlayerUsage.season == self.current_season,
                        PlayerUsage.week == self.test_week
                    )).outerjoin(PlayerProjections, and_(
                        Player.id == PlayerProjections.player_id,
                        PlayerProjections.season == self.current_season,
                        PlayerProjections.week == self.test_week
                    )).outerjoin(RosterEntry, and_(
                        Player.id == RosterEntry.player_id,
                        RosterEntry.is_active == True
                    )).filter(
                        Player.position.in_(['QB', 'RB', 'WR', 'TE'])
                    ).limit(150).all()
                }
            ]
            
            join_performance = {
                'joins_tested': len(join_tests),
                'successful_joins': 0,
                'join_results': {},
                'join_errors': []
            }
            
            join_threshold = self.performance_thresholds['join_query_max_seconds']
            
            for join_test in join_tests:
                try:
                    start_time = time.time()
                    results = join_test['query']()
                    end_time = time.time()
                    
                    execution_time = end_time - start_time
                    result_count = len(results) if isinstance(results, list) else 0
                    
                    join_performance['successful_joins'] += 1
                    join_performance['join_results'][join_test['name']] = {
                        'execution_time': execution_time,
                        'result_count': result_count,
                        'description': join_test['description'],
                        'performance_ok': execution_time < join_threshold,
                        'is_epic_a_critical': 'waiver_candidates' in join_test['name']
                    }
                    
                    performance_ok = "✓" if execution_time < join_threshold else "❌"
                    critical_marker = " 🎯" if 'waiver_candidates' in join_test['name'] else ""
                    print(f"     {performance_ok} {join_test['name']}: {execution_time:.3f}s, {result_count} results{critical_marker}")
                    
                except Exception as e:
                    join_performance['join_errors'].append({
                        'join': join_test['name'],
                        'error': str(e)
                    })
                    print(f"     ❌ {join_test['name']}: ERROR - {e}")
            
            # Calculate metrics
            successful_joins = list(join_performance['join_results'].values())
            
            if successful_joins:
                avg_join_time = sum(j['execution_time'] for j in successful_joins) / len(successful_joins)
                max_join_time = max(j['execution_time'] for j in successful_joins)
                fast_joins = sum(1 for j in successful_joins if j['performance_ok'])
                fast_join_rate = fast_joins / len(successful_joins)
                
                # Check Epic A critical join performance
                epic_a_joins = [j for j in successful_joins if j.get('is_epic_a_critical', False)]
                epic_a_performance_ok = all(j['performance_ok'] for j in epic_a_joins) if epic_a_joins else True
            else:
                avg_join_time = max_join_time = float('inf')
                fast_join_rate = 0
                epic_a_performance_ok = False
            
            success = (
                join_performance['successful_joins'] >= len(join_tests) * 0.75 and  # 75% success rate
                avg_join_time < join_threshold and  # Average meets threshold
                fast_join_rate >= 0.75 and  # 75% of joins meet performance threshold
                epic_a_performance_ok  # Epic A critical joins perform well
            )
            
            print(f"   ✓ Successful joins: {join_performance['successful_joins']}/{join_performance['joins_tested']}")
            print(f"   ✓ Average join time: {avg_join_time:.3f}s")
            print(f"   ✓ Fast joins rate: {fast_join_rate:.1%}")
            print(f"   ✓ Epic A critical joins: {'✅ OK' if epic_a_performance_ok else '❌ FAIL'}")
            
            return {
                'success': success,
                'joins_tested': join_performance['joins_tested'],
                'successful_joins': join_performance['successful_joins'],
                'avg_join_time': avg_join_time,
                'max_join_time': max_join_time,
                'fast_join_rate': fast_join_rate,
                'epic_a_performance_ok': epic_a_performance_ok,
                'join_threshold': join_threshold,
                'join_results': join_performance['join_results']
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Join operation performance validation failed: {e}'
            }
    
    def validate_bulk_operations_performance(self) -> Dict[str, Any]:
        """
        Validate performance of bulk data operations
        
        Success Criteria:
        - Bulk data loading operations perform adequately
        - Large result set queries are efficient
        - Data synchronization operations meet time requirements
        """
        try:
            print("   Testing bulk operations performance...")
            
            bulk_tests = [
                {
                    'name': 'large_player_query',
                    'description': 'Query all players',
                    'operation': lambda: self.db.query(Player).limit(1000).all()
                },
                {
                    'name': 'bulk_usage_query',
                    'description': 'Query usage for multiple weeks',
                    'operation': lambda: self.db.query(PlayerUsage).filter(
                        PlayerUsage.season == self.current_season,
                        PlayerUsage.week.between(1, 6)
                    ).limit(2000).all()
                },
                {
                    'name': 'bulk_projections_query',
                    'description': 'Query projections for multiple weeks',
                    'operation': lambda: self.db.query(PlayerProjections).filter(
                        PlayerProjections.season == self.current_season,
                        PlayerProjections.week.between(1, 6)
                    ).limit(2000).all()
                },
                {
                    'name': 'roster_aggregation',
                    'description': 'Aggregate roster statistics',
                    'operation': lambda: self.db.query(
                        RosterEntry.league_id,
                        func.count(RosterEntry.player_id).label('total_players'),
                        func.count(distinct(RosterEntry.user_id)).label('teams')
                    ).filter(
                        RosterEntry.is_active == True
                    ).group_by(RosterEntry.league_id).all()
                }
            ]
            
            bulk_performance = {
                'operations_tested': len(bulk_tests),
                'successful_operations': 0,
                'operation_results': {},
                'operation_errors': []
            }
            
            bulk_threshold = self.performance_thresholds['bulk_operation_max_seconds']
            
            for bulk_test in bulk_tests:
                try:
                    start_time = time.time()
                    results = bulk_test['operation']()
                    end_time = time.time()
                    
                    execution_time = end_time - start_time
                    result_count = len(results) if isinstance(results, list) else 0
                    
                    bulk_performance['successful_operations'] += 1
                    bulk_performance['operation_results'][bulk_test['name']] = {
                        'execution_time': execution_time,
                        'result_count': result_count,
                        'description': bulk_test['description'],
                        'performance_ok': execution_time < bulk_threshold
                    }
                    
                    performance_ok = "✓" if execution_time < bulk_threshold else "❌"
                    print(f"     {performance_ok} {bulk_test['name']}: {execution_time:.3f}s, {result_count} results")
                    
                except Exception as e:
                    bulk_performance['operation_errors'].append({
                        'operation': bulk_test['name'],
                        'error': str(e)
                    })
                    print(f"     ❌ {bulk_test['name']}: ERROR - {e}")
            
            # Calculate metrics
            successful_operations = list(bulk_performance['operation_results'].values())
            
            if successful_operations:
                avg_bulk_time = sum(op['execution_time'] for op in successful_operations) / len(successful_operations)
                max_bulk_time = max(op['execution_time'] for op in successful_operations)
                fast_operations = sum(1 for op in successful_operations if op['performance_ok'])
                fast_operation_rate = fast_operations / len(successful_operations)
            else:
                avg_bulk_time = max_bulk_time = float('inf')
                fast_operation_rate = 0
            
            success = (
                bulk_performance['successful_operations'] >= len(bulk_tests) * 0.75 and  # 75% success rate
                avg_bulk_time < bulk_threshold and  # Average meets threshold
                fast_operation_rate >= 0.75  # 75% of operations meet performance threshold
            )
            
            print(f"   ✓ Successful operations: {bulk_performance['successful_operations']}/{bulk_performance['operations_tested']}")
            print(f"   ✓ Average operation time: {avg_bulk_time:.3f}s")
            print(f"   ✓ Fast operations rate: {fast_operation_rate:.1%}")
            
            return {
                'success': success,
                'operations_tested': bulk_performance['operations_tested'],
                'successful_operations': bulk_performance['successful_operations'],
                'avg_bulk_time': avg_bulk_time,
                'max_bulk_time': max_bulk_time,
                'fast_operation_rate': fast_operation_rate,
                'bulk_threshold': bulk_threshold,
                'operation_results': bulk_performance['operation_results']
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Bulk operations performance validation failed: {e}'
            }
    
    def validate_memory_resource_usage(self) -> Dict[str, Any]:
        """
        Validate memory and resource usage during operations
        
        Success Criteria:
        - Memory usage stays within reasonable limits
        - No memory leaks during operations
        - Resource cleanup is effective
        """
        try:
            print("   Testing memory and resource usage...")
            
            # Get initial memory usage
            process = psutil.Process()
            initial_memory = process.memory_info().rss / (1024 * 1024)  # MB
            
            memory_tests = []
            
            # Test memory usage during waiver candidates refresh
            print("     Testing memory usage during waiver refresh...")
            memory_before = process.memory_info().rss / (1024 * 1024)
            
            # Execute memory-intensive operation
            start_time = time.time()
            try:
                refresh_result = self.enhanced_builder.refresh_waiver_candidates_for_league(
                    league_id=self.test_league_id,
                    week=self.test_week
                )
                operation_success = refresh_result.get('success', False)
            except Exception as e:
                operation_success = False
                print(f"       Operation error: {e}")
            
            end_time = time.time()
            memory_after = process.memory_info().rss / (1024 * 1024)
            
            memory_delta = memory_after - memory_before
            operation_time = end_time - start_time
            
            memory_tests.append({
                'operation': 'waiver_refresh',
                'memory_before': memory_before,
                'memory_after': memory_after,
                'memory_delta': memory_delta,
                'operation_time': operation_time,
                'operation_success': operation_success,
                'description': 'Enhanced waiver candidates refresh'
            })
            
            print(f"       Memory delta: {memory_delta:+.1f} MB")
            
            # Test memory usage during bulk queries
            print("     Testing memory usage during bulk queries...")
            memory_before = process.memory_info().rss / (1024 * 1024)
            
            start_time = time.time()
            try:
                # Execute bulk query
                bulk_results = self.db.query(Player).join(PlayerUsage).filter(
                    PlayerUsage.season == self.current_season
                ).limit(1000).all()
                operation_success = len(bulk_results) > 0
            except Exception as e:
                operation_success = False
                bulk_results = []
                print(f"       Bulk query error: {e}")
            
            end_time = time.time()
            memory_after = process.memory_info().rss / (1024 * 1024)
            
            memory_delta = memory_after - memory_before
            operation_time = end_time - start_time
            
            memory_tests.append({
                'operation': 'bulk_query',
                'memory_before': memory_before,
                'memory_after': memory_after,
                'memory_delta': memory_delta,
                'operation_time': operation_time,
                'operation_success': operation_success,
                'result_count': len(bulk_results),
                'description': 'Bulk player + usage query'
            })
            
            print(f"       Memory delta: {memory_delta:+.1f} MB")
            
            # Analyze memory usage
            final_memory = process.memory_info().rss / (1024 * 1024)
            total_memory_growth = final_memory - initial_memory
            max_memory_delta = max(test.get('memory_delta', 0) for test in memory_tests)
            
            memory_threshold = self.performance_thresholds['memory_usage_max_mb']
            
            success = (
                max_memory_delta < memory_threshold and  # Memory spikes stay under threshold
                total_memory_growth < memory_threshold * 0.5 and  # Total growth reasonable
                all(test.get('operation_success', False) for test in memory_tests)  # All operations successful
            )
            
            print(f"   ✓ Initial memory: {initial_memory:.1f} MB")
            print(f"   ✓ Final memory: {final_memory:.1f} MB")
            print(f"   ✓ Total memory growth: {total_memory_growth:+.1f} MB")
            print(f"   ✓ Max memory spike: {max_memory_delta:.1f} MB")
            print(f"   ✓ Memory threshold: {memory_threshold} MB")
            
            return {
                'success': success,
                'initial_memory_mb': initial_memory,
                'final_memory_mb': final_memory,
                'total_memory_growth_mb': total_memory_growth,
                'max_memory_delta_mb': max_memory_delta,
                'memory_threshold_mb': memory_threshold,
                'memory_tests': memory_tests
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Memory resource usage validation failed: {e}'
            }
    
    def validate_concurrent_operations_performance(self) -> Dict[str, Any]:
        """
        Validate performance under concurrent operations
        
        Success Criteria:
        - System handles multiple simultaneous operations
        - Performance degradation under load is acceptable
        - No deadlocks or resource contention issues
        """
        try:
            print("   Testing concurrent operations performance...")
            
            # Define concurrent operations
            def waiver_refresh_operation():
                try:
                    db = SessionLocal()
                    try:
                        builder = EnhancedWaiverCandidatesBuilder()
                        start_time = time.time()
                        result = builder.refresh_waiver_candidates_for_league(
                            league_id=self.test_league_id,
                            week=self.test_week
                        )
                        end_time = time.time()
                        return {
                            'operation': 'waiver_refresh',
                            'duration': end_time - start_time,
                            'success': result.get('success', False),
                            'candidates_count': result.get('candidates_count', 0)
                        }
                    finally:
                        db.close()
                except Exception as e:
                    return {
                        'operation': 'waiver_refresh',
                        'error': str(e),
                        'success': False
                    }
            
            def query_operation():
                try:
                    db = SessionLocal()
                    try:
                        start_time = time.time()
                        results = db.query(Player).join(PlayerUsage).filter(
                            PlayerUsage.season == self.current_season
                        ).limit(200).all()
                        end_time = time.time()
                        return {
                            'operation': 'query',
                            'duration': end_time - start_time,
                            'success': len(results) > 0,
                            'result_count': len(results)
                        }
                    finally:
                        db.close()
                except Exception as e:
                    return {
                        'operation': 'query',
                        'error': str(e),
                        'success': False
                    }
            
            # Run concurrent operations
            print("     Executing concurrent operations...")
            
            start_time = time.time()
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                # Submit multiple operations concurrently
                futures = []
                
                # 2 waiver refresh operations
                futures.append(executor.submit(waiver_refresh_operation))
                futures.append(executor.submit(waiver_refresh_operation))
                
                # 2 query operations
                futures.append(executor.submit(query_operation))
                futures.append(executor.submit(query_operation))
                
                # Wait for all operations to complete
                concurrent_results = []
                for future in concurrent.futures.as_completed(futures, timeout=120):
                    try:
                        result = future.result()
                        concurrent_results.append(result)
                    except Exception as e:
                        concurrent_results.append({
                            'operation': 'unknown',
                            'error': str(e),
                            'success': False
                        })
            
            end_time = time.time()
            total_concurrent_time = end_time - start_time
            
            # Analyze results
            successful_operations = [r for r in concurrent_results if r.get('success', False)]
            failed_operations = [r for r in concurrent_results if not r.get('success', False)]
            
            if successful_operations:
                operation_times = [r['duration'] for r in successful_operations if 'duration' in r]
                avg_operation_time = sum(operation_times) / len(operation_times) if operation_times else 0
                max_operation_time = max(operation_times) if operation_times else 0
            else:
                avg_operation_time = max_operation_time = 0
            
            concurrent_threshold = self.performance_thresholds['concurrent_operations_max_seconds']
            
            success = (
                len(successful_operations) >= 3 and  # At least 3 operations successful
                total_concurrent_time < concurrent_threshold and  # Total time reasonable
                len(failed_operations) <= 1 and  # Few failures
                avg_operation_time < 30.0  # Individual operations still reasonable
            )
            
            print(f"     ✓ Total concurrent time: {total_concurrent_time:.2f}s")
            print(f"     ✓ Successful operations: {len(successful_operations)}/{len(concurrent_results)}")
            print(f"     ✓ Failed operations: {len(failed_operations)}")
            print(f"     ✓ Average operation time: {avg_operation_time:.2f}s")
            
            # Show operation breakdown
            for result in concurrent_results:
                if result.get('success', False):
                    print(f"       ✓ {result['operation']}: {result.get('duration', 0):.2f}s")
                else:
                    print(f"       ❌ {result['operation']}: {result.get('error', 'Unknown error')}")
            
            return {
                'success': success,
                'total_concurrent_time': total_concurrent_time,
                'successful_operations': len(successful_operations),
                'failed_operations': len(failed_operations),
                'avg_operation_time': avg_operation_time,
                'max_operation_time': max_operation_time,
                'concurrent_threshold': concurrent_threshold,
                'concurrent_results': concurrent_results
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Concurrent operations performance validation failed: {e}'
            }
    
    def _print_validation_summary(self, results: Dict[str, Any]):
        """Print comprehensive validation summary"""
        print("\n" + "=" * 80)
        print("PERFORMANCE REQUIREMENT VALIDATION SUMMARY")
        print("=" * 80)
        
        overall_success = results['overall_success']
        status_icon = "✅" if overall_success else "❌"
        
        print(f"\n{status_icon} OVERALL VALIDATION STATUS: {'PASS' if overall_success else 'FAIL'}")
        print(f"📅 Test Date: {results['timestamp']}")
        print(f"💾 Database: {results['database']}")
        print(f"🏈 Season: {results['season']}, Week: {results['test_week']}")
        
        print(f"\n📊 Individual Validation Results:")
        for validation_name, validation_result in results.get('validations', {}).items():
            success = validation_result.get('success', False)
            icon = "✅" if success else "❌"
            print(f"  {icon} {validation_name.replace('_', ' ').title()}: {'PASS' if success else 'FAIL'}")
            
            if not success and 'error' in validation_result:
                print(f"      Error: {validation_result['error']}")
        
        # Epic A US-A2 Critical Metrics
        print(f"\n🎯 Epic A US-A2 Critical Performance Metrics:")
        
        if 'waiver_refresh_performance' in results.get('validations', {}):
            refresh_test = results['validations']['waiver_refresh_performance']
            epic_compliance = refresh_test.get('epic_a_us_a2_compliant', False)
            avg_duration = refresh_test.get('avg_duration', float('inf'))
            target = refresh_test.get('performance_target', 60)
            
            compliance_icon = "✅" if epic_compliance else "❌"
            print(f"  {compliance_icon} Waiver Refresh Performance: {avg_duration:.2f}s (target: <{target}s)")
            print(f"      Epic A US-A2 Requirement: {'COMPLIANT' if epic_compliance else 'NON-COMPLIANT'}")
        
        # Other Key Performance Metrics
        print(f"\n⚡ Additional Performance Metrics:")
        
        if 'database_query_performance' in results.get('validations', {}):
            query_test = results['validations']['database_query_performance']
            print(f"  • Database Query Average: {query_test.get('avg_query_time', 0):.3f}s")
            print(f"  • Fast Query Rate: {query_test.get('fast_query_rate', 0):.1%}")
        
        if 'join_operation_performance' in results.get('validations', {}):
            join_test = results['validations']['join_operation_performance']
            epic_joins_ok = join_test.get('epic_a_performance_ok', False)
            print(f"  • Join Operation Average: {join_test.get('avg_join_time', 0):.3f}s")
            print(f"  • Epic A Critical Joins: {'✅ OK' if epic_joins_ok else '❌ FAIL'}")
        
        if 'memory_resource_usage' in results.get('validations', {}):
            memory_test = results['validations']['memory_resource_usage']
            print(f"  • Max Memory Spike: {memory_test.get('max_memory_delta_mb', 0):.1f} MB")
            print(f"  • Total Memory Growth: {memory_test.get('total_memory_growth_mb', 0):+.1f} MB")
        
        if 'concurrent_operations_performance' in results.get('validations', {}):
            concurrent_test = results['validations']['concurrent_operations_performance']
            print(f"  • Concurrent Operations: {concurrent_test.get('successful_operations', 0)} successful")
            print(f"  • Concurrent Total Time: {concurrent_test.get('total_concurrent_time', 0):.2f}s")
        
        print(f"\n{'🎉 Performance Requirements are FULLY SATISFIED!' if overall_success else '⚠️  Performance Requirements need attention'}")
        print("=" * 80)

def main():
    """Run performance requirement validation tests"""
    print("Starting Performance Requirement Validation...")
    
    try:
        validator = PerformanceRequirementValidator()
        results = validator.run_all_validations()
        
        # Return appropriate exit code
        exit_code = 0 if results['overall_success'] else 1
        print(f"\nExiting with code {exit_code}")
        return exit_code
        
    except Exception as e:
        print(f"❌ CRITICAL ERROR: {e}")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)