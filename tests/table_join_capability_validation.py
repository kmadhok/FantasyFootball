#!/usr/bin/env python3
"""
Table Join Capability Validation Tests

Epic A US-A1 Acceptance Criteria:
"All tables can be joined to materialize a waiver_candidates view."

This script provides comprehensive validation of table join capabilities
ensuring all Epic A tables can be successfully joined to create the
waiver candidates materialized view as required by US-A2.
"""

import os
import sys
import traceback
import time
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from collections import defaultdict

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.config import get_config
from src.database import SessionLocal, Player, PlayerUsage, PlayerProjections, RosterEntry
from src.utils.player_id_mapper import PlayerIDMapper
from sqlalchemy import func, distinct, and_, or_, text
from sqlalchemy.orm import aliased

class TableJoinCapabilityValidator:
    """
    Comprehensive validation of table join capabilities for Epic A materialized view
    """
    
    def __init__(self):
        self.config = get_config()
        self.db = SessionLocal()
        self.player_mapper = PlayerIDMapper()
        
        self.current_season = 2025
        self.test_week = 4
        
        # Epic A tables that must be joinable
        self.epic_a_tables = {
            'players': Player,
            'roster_snapshots': RosterEntry, 
            'usage': PlayerUsage,
            'projections': PlayerProjections
        }
        
        print("=" * 80)
        print("TABLE JOIN CAPABILITY VALIDATION")
        print("Epic A US-A1: Table Join Capability for Waiver Candidates View")
        print("=" * 80)
    
    def run_all_validations(self) -> Dict[str, Any]:
        """
        Execute all table join capability validation tests
        
        Returns comprehensive validation results
        """
        validation_results = {
            'overall_success': True,
            'timestamp': datetime.utcnow().isoformat(),
            'database': self.config.DATABASE_URL,
            'season': self.current_season,
            'test_week': self.test_week,
            'validations': {}
        }
        
        try:
            print(f"\n🔗 Running Table Join Capability Validations")
            print(f"Database: {self.config.DATABASE_URL}")
            print(f"Season: {self.current_season}, Week: {self.test_week}")
            print("-" * 60)
            
            # Validation 1: Basic Join Capability
            print("\n🔧 VALIDATION 1: Basic Join Capability")
            result_1 = self.validate_basic_join_capability()
            validation_results['validations']['basic_join_capability'] = result_1
            if not result_1.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 2: Foreign Key Relationships
            print("\n🔑 VALIDATION 2: Foreign Key Relationships")
            result_2 = self.validate_foreign_key_relationships()
            validation_results['validations']['foreign_key_relationships'] = result_2
            if not result_2.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 3: Multi-Table Join Performance
            print("\n⚡ VALIDATION 3: Multi-Table Join Performance")
            result_3 = self.validate_join_performance()
            validation_results['validations']['join_performance'] = result_3
            if not result_3.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 4: Waiver Candidates Materialization
            print("\n📋 VALIDATION 4: Waiver Candidates Materialization")
            result_4 = self.validate_waiver_candidates_materialization()
            validation_results['validations']['waiver_candidates_materialization'] = result_4
            if not result_4.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 5: Join Data Consistency
            print("\n🔍 VALIDATION 5: Join Data Consistency")
            result_5 = self.validate_join_data_consistency()
            validation_results['validations']['join_data_consistency'] = result_5
            if not result_5.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 6: Complex Query Capability
            print("\n🧩 VALIDATION 6: Complex Query Capability")
            result_6 = self.validate_complex_query_capability()
            validation_results['validations']['complex_query_capability'] = result_6
            if not result_6.get('success', False):
                validation_results['overall_success'] = False
            
            # Final Summary
            self._print_validation_summary(validation_results)
            
            return validation_results
            
        except Exception as e:
            print(f"❌ CRITICAL ERROR in table join capability validation: {e}")
            traceback.print_exc()
            validation_results['overall_success'] = False
            validation_results['error'] = str(e)
            return validation_results
        
        finally:
            self.db.close()
    
    def validate_basic_join_capability(self) -> Dict[str, Any]:
        """
        Validate basic join capability between Epic A tables
        
        Success Criteria:
        - All Epic A tables can be joined via player_id
        - Joins return results without errors
        - Join keys are properly indexed and functional
        """
        try:
            print("   Testing basic join capability...")
            
            join_results = {
                'join_tests': {},
                'successful_joins': 0,
                'total_join_tests': 0,
                'join_errors': []
            }
            
            # Test pairwise joins between Epic A tables
            join_pairs = [
                ('players', 'usage', Player, PlayerUsage, 'id', 'player_id'),
                ('players', 'projections', Player, PlayerProjections, 'id', 'player_id'),
                ('players', 'roster_snapshots', Player, RosterEntry, 'id', 'player_id'),
                ('usage', 'projections', PlayerUsage, PlayerProjections, 'player_id', 'player_id'),
                ('usage', 'roster_snapshots', PlayerUsage, RosterEntry, 'player_id', 'player_id'),
                ('projections', 'roster_snapshots', PlayerProjections, RosterEntry, 'player_id', 'player_id')
            ]
            
            for table1_name, table2_name, table1_class, table2_class, key1, key2 in join_pairs:
                join_results['total_join_tests'] += 1
                join_name = f"{table1_name}_{table2_name}"
                
                try:
                    # Perform basic join
                    query = self.db.query(
                        func.count(getattr(table1_class, key1)).label('join_count')
                    ).join(
                        table2_class,
                        getattr(table1_class, key1) == getattr(table2_class, key2)
                    ).filter(
                        # Add filters to ensure meaningful data
                        and_(
                            getattr(table1_class, key1).isnot(None),
                            getattr(table2_class, key2).isnot(None)
                        )
                    )
                    
                    # Add season filter if applicable
                    if hasattr(table2_class, 'season'):
                        query = query.filter(getattr(table2_class, 'season') == self.current_season)
                    
                    result = query.first()
                    join_count = result.join_count if result else 0
                    
                    if join_count > 0:
                        join_results['successful_joins'] += 1
                        join_results['join_tests'][join_name] = {
                            'success': True,
                            'result_count': join_count,
                            'tables': f"{table1_name} -> {table2_name}",
                            'join_keys': f"{key1} = {key2}"
                        }
                    else:
                        join_results['join_tests'][join_name] = {
                            'success': False,
                            'result_count': 0,
                            'error': 'No results from join',
                            'tables': f"{table1_name} -> {table2_name}"
                        }
                        join_results['join_errors'].append(f"{join_name}: No results")
                
                except Exception as e:
                    join_results['join_tests'][join_name] = {
                        'success': False,
                        'error': str(e),
                        'tables': f"{table1_name} -> {table2_name}"
                    }
                    join_results['join_errors'].append(f"{join_name}: {e}")
            
            # Calculate success metrics
            join_success_rate = join_results['successful_joins'] / join_results['total_join_tests']
            
            success = (
                join_success_rate >= 0.7 and  # At least 70% of joins successful
                join_results['successful_joins'] >= 4 and  # At least 4 successful joins
                len(join_results['join_errors']) <= 2  # Few join errors
            )
            
            print(f"   ✓ Join tests performed: {join_results['total_join_tests']}")
            print(f"   ✓ Successful joins: {join_results['successful_joins']}")
            print(f"   ✓ Join success rate: {join_success_rate:.1%}")
            print(f"   ✓ Join errors: {len(join_results['join_errors'])}")
            
            # Show successful joins
            for join_name, join_data in join_results['join_tests'].items():
                if join_data['success']:
                    print(f"   ✓ {join_name}: {join_data['result_count']} records")
                else:
                    print(f"   ❌ {join_name}: {join_data.get('error', 'Unknown error')}")
            
            return {
                'success': success,
                'total_join_tests': join_results['total_join_tests'],
                'successful_joins': join_results['successful_joins'],
                'join_success_rate': join_success_rate,
                'join_errors': len(join_results['join_errors']),
                'join_test_results': join_results['join_tests'],
                'sample_errors': join_results['join_errors'][:3]
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Basic join capability validation failed: {e}'
            }
    
    def validate_foreign_key_relationships(self) -> Dict[str, Any]:
        """
        Validate foreign key relationships and referential integrity
        
        Success Criteria:
        - player_id references exist in Player table
        - No orphaned records in dependent tables
        - Referential integrity is maintained
        """
        try:
            print("   Testing foreign key relationships...")
            
            fk_analysis = {
                'tables_analyzed': 0,
                'total_records_checked': 0,
                'orphaned_records': 0,
                'integrity_violations': [],
                'fk_statistics': {}
            }
            
            # Check foreign key relationships for Epic A tables
            fk_checks = [
                ('PlayerUsage', 'player_id', Player, 'id'),
                ('PlayerProjections', 'player_id', Player, 'id'),
                ('RosterEntry', 'player_id', Player, 'id')
            ]
            
            for table_name, fk_column, parent_table, parent_key in fk_checks:
                fk_analysis['tables_analyzed'] += 1
                
                try:
                    # Get the table class by name
                    table_class = globals().get(table_name)
                    if not table_class:
                        continue
                    
                    # Count total records in dependent table
                    total_records = self.db.query(table_class).filter(
                        getattr(table_class, fk_column).isnot(None)
                    ).count()
                    
                    fk_analysis['total_records_checked'] += total_records
                    
                    # Count orphaned records (records with FK that don't exist in parent)
                    orphaned_query = self.db.query(table_class).outerjoin(
                        parent_table,
                        getattr(table_class, fk_column) == getattr(parent_table, parent_key)
                    ).filter(
                        getattr(parent_table, parent_key).is_(None),
                        getattr(table_class, fk_column).isnot(None)
                    ).count()
                    
                    orphaned_count = orphaned_query
                    fk_analysis['orphaned_records'] += orphaned_count
                    
                    # Calculate integrity metrics
                    integrity_rate = (total_records - orphaned_count) / total_records if total_records > 0 else 1.0
                    
                    fk_analysis['fk_statistics'][table_name] = {
                        'total_records': total_records,
                        'orphaned_records': orphaned_count,
                        'integrity_rate': integrity_rate,
                        'foreign_key': f"{fk_column} -> {parent_table.__name__}.{parent_key}"
                    }
                    
                    if integrity_rate < 0.95:  # Less than 95% integrity
                        fk_analysis['integrity_violations'].append({
                            'table': table_name,
                            'integrity_rate': integrity_rate,
                            'orphaned_count': orphaned_count,
                            'total_records': total_records
                        })
                
                except Exception as e:
                    fk_analysis['integrity_violations'].append({
                        'table': table_name,
                        'error': str(e)
                    })
            
            # Calculate overall metrics
            overall_orphaned_rate = fk_analysis['orphaned_records'] / fk_analysis['total_records_checked'] if fk_analysis['total_records_checked'] > 0 else 0
            
            success = (
                overall_orphaned_rate <= 0.05 and  # Less than 5% orphaned records
                len(fk_analysis['integrity_violations']) <= 1 and  # At most 1 table with integrity issues
                fk_analysis['tables_analyzed'] >= 3  # At least 3 tables analyzed
            )
            
            print(f"   ✓ Tables analyzed: {fk_analysis['tables_analyzed']}")
            print(f"   ✓ Total records checked: {fk_analysis['total_records_checked']}")
            print(f"   ✓ Orphaned records: {fk_analysis['orphaned_records']}")
            print(f"   ✓ Overall orphaned rate: {overall_orphaned_rate:.2%}")
            print(f"   ✓ Integrity violations: {len(fk_analysis['integrity_violations'])}")
            
            # Show table-specific integrity
            for table_name, stats in fk_analysis['fk_statistics'].items():
                print(f"   ✓ {table_name}: {stats['integrity_rate']:.1%} integrity ({stats['orphaned_records']}/{stats['total_records']} orphaned)")
            
            return {
                'success': success,
                'tables_analyzed': fk_analysis['tables_analyzed'],
                'total_records_checked': fk_analysis['total_records_checked'],
                'orphaned_records': fk_analysis['orphaned_records'],
                'overall_orphaned_rate': overall_orphaned_rate,
                'integrity_violations': len(fk_analysis['integrity_violations']),
                'fk_statistics': fk_analysis['fk_statistics'],
                'sample_violations': fk_analysis['integrity_violations'][:2]
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Foreign key relationships validation failed: {e}'
            }
    
    def validate_join_performance(self) -> Dict[str, Any]:
        """
        Validate performance of multi-table joins
        
        Success Criteria:
        - Complex joins execute in reasonable time
        - Join operations scale appropriately
        - Query plans are efficient
        """
        try:
            print("   Testing join performance...")
            
            performance_tests = [
                {
                    'name': 'two_table_join',
                    'description': 'Players + Usage join',
                    'query': lambda: self.db.query(
                        Player.name,
                        PlayerUsage.snap_pct
                    ).join(PlayerUsage).filter(
                        PlayerUsage.season == self.current_season,
                        PlayerUsage.week == self.test_week
                    ).limit(100).all()
                },
                {
                    'name': 'three_table_join',
                    'description': 'Players + Usage + Projections join',
                    'query': lambda: self.db.query(
                        Player.name,
                        PlayerUsage.snap_pct,
                        PlayerProjections.mean
                    ).join(PlayerUsage).join(PlayerProjections).filter(
                        PlayerUsage.season == self.current_season,
                        PlayerUsage.week == self.test_week,
                        PlayerProjections.season == self.current_season,
                        PlayerProjections.week == self.test_week
                    ).limit(50).all()
                },
                {
                    'name': 'four_table_join',
                    'description': 'Full waiver candidates join',
                    'query': lambda: self.db.query(
                        Player.name,
                        Player.position,
                        PlayerUsage.snap_pct,
                        PlayerProjections.mean,
                        RosterEntry.league_id
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
                    )).limit(30).all()
                },
                {
                    'name': 'aggregate_join',
                    'description': 'Join with aggregation',
                    'query': lambda: self.db.query(
                        Player.position,
                        func.avg(PlayerUsage.snap_pct).label('avg_snap_pct'),
                        func.count(Player.id).label('player_count')
                    ).join(PlayerUsage).filter(
                        PlayerUsage.season == self.current_season,
                        PlayerUsage.week == self.test_week
                    ).group_by(Player.position).all()
                }
            ]
            
            performance_results = {
                'tests_performed': len(performance_tests),
                'successful_tests': 0,
                'performance_metrics': {},
                'performance_errors': []
            }
            
            for test in performance_tests:
                try:
                    start_time = time.time()
                    results = test['query']()
                    end_time = time.time()
                    
                    execution_time = end_time - start_time
                    result_count = len(results) if isinstance(results, list) else 1
                    
                    performance_results['successful_tests'] += 1
                    performance_results['performance_metrics'][test['name']] = {
                        'execution_time': execution_time,
                        'result_count': result_count,
                        'description': test['description'],
                        'performance_ok': execution_time < 5.0  # 5 second threshold
                    }
                    
                except Exception as e:
                    performance_results['performance_errors'].append({
                        'test': test['name'],
                        'error': str(e)
                    })
                    performance_results['performance_metrics'][test['name']] = {
                        'error': str(e),
                        'description': test['description']
                    }
            
            # Calculate performance metrics
            successful_rate = performance_results['successful_tests'] / performance_results['tests_performed']
            
            execution_times = [
                metrics['execution_time'] 
                for metrics in performance_results['performance_metrics'].values()
                if 'execution_time' in metrics
            ]
            
            avg_execution_time = sum(execution_times) / len(execution_times) if execution_times else float('inf')
            max_execution_time = max(execution_times) if execution_times else float('inf')
            
            fast_queries = sum(1 for t in execution_times if t < 2.0)
            fast_query_rate = fast_queries / len(execution_times) if execution_times else 0
            
            success = (
                successful_rate >= 0.75 and  # At least 75% of tests successful
                avg_execution_time < 3.0 and  # Average execution < 3 seconds
                max_execution_time < 10.0 and  # Max execution < 10 seconds
                fast_query_rate >= 0.5  # At least 50% of queries < 2 seconds
            )
            
            print(f"   ✓ Performance tests performed: {performance_results['tests_performed']}")
            print(f"   ✓ Successful tests: {performance_results['successful_tests']}")
            print(f"   ✓ Success rate: {successful_rate:.1%}")
            print(f"   ✓ Average execution time: {avg_execution_time:.3f}s")
            print(f"   ✓ Max execution time: {max_execution_time:.3f}s")
            print(f"   ✓ Fast queries (< 2s): {fast_query_rate:.1%}")
            
            # Show individual test results
            for test_name, metrics in performance_results['performance_metrics'].items():
                if 'execution_time' in metrics:
                    performance_ok = "✓" if metrics['performance_ok'] else "⚠️"
                    print(f"   {performance_ok} {test_name}: {metrics['execution_time']:.3f}s ({metrics['result_count']} results)")
                else:
                    print(f"   ❌ {test_name}: {metrics.get('error', 'Unknown error')}")
            
            return {
                'success': success,
                'tests_performed': performance_results['tests_performed'],
                'successful_tests': performance_results['successful_tests'],
                'success_rate': successful_rate,
                'avg_execution_time': avg_execution_time,
                'max_execution_time': max_execution_time,
                'fast_query_rate': fast_query_rate,
                'performance_metrics': performance_results['performance_metrics']
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Join performance validation failed: {e}'
            }
    
    def validate_waiver_candidates_materialization(self) -> Dict[str, Any]:
        """
        Validate that waiver candidates view can be materialized from joins
        
        Success Criteria:
        - Complete waiver candidates query executes successfully
        - All Epic A required fields are available in joined result
        - Materialized view contains meaningful data
        """
        try:
            print("   Testing waiver candidates materialization...")
            
            # Attempt to materialize full waiver candidates view
            try:
                waiver_candidates_query = self.db.query(
                    Player.id.label('player_id'),
                    Player.name.label('player_name'),
                    Player.position.label('pos'),
                    Player.team,
                    
                    # Usage data
                    PlayerUsage.snap_pct,
                    PlayerUsage.route_pct,
                    PlayerUsage.target_share,
                    PlayerUsage.carry_share,
                    PlayerUsage.rz_touches,
                    PlayerUsage.ez_targets,
                    
                    # Projections data
                    PlayerProjections.mean.label('proj_next'),
                    PlayerProjections.floor,
                    PlayerProjections.ceiling,
                    PlayerProjections.source.label('proj_source'),
                    
                    # Roster status
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
                    Player.position.in_(['QB', 'RB', 'WR', 'TE'])  # Focus on skill positions
                )
                
                # Execute query
                start_time = time.time()
                waiver_candidates = waiver_candidates_query.limit(100).all()
                execution_time = time.time() - start_time
                
                materialization_analysis = {
                    'query_successful': True,
                    'execution_time': execution_time,
                    'result_count': len(waiver_candidates),
                    'field_coverage': {},
                    'data_quality': {}
                }
                
                if waiver_candidates:
                    # Analyze field coverage in results
                    sample_result = waiver_candidates[0]
                    
                    # Epic A required fields for waiver candidates
                    required_fields = [
                        'player_id', 'player_name', 'pos', 'team',
                        'snap_pct', 'route_pct', 'target_share', 'carry_share', 'rz_touches', 'ez_targets',
                        'proj_next', 'floor', 'ceiling', 'rostered'
                    ]
                    
                    fields_present = 0
                    fields_with_data = 0
                    
                    for field in required_fields:
                        if hasattr(sample_result, field):
                            fields_present += 1
                            field_value = getattr(sample_result, field)
                            if field_value is not None:
                                fields_with_data += 1
                    
                    materialization_analysis['field_coverage'] = {
                        'total_required': len(required_fields),
                        'fields_present': fields_present,
                        'fields_with_data': fields_with_data,
                        'presence_rate': fields_present / len(required_fields),
                        'data_rate': fields_with_data / fields_present if fields_present > 0 else 0
                    }
                    
                    # Analyze data quality
                    players_with_usage = sum(1 for r in waiver_candidates if r.snap_pct is not None)
                    players_with_projections = sum(1 for r in waiver_candidates if r.proj_next is not None)
                    rostered_players = sum(1 for r in waiver_candidates if r.rostered)
                    
                    materialization_analysis['data_quality'] = {
                        'players_with_usage': players_with_usage,
                        'players_with_projections': players_with_projections,
                        'rostered_players': rostered_players,
                        'usage_rate': players_with_usage / len(waiver_candidates),
                        'projection_rate': players_with_projections / len(waiver_candidates),
                        'roster_rate': rostered_players / len(waiver_candidates)
                    }
                
                else:
                    materialization_analysis['field_coverage'] = {'error': 'No results returned'}
                    materialization_analysis['data_quality'] = {'error': 'No results returned'}
                
            except Exception as e:
                materialization_analysis = {
                    'query_successful': False,
                    'error': str(e)
                }
            
            # Success criteria
            if materialization_analysis.get('query_successful', False):
                field_coverage = materialization_analysis.get('field_coverage', {})
                data_quality = materialization_analysis.get('data_quality', {})
                
                success = (
                    materialization_analysis.get('result_count', 0) >= 10 and  # At least 10 results
                    field_coverage.get('presence_rate', 0) >= 0.9 and  # 90% field presence
                    field_coverage.get('data_rate', 0) >= 0.4 and  # 40% fields have data
                    materialization_analysis.get('execution_time', float('inf')) < 10.0  # Executes < 10s
                )
            else:
                success = False
            
            print(f"   ✓ Query successful: {materialization_analysis.get('query_successful', False)}")
            print(f"   ✓ Execution time: {materialization_analysis.get('execution_time', 'N/A'):.3f}s")
            print(f"   ✓ Result count: {materialization_analysis.get('result_count', 0)}")
            
            if 'field_coverage' in materialization_analysis and 'presence_rate' in materialization_analysis['field_coverage']:
                fc = materialization_analysis['field_coverage']
                print(f"   ✓ Field presence: {fc['fields_present']}/{fc['total_required']} ({fc['presence_rate']:.1%})")
                print(f"   ✓ Fields with data: {fc['fields_with_data']}/{fc['fields_present']} ({fc['data_rate']:.1%})")
            
            if 'data_quality' in materialization_analysis and 'usage_rate' in materialization_analysis['data_quality']:
                dq = materialization_analysis['data_quality']
                print(f"   ✓ Usage data rate: {dq['usage_rate']:.1%}")
                print(f"   ✓ Projection data rate: {dq['projection_rate']:.1%}")
                print(f"   ✓ Roster data rate: {dq['roster_rate']:.1%}")
            
            return {
                'success': success,
                'query_successful': materialization_analysis.get('query_successful', False),
                'execution_time': materialization_analysis.get('execution_time', 0),
                'result_count': materialization_analysis.get('result_count', 0),
                'field_coverage': materialization_analysis.get('field_coverage', {}),
                'data_quality': materialization_analysis.get('data_quality', {}),
                'error': materialization_analysis.get('error')
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Waiver candidates materialization validation failed: {e}'
            }
    
    def validate_join_data_consistency(self) -> Dict[str, Any]:
        """
        Validate data consistency across joined tables
        
        Success Criteria:
        - Joined data is logically consistent
        - No data corruption in joins
        - Player information is consistent across tables
        """
        try:
            print("   Testing join data consistency...")
            
            # Test consistency of player data across joins
            consistency_query = self.db.query(
                Player.id.label('player_id'),
                Player.name.label('player_name'),
                Player.position,
                Player.team,
                PlayerUsage.player_id.label('usage_player_id'),
                PlayerProjections.player_id.label('proj_player_id'),
                RosterEntry.player_id.label('roster_player_id')
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
            )).limit(200).all()
            
            consistency_analysis = {
                'total_records_checked': len(consistency_query),
                'consistency_violations': 0,
                'player_id_mismatches': 0,
                'null_key_issues': 0,
                'data_corruption_indicators': [],
                'consistency_rate': 0
            }
            
            if consistency_query:
                for record in consistency_query:
                    # Check player ID consistency
                    player_ids = [
                        record.player_id,
                        record.usage_player_id,
                        record.proj_player_id,
                        record.roster_player_id
                    ]
                    
                    # Remove None values and check if remaining IDs are consistent
                    valid_ids = [pid for pid in player_ids if pid is not None]
                    unique_ids = set(valid_ids)
                    
                    if len(unique_ids) > 1:
                        consistency_analysis['player_id_mismatches'] += 1
                        consistency_analysis['consistency_violations'] += 1
                        
                        if len(consistency_analysis['data_corruption_indicators']) < 5:
                            consistency_analysis['data_corruption_indicators'].append({
                                'type': 'player_id_mismatch',
                                'player_name': record.player_name,
                                'player_ids': list(unique_ids)
                            })
                    
                    # Check for null key issues where we expect joins to work
                    if record.player_id is None:
                        consistency_analysis['null_key_issues'] += 1
                        consistency_analysis['consistency_violations'] += 1
                
                consistency_analysis['consistency_rate'] = (
                    consistency_analysis['total_records_checked'] - consistency_analysis['consistency_violations']
                ) / consistency_analysis['total_records_checked']
            
            # Additional consistency check: verify player names match across tables
            name_consistency_query = self.db.query(
                Player.name,
                PlayerUsage.player_id,
                func.count(distinct(Player.name)).label('name_variants')
            ).join(PlayerUsage).filter(
                PlayerUsage.season == self.current_season
            ).group_by(PlayerUsage.player_id).having(
                func.count(distinct(Player.name)) > 1
            ).limit(10).all()
            
            name_inconsistencies = len(name_consistency_query)
            
            success = (
                consistency_analysis['consistency_rate'] >= 0.95 and  # 95% consistency
                consistency_analysis['player_id_mismatches'] <= 5 and  # Few ID mismatches
                name_inconsistencies <= 2 and  # Few name inconsistencies
                consistency_analysis['null_key_issues'] == 0  # No null key issues
            )
            
            print(f"   ✓ Records checked: {consistency_analysis['total_records_checked']}")
            print(f"   ✓ Consistency rate: {consistency_analysis['consistency_rate']:.1%}")
            print(f"   ✓ Player ID mismatches: {consistency_analysis['player_id_mismatches']}")
            print(f"   ✓ Null key issues: {consistency_analysis['null_key_issues']}")
            print(f"   ✓ Name inconsistencies: {name_inconsistencies}")
            
            # Show sample corruption indicators
            for indicator in consistency_analysis['data_corruption_indicators'][:3]:
                print(f"   ⚠️  {indicator['type']}: {indicator['player_name']} has IDs {indicator['player_ids']}")
            
            return {
                'success': success,
                'total_records_checked': consistency_analysis['total_records_checked'],
                'consistency_rate': consistency_analysis['consistency_rate'],
                'player_id_mismatches': consistency_analysis['player_id_mismatches'],
                'null_key_issues': consistency_analysis['null_key_issues'],
                'name_inconsistencies': name_inconsistencies,
                'sample_corruption_indicators': consistency_analysis['data_corruption_indicators'][:3]
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Join data consistency validation failed: {e}'
            }
    
    def validate_complex_query_capability(self) -> Dict[str, Any]:
        """
        Validate capability to execute complex queries with joins
        
        Success Criteria:
        - Complex analytical queries execute successfully
        - Subqueries with joins work properly
        - Advanced SQL features are supported
        """
        try:
            print("   Testing complex query capability...")
            
            complex_queries = [
                {
                    'name': 'correlated_subquery',
                    'description': 'Players with above-average usage',
                    'query': lambda: self.db.query(
                        Player.name,
                        PlayerUsage.snap_pct
                    ).join(PlayerUsage).filter(
                        PlayerUsage.season == self.current_season,
                        PlayerUsage.snap_pct > self.db.query(
                            func.avg(PlayerUsage.snap_pct)
                        ).filter(
                            PlayerUsage.season == self.current_season
                        ).scalar_subquery()
                    ).limit(20).all()
                },
                {
                    'name': 'window_function',
                    'description': 'Player rankings within position',
                    'query': lambda: self.db.query(
                        Player.name,
                        Player.position,
                        PlayerProjections.mean,
                        func.row_number().over(
                            partition_by=Player.position,
                            order_by=PlayerProjections.mean.desc()
                        ).label('position_rank')
                    ).join(PlayerProjections).filter(
                        PlayerProjections.season == self.current_season,
                        PlayerProjections.week == self.test_week
                    ).limit(30).all()
                },
                {
                    'name': 'multiple_aggregations',
                    'description': 'Team-level statistics',
                    'query': lambda: self.db.query(
                        Player.team,
                        func.count(Player.id).label('player_count'),
                        func.avg(PlayerUsage.snap_pct).label('avg_snap_pct'),
                        func.max(PlayerProjections.mean).label('max_projection'),
                        func.count(RosterEntry.id).label('rostered_count')
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
                    )).group_by(Player.team).having(
                        func.count(Player.id) >= 1
                    ).limit(32).all()
                }
            ]
            
            complex_query_results = {
                'queries_tested': len(complex_queries),
                'successful_queries': 0,
                'query_results': {},
                'query_errors': []
            }
            
            for query_test in complex_queries:
                try:
                    start_time = time.time()
                    results = query_test['query']()
                    execution_time = time.time() - start_time
                    
                    complex_query_results['successful_queries'] += 1
                    complex_query_results['query_results'][query_test['name']] = {
                        'success': True,
                        'execution_time': execution_time,
                        'result_count': len(results),
                        'description': query_test['description']
                    }
                    
                except Exception as e:
                    complex_query_results['query_errors'].append({
                        'query': query_test['name'],
                        'error': str(e)
                    })
                    complex_query_results['query_results'][query_test['name']] = {
                        'success': False,
                        'error': str(e),
                        'description': query_test['description']
                    }
            
            # Calculate success metrics
            success_rate = complex_query_results['successful_queries'] / complex_query_results['queries_tested']
            
            execution_times = [
                result['execution_time']
                for result in complex_query_results['query_results'].values()
                if 'execution_time' in result
            ]
            
            avg_execution_time = sum(execution_times) / len(execution_times) if execution_times else 0
            
            success = (
                success_rate >= 0.75 and  # At least 75% of complex queries successful
                complex_query_results['successful_queries'] >= 2 and  # At least 2 successful
                avg_execution_time < 5.0  # Average execution < 5 seconds
            )
            
            print(f"   ✓ Complex queries tested: {complex_query_results['queries_tested']}")
            print(f"   ✓ Successful queries: {complex_query_results['successful_queries']}")
            print(f"   ✓ Success rate: {success_rate:.1%}")
            print(f"   ✓ Average execution time: {avg_execution_time:.3f}s")
            
            # Show individual query results
            for query_name, result in complex_query_results['query_results'].items():
                if result['success']:
                    print(f"   ✓ {query_name}: {result['execution_time']:.3f}s ({result['result_count']} results)")
                else:
                    print(f"   ❌ {query_name}: {result.get('error', 'Unknown error')}")
            
            return {
                'success': success,
                'queries_tested': complex_query_results['queries_tested'],
                'successful_queries': complex_query_results['successful_queries'],
                'success_rate': success_rate,
                'avg_execution_time': avg_execution_time,
                'query_results': complex_query_results['query_results'],
                'sample_errors': complex_query_results['query_errors'][:2]
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Complex query capability validation failed: {e}'
            }
    
    def _print_validation_summary(self, results: Dict[str, Any]):
        """Print comprehensive validation summary"""
        print("\n" + "=" * 80)
        print("TABLE JOIN CAPABILITY VALIDATION SUMMARY")
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
        
        # Key metrics summary
        print(f"\n🎯 Key Table Join Metrics:")
        
        if 'basic_join_capability' in results.get('validations', {}):
            basic_test = results['validations']['basic_join_capability']
            print(f"  • Basic Join Success Rate: {basic_test.get('join_success_rate', 0):.1%}")
            print(f"  • Successful Joins: {basic_test.get('successful_joins', 0)}/{basic_test.get('total_join_tests', 0)}")
        
        if 'foreign_key_relationships' in results.get('validations', {}):
            fk_test = results['validations']['foreign_key_relationships']
            print(f"  • Orphaned Records Rate: {fk_test.get('overall_orphaned_rate', 0):.2%}")
        
        if 'join_performance' in results.get('validations', {}):
            perf_test = results['validations']['join_performance']
            print(f"  • Average Join Time: {perf_test.get('avg_execution_time', 0):.3f}s")
            print(f"  • Fast Query Rate: {perf_test.get('fast_query_rate', 0):.1%}")
        
        if 'waiver_candidates_materialization' in results.get('validations', {}):
            mat_test = results['validations']['waiver_candidates_materialization']
            print(f"  • Materialization Success: {'✅' if mat_test.get('query_successful', False) else '❌'}")
            print(f"  • Waiver Candidates Count: {mat_test.get('result_count', 0)}")
        
        if 'join_data_consistency' in results.get('validations', {}):
            cons_test = results['validations']['join_data_consistency']
            print(f"  • Data Consistency Rate: {cons_test.get('consistency_rate', 0):.1%}")
        
        print(f"\n{'🎉 Table Join Capability is FULLY VALIDATED!' if overall_success else '⚠️  Table Join Capability needs attention'}")
        print("=" * 80)

def main():
    """Run table join capability validation tests"""
    print("Starting Table Join Capability Validation...")
    
    try:
        validator = TableJoinCapabilityValidator()
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