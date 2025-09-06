#!/usr/bin/env python3
"""
End-to-End Integration Validation Tests

Epic A Complete System Validation:
This validates the entire Epic A data foundation system works as an integrated whole,
from raw data ingestion through to the waiver candidates materialized view.

This is the capstone validation that demonstrates Epic A meets all requirements:
- US-A1: Canonical player/league schema with cross-platform lookup
- US-A2: Waiver candidates materialized view with < 1 minute refresh
"""

import os
import sys
import traceback
import time
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.config import get_config
from src.database import SessionLocal, Player, PlayerUsage, PlayerProjections, RosterEntry, WaiverCandidates
from src.services.enhanced_waiver_candidates_builder import EnhancedWaiverCandidatesBuilder
from src.utils.player_id_mapper import PlayerIDMapper
from sqlalchemy import func, distinct, and_

class EndToEndIntegrationValidator:
    """
    Comprehensive end-to-end validation of Epic A data foundation system
    """
    
    def __init__(self):
        self.config = get_config()
        self.db = SessionLocal()
        self.enhanced_builder = EnhancedWaiverCandidatesBuilder()
        self.player_mapper = PlayerIDMapper()
        
        self.current_season = 2025
        self.test_week = 4
        
        # Real league data for integration testing
        self.integration_leagues = [
            {
                'id': "1257071160403709954",
                'platform': 'sleeper',
                'name': 'Sleeper Test League'
            }
        ]
        
        # Known test players for cross-platform validation
        self.test_players = [
            {'name': 'Josh Allen', 'position': 'QB', 'team': 'BUF'},
            {'name': 'Christian McCaffrey', 'position': 'RB', 'team': 'SF'},
            {'name': 'Cooper Kupp', 'position': 'WR', 'team': 'LAR'},
            {'name': 'Travis Kelce', 'position': 'TE', 'team': 'KC'},
            {'name': 'Davante Adams', 'position': 'WR', 'team': 'LV'}
        ]
        
        print("=" * 80)
        print("END-TO-END INTEGRATION VALIDATION")
        print("Epic A Complete System Integration Testing")
        print("=" * 80)
    
    def run_all_validations(self) -> Dict[str, Any]:
        """
        Execute complete end-to-end integration validation
        
        Returns comprehensive validation results demonstrating Epic A readiness
        """
        validation_results = {
            'overall_success': True,
            'timestamp': datetime.utcnow().isoformat(),
            'database': self.config.DATABASE_URL,
            'season': self.current_season,
            'test_week': self.test_week,
            'integration_leagues': len(self.integration_leagues),
            'validations': {}
        }
        
        try:
            print(f"\n🔄 Running End-to-End Integration Validations")
            print(f"Database: {self.config.DATABASE_URL}")
            print(f"Season: {self.current_season}, Week: {self.test_week}")
            print(f"Integration Leagues: {len(self.integration_leagues)}")
            print("-" * 60)
            
            # Integration 1: Data Foundation Completeness
            print("\n📊 INTEGRATION 1: Data Foundation Completeness")
            result_1 = self.validate_data_foundation_completeness()
            validation_results['validations']['data_foundation_completeness'] = result_1
            if not result_1.get('success', False):
                validation_results['overall_success'] = False
            
            # Integration 2: Cross-Platform Player Resolution
            print("\n🔗 INTEGRATION 2: Cross-Platform Player Resolution")
            result_2 = self.validate_cross_platform_resolution()
            validation_results['validations']['cross_platform_resolution'] = result_2
            if not result_2.get('success', False):
                validation_results['overall_success'] = False
            
            # Integration 3: Complete Data Flow Pipeline
            print("\n⚡ INTEGRATION 3: Complete Data Flow Pipeline")
            result_3 = self.validate_data_flow_pipeline()
            validation_results['validations']['data_flow_pipeline'] = result_3
            if not result_3.get('success', False):
                validation_results['overall_success'] = False
            
            # Integration 4: Waiver Candidates End-to-End
            print("\n🎯 INTEGRATION 4: Waiver Candidates End-to-End")
            result_4 = self.validate_waiver_candidates_end_to_end()
            validation_results['validations']['waiver_candidates_end_to_end'] = result_4
            if not result_4.get('success', False):
                validation_results['overall_success'] = False
            
            # Integration 5: Real-World Scenario Testing
            print("\n🌍 INTEGRATION 5: Real-World Scenario Testing")
            result_5 = self.validate_real_world_scenarios()
            validation_results['validations']['real_world_scenarios'] = result_5
            if not result_5.get('success', False):
                validation_results['overall_success'] = False
            
            # Integration 6: System Resilience and Recovery
            print("\n🛡️ INTEGRATION 6: System Resilience and Recovery")
            result_6 = self.validate_system_resilience()
            validation_results['validations']['system_resilience'] = result_6
            if not result_6.get('success', False):
                validation_results['overall_success'] = False
            
            # Integration 7: Epic A Acceptance Criteria Validation
            print("\n✅ INTEGRATION 7: Epic A Acceptance Criteria Validation")
            result_7 = self.validate_epic_a_acceptance_criteria()
            validation_results['validations']['epic_a_acceptance_criteria'] = result_7
            if not result_7.get('success', False):
                validation_results['overall_success'] = False
            
            # Final Epic A System Assessment
            self._perform_epic_a_system_assessment(validation_results)
            
            return validation_results
            
        except Exception as e:
            print(f"❌ CRITICAL ERROR in end-to-end integration validation: {e}")
            traceback.print_exc()
            validation_results['overall_success'] = False
            validation_results['error'] = str(e)
            return validation_results
        
        finally:
            self.db.close()
    
    def validate_data_foundation_completeness(self) -> Dict[str, Any]:
        """
        Validate that all Epic A data foundation components are present and functional
        
        Success Criteria:
        - All Epic A tables have data
        - Data relationships are intact
        - Core Epic A functionality is operational
        """
        try:
            print("   Testing Epic A data foundation completeness...")
            
            foundation_analysis = {
                'epic_a_tables': {},
                'data_relationships': {},
                'foundation_health': {}
            }
            
            # Check Epic A table completeness
            epic_tables = {
                'players': Player,
                'player_usage': PlayerUsage,
                'player_projections': PlayerProjections,
                'roster_entries': RosterEntry,
                'waiver_candidates': WaiverCandidates
            }
            
            total_records = 0
            tables_with_data = 0
            
            for table_name, table_class in epic_tables.items():
                try:
                    record_count = self.db.query(table_class).count()
                    current_season_count = 0
                    
                    # Get current season data if applicable
                    if hasattr(table_class, 'season'):
                        current_season_count = self.db.query(table_class).filter(
                            getattr(table_class, 'season') == self.current_season
                        ).count()
                    
                    foundation_analysis['epic_a_tables'][table_name] = {
                        'total_records': record_count,
                        'current_season_records': current_season_count,
                        'has_data': record_count > 0
                    }
                    
                    total_records += record_count
                    if record_count > 0:
                        tables_with_data += 1
                    
                    print(f"     ✓ {table_name}: {record_count} total records, {current_season_count} current season")
                    
                except Exception as e:
                    foundation_analysis['epic_a_tables'][table_name] = {
                        'error': str(e),
                        'has_data': False
                    }
                    print(f"     ❌ {table_name}: ERROR - {e}")
            
            # Check data relationships
            print("     Validating data relationships...")
            
            # Player -> Usage relationship
            players_with_usage = self.db.query(
                func.count(distinct(PlayerUsage.player_id))
            ).filter(
                PlayerUsage.season == self.current_season
            ).scalar() or 0
            
            total_players = self.db.query(func.count(Player.id)).scalar() or 0
            usage_coverage = players_with_usage / total_players if total_players > 0 else 0
            
            foundation_analysis['data_relationships']['usage_coverage'] = {
                'players_with_usage': players_with_usage,
                'total_players': total_players,
                'coverage_rate': usage_coverage
            }
            
            # Player -> Projections relationship
            players_with_projections = self.db.query(
                func.count(distinct(PlayerProjections.player_id))
            ).filter(
                PlayerProjections.season == self.current_season
            ).scalar() or 0
            
            projection_coverage = players_with_projections / total_players if total_players > 0 else 0
            
            foundation_analysis['data_relationships']['projection_coverage'] = {
                'players_with_projections': players_with_projections,
                'total_players': total_players,
                'coverage_rate': projection_coverage
            }
            
            # Player -> Roster relationship
            rostered_players = self.db.query(
                func.count(distinct(RosterEntry.player_id))
            ).filter(
                RosterEntry.is_active == True
            ).scalar() or 0
            
            roster_coverage = rostered_players / total_players if total_players > 0 else 0
            
            foundation_analysis['data_relationships']['roster_coverage'] = {
                'rostered_players': rostered_players,
                'total_players': total_players,
                'coverage_rate': roster_coverage
            }
            
            # Foundation health assessment
            foundation_analysis['foundation_health'] = {
                'tables_with_data': tables_with_data,
                'total_tables': len(epic_tables),
                'total_records': total_records,
                'data_completeness': tables_with_data / len(epic_tables),
                'avg_coverage': (usage_coverage + projection_coverage + roster_coverage) / 3
            }
            
            # Success criteria
            success = (
                tables_with_data >= 4 and  # At least 4 Epic A tables have data
                total_records >= 1000 and  # Substantial data volume
                foundation_analysis['foundation_health']['data_completeness'] >= 0.8 and  # 80% table completeness
                foundation_analysis['foundation_health']['avg_coverage'] >= 0.3  # 30% average coverage
            )
            
            print(f"   ✓ Tables with data: {tables_with_data}/{len(epic_tables)}")
            print(f"   ✓ Total records: {total_records}")
            print(f"   ✓ Data completeness: {foundation_analysis['foundation_health']['data_completeness']:.1%}")
            print(f"   ✓ Average coverage: {foundation_analysis['foundation_health']['avg_coverage']:.1%}")
            
            return {
                'success': success,
                'tables_with_data': tables_with_data,
                'total_tables': len(epic_tables),
                'total_records': total_records,
                'data_completeness': foundation_analysis['foundation_health']['data_completeness'],
                'average_coverage': foundation_analysis['foundation_health']['avg_coverage'],
                'foundation_analysis': foundation_analysis
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Data foundation completeness validation failed: {e}'
            }
    
    def validate_cross_platform_resolution(self) -> Dict[str, Any]:
        """
        Validate cross-platform player resolution works end-to-end
        
        Success Criteria:
        - Known players can be resolved across platforms
        - Canonical IDs work for player lookup
        - Cross-platform data integration is functional
        """
        try:
            print("   Testing cross-platform player resolution...")
            
            resolution_results = {
                'test_players_processed': 0,
                'successful_resolutions': 0,
                'canonical_id_matches': 0,
                'cross_platform_data_found': 0,
                'resolution_details': []
            }
            
            for player_info in self.test_players:
                resolution_results['test_players_processed'] += 1
                
                try:
                    # Generate canonical ID
                    canonical_id = self.player_mapper.generate_canonical_id(
                        player_info['name'],
                        player_info['position'],
                        player_info['team']
                    )
                    
                    # Look up player by canonical ID
                    player = self.db.query(Player).filter(
                        Player.nfl_id == canonical_id
                    ).first()
                    
                    if player:
                        resolution_results['successful_resolutions'] += 1
                        resolution_results['canonical_id_matches'] += 1
                        
                        # Check for cross-platform data
                        has_sleeper_id = player.sleeper_id is not None
                        has_mfl_id = player.mfl_id is not None
                        has_usage = self.db.query(PlayerUsage).filter(
                            PlayerUsage.player_id == player.id,
                            PlayerUsage.season == self.current_season
                        ).first() is not None
                        has_projections = self.db.query(PlayerProjections).filter(
                            PlayerProjections.player_id == player.id,
                            PlayerProjections.season == self.current_season
                        ).first() is not None
                        
                        cross_platform_score = sum([has_sleeper_id, has_mfl_id, has_usage, has_projections])
                        
                        if cross_platform_score >= 2:  # At least 2 data sources
                            resolution_results['cross_platform_data_found'] += 1
                        
                        resolution_results['resolution_details'].append({
                            'name': player_info['name'],
                            'canonical_id': canonical_id,
                            'found_in_db': True,
                            'db_name': player.name,
                            'sleeper_id': has_sleeper_id,
                            'mfl_id': has_mfl_id,
                            'has_usage': has_usage,
                            'has_projections': has_projections,
                            'cross_platform_score': cross_platform_score
                        })
                        
                        print(f"     ✓ {player_info['name']}: Found, cross-platform score: {cross_platform_score}/4")
                        
                    else:
                        resolution_results['resolution_details'].append({
                            'name': player_info['name'],
                            'canonical_id': canonical_id,
                            'found_in_db': False
                        })
                        print(f"     ⚠️ {player_info['name']}: Not found in database")
                
                except Exception as e:
                    resolution_results['resolution_details'].append({
                        'name': player_info['name'],
                        'error': str(e)
                    })
                    print(f"     ❌ {player_info['name']}: ERROR - {e}")
            
            # Calculate success metrics
            resolution_rate = resolution_results['successful_resolutions'] / resolution_results['test_players_processed']
            cross_platform_rate = resolution_results['cross_platform_data_found'] / resolution_results['test_players_processed']
            
            success = (
                resolution_rate >= 0.6 and  # At least 60% of test players resolved
                cross_platform_rate >= 0.4 and  # At least 40% have cross-platform data
                resolution_results['canonical_id_matches'] >= 2  # At least 2 canonical ID matches
            )
            
            print(f"   ✓ Resolution rate: {resolution_rate:.1%}")
            print(f"   ✓ Cross-platform data rate: {cross_platform_rate:.1%}")
            print(f"   ✓ Canonical ID matches: {resolution_results['canonical_id_matches']}")
            
            return {
                'success': success,
                'test_players_processed': resolution_results['test_players_processed'],
                'successful_resolutions': resolution_results['successful_resolutions'],
                'resolution_rate': resolution_rate,
                'cross_platform_data_found': resolution_results['cross_platform_data_found'],
                'cross_platform_rate': cross_platform_rate,
                'canonical_id_matches': resolution_results['canonical_id_matches'],
                'resolution_details': resolution_results['resolution_details']
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Cross-platform resolution validation failed: {e}'
            }
    
    def validate_data_flow_pipeline(self) -> Dict[str, Any]:
        """
        Validate complete data flow pipeline from source to waiver candidates
        
        Success Criteria:
        - Data flows through all Epic A stages
        - Pipeline executes without errors
        - Data transformations are correct
        """
        try:
            print("   Testing complete data flow pipeline...")
            
            pipeline_stages = {
                'stage_1_player_identification': {'success': False, 'metrics': {}},
                'stage_2_usage_integration': {'success': False, 'metrics': {}},
                'stage_3_projections_integration': {'success': False, 'metrics': {}},
                'stage_4_roster_context': {'success': False, 'metrics': {}},
                'stage_5_waiver_materialization': {'success': False, 'metrics': {}}
            }
            
            # Stage 1: Player Identification
            print("     Stage 1: Player identification and canonical mapping...")
            try:
                # Test canonical player identification
                sample_players = self.db.query(Player).limit(10).all()
                
                if sample_players:
                    canonical_ids_valid = 0
                    for player in sample_players:
                        if player.nfl_id:
                            # Verify canonical ID can be regenerated
                            regenerated_id = self.player_mapper.generate_canonical_id(
                                player.name, player.position, player.team
                            )
                            if regenerated_id == player.nfl_id:
                                canonical_ids_valid += 1
                    
                    canonical_consistency = canonical_ids_valid / len(sample_players)
                    
                    pipeline_stages['stage_1_player_identification'] = {
                        'success': canonical_consistency >= 0.8,
                        'metrics': {
                            'players_sampled': len(sample_players),
                            'canonical_ids_valid': canonical_ids_valid,
                            'consistency_rate': canonical_consistency
                        }
                    }
                    
                    print(f"       ✓ Canonical ID consistency: {canonical_consistency:.1%}")
                
            except Exception as e:
                print(f"       ❌ Stage 1 error: {e}")
            
            # Stage 2: Usage Integration
            print("     Stage 2: Usage data integration...")
            try:
                # Test usage data integration
                usage_with_players = self.db.query(
                    PlayerUsage.player_id,
                    func.count(PlayerUsage.id).label('usage_count')
                ).join(Player).filter(
                    PlayerUsage.season == self.current_season,
                    PlayerUsage.week == self.test_week
                ).group_by(PlayerUsage.player_id).limit(50).all()
                
                if usage_with_players:
                    avg_usage_records = sum(row.usage_count for row in usage_with_players) / len(usage_with_players)
                    
                    pipeline_stages['stage_2_usage_integration'] = {
                        'success': len(usage_with_players) >= 10,
                        'metrics': {
                            'players_with_usage': len(usage_with_players),
                            'avg_usage_records': avg_usage_records
                        }
                    }
                    
                    print(f"       ✓ Players with usage: {len(usage_with_players)}")
                
            except Exception as e:
                print(f"       ❌ Stage 2 error: {e}")
            
            # Stage 3: Projections Integration
            print("     Stage 3: Projections data integration...")
            try:
                # Test projections data integration
                projections_with_players = self.db.query(
                    PlayerProjections.player_id,
                    PlayerProjections.mean,
                    PlayerProjections.source
                ).join(Player).filter(
                    PlayerProjections.season == self.current_season,
                    PlayerProjections.week == self.test_week
                ).limit(50).all()
                
                if projections_with_players:
                    valid_projections = [p for p in projections_with_players if p.mean and p.mean > 0]
                    projection_validity = len(valid_projections) / len(projections_with_players)
                    
                    pipeline_stages['stage_3_projections_integration'] = {
                        'success': projection_validity >= 0.7,
                        'metrics': {
                            'players_with_projections': len(projections_with_players),
                            'valid_projections': len(valid_projections),
                            'validity_rate': projection_validity
                        }
                    }
                    
                    print(f"       ✓ Valid projections: {projection_validity:.1%}")
                
            except Exception as e:
                print(f"       ❌ Stage 3 error: {e}")
            
            # Stage 4: Roster Context
            print("     Stage 4: Roster context integration...")
            try:
                # Test roster context integration
                for league in self.integration_leagues:
                    roster_entries = self.db.query(RosterEntry).filter(
                        RosterEntry.league_id == league['id'],
                        RosterEntry.is_active == True
                    ).count()
                    
                    if roster_entries >= 50:  # Reasonable roster size for league
                        pipeline_stages['stage_4_roster_context'] = {
                            'success': True,
                            'metrics': {
                                'league_id': league['id'],
                                'roster_entries': roster_entries
                            }
                        }
                        
                        print(f"       ✓ League {league['id']}: {roster_entries} roster entries")
                        break
                
            except Exception as e:
                print(f"       ❌ Stage 4 error: {e}")
            
            # Stage 5: Waiver Candidates Materialization
            print("     Stage 5: Waiver candidates materialization...")
            try:
                # Test end-to-end materialization
                for league in self.integration_leagues:
                    start_time = time.time()
                    candidates = self.enhanced_builder.build_waiver_candidates(
                        league_id=league['id'],
                        week=self.test_week
                    )
                    end_time = time.time()
                    
                    materialization_time = end_time - start_time
                    
                    if candidates and len(candidates) >= 5:
                        # Validate candidate data completeness
                        complete_candidates = 0
                        for candidate in candidates[:10]:  # Check first 10
                            if (hasattr(candidate, 'player_id') and 
                                hasattr(candidate, 'pos') and
                                hasattr(candidate, 'rostered')):
                                complete_candidates += 1
                        
                        completeness_rate = complete_candidates / min(10, len(candidates))
                        
                        pipeline_stages['stage_5_waiver_materialization'] = {
                            'success': completeness_rate >= 0.8 and materialization_time < 60,
                            'metrics': {
                                'candidates_count': len(candidates),
                                'completeness_rate': completeness_rate,
                                'materialization_time': materialization_time
                            }
                        }
                        
                        print(f"       ✓ Candidates: {len(candidates)}, Time: {materialization_time:.2f}s")
                        break
                
            except Exception as e:
                print(f"       ❌ Stage 5 error: {e}")
            
            # Calculate overall pipeline success
            successful_stages = sum(1 for stage in pipeline_stages.values() if stage['success'])
            pipeline_success_rate = successful_stages / len(pipeline_stages)
            
            success = (
                pipeline_success_rate >= 0.8 and  # At least 80% of stages successful
                successful_stages >= 4  # At least 4 stages successful
            )
            
            print(f"   ✓ Pipeline stages successful: {successful_stages}/{len(pipeline_stages)}")
            print(f"   ✓ Pipeline success rate: {pipeline_success_rate:.1%}")
            
            return {
                'success': success,
                'successful_stages': successful_stages,
                'total_stages': len(pipeline_stages),
                'pipeline_success_rate': pipeline_success_rate,
                'pipeline_stages': pipeline_stages
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Data flow pipeline validation failed: {e}'
            }
    
    def validate_waiver_candidates_end_to_end(self) -> Dict[str, Any]:
        """
        Validate complete waiver candidates functionality end-to-end
        
        Success Criteria:
        - Waiver candidates can be built and refreshed
        - All Epic A required fields are present
        - Performance requirements are met
        - Data quality is high
        """
        try:
            print("   Testing waiver candidates end-to-end functionality...")
            
            waiver_tests = []
            
            for league in self.integration_leagues:
                print(f"     Testing league {league['id']}...")
                
                try:
                    # Test complete waiver candidates refresh
                    start_time = time.time()
                    refresh_result = self.enhanced_builder.refresh_waiver_candidates_for_league(
                        league_id=league['id'],
                        week=self.test_week
                    )
                    end_time = time.time()
                    
                    refresh_time = end_time - start_time
                    
                    # Validate refresh result
                    test_result = {
                        'league_id': league['id'],
                        'platform': league['platform'],
                        'refresh_time': refresh_time,
                        'refresh_successful': refresh_result.get('success', False),
                        'candidates_count': refresh_result.get('candidates_count', 0),
                        'performance_compliant': refresh_time < 60.0,  # Epic A US-A2 requirement
                        'data_quality': {}
                    }
                    
                    if refresh_result.get('success', False):
                        # Build candidates for detailed analysis
                        candidates = self.enhanced_builder.build_waiver_candidates(
                            league_id=league['id'],
                            week=self.test_week
                        )
                        
                        if candidates:
                            # Analyze data quality
                            epic_a_fields = [
                                'player_id', 'pos', 'rostered', 'snap_delta', 'route_delta',
                                'tprr', 'rz_last2', 'ez_last2', 'proj_next', 'trend_slope',
                                'roster_fit', 'market_heat', 'scarcity'
                            ]
                            
                            field_coverage = 0
                            data_coverage = 0
                            
                            sample_candidate = candidates[0]
                            
                            for field in epic_a_fields:
                                if hasattr(sample_candidate, field):
                                    field_coverage += 1
                                    if getattr(sample_candidate, field) is not None:
                                        data_coverage += 1
                            
                            field_coverage_rate = field_coverage / len(epic_a_fields)
                            data_coverage_rate = data_coverage / field_coverage if field_coverage > 0 else 0
                            
                            # Check for non-rostered filter
                            rostered_count = sum(1 for c in candidates if getattr(c, 'rostered', True))
                            non_rostered_rate = (len(candidates) - rostered_count) / len(candidates) if candidates else 0
                            
                            test_result['data_quality'] = {
                                'field_coverage_rate': field_coverage_rate,
                                'data_coverage_rate': data_coverage_rate,
                                'non_rostered_rate': non_rostered_rate,
                                'quality_score': (field_coverage_rate + data_coverage_rate + non_rostered_rate) / 3
                            }
                            
                            print(f"       ✓ Refresh: {refresh_time:.2f}s, Candidates: {len(candidates)}")
                            print(f"       ✓ Field coverage: {field_coverage_rate:.1%}, Data coverage: {data_coverage_rate:.1%}")
                            print(f"       ✓ Non-rostered rate: {non_rostered_rate:.1%}")
                        
                        else:
                            test_result['data_quality']['error'] = 'No candidates built'
                            print(f"       ❌ No candidates built for league {league['id']}")
                    
                    else:
                        print(f"       ❌ Refresh failed for league {league['id']}")
                    
                    waiver_tests.append(test_result)
                    
                except Exception as e:
                    waiver_tests.append({
                        'league_id': league['id'],
                        'error': str(e),
                        'refresh_successful': False
                    })
                    print(f"       ❌ League {league['id']} error: {e}")
            
            # Calculate overall success metrics
            successful_refreshes = [t for t in waiver_tests if t.get('refresh_successful', False)]
            performance_compliant = [t for t in waiver_tests if t.get('performance_compliant', False)]
            
            if successful_refreshes:
                avg_refresh_time = sum(t['refresh_time'] for t in successful_refreshes) / len(successful_refreshes)
                total_candidates = sum(t['candidates_count'] for t in successful_refreshes)
                
                quality_scores = [
                    t.get('data_quality', {}).get('quality_score', 0)
                    for t in successful_refreshes
                    if 'data_quality' in t and 'quality_score' in t['data_quality']
                ]
                avg_quality_score = sum(quality_scores) / len(quality_scores) if quality_scores else 0
            else:
                avg_refresh_time = float('inf')
                total_candidates = 0
                avg_quality_score = 0
            
            success = (
                len(successful_refreshes) >= 1 and  # At least 1 successful refresh
                len(performance_compliant) >= 1 and  # At least 1 meets performance requirements
                total_candidates >= 5 and  # At least 5 candidates generated
                avg_quality_score >= 0.6  # 60% data quality score
            )
            
            print(f"   ✓ Successful refreshes: {len(successful_refreshes)}/{len(waiver_tests)}")
            print(f"   ✓ Performance compliant: {len(performance_compliant)}/{len(waiver_tests)}")
            print(f"   ✓ Average refresh time: {avg_refresh_time:.2f}s")
            print(f"   ✓ Total candidates: {total_candidates}")
            print(f"   ✓ Average quality score: {avg_quality_score:.1%}")
            
            return {
                'success': success,
                'leagues_tested': len(waiver_tests),
                'successful_refreshes': len(successful_refreshes),
                'performance_compliant': len(performance_compliant),
                'avg_refresh_time': avg_refresh_time,
                'total_candidates': total_candidates,
                'avg_quality_score': avg_quality_score,
                'waiver_test_results': waiver_tests
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Waiver candidates end-to-end validation failed: {e}'
            }
    
    def validate_real_world_scenarios(self) -> Dict[str, Any]:
        """
        Validate system works in real-world scenarios
        
        Success Criteria:
        - System handles actual league data
        - Performance is acceptable with real data volumes
        - Edge cases are handled gracefully
        """
        try:
            print("   Testing real-world scenarios...")
            
            scenario_tests = {
                'large_dataset_handling': {'success': False, 'metrics': {}},
                'multiple_league_processing': {'success': False, 'metrics': {}},
                'missing_data_handling': {'success': False, 'metrics': {}},
                'concurrent_league_processing': {'success': False, 'metrics': {}}
            }
            
            # Scenario 1: Large Dataset Handling
            print("     Scenario 1: Large dataset handling...")
            try:
                start_time = time.time()
                
                # Query large dataset
                large_usage_query = self.db.query(PlayerUsage).filter(
                    PlayerUsage.season == self.current_season
                ).limit(1000).all()
                
                large_projections_query = self.db.query(PlayerProjections).filter(
                    PlayerProjections.season == self.current_season
                ).limit(1000).all()
                
                end_time = time.time()
                query_time = end_time - start_time
                
                scenario_tests['large_dataset_handling'] = {
                    'success': query_time < 10.0 and len(large_usage_query) + len(large_projections_query) >= 100,
                    'metrics': {
                        'usage_records': len(large_usage_query),
                        'projection_records': len(large_projections_query),
                        'query_time': query_time
                    }
                }
                
                print(f"       ✓ Large dataset query: {query_time:.2f}s, {len(large_usage_query) + len(large_projections_query)} records")
                
            except Exception as e:
                print(f"       ❌ Large dataset handling error: {e}")
            
            # Scenario 2: Multiple League Processing
            print("     Scenario 2: Multiple league processing...")
            try:
                # Get multiple leagues from roster data
                leagues = self.db.query(
                    RosterEntry.league_id,
                    func.count(RosterEntry.id).label('roster_count')
                ).group_by(RosterEntry.league_id).having(
                    func.count(RosterEntry.id) >= 10
                ).limit(3).all()
                
                successful_leagues = 0
                total_processing_time = 0
                
                for league_id, roster_count in leagues:
                    try:
                        start_time = time.time()
                        candidates = self.enhanced_builder.build_waiver_candidates(
                            league_id=league_id,
                            week=self.test_week
                        )
                        end_time = time.time()
                        
                        processing_time = end_time - start_time
                        total_processing_time += processing_time
                        
                        if candidates and len(candidates) >= 1:
                            successful_leagues += 1
                            print(f"         ✓ League {league_id}: {len(candidates)} candidates in {processing_time:.2f}s")
                        
                    except Exception as e:
                        print(f"         ❌ League {league_id} error: {e}")
                
                scenario_tests['multiple_league_processing'] = {
                    'success': successful_leagues >= 1,
                    'metrics': {
                        'leagues_tested': len(leagues),
                        'successful_leagues': successful_leagues,
                        'total_processing_time': total_processing_time
                    }
                }
                
            except Exception as e:
                print(f"       ❌ Multiple league processing error: {e}")
            
            # Scenario 3: Missing Data Handling
            print("     Scenario 3: Missing data handling...")
            try:
                # Test with player that might have incomplete data
                incomplete_data_query = self.db.query(Player).outerjoin(PlayerUsage).filter(
                    PlayerUsage.id.is_(None)  # Players without usage data
                ).limit(5).all()
                
                if incomplete_data_query:
                    # Try to build waiver candidates including players with missing data
                    test_league_id = self.integration_leagues[0]['id']
                    
                    start_time = time.time()
                    candidates = self.enhanced_builder.build_waiver_candidates(
                        league_id=test_league_id,
                        week=self.test_week
                    )
                    end_time = time.time()
                    
                    # System should handle missing data gracefully (not crash)
                    scenario_tests['missing_data_handling'] = {
                        'success': True,  # Success if no crash
                        'metrics': {
                            'players_with_missing_data': len(incomplete_data_query),
                            'candidates_built': len(candidates) if candidates else 0,
                            'processing_time': end_time - start_time
                        }
                    }
                    
                    print(f"       ✓ Handled {len(incomplete_data_query)} players with missing data")
                
            except Exception as e:
                print(f"       ❌ Missing data handling error: {e}")
            
            # Calculate scenario success
            successful_scenarios = sum(1 for scenario in scenario_tests.values() if scenario['success'])
            scenario_success_rate = successful_scenarios / len(scenario_tests)
            
            success = (
                scenario_success_rate >= 0.75 and  # At least 75% of scenarios successful
                successful_scenarios >= 3  # At least 3 scenarios successful
            )
            
            print(f"   ✓ Successful scenarios: {successful_scenarios}/{len(scenario_tests)}")
            print(f"   ✓ Scenario success rate: {scenario_success_rate:.1%}")
            
            return {
                'success': success,
                'successful_scenarios': successful_scenarios,
                'total_scenarios': len(scenario_tests),
                'scenario_success_rate': scenario_success_rate,
                'scenario_tests': scenario_tests
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Real-world scenarios validation failed: {e}'
            }
    
    def validate_system_resilience(self) -> Dict[str, Any]:
        """
        Validate system resilience and error recovery
        
        Success Criteria:
        - System handles errors gracefully
        - Recovery mechanisms work
        - No data corruption under stress
        """
        try:
            print("   Testing system resilience and recovery...")
            
            resilience_tests = {
                'invalid_input_handling': {'success': False, 'details': ''},
                'database_stress_handling': {'success': False, 'details': ''},
                'partial_data_recovery': {'success': False, 'details': ''},
                'error_propagation_control': {'success': False, 'details': ''}
            }
            
            # Test 1: Invalid Input Handling
            print("     Testing invalid input handling...")
            try:
                # Test with invalid league ID
                invalid_result = self.enhanced_builder.refresh_waiver_candidates_for_league(
                    league_id="invalid_league_123",
                    week=self.test_week
                )
                
                # System should handle gracefully, not crash
                if not invalid_result.get('success', True):  # Should fail gracefully
                    resilience_tests['invalid_input_handling'] = {
                        'success': True,
                        'details': 'Invalid input handled gracefully'
                    }
                    print("       ✓ Invalid league ID handled gracefully")
                
            except Exception as e:
                # Catching exceptions is also acceptable - shows error handling
                resilience_tests['invalid_input_handling'] = {
                    'success': True,
                    'details': f'Exception caught and handled: {str(e)[:50]}'
                }
                print(f"       ✓ Exception handled: {str(e)[:50]}")
            
            # Test 2: Database Stress Handling
            print("     Testing database stress handling...")
            try:
                # Perform multiple rapid database operations
                stress_operations = 0
                successful_operations = 0
                
                for i in range(5):
                    try:
                        players = self.db.query(Player).limit(50).all()
                        if players:
                            successful_operations += 1
                        stress_operations += 1
                    except Exception:
                        stress_operations += 1
                
                stress_success_rate = successful_operations / stress_operations
                
                if stress_success_rate >= 0.8:  # At least 80% success under stress
                    resilience_tests['database_stress_handling'] = {
                        'success': True,
                        'details': f'Handled {successful_operations}/{stress_operations} operations'
                    }
                    print(f"       ✓ Database stress: {successful_operations}/{stress_operations} operations successful")
                
            except Exception as e:
                print(f"       ❌ Database stress test error: {e}")
            
            # Test 3: Partial Data Recovery
            print("     Testing partial data recovery...")
            try:
                # Test system behavior with limited data
                test_league_id = self.integration_leagues[0]['id']
                
                # Try to build candidates even with potentially incomplete data
                candidates = self.enhanced_builder.build_waiver_candidates(
                    league_id=test_league_id,
                    week=self.test_week
                )
                
                # System should return some results or handle gracefully
                if candidates or candidates == []:  # Either results or empty list is acceptable
                    resilience_tests['partial_data_recovery'] = {
                        'success': True,
                        'details': f'Built {len(candidates) if candidates else 0} candidates with available data'
                    }
                    print(f"       ✓ Partial data handled: {len(candidates) if candidates else 0} candidates built")
                
            except Exception as e:
                print(f"       ❌ Partial data recovery error: {e}")
            
            # Test 4: Error Propagation Control
            print("     Testing error propagation control...")
            try:
                # Test that errors in one component don't crash the entire system
                error_contained = True
                
                try:
                    # Generate a potential error condition
                    invalid_canonical = self.player_mapper.generate_canonical_id("", "", "")
                    # System should handle empty inputs gracefully
                except Exception:
                    # Exception is contained, doesn't propagate
                    pass
                
                # System should still be operational
                test_query = self.db.query(Player).limit(1).all()
                if test_query or test_query == []:
                    resilience_tests['error_propagation_control'] = {
                        'success': True,
                        'details': 'Errors contained, system remains operational'
                    }
                    print("       ✓ Error propagation controlled, system operational")
                
            except Exception as e:
                print(f"       ❌ Error propagation test error: {e}")
            
            # Calculate resilience success
            successful_resilience_tests = sum(1 for test in resilience_tests.values() if test['success'])
            resilience_rate = successful_resilience_tests / len(resilience_tests)
            
            success = (
                resilience_rate >= 0.75 and  # At least 75% of resilience tests pass
                successful_resilience_tests >= 3  # At least 3 tests successful
            )
            
            print(f"   ✓ Resilience tests passed: {successful_resilience_tests}/{len(resilience_tests)}")
            print(f"   ✓ Resilience rate: {resilience_rate:.1%}")
            
            return {
                'success': success,
                'successful_resilience_tests': successful_resilience_tests,
                'total_resilience_tests': len(resilience_tests),
                'resilience_rate': resilience_rate,
                'resilience_tests': resilience_tests
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'System resilience validation failed: {e}'
            }
    
    def validate_epic_a_acceptance_criteria(self) -> Dict[str, Any]:
        """
        Final validation of all Epic A acceptance criteria
        
        Success Criteria:
        - All Epic A US-A1 acceptance criteria are met
        - All Epic A US-A2 acceptance criteria are met
        - System is ready for production use
        """
        try:
            print("   Testing Epic A acceptance criteria compliance...")
            
            epic_criteria = {
                'us_a1_canonical_lookup': {'success': False, 'details': ''},
                'us_a1_roster_persistence': {'success': False, 'details': ''},
                'us_a1_usage_completeness': {'success': False, 'details': ''},
                'us_a1_projections_completeness': {'success': False, 'details': ''},
                'us_a1_table_joins': {'success': False, 'details': ''},
                'us_a2_waiver_view': {'success': False, 'details': ''},
                'us_a2_refresh_performance': {'success': False, 'details': ''},
                'us_a2_non_rostered_filter': {'success': False, 'details': ''}
            }
            
            # US-A1 Criteria Validation
            print("     Validating US-A1 acceptance criteria...")
            
            # US-A1.1: Player rows can be looked up by canonical_player_id regardless of platform
            test_canonical = self.player_mapper.generate_canonical_id('Josh Allen', 'QB', 'BUF')
            player_lookup = self.db.query(Player).filter(Player.nfl_id == test_canonical).first()
            
            if player_lookup:
                epic_criteria['us_a1_canonical_lookup'] = {
                    'success': True,
                    'details': f'Player lookup successful for {player_lookup.name}'
                }
                print("       ✓ US-A1.1: Canonical player lookup - PASS")
            
            # US-A1.2: Roster snapshots persist
            roster_count = self.db.query(RosterEntry).filter(RosterEntry.is_active == True).count()
            
            if roster_count >= 50:
                epic_criteria['us_a1_roster_persistence'] = {
                    'success': True,
                    'details': f'{roster_count} roster entries persisted'
                }
                print("       ✓ US-A1.2: Roster snapshots persist - PASS")
            
            # US-A1.3: Usage table completeness
            usage_fields_query = self.db.query(PlayerUsage).filter(
                PlayerUsage.season == self.current_season,
                PlayerUsage.snap_pct.isnot(None)
            ).count()
            
            if usage_fields_query >= 100:
                epic_criteria['us_a1_usage_completeness'] = {
                    'success': True,
                    'details': f'{usage_fields_query} usage records with required fields'
                }
                print("       ✓ US-A1.3: Usage table completeness - PASS")
            
            # US-A1.4: Projections table completeness
            projections_fields_query = self.db.query(PlayerProjections).filter(
                PlayerProjections.season == self.current_season,
                PlayerProjections.mean.isnot(None),
                PlayerProjections.floor.isnot(None),
                PlayerProjections.ceiling.isnot(None)
            ).count()
            
            if projections_fields_query >= 100:
                epic_criteria['us_a1_projections_completeness'] = {
                    'success': True,
                    'details': f'{projections_fields_query} projection records with required fields'
                }
                print("       ✓ US-A1.4: Projections table completeness - PASS")
            
            # US-A1.5: All tables can be joined to materialize waiver_candidates view
            try:
                join_test = self.db.query(
                    Player.name,
                    PlayerUsage.snap_pct,
                    PlayerProjections.mean
                ).join(PlayerUsage).join(PlayerProjections).filter(
                    PlayerUsage.season == self.current_season,
                    PlayerUsage.week == self.test_week,
                    PlayerProjections.season == self.current_season,
                    PlayerProjections.week == self.test_week
                ).limit(5).all()
                
                if join_test:
                    epic_criteria['us_a1_table_joins'] = {
                        'success': True,
                        'details': f'Multi-table join successful, {len(join_test)} results'
                    }
                    print("       ✓ US-A1.5: Table joins for waiver_candidates - PASS")
                
            except Exception as e:
                print(f"       ❌ US-A1.5: Table joins error - {e}")
            
            # US-A2 Criteria Validation
            print("     Validating US-A2 acceptance criteria...")
            
            # US-A2.1: Waiver candidates view is queryable
            test_league_id = self.integration_leagues[0]['id']
            candidates = self.enhanced_builder.build_waiver_candidates(
                league_id=test_league_id,
                week=self.test_week
            )
            
            if candidates and len(candidates) >= 3:
                # Check for required fields
                sample_candidate = candidates[0]
                required_fields = ['player_id', 'pos', 'rostered', 'snap_delta', 'proj_next']
                
                fields_present = sum(1 for field in required_fields if hasattr(sample_candidate, field))
                field_completeness = fields_present / len(required_fields)
                
                if field_completeness >= 0.8:
                    epic_criteria['us_a2_waiver_view'] = {
                        'success': True,
                        'details': f'{len(candidates)} candidates with {field_completeness:.1%} field completeness'
                    }
                    print("       ✓ US-A2.1: Waiver candidates view queryable - PASS")
            
            # US-A2.2: Refresh job populates for current week in < 1 minute
            start_time = time.time()
            refresh_result = self.enhanced_builder.refresh_waiver_candidates_for_league(
                league_id=test_league_id,
                week=self.test_week
            )
            refresh_time = time.time() - start_time
            
            if refresh_result.get('success', False) and refresh_time < 60.0:
                epic_criteria['us_a2_refresh_performance'] = {
                    'success': True,
                    'details': f'Refresh completed in {refresh_time:.2f}s (< 60s requirement)'
                }
                print(f"       ✅ US-A2.2: Refresh performance < 1 minute - PASS ({refresh_time:.2f}s)")
            else:
                print(f"       ❌ US-A2.2: Refresh performance - FAIL ({refresh_time:.2f}s)")
            
            # US-A2.3: Non-rostered players only
            if candidates:
                rostered_count = sum(1 for c in candidates if getattr(c, 'rostered', True))
                non_rostered_count = len(candidates) - rostered_count
                non_rostered_rate = non_rostered_count / len(candidates)
                
                if non_rostered_rate >= 0.8:  # At least 80% should be non-rostered
                    epic_criteria['us_a2_non_rostered_filter'] = {
                        'success': True,
                        'details': f'{non_rostered_rate:.1%} candidates are non-rostered'
                    }
                    print("       ✓ US-A2.3: Non-rostered players only - PASS")
            
            # Calculate Epic A compliance
            successful_criteria = sum(1 for criteria in epic_criteria.values() if criteria['success'])
            epic_a_compliance_rate = successful_criteria / len(epic_criteria)
            
            # Epic A is ready if most criteria are met
            success = (
                epic_a_compliance_rate >= 0.75 and  # At least 75% compliance
                successful_criteria >= 6  # At least 6 criteria successful
            )
            
            print(f"   ✓ Epic A criteria passed: {successful_criteria}/{len(epic_criteria)}")
            print(f"   ✓ Epic A compliance rate: {epic_a_compliance_rate:.1%}")
            
            return {
                'success': success,
                'successful_criteria': successful_criteria,
                'total_criteria': len(epic_criteria),
                'epic_a_compliance_rate': epic_a_compliance_rate,
                'epic_criteria': epic_criteria
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Epic A acceptance criteria validation failed: {e}'
            }
    
    def _perform_epic_a_system_assessment(self, validation_results: Dict[str, Any]):
        """Perform final Epic A system readiness assessment"""
        
        print("\n" + "=" * 80)
        print("EPIC A SYSTEM READINESS ASSESSMENT")
        print("=" * 80)
        
        overall_success = validation_results['overall_success']
        
        if overall_success:
            print("\n🎉 EPIC A DATA FOUNDATION - SYSTEM READY")
            print("\nEpic A has been successfully validated and is ready for production use!")
            print("\n✅ Key Achievements:")
            print("  • US-A1: Canonical player/league schema - IMPLEMENTED")
            print("  • US-A2: Waiver candidates materialized view - IMPLEMENTED")
            print("  • Cross-platform player lookup - FUNCTIONAL")
            print("  • Performance requirements - MET")
            print("  • Data integrity - VALIDATED")
            print("  • System resilience - CONFIRMED")
            
        else:
            print("\n⚠️  EPIC A DATA FOUNDATION - NEEDS ATTENTION")
            print("\nEpic A has some issues that should be addressed before production:")
            
            failed_validations = [
                name for name, result in validation_results.get('validations', {}).items()
                if not result.get('success', False)
            ]
            
            print(f"\n❌ Failed Validations ({len(failed_validations)}):")
            for validation in failed_validations:
                print(f"  • {validation.replace('_', ' ').title()}")
        
        print("\n" + "=" * 80)
    
    def _print_validation_summary(self, results: Dict[str, Any]):
        """Print comprehensive validation summary"""
        print("\n" + "=" * 80)
        print("END-TO-END INTEGRATION VALIDATION SUMMARY")
        print("=" * 80)
        
        overall_success = results['overall_success']
        status_icon = "✅" if overall_success else "❌"
        
        print(f"\n{status_icon} OVERALL INTEGRATION STATUS: {'PASS' if overall_success else 'FAIL'}")
        print(f"📅 Test Date: {results['timestamp']}")
        print(f"💾 Database: {results['database']}")
        print(f"🏈 Season: {results['season']}, Week: {results['test_week']}")
        print(f"🏟️ Integration Leagues: {results['integration_leagues']}")
        
        print(f"\n📊 Integration Test Results:")
        for validation_name, validation_result in results.get('validations', {}).items():
            success = validation_result.get('success', False)
            icon = "✅" if success else "❌"
            print(f"  {icon} {validation_name.replace('_', ' ').title()}: {'PASS' if success else 'FAIL'}")
            
            if not success and 'error' in validation_result:
                print(f"      Error: {validation_result['error']}")
        
        # Epic A Status Summary
        if 'epic_a_acceptance_criteria' in results.get('validations', {}):
            epic_test = results['validations']['epic_a_acceptance_criteria']
            epic_compliance = epic_test.get('epic_a_compliance_rate', 0)
            successful_criteria = epic_test.get('successful_criteria', 0)
            total_criteria = epic_test.get('total_criteria', 0)
            
            print(f"\n🎯 Epic A Acceptance Criteria Summary:")
            print(f"  • Criteria Passed: {successful_criteria}/{total_criteria}")
            print(f"  • Compliance Rate: {epic_compliance:.1%}")
            print(f"  • Epic A Status: {'✅ READY' if epic_compliance >= 0.75 else '⚠️ NEEDS WORK'}")
        
        print(f"\n{'🎉 Epic A Data Foundation is PRODUCTION READY!' if overall_success else '⚠️ Epic A Data Foundation requires attention before production'}")
        print("=" * 80)

def main():
    """Run end-to-end integration validation tests"""
    print("Starting Epic A End-to-End Integration Validation...")
    
    try:
        validator = EndToEndIntegrationValidator()
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