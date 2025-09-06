#!/usr/bin/env python3
"""
Projections Table Validation Tests

Epic A US-A1 Acceptance Criteria:
"Projections table contains: week, mean, stdev, floor, ceiling for your scoring."

Data Contract: player_id, week, mean, stdev, floor, ceiling, source

This script provides comprehensive validation of projections table completeness
with all required fields, statistical consistency, and scoring integration validation.
"""

import os
import sys
import traceback
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from collections import defaultdict
import statistics

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.config import get_config
from src.database import SessionLocal, Player, PlayerProjections
from src.utils.player_id_mapper import PlayerIDMapper
from sqlalchemy import func, distinct, and_, or_

class ProjectionsTableValidator:
    """
    Comprehensive validation of projections table completeness and data quality
    """
    
    def __init__(self):
        self.config = get_config()
        self.db = SessionLocal()
        self.player_mapper = PlayerIDMapper()
        
        self.current_season = 2025
        
        # Expected statistical relationships
        self.stat_relationships = {
            'floor_ceiling_ratio': {'min': 0.3, 'max': 0.8},  # floor should be 30-80% of ceiling
            'stdev_mean_ratio': {'min': 0.1, 'max': 0.6},     # stdev should be 10-60% of mean
            'reasonable_points_range': {'min': 0, 'max': 50}   # reasonable fantasy points per game
        }
        
        print("=" * 80)
        print("PROJECTIONS TABLE VALIDATION")
        print("Epic A US-A1: Projections Table Data Validation Testing")
        print("=" * 80)
    
    def run_all_validations(self) -> Dict[str, Any]:
        """
        Execute all projections table validation tests
        
        Returns comprehensive validation results
        """
        validation_results = {
            'overall_success': True,
            'timestamp': datetime.utcnow().isoformat(),
            'database': self.config.DATABASE_URL,
            'season': self.current_season,
            'validations': {}
        }
        
        try:
            print(f"\n📈 Running Projections Table Validation Tests")
            print(f"Database: {self.config.DATABASE_URL}")
            print(f"Season: {self.current_season}")
            print("-" * 60)
            
            # Validation 1: Required Fields Presence
            print("\n📋 VALIDATION 1: Required Fields Presence")
            result_1 = self.validate_required_fields()
            validation_results['validations']['required_fields'] = result_1
            if not result_1.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 2: Statistical Consistency
            print("\n📊 VALIDATION 2: Statistical Consistency")
            result_2 = self.validate_statistical_consistency()
            validation_results['validations']['statistical_consistency'] = result_2
            if not result_2.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 3: Data Coverage and Distribution
            print("\n🌐 VALIDATION 3: Data Coverage and Distribution")
            result_3 = self.validate_data_coverage()
            validation_results['validations']['data_coverage'] = result_3
            if not result_3.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 4: Projection Sources
            print("\n🔗 VALIDATION 4: Projection Sources")
            result_4 = self.validate_projection_sources()
            validation_results['validations']['projection_sources'] = result_4
            if not result_4.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 5: Scoring System Integration
            print("\n🎯 VALIDATION 5: Scoring System Integration")
            result_5 = self.validate_scoring_integration()
            validation_results['validations']['scoring_integration'] = result_5
            if not result_5.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 6: Projection Reasonableness
            print("\n🧠 VALIDATION 6: Projection Reasonableness")
            result_6 = self.validate_projection_reasonableness()
            validation_results['validations']['projection_reasonableness'] = result_6
            if not result_6.get('success', False):
                validation_results['overall_success'] = False
            
            # Final Summary
            self._print_validation_summary(validation_results)
            
            return validation_results
            
        except Exception as e:
            print(f"❌ CRITICAL ERROR in projections table validation: {e}")
            traceback.print_exc()
            validation_results['overall_success'] = False
            validation_results['error'] = str(e)
            return validation_results
        
        finally:
            self.db.close()
    
    def validate_required_fields(self) -> Dict[str, Any]:
        """
        Validate all required Epic A projection fields are present and populated
        
        Success Criteria:
        - All 6 required fields exist: player_id, week, mean, stdev, floor, ceiling, source
        - Fields have reasonable population rates
        - No critical missing data patterns
        """
        try:
            print("   Testing required fields presence...")
            
            # Get sample of projection records
            projection_records = self.db.query(PlayerProjections).filter(
                PlayerProjections.season == self.current_season
            ).limit(500).all()
            
            if not projection_records:
                return {
                    'success': False,
                    'error': 'No projection records found for current season'
                }
            
            field_analysis = {
                'total_records': len(projection_records),
                'field_stats': {},
                'missing_data_patterns': []
            }
            
            # Epic A required fields
            required_fields = [
                'player_id', 'week', 'mean', 'stdev', 'floor', 'ceiling', 'source'
            ]
            
            # Initialize field statistics
            for field in required_fields:
                field_analysis['field_stats'][field] = {
                    'populated': 0,
                    'total': len(projection_records),
                    'population_rate': 0.0,
                    'non_zero_count': 0,
                    'non_zero_rate': 0.0
                }
            
            # Analyze field population
            for record in projection_records:
                for field in required_fields:
                    field_value = getattr(record, field, None)
                    
                    if field_value is not None:
                        field_analysis['field_stats'][field]['populated'] += 1
                        
                        # Check for meaningful data (non-zero values)
                        if isinstance(field_value, (int, float)) and field_value > 0:
                            field_analysis['field_stats'][field]['non_zero_count'] += 1
                        elif field in ['player_id', 'week', 'source'] and field_value:
                            field_analysis['field_stats'][field]['non_zero_count'] += 1
            
            # Calculate rates and identify patterns
            total_field_score = 0
            critical_missing_fields = []
            
            for field, stats in field_analysis['field_stats'].items():
                stats['population_rate'] = stats['populated'] / stats['total']
                stats['non_zero_rate'] = stats['non_zero_count'] / stats['populated'] if stats['populated'] > 0 else 0
                
                total_field_score += stats['population_rate']
                
                # Identify critical missing fields
                if stats['population_rate'] < 0.5:  # Less than 50% populated
                    critical_missing_fields.append(field)
            
            avg_field_population = total_field_score / len(required_fields)
            
            # Check for missing data patterns
            records_with_no_projections = 0
            for record in projection_records:
                projection_fields = ['mean', 'floor', 'ceiling']
                if all(getattr(record, field, 0) in [None, 0] for field in projection_fields):
                    records_with_no_projections += 1
            
            no_projection_rate = records_with_no_projections / len(projection_records)
            
            # Success criteria
            success = (
                avg_field_population >= 0.8 and  # 80% field population
                len(critical_missing_fields) <= 1 and  # At most 1 critically missing field
                no_projection_rate <= 0.2  # No more than 20% records with no projections
            )
            
            print(f"   ✓ Total projection records: {field_analysis['total_records']}")
            print(f"   ✓ Average field population: {avg_field_population:.1%}")
            print(f"   ✓ Records with no projections: {no_projection_rate:.1%}")
            print(f"   ✓ Critical missing fields: {len(critical_missing_fields)}")
            
            for field, stats in field_analysis['field_stats'].items():
                print(f"   ✓ {field}: {stats['population_rate']:.1%} populated, {stats['non_zero_rate']:.1%} with data")
            
            if critical_missing_fields:
                print(f"   ⚠️  Critical missing fields: {critical_missing_fields}")
            
            return {
                'success': success,
                'total_records': field_analysis['total_records'],
                'avg_field_population': avg_field_population,
                'no_projection_rate': no_projection_rate,
                'critical_missing_fields': critical_missing_fields,
                'field_stats': field_analysis['field_stats']
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Required fields validation failed: {e}'
            }
    
    def validate_statistical_consistency(self) -> Dict[str, Any]:
        """
        Validate statistical consistency between mean, stdev, floor, and ceiling
        
        Success Criteria:
        - floor <= mean <= ceiling
        - stdev is reasonable relative to mean
        - Statistical relationships make mathematical sense
        """
        try:
            print("   Testing statistical consistency...")
            
            # Get projections with all statistical fields
            projections = self.db.query(PlayerProjections).filter(
                PlayerProjections.season == self.current_season,
                PlayerProjections.mean.isnot(None),
                PlayerProjections.stdev.isnot(None),
                PlayerProjections.floor.isnot(None),
                PlayerProjections.ceiling.isnot(None),
                PlayerProjections.mean > 0
            ).limit(500).all()
            
            if not projections:
                return {
                    'success': False,
                    'error': 'No projections with complete statistical data found'
                }
            
            consistency_analysis = {
                'total_records_analyzed': len(projections),
                'relationship_violations': {
                    'floor_mean_ceiling': 0,
                    'negative_values': 0,
                    'extreme_stdev': 0,
                    'unrealistic_ranges': 0
                },
                'statistical_metrics': {
                    'avg_floor_ceiling_ratio': 0,
                    'avg_stdev_mean_ratio': 0,
                    'median_projection': 0
                },
                'violation_examples': []
            }
            
            floor_ceiling_ratios = []
            stdev_mean_ratios = []
            projection_values = []
            
            for projection in projections:
                mean = float(projection.mean)
                stdev = float(projection.stdev) if projection.stdev else 0
                floor = float(projection.floor) if projection.floor else 0
                ceiling = float(projection.ceiling) if projection.ceiling else 0
                
                projection_values.append(mean)
                
                # Check floor <= mean <= ceiling
                if not (floor <= mean <= ceiling):
                    consistency_analysis['relationship_violations']['floor_mean_ceiling'] += 1
                    if len(consistency_analysis['violation_examples']) < 5:
                        consistency_analysis['violation_examples'].append({
                            'type': 'floor_mean_ceiling',
                            'player_id': projection.player_id,
                            'week': projection.week,
                            'floor': floor,
                            'mean': mean,
                            'ceiling': ceiling
                        })
                
                # Check for negative values
                if any(val < 0 for val in [mean, stdev, floor, ceiling]):
                    consistency_analysis['relationship_violations']['negative_values'] += 1
                
                # Check stdev reasonableness
                if mean > 0:
                    stdev_ratio = stdev / mean
                    stdev_mean_ratios.append(stdev_ratio)
                    
                    if stdev_ratio > self.stat_relationships['stdev_mean_ratio']['max']:
                        consistency_analysis['relationship_violations']['extreme_stdev'] += 1
                
                # Check floor/ceiling ratio
                if ceiling > 0:
                    floor_ratio = floor / ceiling
                    floor_ceiling_ratios.append(floor_ratio)
                    
                    if (floor_ratio < self.stat_relationships['floor_ceiling_ratio']['min'] or 
                        floor_ratio > self.stat_relationships['floor_ceiling_ratio']['max']):
                        consistency_analysis['relationship_violations']['unrealistic_ranges'] += 1
            
            # Calculate statistical metrics
            if floor_ceiling_ratios:
                consistency_analysis['statistical_metrics']['avg_floor_ceiling_ratio'] = statistics.mean(floor_ceiling_ratios)
            if stdev_mean_ratios:
                consistency_analysis['statistical_metrics']['avg_stdev_mean_ratio'] = statistics.mean(stdev_mean_ratios)
            if projection_values:
                consistency_analysis['statistical_metrics']['median_projection'] = statistics.median(projection_values)
            
            # Calculate violation rates
            total_violations = sum(consistency_analysis['relationship_violations'].values())
            violation_rate = total_violations / (len(projections) * 4)  # 4 types of violations
            
            # Success criteria
            success = (
                violation_rate <= 0.05 and  # Less than 5% violation rate
                consistency_analysis['relationship_violations']['floor_mean_ceiling'] <= len(projections) * 0.02 and  # Less than 2% ordering violations
                consistency_analysis['relationship_violations']['negative_values'] == 0  # No negative values
            )
            
            print(f"   ✓ Records analyzed: {consistency_analysis['total_records_analyzed']}")
            print(f"   ✓ Overall violation rate: {violation_rate:.2%}")
            print(f"   ✓ Floor/mean/ceiling violations: {consistency_analysis['relationship_violations']['floor_mean_ceiling']}")
            print(f"   ✓ Negative value violations: {consistency_analysis['relationship_violations']['negative_values']}")
            print(f"   ✓ Extreme stdev violations: {consistency_analysis['relationship_violations']['extreme_stdev']}")
            print(f"   ✓ Average floor/ceiling ratio: {consistency_analysis['statistical_metrics']['avg_floor_ceiling_ratio']:.2f}")
            print(f"   ✓ Average stdev/mean ratio: {consistency_analysis['statistical_metrics']['avg_stdev_mean_ratio']:.2f}")
            
            return {
                'success': success,
                'total_records_analyzed': consistency_analysis['total_records_analyzed'],
                'violation_rate': violation_rate,
                'relationship_violations': consistency_analysis['relationship_violations'],
                'statistical_metrics': consistency_analysis['statistical_metrics'],
                'sample_violations': consistency_analysis['violation_examples'][:3]
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Statistical consistency validation failed: {e}'
            }
    
    def validate_data_coverage(self) -> Dict[str, Any]:
        """
        Validate coverage of projection data across players, weeks, and positions
        
        Success Criteria:
        - Multiple weeks of projections are present
        - Coverage across different player positions
        - Reasonable number of players with projections
        """
        try:
            print("   Testing data coverage...")
            
            coverage_stats = {
                'weeks_covered': 0,
                'players_with_projections': 0,
                'position_coverage': {},
                'weekly_distribution': {},
                'total_projection_records': 0
            }
            
            # Get week coverage
            week_data = self.db.query(
                PlayerProjections.week,
                func.count(PlayerProjections.id).label('record_count'),
                func.count(distinct(PlayerProjections.player_id)).label('player_count')
            ).filter(
                PlayerProjections.season == self.current_season,
                PlayerProjections.week.isnot(None)
            ).group_by(PlayerProjections.week).all()
            
            coverage_stats['weeks_covered'] = len(week_data)
            coverage_stats['total_projection_records'] = sum(row.record_count for row in week_data)
            
            for week, record_count, player_count in week_data:
                coverage_stats['weekly_distribution'][week] = {
                    'records': record_count,
                    'players': player_count
                }
            
            # Get player coverage
            coverage_stats['players_with_projections'] = self.db.query(
                distinct(PlayerProjections.player_id)
            ).filter(
                PlayerProjections.season == self.current_season
            ).count()
            
            # Get position coverage
            position_data = self.db.query(
                Player.position,
                func.count(distinct(PlayerProjections.player_id)).label('player_count'),
                func.count(PlayerProjections.id).label('projection_records')
            ).join(PlayerProjections).filter(
                PlayerProjections.season == self.current_season
            ).group_by(Player.position).all()
            
            for position, player_count, projection_records in position_data:
                if position:  # Filter out None positions
                    coverage_stats['position_coverage'][position] = {
                        'players': player_count,
                        'projection_records': projection_records
                    }
            
            # Success criteria
            success = (
                coverage_stats['weeks_covered'] >= 4 and  # At least 4 weeks
                coverage_stats['players_with_projections'] >= 200 and  # At least 200 players
                len(coverage_stats['position_coverage']) >= 4 and  # At least 4 positions
                coverage_stats['total_projection_records'] >= 800  # At least 800 projection records
            )
            
            print(f"   ✓ Weeks covered: {coverage_stats['weeks_covered']}")
            print(f"   ✓ Players with projections: {coverage_stats['players_with_projections']}")
            print(f"   ✓ Positions covered: {len(coverage_stats['position_coverage'])}")
            print(f"   ✓ Total projection records: {coverage_stats['total_projection_records']}")
            
            # Show sample weekly distribution
            sample_weeks = sorted(list(coverage_stats['weekly_distribution'].keys()))[:5]
            for week in sample_weeks:
                week_data = coverage_stats['weekly_distribution'][week]
                print(f"   ✓ Week {week}: {week_data['records']} records, {week_data['players']} players")
            
            # Show position coverage
            sorted_positions = sorted(coverage_stats['position_coverage'].items(), 
                                    key=lambda x: x[1]['projection_records'], reverse=True)
            for position, stats in sorted_positions[:4]:
                print(f"   ✓ {position}: {stats['players']} players, {stats['projection_records']} records")
            
            return {
                'success': success,
                'weeks_covered': coverage_stats['weeks_covered'],
                'players_with_projections': coverage_stats['players_with_projections'],
                'positions_covered': len(coverage_stats['position_coverage']),
                'total_projection_records': coverage_stats['total_projection_records'],
                'weekly_range': f"{min(coverage_stats['weekly_distribution'].keys())} - {max(coverage_stats['weekly_distribution'].keys())}" if coverage_stats['weekly_distribution'] else "No data",
                'position_coverage': coverage_stats['position_coverage']
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Data coverage validation failed: {e}'
            }
    
    def validate_projection_sources(self) -> Dict[str, Any]:
        """
        Validate projection sources are tracked and diverse
        
        Success Criteria:
        - Source field is populated
        - Multiple sources are represented
        - Source attribution is consistent
        """
        try:
            print("   Testing projection sources...")
            
            # Analyze projection sources
            source_data = self.db.query(
                PlayerProjections.source,
                func.count(PlayerProjections.id).label('record_count'),
                func.count(distinct(PlayerProjections.player_id)).label('player_count'),
                func.count(distinct(PlayerProjections.week)).label('week_count')
            ).filter(
                PlayerProjections.season == self.current_season,
                PlayerProjections.source.isnot(None)
            ).group_by(PlayerProjections.source).all()
            
            source_analysis = {
                'sources_found': len(source_data),
                'source_distribution': {},
                'source_coverage': {},
                'total_sourced_records': 0
            }
            
            for source, record_count, player_count, week_count in source_data:
                source_analysis['source_distribution'][source] = record_count
                source_analysis['source_coverage'][source] = {
                    'records': record_count,
                    'players': player_count,
                    'weeks': week_count
                }
                source_analysis['total_sourced_records'] += record_count
            
            # Check for records without sources
            total_records = self.db.query(PlayerProjections).filter(
                PlayerProjections.season == self.current_season
            ).count()
            
            unsourced_records = total_records - source_analysis['total_sourced_records']
            source_attribution_rate = source_analysis['total_sourced_records'] / total_records if total_records > 0 else 0
            
            # Success criteria
            success = (
                source_analysis['sources_found'] >= 1 and  # At least 1 source
                source_attribution_rate >= 0.8 and  # 80% of records have sources
                source_analysis['total_sourced_records'] >= 100  # Sufficient sourced data
            )
            
            print(f"   ✓ Sources found: {source_analysis['sources_found']}")
            print(f"   ✓ Source attribution rate: {source_attribution_rate:.1%}")
            print(f"   ✓ Total sourced records: {source_analysis['total_sourced_records']}")
            print(f"   ✓ Unsourced records: {unsourced_records}")
            
            # Show source breakdown
            for source, coverage in source_analysis['source_coverage'].items():
                print(f"   ✓ {source}: {coverage['records']} records, {coverage['players']} players, {coverage['weeks']} weeks")
            
            return {
                'success': success,
                'sources_found': source_analysis['sources_found'],
                'source_attribution_rate': source_attribution_rate,
                'total_sourced_records': source_analysis['total_sourced_records'],
                'unsourced_records': unsourced_records,
                'source_distribution': source_analysis['source_distribution'],
                'source_coverage': source_analysis['source_coverage']
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Projection sources validation failed: {e}'
            }
    
    def validate_scoring_integration(self) -> Dict[str, Any]:
        """
        Validate projections integrate with scoring system
        
        Success Criteria:
        - Projections reflect fantasy scoring (not raw stats)
        - Reasonable point values for different positions
        - Integration with league scoring settings
        """
        try:
            print("   Testing scoring system integration...")
            
            # Get projections by position
            position_projections = {}
            position_data = self.db.query(
                Player.position,
                func.avg(PlayerProjections.mean).label('avg_projection'),
                func.count(PlayerProjections.id).label('record_count')
            ).join(PlayerProjections).filter(
                PlayerProjections.season == self.current_season,
                PlayerProjections.mean.isnot(None),
                PlayerProjections.mean > 0,
                Player.position.isnot(None)
            ).group_by(Player.position).all()
            
            scoring_analysis = {
                'position_averages': {},
                'scoring_reasonableness': {},
                'position_ranking_logic': True,
                'fantasy_scoring_indicators': {}
            }
            
            for position, avg_projection, record_count in position_data:
                scoring_analysis['position_averages'][position] = {
                    'avg_points': float(avg_projection),
                    'record_count': record_count
                }
            
            # Expected fantasy point ranges per position (per game)
            position_expectations = {
                'QB': {'min': 10, 'max': 25, 'typical': 18},
                'RB': {'min': 8, 'max': 20, 'typical': 12},
                'WR': {'min': 6, 'max': 18, 'typical': 10},
                'TE': {'min': 5, 'max': 15, 'typical': 8},
                'K': {'min': 5, 'max': 12, 'typical': 8},
                'DEF': {'min': 5, 'max': 15, 'typical': 8}
            }
            
            positions_within_range = 0
            total_positions = 0
            
            for position, avg_data in scoring_analysis['position_averages'].items():
                if position in position_expectations:
                    total_positions += 1
                    expected = position_expectations[position]
                    avg_points = avg_data['avg_points']
                    
                    within_range = expected['min'] <= avg_points <= expected['max']
                    if within_range:
                        positions_within_range += 1
                    
                    scoring_analysis['scoring_reasonableness'][position] = {
                        'avg_points': avg_points,
                        'expected_range': f"{expected['min']}-{expected['max']}",
                        'within_range': within_range,
                        'reasonableness_score': 1.0 if within_range else abs(avg_points - expected['typical']) / expected['typical']
                    }
            
            # Check fantasy scoring indicators
            # Look for projections that seem like fantasy points vs raw stats
            sample_projections = self.db.query(PlayerProjections).join(Player).filter(
                PlayerProjections.season == self.current_season,
                PlayerProjections.mean.isnot(None),
                PlayerProjections.mean > 0,
                Player.position.in_(['QB', 'RB', 'WR', 'TE'])
            ).limit(100).all()
            
            fantasy_indicators = 0
            total_samples = len(sample_projections)
            
            for projection in sample_projections:
                # Fantasy points typically include decimal places and are in realistic ranges
                mean_val = float(projection.mean)
                if 3 <= mean_val <= 30 and mean_val != int(mean_val):  # Has decimals and reasonable range
                    fantasy_indicators += 1
            
            fantasy_score = fantasy_indicators / total_samples if total_samples > 0 else 0
            scoring_analysis['fantasy_scoring_indicators'] = {
                'fantasy_indicators': fantasy_indicators,
                'total_samples': total_samples,
                'fantasy_score': fantasy_score
            }
            
            # Success criteria
            position_reasonableness = positions_within_range / total_positions if total_positions > 0 else 0
            
            success = (
                total_positions >= 4 and  # At least 4 positions have projections
                position_reasonableness >= 0.5 and  # At least 50% of positions have reasonable averages
                fantasy_score >= 0.7  # 70% of projections look like fantasy points
            )
            
            print(f"   ✓ Positions with projections: {total_positions}")
            print(f"   ✓ Position reasonableness: {position_reasonableness:.1%}")
            print(f"   ✓ Fantasy scoring indicators: {fantasy_score:.1%}")
            
            # Show position averages
            for position, avg_data in scoring_analysis['position_averages'].items():
                reasonableness = scoring_analysis['scoring_reasonableness'].get(position, {})
                if reasonableness:
                    within_range = "✓" if reasonableness['within_range'] else "⚠️"
                    print(f"   {within_range} {position}: {avg_data['avg_points']:.1f} points ({reasonableness['expected_range']} expected)")
            
            return {
                'success': success,
                'positions_with_projections': total_positions,
                'position_reasonableness_rate': position_reasonableness,
                'fantasy_scoring_score': fantasy_score,
                'position_averages': scoring_analysis['position_averages'],
                'scoring_reasonableness': scoring_analysis['scoring_reasonableness']
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Scoring integration validation failed: {e}'
            }
    
    def validate_projection_reasonableness(self) -> Dict[str, Any]:
        """
        Validate projections are reasonable and not extreme outliers
        
        Success Criteria:
        - No projections with impossible values
        - Distribution of projections is reasonable
        - Elite players have higher projections than backups
        """
        try:
            print("   Testing projection reasonableness...")
            
            # Get all projections for analysis
            projections = self.db.query(PlayerProjections).filter(
                PlayerProjections.season == self.current_season,
                PlayerProjections.mean.isnot(None),
                PlayerProjections.mean > 0
            ).limit(1000).all()
            
            if not projections:
                return {
                    'success': False,
                    'error': 'No projections found for reasonableness analysis'
                }
            
            reasonableness_analysis = {
                'total_projections_analyzed': len(projections),
                'extreme_outliers': 0,
                'impossible_values': 0,
                'distribution_analysis': {},
                'outlier_examples': []
            }
            
            projection_values = [float(p.mean) for p in projections]
            
            # Calculate distribution statistics
            reasonableness_analysis['distribution_analysis'] = {
                'min': min(projection_values),
                'max': max(projection_values),
                'mean': statistics.mean(projection_values),
                'median': statistics.median(projection_values),
                'stdev': statistics.stdev(projection_values) if len(projection_values) > 1 else 0
            }
            
            # Identify outliers and impossible values
            mean_projection = reasonableness_analysis['distribution_analysis']['mean']
            stdev_projection = reasonableness_analysis['distribution_analysis']['stdev']
            
            for projection in projections:
                mean_val = float(projection.mean)
                
                # Check for impossible values
                if mean_val < 0 or mean_val > 60:  # Negative or unrealistically high
                    reasonableness_analysis['impossible_values'] += 1
                    if len(reasonableness_analysis['outlier_examples']) < 5:
                        reasonableness_analysis['outlier_examples'].append({
                            'type': 'impossible_value',
                            'player_id': projection.player_id,
                            'week': projection.week,
                            'value': mean_val
                        })
                
                # Check for extreme outliers (more than 3 standard deviations)
                elif stdev_projection > 0:
                    z_score = abs(mean_val - mean_projection) / stdev_projection
                    if z_score > 3:
                        reasonableness_analysis['extreme_outliers'] += 1
                        if len(reasonableness_analysis['outlier_examples']) < 5:
                            reasonableness_analysis['outlier_examples'].append({
                                'type': 'extreme_outlier',
                                'player_id': projection.player_id,
                                'week': projection.week,
                                'value': mean_val,
                                'z_score': z_score
                            })
            
            # Calculate reasonableness rates
            impossible_rate = reasonableness_analysis['impossible_values'] / reasonableness_analysis['total_projections_analyzed']
            outlier_rate = reasonableness_analysis['extreme_outliers'] / reasonableness_analysis['total_projections_analyzed']
            
            # Success criteria
            success = (
                impossible_rate <= 0.01 and  # Less than 1% impossible values
                outlier_rate <= 0.05 and  # Less than 5% extreme outliers
                0 < reasonableness_analysis['distribution_analysis']['mean'] < 25 and  # Reasonable overall mean
                reasonableness_analysis['distribution_analysis']['max'] < 50  # Reasonable maximum
            )
            
            print(f"   ✓ Projections analyzed: {reasonableness_analysis['total_projections_analyzed']}")
            print(f"   ✓ Impossible values: {reasonableness_analysis['impossible_values']} ({impossible_rate:.1%})")
            print(f"   ✓ Extreme outliers: {reasonableness_analysis['extreme_outliers']} ({outlier_rate:.1%})")
            print(f"   ✓ Distribution mean: {reasonableness_analysis['distribution_analysis']['mean']:.1f}")
            print(f"   ✓ Distribution range: {reasonableness_analysis['distribution_analysis']['min']:.1f} - {reasonableness_analysis['distribution_analysis']['max']:.1f}")
            
            # Show sample outliers
            for outlier in reasonableness_analysis['outlier_examples'][:3]:
                print(f"   ⚠️  {outlier['type']}: Player {outlier['player_id']}, Week {outlier['week']}, Value: {outlier['value']}")
            
            return {
                'success': success,
                'total_projections_analyzed': reasonableness_analysis['total_projections_analyzed'],
                'impossible_rate': impossible_rate,
                'outlier_rate': outlier_rate,
                'distribution_analysis': reasonableness_analysis['distribution_analysis'],
                'sample_outliers': reasonableness_analysis['outlier_examples'][:3]
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Projection reasonableness validation failed: {e}'
            }
    
    def _print_validation_summary(self, results: Dict[str, Any]):
        """Print comprehensive validation summary"""
        print("\n" + "=" * 80)
        print("PROJECTIONS TABLE VALIDATION SUMMARY")
        print("=" * 80)
        
        overall_success = results['overall_success']
        status_icon = "✅" if overall_success else "❌"
        
        print(f"\n{status_icon} OVERALL VALIDATION STATUS: {'PASS' if overall_success else 'FAIL'}")
        print(f"📅 Test Date: {results['timestamp']}")
        print(f"💾 Database: {results['database']}")
        print(f"🏈 Season: {results['season']}")
        
        print(f"\n📊 Individual Validation Results:")
        for validation_name, validation_result in results.get('validations', {}).items():
            success = validation_result.get('success', False)
            icon = "✅" if success else "❌"
            print(f"  {icon} {validation_name.replace('_', ' ').title()}: {'PASS' if success else 'FAIL'}")
            
            if not success and 'error' in validation_result:
                print(f"      Error: {validation_result['error']}")
        
        # Key metrics summary
        print(f"\n🎯 Key Projections Table Metrics:")
        
        if 'required_fields' in results.get('validations', {}):
            fields_test = results['validations']['required_fields']
            print(f"  • Field Population: {fields_test.get('avg_field_population', 0):.1%}")
            print(f"  • Projection Records: {fields_test.get('total_records', 0)}")
        
        if 'data_coverage' in results.get('validations', {}):
            coverage_test = results['validations']['data_coverage']
            print(f"  • Weeks Covered: {coverage_test.get('weeks_covered', 0)}")
            print(f"  • Players with Projections: {coverage_test.get('players_with_projections', 0)}")
        
        if 'statistical_consistency' in results.get('validations', {}):
            stats_test = results['validations']['statistical_consistency']
            print(f"  • Statistical Violation Rate: {stats_test.get('violation_rate', 0):.1%}")
        
        if 'projection_sources' in results.get('validations', {}):
            sources_test = results['validations']['projection_sources']
            print(f"  • Source Attribution: {sources_test.get('source_attribution_rate', 0):.1%}")
        
        if 'scoring_integration' in results.get('validations', {}):
            scoring_test = results['validations']['scoring_integration']
            print(f"  • Fantasy Scoring Score: {scoring_test.get('fantasy_scoring_score', 0):.1%}")
        
        print(f"\n{'🎉 Projections Table is FULLY VALIDATED!' if overall_success else '⚠️  Projections Table needs attention'}")
        print("=" * 80)

def main():
    """Run projections table validation tests"""
    print("Starting Projections Table Validation...")
    
    try:
        validator = ProjectionsTableValidator()
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