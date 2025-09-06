#!/usr/bin/env python3
"""
Usage Table Completeness Validation Tests

Epic A US-A1 Acceptance Criteria:
"Usage table contains: week, snap_pct, route_pct, target_share, carry_share, rz_touches, ez_targets."

Data Contract: player_id, week, snap_pct, route_pct, target_share, carry_share, rz_touches, ez_targets

This script provides comprehensive validation of usage table completeness
with all required fields, data integrity, and NFL data integration validation.
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
from src.database import SessionLocal, Player, PlayerUsage
from src.utils.player_id_mapper import PlayerIDMapper
from sqlalchemy import func, distinct, and_, or_

class UsageTableCompletenessValidator:
    """
    Comprehensive validation of usage table completeness and data quality
    """
    
    def __init__(self):
        self.config = get_config()
        self.db = SessionLocal()
        self.player_mapper = PlayerIDMapper()
        
        self.current_season = 2025
        
        # Expected value ranges for validation
        self.value_ranges = {
            'snap_pct': {'min': 0, 'max': 100, 'typical_max': 95},
            'route_pct': {'min': 0, 'max': 100, 'typical_max': 90},
            'target_share': {'min': 0, 'max': 100, 'typical_max': 40},
            'carry_share': {'min': 0, 'max': 100, 'typical_max': 80},
            'rz_touches': {'min': 0, 'max': 20, 'typical_max': 8},
            'ez_targets': {'min': 0, 'max': 15, 'typical_max': 5}
        }
        
        print("=" * 80)
        print("USAGE TABLE COMPLETENESS VALIDATION")
        print("Epic A US-A1: Usage Table Data Completeness Testing")
        print("=" * 80)
    
    def run_all_validations(self) -> Dict[str, Any]:
        """
        Execute all usage table completeness validation tests
        
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
            print(f"\n📊 Running Usage Table Completeness Validations")
            print(f"Database: {self.config.DATABASE_URL}")
            print(f"Season: {self.current_season}")
            print("-" * 60)
            
            # Validation 1: Required Fields Presence
            print("\n📋 VALIDATION 1: Required Fields Presence")
            result_1 = self.validate_required_fields()
            validation_results['validations']['required_fields'] = result_1
            if not result_1.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 2: Data Coverage and Distribution
            print("\n🌐 VALIDATION 2: Data Coverage and Distribution")
            result_2 = self.validate_data_coverage()
            validation_results['validations']['data_coverage'] = result_2
            if not result_2.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 3: Value Ranges and Integrity
            print("\n🎯 VALIDATION 3: Value Ranges and Integrity")
            result_3 = self.validate_value_ranges()
            validation_results['validations']['value_ranges'] = result_3
            if not result_3.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 4: Position-Specific Data
            print("\n🏈 VALIDATION 4: Position-Specific Data")
            result_4 = self.validate_position_specific_data()
            validation_results['validations']['position_specific'] = result_4
            if not result_4.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 5: Multi-Week Consistency
            print("\n📅 VALIDATION 5: Multi-Week Consistency")
            result_5 = self.validate_multi_week_consistency()
            validation_results['validations']['multi_week_consistency'] = result_5
            if not result_5.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 6: NFL Data Integration Quality
            print("\n🔗 VALIDATION 6: NFL Data Integration Quality")
            result_6 = self.validate_nfl_data_integration()
            validation_results['validations']['nfl_data_integration'] = result_6
            if not result_6.get('success', False):
                validation_results['overall_success'] = False
            
            # Final Summary
            self._print_validation_summary(validation_results)
            
            return validation_results
            
        except Exception as e:
            print(f"❌ CRITICAL ERROR in usage table completeness validation: {e}")
            traceback.print_exc()
            validation_results['overall_success'] = False
            validation_results['error'] = str(e)
            return validation_results
        
        finally:
            self.db.close()
    
    def validate_required_fields(self) -> Dict[str, Any]:
        """
        Validate all required Epic A usage fields are present and populated
        
        Success Criteria:
        - All 8 required fields exist: player_id, week, snap_pct, route_pct, 
          target_share, carry_share, rz_touches, ez_targets
        - Fields have reasonable population rates
        - No critical missing data patterns
        """
        try:
            print("   Testing required fields presence...")
            
            # Get sample of usage records
            usage_records = self.db.query(PlayerUsage).filter(
                PlayerUsage.season == self.current_season
            ).limit(500).all()
            
            if not usage_records:
                return {
                    'success': False,
                    'error': 'No usage records found for current season'
                }
            
            field_analysis = {
                'total_records': len(usage_records),
                'field_stats': {},
                'missing_data_patterns': []
            }
            
            # Epic A required fields
            required_fields = [
                'player_id', 'week', 'snap_pct', 'route_pct', 
                'target_share', 'carry_share', 'rz_touches', 'ez_targets'
            ]
            
            # Initialize field statistics
            for field in required_fields:
                field_analysis['field_stats'][field] = {
                    'populated': 0,
                    'total': len(usage_records),
                    'population_rate': 0.0,
                    'non_zero_count': 0,
                    'non_zero_rate': 0.0
                }
            
            # Analyze field population
            for record in usage_records:
                for field in required_fields:
                    field_value = getattr(record, field, None)
                    
                    if field_value is not None:
                        field_analysis['field_stats'][field]['populated'] += 1
                        
                        # Check for meaningful data (non-zero values)
                        if isinstance(field_value, (int, float)) and field_value > 0:
                            field_analysis['field_stats'][field]['non_zero_count'] += 1
                        elif field in ['player_id', 'week'] and field_value:
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
            records_with_no_usage = 0
            for record in usage_records:
                usage_fields = ['snap_pct', 'route_pct', 'target_share', 'carry_share']
                if all(getattr(record, field, 0) in [None, 0] for field in usage_fields):
                    records_with_no_usage += 1
            
            no_usage_rate = records_with_no_usage / len(usage_records)
            
            # Success criteria
            success = (
                avg_field_population >= 0.8 and  # 80% field population
                len(critical_missing_fields) <= 1 and  # At most 1 critically missing field
                no_usage_rate <= 0.3  # No more than 30% records with no usage data
            )
            
            print(f"   ✓ Total usage records: {field_analysis['total_records']}")
            print(f"   ✓ Average field population: {avg_field_population:.1%}")
            print(f"   ✓ Records with no usage data: {no_usage_rate:.1%}")
            print(f"   ✓ Critical missing fields: {len(critical_missing_fields)}")
            
            for field, stats in field_analysis['field_stats'].items():
                print(f"   ✓ {field}: {stats['population_rate']:.1%} populated, {stats['non_zero_rate']:.1%} with data")
            
            if critical_missing_fields:
                print(f"   ⚠️  Critical missing fields: {critical_missing_fields}")
            
            return {
                'success': success,
                'total_records': field_analysis['total_records'],
                'avg_field_population': avg_field_population,
                'no_usage_data_rate': no_usage_rate,
                'critical_missing_fields': critical_missing_fields,
                'field_stats': field_analysis['field_stats']
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Required fields validation failed: {e}'
            }
    
    def validate_data_coverage(self) -> Dict[str, Any]:
        """
        Validate coverage of usage data across players, weeks, and positions
        
        Success Criteria:
        - Multiple weeks of data are present
        - Coverage across different player positions
        - Reasonable number of players with usage data
        """
        try:
            print("   Testing data coverage...")
            
            coverage_stats = {
                'weeks_covered': 0,
                'players_with_usage': 0,
                'position_coverage': {},
                'weekly_distribution': {},
                'total_usage_records': 0
            }
            
            # Get week coverage
            week_data = self.db.query(
                PlayerUsage.week,
                func.count(PlayerUsage.id).label('record_count'),
                func.count(distinct(PlayerUsage.player_id)).label('player_count')
            ).filter(
                PlayerUsage.season == self.current_season,
                PlayerUsage.week.isnot(None)
            ).group_by(PlayerUsage.week).all()
            
            coverage_stats['weeks_covered'] = len(week_data)
            coverage_stats['total_usage_records'] = sum(row.record_count for row in week_data)
            
            for week, record_count, player_count in week_data:
                coverage_stats['weekly_distribution'][week] = {
                    'records': record_count,
                    'players': player_count
                }
            
            # Get player coverage
            coverage_stats['players_with_usage'] = self.db.query(
                distinct(PlayerUsage.player_id)
            ).filter(
                PlayerUsage.season == self.current_season
            ).count()
            
            # Get position coverage
            position_data = self.db.query(
                Player.position,
                func.count(distinct(PlayerUsage.player_id)).label('player_count'),
                func.count(PlayerUsage.id).label('usage_records')
            ).join(PlayerUsage).filter(
                PlayerUsage.season == self.current_season
            ).group_by(Player.position).all()
            
            for position, player_count, usage_records in position_data:
                coverage_stats['position_coverage'][position] = {
                    'players': player_count,
                    'usage_records': usage_records
                }
            
            # Success criteria
            success = (
                coverage_stats['weeks_covered'] >= 4 and  # At least 4 weeks
                coverage_stats['players_with_usage'] >= 100 and  # At least 100 players
                len(coverage_stats['position_coverage']) >= 4 and  # At least 4 positions
                coverage_stats['total_usage_records'] >= 500  # At least 500 usage records
            )
            
            print(f"   ✓ Weeks covered: {coverage_stats['weeks_covered']}")
            print(f"   ✓ Players with usage: {coverage_stats['players_with_usage']}")
            print(f"   ✓ Positions covered: {len(coverage_stats['position_coverage'])}")
            print(f"   ✓ Total usage records: {coverage_stats['total_usage_records']}")
            
            # Show sample weekly distribution
            sample_weeks = sorted(list(coverage_stats['weekly_distribution'].keys()))[:5]
            for week in sample_weeks:
                week_data = coverage_stats['weekly_distribution'][week]
                print(f"   ✓ Week {week}: {week_data['records']} records, {week_data['players']} players")
            
            # Show position coverage
            for position, stats in list(coverage_stats['position_coverage'].items())[:4]:
                print(f"   ✓ {position}: {stats['players']} players, {stats['usage_records']} records")
            
            return {
                'success': success,
                'weeks_covered': coverage_stats['weeks_covered'],
                'players_with_usage': coverage_stats['players_with_usage'],
                'positions_covered': len(coverage_stats['position_coverage']),
                'total_usage_records': coverage_stats['total_usage_records'],
                'weekly_range': f"{min(coverage_stats['weekly_distribution'].keys())} - {max(coverage_stats['weekly_distribution'].keys())}" if coverage_stats['weekly_distribution'] else "No data",
                'position_coverage': coverage_stats['position_coverage']
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Data coverage validation failed: {e}'
            }
    
    def validate_value_ranges(self) -> Dict[str, Any]:
        """
        Validate usage data values are within reasonable ranges
        
        Success Criteria:
        - Percentage fields (snap_pct, route_pct, target_share, carry_share) are 0-100
        - Count fields (rz_touches, ez_targets) are reasonable
        - No extreme outliers that indicate data quality issues
        """
        try:
            print("   Testing value ranges...")
            
            # Get sample of usage data for analysis
            usage_data = self.db.query(PlayerUsage).filter(
                PlayerUsage.season == self.current_season,
                or_(
                    PlayerUsage.snap_pct.isnot(None),
                    PlayerUsage.route_pct.isnot(None),
                    PlayerUsage.target_share.isnot(None),
                    PlayerUsage.carry_share.isnot(None),
                    PlayerUsage.rz_touches.isnot(None),
                    PlayerUsage.ez_targets.isnot(None)
                )
            ).limit(1000).all()
            
            if not usage_data:
                return {
                    'success': False,
                    'error': 'No usage data with values found'
                }
            
            range_analysis = {
                'total_records_analyzed': len(usage_data),
                'field_analysis': {},
                'outliers': {},
                'value_distribution': {}
            }
            
            usage_fields = ['snap_pct', 'route_pct', 'target_share', 'carry_share', 'rz_touches', 'ez_targets']
            
            for field in usage_fields:
                field_values = []
                range_violations = 0
                extreme_outliers = []
                
                for record in usage_data:
                    value = getattr(record, field, None)
                    if value is not None:
                        field_values.append(value)
                        
                        # Check range violations
                        expected_range = self.value_ranges[field]
                        if value < expected_range['min'] or value > expected_range['max']:
                            range_violations += 1
                            
                            if value > expected_range['typical_max'] * 2:  # Extreme outlier
                                extreme_outliers.append({
                                    'player_id': record.player_id,
                                    'week': record.week,
                                    'value': value,
                                    'field': field
                                })
                
                if field_values:
                    # Calculate statistics
                    range_analysis['field_analysis'][field] = {
                        'count': len(field_values),
                        'min': min(field_values),
                        'max': max(field_values),
                        'mean': statistics.mean(field_values),
                        'median': statistics.median(field_values),
                        'range_violations': range_violations,
                        'range_violation_rate': range_violations / len(field_values),
                        'extreme_outliers': len(extreme_outliers)
                    }
                    
                    range_analysis['outliers'][field] = extreme_outliers[:3]  # Keep top 3
                    
                    # Distribution buckets
                    buckets = defaultdict(int)
                    for value in field_values:
                        if field in ['snap_pct', 'route_pct', 'target_share', 'carry_share']:
                            bucket = f"{int(value//20)*20}-{int(value//20)*20+19}%"
                        else:
                            bucket = f"{int(value//5)*5}-{int(value//5)*5+4}"
                        buckets[bucket] += 1
                    
                    range_analysis['value_distribution'][field] = dict(buckets)
                else:
                    range_analysis['field_analysis'][field] = {
                        'count': 0,
                        'error': 'No data found'
                    }
            
            # Calculate overall success
            total_violation_rate = 0
            total_fields_with_data = 0
            total_extreme_outliers = 0
            
            for field, analysis in range_analysis['field_analysis'].items():
                if 'range_violation_rate' in analysis:
                    total_violation_rate += analysis['range_violation_rate']
                    total_fields_with_data += 1
                    total_extreme_outliers += analysis['extreme_outliers']
            
            avg_violation_rate = total_violation_rate / total_fields_with_data if total_fields_with_data > 0 else 1
            
            success = (
                avg_violation_rate <= 0.05 and  # Less than 5% range violations
                total_extreme_outliers <= 10 and  # Few extreme outliers
                total_fields_with_data >= 4  # At least 4 fields have data
            )
            
            print(f"   ✓ Records analyzed: {range_analysis['total_records_analyzed']}")
            print(f"   ✓ Fields with data: {total_fields_with_data}/{len(usage_fields)}")
            print(f"   ✓ Average violation rate: {avg_violation_rate:.2%}")
            print(f"   ✓ Total extreme outliers: {total_extreme_outliers}")
            
            # Show field statistics
            for field, analysis in range_analysis['field_analysis'].items():
                if 'mean' in analysis:
                    print(f"   ✓ {field}: mean={analysis['mean']:.1f}, range=[{analysis['min']}-{analysis['max']}], violations={analysis['range_violation_rate']:.1%}")
                else:
                    print(f"   ⚠️  {field}: {analysis.get('error', 'No analysis available')}")
            
            return {
                'success': success,
                'total_records_analyzed': range_analysis['total_records_analyzed'],
                'fields_with_data': total_fields_with_data,
                'avg_violation_rate': avg_violation_rate,
                'total_extreme_outliers': total_extreme_outliers,
                'field_analysis': range_analysis['field_analysis'],
                'sample_outliers': {k: v for k, v in range_analysis['outliers'].items() if v}
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Value ranges validation failed: {e}'
            }
    
    def validate_position_specific_data(self) -> Dict[str, Any]:
        """
        Validate position-specific usage patterns make sense
        
        Success Criteria:
        - WR/TE have meaningful route_pct and target_share data
        - RB have meaningful carry_share data
        - Usage patterns align with NFL positional expectations
        """
        try:
            print("   Testing position-specific data...")
            
            position_analysis = {
                'position_patterns': {},
                'data_quality_by_position': {},
                'position_specific_errors': []
            }
            
            # Key positions to analyze
            key_positions = ['QB', 'RB', 'WR', 'TE']
            
            for position in key_positions:
                # Get usage data for this position
                position_usage = self.db.query(PlayerUsage).join(Player).filter(
                    Player.position == position,
                    PlayerUsage.season == self.current_season
                ).limit(100).all()
                
                if not position_usage:
                    position_analysis['position_patterns'][position] = {'error': 'No usage data found'}
                    continue
                
                # Analyze position-specific patterns
                patterns = {
                    'total_records': len(position_usage),
                    'snap_pct_data': 0,
                    'route_pct_data': 0,
                    'target_share_data': 0,
                    'carry_share_data': 0,
                    'rz_touches_data': 0,
                    'ez_targets_data': 0,
                    'expected_field_coverage': {}
                }
                
                for record in position_usage:
                    if record.snap_pct and record.snap_pct > 0:
                        patterns['snap_pct_data'] += 1
                    if record.route_pct and record.route_pct > 0:
                        patterns['route_pct_data'] += 1
                    if record.target_share and record.target_share > 0:
                        patterns['target_share_data'] += 1
                    if record.carry_share and record.carry_share > 0:
                        patterns['carry_share_data'] += 1
                    if record.rz_touches and record.rz_touches > 0:
                        patterns['rz_touches_data'] += 1
                    if record.ez_targets and record.ez_targets > 0:
                        patterns['ez_targets_data'] += 1
                
                # Calculate coverage rates
                for field in ['snap_pct_data', 'route_pct_data', 'target_share_data', 'carry_share_data', 'rz_touches_data', 'ez_targets_data']:
                    field_name = field.replace('_data', '')
                    patterns['expected_field_coverage'][field_name] = patterns[field] / patterns['total_records']
                
                position_analysis['position_patterns'][position] = patterns
                
                # Position-specific validation
                quality_score = 0
                max_score = 0
                
                if position in ['WR', 'TE']:
                    # WR/TE should have route and target data
                    route_coverage = patterns['expected_field_coverage']['route_pct']
                    target_coverage = patterns['expected_field_coverage']['target_share']
                    
                    quality_score += route_coverage * 2  # Double weight for routes
                    quality_score += target_coverage * 2  # Double weight for targets
                    max_score += 4
                    
                    if route_coverage < 0.3:
                        position_analysis['position_specific_errors'].append(
                            f"{position}: Low route_pct coverage ({route_coverage:.1%})"
                        )
                
                elif position == 'RB':
                    # RB should have carry data
                    carry_coverage = patterns['expected_field_coverage']['carry_share']
                    
                    quality_score += carry_coverage * 3  # Triple weight for carries
                    max_score += 3
                    
                    if carry_coverage < 0.4:
                        position_analysis['position_specific_errors'].append(
                            f"{position}: Low carry_share coverage ({carry_coverage:.1%})"
                        )
                
                # Common fields for all positions
                snap_coverage = patterns['expected_field_coverage']['snap_pct']
                quality_score += snap_coverage
                max_score += 1
                
                if snap_coverage < 0.5:
                    position_analysis['position_specific_errors'].append(
                        f"{position}: Low snap_pct coverage ({snap_coverage:.1%})"
                    )
                
                position_analysis['data_quality_by_position'][position] = {
                    'quality_score': quality_score / max_score if max_score > 0 else 0,
                    'records_analyzed': patterns['total_records']
                }
            
            # Calculate overall success
            total_quality_score = 0
            positions_with_data = 0
            
            for position, quality_data in position_analysis['data_quality_by_position'].items():
                if 'quality_score' in quality_data:
                    total_quality_score += quality_data['quality_score']
                    positions_with_data += 1
            
            avg_quality_score = total_quality_score / positions_with_data if positions_with_data > 0 else 0
            
            success = (
                positions_with_data >= 3 and  # At least 3 positions have data
                avg_quality_score >= 0.6 and  # 60% quality score
                len(position_analysis['position_specific_errors']) <= 3  # Few position-specific errors
            )
            
            print(f"   ✓ Positions with data: {positions_with_data}/{len(key_positions)}")
            print(f"   ✓ Average quality score: {avg_quality_score:.1%}")
            print(f"   ✓ Position-specific errors: {len(position_analysis['position_specific_errors'])}")
            
            # Show position breakdown
            for position, quality_data in position_analysis['data_quality_by_position'].items():
                print(f"   ✓ {position}: {quality_data['quality_score']:.1%} quality ({quality_data['records_analyzed']} records)")
            
            if position_analysis['position_specific_errors']:
                print(f"   ⚠️  Sample errors: {position_analysis['position_specific_errors'][:2]}")
            
            return {
                'success': success,
                'positions_with_data': positions_with_data,
                'avg_quality_score': avg_quality_score,
                'position_specific_errors': len(position_analysis['position_specific_errors']),
                'position_patterns': position_analysis['position_patterns'],
                'data_quality_by_position': position_analysis['data_quality_by_position']
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Position-specific data validation failed: {e}'
            }
    
    def validate_multi_week_consistency(self) -> Dict[str, Any]:
        """
        Validate consistency of usage data across multiple weeks
        
        Success Criteria:
        - Players have usage data across multiple weeks
        - Usage patterns show reasonable week-to-week variation
        - No unexplained data gaps
        """
        try:
            print("   Testing multi-week consistency...")
            
            # Find players with multi-week usage data
            multi_week_players = self.db.query(
                PlayerUsage.player_id,
                func.count(distinct(PlayerUsage.week)).label('week_count'),
                func.min(PlayerUsage.week).label('first_week'),
                func.max(PlayerUsage.week).label('last_week'),
                func.count(PlayerUsage.id).label('total_records')
            ).filter(
                PlayerUsage.season == self.current_season
            ).group_by(PlayerUsage.player_id).having(
                func.count(distinct(PlayerUsage.week)) >= 2
            ).limit(50).all()
            
            consistency_analysis = {
                'players_with_multi_week_data': len(multi_week_players),
                'avg_weeks_per_player': 0,
                'consistency_scores': [],
                'gap_analysis': {},
                'sample_player_patterns': []
            }
            
            if not multi_week_players:
                return {
                    'success': False,
                    'error': 'No players found with multi-week usage data'
                }
            
            total_weeks = sum(player.week_count for player in multi_week_players)
            consistency_analysis['avg_weeks_per_player'] = total_weeks / len(multi_week_players)
            
            # Analyze consistency for sample players
            for player_id, week_count, first_week, last_week, total_records in multi_week_players[:10]:
                # Get all usage records for this player
                player_usage = self.db.query(PlayerUsage).filter(
                    PlayerUsage.player_id == player_id,
                    PlayerUsage.season == self.current_season
                ).order_by(PlayerUsage.week).all()
                
                # Calculate consistency metrics
                snap_pcts = [r.snap_pct for r in player_usage if r.snap_pct is not None]
                
                consistency_score = 0
                if len(snap_pcts) >= 2:
                    # Calculate coefficient of variation (lower = more consistent)
                    mean_snap = statistics.mean(snap_pcts)
                    if mean_snap > 0:
                        stdev_snap = statistics.stdev(snap_pcts) if len(snap_pcts) > 1 else 0
                        cv = stdev_snap / mean_snap
                        consistency_score = max(0, 1 - cv)  # Convert to 0-1 scale (higher = better)
                
                consistency_analysis['consistency_scores'].append(consistency_score)
                
                # Check for data gaps
                weeks_represented = set(r.week for r in player_usage if r.week)
                expected_weeks = set(range(first_week, last_week + 1))
                missing_weeks = expected_weeks - weeks_represented
                
                if len(missing_weeks) > 0:
                    if 'gap_patterns' not in consistency_analysis['gap_analysis']:
                        consistency_analysis['gap_analysis']['gap_patterns'] = []
                    consistency_analysis['gap_analysis']['gap_patterns'].append({
                        'player_id': player_id,
                        'missing_weeks': list(missing_weeks),
                        'gap_count': len(missing_weeks),
                        'week_range': f"{first_week}-{last_week}"
                    })
                
                # Store sample pattern
                if len(consistency_analysis['sample_player_patterns']) < 5:
                    consistency_analysis['sample_player_patterns'].append({
                        'player_id': player_id,
                        'weeks_tracked': week_count,
                        'week_range': f"{first_week}-{last_week}",
                        'consistency_score': consistency_score,
                        'total_records': total_records
                    })
            
            # Calculate overall metrics
            avg_consistency = statistics.mean(consistency_analysis['consistency_scores']) if consistency_analysis['consistency_scores'] else 0
            gap_rate = len(consistency_analysis['gap_analysis'].get('gap_patterns', [])) / len(multi_week_players)
            
            success = (
                consistency_analysis['players_with_multi_week_data'] >= 10 and  # At least 10 players
                consistency_analysis['avg_weeks_per_player'] >= 3 and  # Average 3+ weeks per player
                avg_consistency >= 0.4 and  # Reasonable consistency
                gap_rate <= 0.3  # Less than 30% have significant gaps
            )
            
            print(f"   ✓ Players with multi-week data: {consistency_analysis['players_with_multi_week_data']}")
            print(f"   ✓ Average weeks per player: {consistency_analysis['avg_weeks_per_player']:.1f}")
            print(f"   ✓ Average consistency score: {avg_consistency:.2f}")
            print(f"   ✓ Gap rate: {gap_rate:.1%}")
            
            # Show sample patterns
            for pattern in consistency_analysis['sample_player_patterns'][:3]:
                print(f"   ✓ Player {pattern['player_id']}: {pattern['week_range']} ({pattern['weeks_tracked']} weeks, {pattern['consistency_score']:.2f} consistency)")
            
            return {
                'success': success,
                'players_with_multi_week_data': consistency_analysis['players_with_multi_week_data'],
                'avg_weeks_per_player': consistency_analysis['avg_weeks_per_player'],
                'avg_consistency_score': avg_consistency,
                'gap_rate': gap_rate,
                'sample_player_patterns': consistency_analysis['sample_player_patterns'][:3]
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Multi-week consistency validation failed: {e}'
            }
    
    def validate_nfl_data_integration(self) -> Dict[str, Any]:
        """
        Validate quality of NFL data integration in usage table
        
        Success Criteria:
        - Usage data aligns with known NFL players and teams
        - Data freshness is reasonable
        - Integration with external NFL data sources appears successful
        """
        try:
            print("   Testing NFL data integration quality...")
            
            integration_analysis = {
                'nfl_teams_represented': 0,
                'recent_weeks_covered': 0,
                'known_player_coverage': 0,
                'data_freshness_score': 0,
                'integration_quality_indicators': {}
            }
            
            # Check NFL team representation
            nfl_teams = self.db.query(
                distinct(Player.team)
            ).join(PlayerUsage).filter(
                PlayerUsage.season == self.current_season,
                Player.team.isnot(None)
            ).all()
            
            integration_analysis['nfl_teams_represented'] = len([team[0] for team in nfl_teams if team[0]])
            
            # Check recent weeks coverage (assuming current season is active)
            recent_weeks = self.db.query(
                distinct(PlayerUsage.week)
            ).filter(
                PlayerUsage.season == self.current_season,
                PlayerUsage.week >= 1  # Current season weeks
            ).all()
            
            integration_analysis['recent_weeks_covered'] = len(recent_weeks)
            
            # Check coverage of well-known players
            known_players = ['Josh Allen', 'Christian McCaffrey', 'Cooper Kupp', 'Travis Kelce', 'Davante Adams']
            
            known_player_usage_count = 0
            for player_name in known_players:
                usage_exists = self.db.query(PlayerUsage).join(Player).filter(
                    Player.name.like(f'%{player_name}%'),
                    PlayerUsage.season == self.current_season
                ).first()
                
                if usage_exists:
                    known_player_usage_count += 1
            
            integration_analysis['known_player_coverage'] = known_player_usage_count
            
            # Data freshness analysis (check for recent data)
            latest_usage_date = self.db.query(
                func.max(PlayerUsage.created_at)
            ).filter(
                PlayerUsage.season == self.current_season
            ).scalar()
            
            if latest_usage_date:
                days_since_update = (datetime.utcnow() - latest_usage_date).days
                integration_analysis['data_freshness_score'] = max(0, 1 - (days_since_update / 30))  # Score decreases over 30 days
            
            # Integration quality indicators
            total_usage_records = self.db.query(PlayerUsage).filter(
                PlayerUsage.season == self.current_season
            ).count()
            
            integration_analysis['integration_quality_indicators'] = {
                'total_usage_records': total_usage_records,
                'records_with_snap_data': self.db.query(PlayerUsage).filter(
                    PlayerUsage.season == self.current_season,
                    PlayerUsage.snap_pct.isnot(None),
                    PlayerUsage.snap_pct > 0
                ).count(),
                'records_with_target_data': self.db.query(PlayerUsage).filter(
                    PlayerUsage.season == self.current_season,
                    PlayerUsage.target_share.isnot(None),
                    PlayerUsage.target_share > 0
                ).count()
            }
            
            # Calculate success metrics
            team_coverage_rate = integration_analysis['nfl_teams_represented'] / 32  # 32 NFL teams
            known_player_rate = integration_analysis['known_player_coverage'] / len(known_players)
            
            success = (
                integration_analysis['nfl_teams_represented'] >= 20 and  # At least 20 NFL teams
                integration_analysis['recent_weeks_covered'] >= 3 and  # At least 3 recent weeks
                known_player_rate >= 0.4 and  # At least 40% of known players
                integration_analysis['data_freshness_score'] >= 0.5  # Data not too stale
            )
            
            print(f"   ✓ NFL teams represented: {integration_analysis['nfl_teams_represented']}/32 ({team_coverage_rate:.1%})")
            print(f"   ✓ Recent weeks covered: {integration_analysis['recent_weeks_covered']}")
            print(f"   ✓ Known player coverage: {integration_analysis['known_player_coverage']}/{len(known_players)} ({known_player_rate:.1%})")
            print(f"   ✓ Data freshness score: {integration_analysis['data_freshness_score']:.2f}")
            print(f"   ✓ Total usage records: {integration_analysis['integration_quality_indicators']['total_usage_records']}")
            
            return {
                'success': success,
                'nfl_teams_represented': integration_analysis['nfl_teams_represented'],
                'team_coverage_rate': team_coverage_rate,
                'recent_weeks_covered': integration_analysis['recent_weeks_covered'],
                'known_player_coverage_rate': known_player_rate,
                'data_freshness_score': integration_analysis['data_freshness_score'],
                'integration_quality_indicators': integration_analysis['integration_quality_indicators']
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'NFL data integration validation failed: {e}'
            }
    
    def _print_validation_summary(self, results: Dict[str, Any]):
        """Print comprehensive validation summary"""
        print("\n" + "=" * 80)
        print("USAGE TABLE COMPLETENESS VALIDATION SUMMARY")
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
        print(f"\n🎯 Key Usage Table Metrics:")
        
        if 'required_fields' in results.get('validations', {}):
            fields_test = results['validations']['required_fields']
            print(f"  • Field Population: {fields_test.get('avg_field_population', 0):.1%}")
            print(f"  • Usage Records: {fields_test.get('total_records', 0)}")
        
        if 'data_coverage' in results.get('validations', {}):
            coverage_test = results['validations']['data_coverage']
            print(f"  • Weeks Covered: {coverage_test.get('weeks_covered', 0)}")
            print(f"  • Players with Usage: {coverage_test.get('players_with_usage', 0)}")
        
        if 'value_ranges' in results.get('validations', {}):
            ranges_test = results['validations']['value_ranges']
            print(f"  • Value Range Violations: {ranges_test.get('avg_violation_rate', 0):.1%}")
        
        if 'nfl_data_integration' in results.get('validations', {}):
            integration_test = results['validations']['nfl_data_integration']
            print(f"  • NFL Teams Coverage: {integration_test.get('team_coverage_rate', 0):.1%}")
        
        print(f"\n{'🎉 Usage Table Completeness is FULLY VALIDATED!' if overall_success else '⚠️  Usage Table Completeness needs attention'}")
        print("=" * 80)

def main():
    """Run usage table completeness validation tests"""
    print("Starting Usage Table Completeness Validation...")
    
    try:
        validator = UsageTableCompletenessValidator()
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