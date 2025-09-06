#!/usr/bin/env python3
"""
Roster Snapshots Persistence Validation Tests

Epic A US-A1 Acceptance Criteria:
"Roster snapshots persist (league_id, team_id, player_id, week, slot)."

Data Contract: platform, league_id, team_id, week, player_id, slot, synced_at

This script provides comprehensive validation of roster snapshot persistence
across multiple platforms with all required fields and data integrity checks.
"""

import os
import sys
import traceback
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from collections import defaultdict

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.config import get_config
from src.database import SessionLocal, Player, RosterEntry
from src.utils.player_id_mapper import PlayerIDMapper
from sqlalchemy import func, distinct

class RosterSnapshotsPersistenceValidator:
    """
    Comprehensive validation of roster snapshots persistence functionality
    """
    
    def __init__(self):
        self.config = get_config()
        self.db = SessionLocal()
        self.player_mapper = PlayerIDMapper()
        
        # Test configuration with real league data
        self.test_leagues = [
            {"id": "1257071160403709954", "platform": "sleeper", "name": "Sleeper League"},
            # Add MFL test league if available
            # {"id": "73756", "platform": "mfl", "name": "MFL League"}
        ]
        
        self.current_season = 2025
        
        print("=" * 80)
        print("ROSTER SNAPSHOTS PERSISTENCE VALIDATION")
        print("Epic A US-A1: Roster Snapshots Data Persistence Testing")
        print("=" * 80)
    
    def run_all_validations(self) -> Dict[str, Any]:
        """
        Execute all roster snapshots persistence validation tests
        
        Returns comprehensive validation results
        """
        validation_results = {
            'overall_success': True,
            'timestamp': datetime.utcnow().isoformat(),
            'database': self.config.DATABASE_URL,
            'leagues_tested': len(self.test_leagues),
            'validations': {}
        }
        
        try:
            print(f"\n🗂️  Running Roster Snapshots Persistence Validations")
            print(f"Database: {self.config.DATABASE_URL}")
            print(f"Leagues to test: {len(self.test_leagues)}")
            print("-" * 60)
            
            # Validation 1: Required Fields Presence
            print("\n📋 VALIDATION 1: Required Fields Presence")
            result_1 = self.validate_required_fields()
            validation_results['validations']['required_fields'] = result_1
            if not result_1.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 2: Data Completeness
            print("\n📊 VALIDATION 2: Data Completeness")
            result_2 = self.validate_data_completeness()
            validation_results['validations']['data_completeness'] = result_2
            if not result_2.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 3: Multi-Week Persistence
            print("\n📅 VALIDATION 3: Multi-Week Persistence")
            result_3 = self.validate_multi_week_persistence()
            validation_results['validations']['multi_week_persistence'] = result_3
            if not result_3.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 4: Platform-Specific Data
            print("\n🔗 VALIDATION 4: Platform-Specific Data")
            result_4 = self.validate_platform_specific_data()
            validation_results['validations']['platform_specific_data'] = result_4
            if not result_4.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 5: Data Integrity
            print("\n🔍 VALIDATION 5: Data Integrity")
            result_5 = self.validate_data_integrity()
            validation_results['validations']['data_integrity'] = result_5
            if not result_5.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 6: Query Performance
            print("\n⚡ VALIDATION 6: Query Performance")
            result_6 = self.validate_query_performance()
            validation_results['validations']['query_performance'] = result_6
            if not result_6.get('success', False):
                validation_results['overall_success'] = False
            
            # Final Summary
            self._print_validation_summary(validation_results)
            
            return validation_results
            
        except Exception as e:
            print(f"❌ CRITICAL ERROR in roster snapshots persistence validation: {e}")
            traceback.print_exc()
            validation_results['overall_success'] = False
            validation_results['error'] = str(e)
            return validation_results
        
        finally:
            self.db.close()
    
    def validate_required_fields(self) -> Dict[str, Any]:
        """
        Validate all required fields are present in roster snapshots
        
        Success Criteria:
        - platform field is populated
        - league_id field is populated
        - team_id field is populated (user_id in RosterEntry)
        - week field is populated
        - player_id field is populated
        - slot field is populated
        - synced_at field is populated (created_at in RosterEntry)
        """
        try:
            print("   Testing required fields presence...")
            
            # Get sample of roster entries
            roster_entries = self.db.query(RosterEntry).limit(100).all()
            
            if not roster_entries:
                return {
                    'success': False,
                    'error': 'No roster entries found in database'
                }
            
            field_validation = {
                'total_entries': len(roster_entries),
                'field_stats': {},
                'missing_field_entries': []
            }
            
            # Required fields mapping (RosterEntry field -> Epic A requirement)
            required_fields = {
                'league_id': 'league_id',
                'user_id': 'team_id', 
                'player_id': 'player_id',
                'week': 'week',
                'slot': 'slot',
                'created_at': 'synced_at',
                'platform': 'platform'
            }
            
            for field_name, epic_name in required_fields.items():
                field_validation['field_stats'][epic_name] = {
                    'populated': 0,
                    'total': len(roster_entries),
                    'population_rate': 0.0
                }
            
            # Check field population
            for entry in roster_entries:
                entry_missing_fields = []
                
                for field_name, epic_name in required_fields.items():
                    field_value = getattr(entry, field_name, None)
                    
                    if field_value is not None and field_value != '':
                        field_validation['field_stats'][epic_name]['populated'] += 1
                    else:
                        entry_missing_fields.append(epic_name)
                
                if entry_missing_fields:
                    field_validation['missing_field_entries'].append({
                        'entry_id': entry.id,
                        'league_id': entry.league_id,
                        'missing_fields': entry_missing_fields
                    })
            
            # Calculate population rates
            total_field_score = 0
            for epic_name, stats in field_validation['field_stats'].items():
                stats['population_rate'] = stats['populated'] / stats['total']
                total_field_score += stats['population_rate']
            
            avg_field_population = total_field_score / len(field_validation['field_stats'])
            success = avg_field_population >= 0.9  # 90% field population required
            
            print(f"   ✓ Total roster entries: {field_validation['total_entries']}")
            print(f"   ✓ Average field population: {avg_field_population:.1%}")
            
            for epic_name, stats in field_validation['field_stats'].items():
                print(f"   ✓ {epic_name}: {stats['population_rate']:.1%} populated")
            
            print(f"   ✓ Entries with missing fields: {len(field_validation['missing_field_entries'])}")
            
            return {
                'success': success,
                'total_entries': field_validation['total_entries'],
                'avg_field_population': avg_field_population,
                'field_stats': field_validation['field_stats'],
                'entries_with_missing_fields': len(field_validation['missing_field_entries']),
                'sample_missing_entries': field_validation['missing_field_entries'][:3]
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Required fields validation failed: {e}'
            }
    
    def validate_data_completeness(self) -> Dict[str, Any]:
        """
        Validate completeness of roster snapshot data across leagues and weeks
        
        Success Criteria:
        - Multiple weeks of data are present
        - Multiple leagues have roster data
        - Player counts per team are reasonable (8-16 players typical)
        """
        try:
            print("   Testing data completeness...")
            
            # Analyze data distribution
            completeness_stats = {
                'leagues_with_data': 0,
                'weeks_with_data': 0,
                'teams_with_data': 0,
                'total_roster_entries': 0,
                'avg_players_per_team': 0,
                'league_distribution': {},
                'week_distribution': {},
                'team_size_distribution': {}
            }
            
            # Get league distribution
            league_data = self.db.query(
                RosterEntry.league_id,
                func.count(RosterEntry.id).label('entry_count'),
                func.count(distinct(RosterEntry.user_id)).label('team_count'),
                func.count(distinct(RosterEntry.week)).label('week_count')
            ).filter(
                RosterEntry.is_active == True
            ).group_by(RosterEntry.league_id).all()
            
            completeness_stats['leagues_with_data'] = len(league_data)
            
            for league_id, entry_count, team_count, week_count in league_data:
                completeness_stats['league_distribution'][league_id] = {
                    'entries': entry_count,
                    'teams': team_count,
                    'weeks': week_count
                }
                completeness_stats['total_roster_entries'] += entry_count
            
            # Get week distribution
            week_data = self.db.query(
                RosterEntry.week,
                func.count(RosterEntry.id).label('entry_count')
            ).filter(
                RosterEntry.is_active == True,
                RosterEntry.week.isnot(None)
            ).group_by(RosterEntry.week).all()
            
            completeness_stats['weeks_with_data'] = len(week_data)
            
            for week, entry_count in week_data:
                completeness_stats['week_distribution'][week] = entry_count
            
            # Get team size distribution
            team_size_data = self.db.query(
                RosterEntry.league_id,
                RosterEntry.user_id,
                func.count(RosterEntry.player_id).label('roster_size')
            ).filter(
                RosterEntry.is_active == True
            ).group_by(RosterEntry.league_id, RosterEntry.user_id).all()
            
            completeness_stats['teams_with_data'] = len(team_size_data)
            
            team_sizes = []
            for league_id, user_id, roster_size in team_size_data:
                team_sizes.append(roster_size)
                
                size_bucket = f"{roster_size//5 * 5}-{roster_size//5 * 5 + 4}"
                if size_bucket not in completeness_stats['team_size_distribution']:
                    completeness_stats['team_size_distribution'][size_bucket] = 0
                completeness_stats['team_size_distribution'][size_bucket] += 1
            
            if team_sizes:
                completeness_stats['avg_players_per_team'] = sum(team_sizes) / len(team_sizes)
            
            # Success criteria
            success = (
                completeness_stats['leagues_with_data'] >= 1 and
                completeness_stats['weeks_with_data'] >= 2 and  # At least 2 weeks
                completeness_stats['teams_with_data'] >= 4 and  # At least 4 teams
                8 <= completeness_stats['avg_players_per_team'] <= 20  # Reasonable roster sizes
            )
            
            print(f"   ✓ Leagues with data: {completeness_stats['leagues_with_data']}")
            print(f"   ✓ Weeks with data: {completeness_stats['weeks_with_data']}")
            print(f"   ✓ Teams with data: {completeness_stats['teams_with_data']}")
            print(f"   ✓ Total roster entries: {completeness_stats['total_roster_entries']}")
            print(f"   ✓ Avg players per team: {completeness_stats['avg_players_per_team']:.1f}")
            
            # Show sample distribution
            sample_weeks = sorted(list(completeness_stats['week_distribution'].keys()))[:5]
            print(f"   ✓ Sample weeks: {sample_weeks}")
            
            return {
                'success': success,
                'leagues_with_data': completeness_stats['leagues_with_data'],
                'weeks_with_data': completeness_stats['weeks_with_data'],
                'teams_with_data': completeness_stats['teams_with_data'],
                'total_roster_entries': completeness_stats['total_roster_entries'],
                'avg_players_per_team': completeness_stats['avg_players_per_team'],
                'sample_league_distribution': dict(list(completeness_stats['league_distribution'].items())[:3]),
                'week_range': f"{min(completeness_stats['week_distribution'].keys())} - {max(completeness_stats['week_distribution'].keys())}" if completeness_stats['week_distribution'] else "No data"
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Data completeness validation failed: {e}'
            }
    
    def validate_multi_week_persistence(self) -> Dict[str, Any]:
        """
        Validate roster snapshots persist across multiple weeks
        
        Success Criteria:
        - Same teams have roster data across multiple weeks
        - Player roster changes are tracked between weeks
        - Historical snapshots are maintained
        """
        try:
            print("   Testing multi-week persistence...")
            
            # Get teams that have data across multiple weeks
            multi_week_teams = self.db.query(
                RosterEntry.league_id,
                RosterEntry.user_id,
                func.count(distinct(RosterEntry.week)).label('week_count'),
                func.min(RosterEntry.week).label('first_week'),
                func.max(RosterEntry.week).label('last_week'),
                func.count(RosterEntry.id).label('total_entries')
            ).filter(
                RosterEntry.is_active == True,
                RosterEntry.week.isnot(None)
            ).group_by(
                RosterEntry.league_id, RosterEntry.user_id
            ).having(
                func.count(distinct(RosterEntry.week)) >= 2
            ).all()
            
            persistence_stats = {
                'teams_with_multi_week_data': len(multi_week_teams),
                'total_weeks_tracked': 0,
                'roster_changes_detected': 0,
                'persistence_examples': []
            }
            
            if not multi_week_teams:
                return {
                    'success': False,
                    'error': 'No teams found with multi-week roster data'
                }
            
            # Analyze persistence for sample teams
            sample_teams = multi_week_teams[:5]
            
            for league_id, user_id, week_count, first_week, last_week, total_entries in sample_teams:
                persistence_stats['total_weeks_tracked'] += week_count
                
                # Get roster changes for this team
                team_rosters_by_week = {}
                team_entries = self.db.query(RosterEntry).filter(
                    RosterEntry.league_id == league_id,
                    RosterEntry.user_id == user_id,
                    RosterEntry.is_active == True
                ).order_by(RosterEntry.week).all()
                
                for entry in team_entries:
                    week = entry.week
                    if week not in team_rosters_by_week:
                        team_rosters_by_week[week] = set()
                    team_rosters_by_week[week].add(entry.player_id)
                
                # Count roster changes between consecutive weeks
                weeks = sorted(team_rosters_by_week.keys())
                changes = 0
                
                for i in range(1, len(weeks)):
                    prev_week = weeks[i-1]
                    curr_week = weeks[i]
                    
                    prev_roster = team_rosters_by_week[prev_week]
                    curr_roster = team_rosters_by_week[curr_week]
                    
                    if prev_roster != curr_roster:
                        changes += 1
                
                persistence_stats['roster_changes_detected'] += changes
                
                persistence_stats['persistence_examples'].append({
                    'league_id': league_id,
                    'user_id': user_id,
                    'weeks_tracked': week_count,
                    'week_range': f"{first_week}-{last_week}",
                    'total_entries': total_entries,
                    'roster_changes': changes
                })
            
            # Success criteria
            avg_weeks_per_team = persistence_stats['total_weeks_tracked'] / len(sample_teams) if sample_teams else 0
            
            success = (
                persistence_stats['teams_with_multi_week_data'] >= 3 and  # At least 3 teams
                avg_weeks_per_team >= 2.5 and  # Average 2.5+ weeks per team
                persistence_stats['roster_changes_detected'] >= 1  # Some roster activity
            )
            
            print(f"   ✓ Teams with multi-week data: {persistence_stats['teams_with_multi_week_data']}")
            print(f"   ✓ Average weeks per team: {avg_weeks_per_team:.1f}")
            print(f"   ✓ Roster changes detected: {persistence_stats['roster_changes_detected']}")
            
            # Show persistence examples
            for example in persistence_stats['persistence_examples'][:3]:
                print(f"   ✓ Team example: {example['week_range']} ({example['weeks_tracked']} weeks, {example['roster_changes']} changes)")
            
            return {
                'success': success,
                'teams_with_multi_week_data': persistence_stats['teams_with_multi_week_data'],
                'avg_weeks_per_team': avg_weeks_per_team,
                'roster_changes_detected': persistence_stats['roster_changes_detected'],
                'persistence_examples': persistence_stats['persistence_examples'][:3]
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Multi-week persistence validation failed: {e}'
            }
    
    def validate_platform_specific_data(self) -> Dict[str, Any]:
        """
        Validate platform-specific roster data handling
        
        Success Criteria:
        - Platform field is correctly populated
        - Platform-specific roster formats are handled
        - Cross-platform player mapping works in roster context
        """
        try:
            print("   Testing platform-specific data...")
            
            # Analyze platform distribution
            platform_data = self.db.query(
                RosterEntry.platform,
                func.count(RosterEntry.id).label('entry_count'),
                func.count(distinct(RosterEntry.league_id)).label('league_count')
            ).filter(
                RosterEntry.is_active == True
            ).group_by(RosterEntry.platform).all()
            
            platform_stats = {
                'platforms_found': len(platform_data),
                'platform_distribution': {},
                'cross_platform_players': 0,
                'platform_consistency_errors': []
            }
            
            for platform, entry_count, league_count in platform_data:
                platform_stats['platform_distribution'][platform] = {
                    'entries': entry_count,
                    'leagues': league_count
                }
            
            # Check cross-platform player consistency
            # Find players that appear in rosters from multiple platforms
            cross_platform_query = self.db.query(
                RosterEntry.player_id,
                func.count(distinct(RosterEntry.platform)).label('platform_count'),
                func.string_agg(distinct(RosterEntry.platform), ',').label('platforms')
            ).filter(
                RosterEntry.is_active == True,
                RosterEntry.platform.isnot(None)
            ).group_by(RosterEntry.player_id).having(
                func.count(distinct(RosterEntry.platform)) > 1
            ).all()
            
            platform_stats['cross_platform_players'] = len(cross_platform_query)
            
            # Validate player data consistency across platforms
            for player_id, platform_count, platforms in cross_platform_query[:5]:
                try:
                    player = self.db.query(Player).filter(Player.id == player_id).first()
                    if player:
                        # Check if player has proper platform IDs
                        platform_list = platforms.split(',')
                        consistency_ok = True
                        
                        if 'sleeper' in platform_list and not player.sleeper_id:
                            consistency_ok = False
                        if 'mfl' in platform_list and not player.mfl_id:
                            consistency_ok = False
                        
                        if not consistency_ok:
                            platform_stats['platform_consistency_errors'].append({
                                'player_id': player_id,
                                'player_name': player.name,
                                'platforms': platforms,
                                'sleeper_id': player.sleeper_id,
                                'mfl_id': player.mfl_id
                            })
                
                except Exception as e:
                    platform_stats['platform_consistency_errors'].append({
                        'player_id': player_id,
                        'error': str(e)
                    })
            
            # Success criteria
            success = (
                platform_stats['platforms_found'] >= 1 and  # At least one platform
                len(platform_stats['platform_consistency_errors']) <= 2  # Few consistency errors
            )
            
            print(f"   ✓ Platforms found: {platform_stats['platforms_found']}")
            print(f"   ✓ Cross-platform players: {platform_stats['cross_platform_players']}")
            print(f"   ✓ Platform consistency errors: {len(platform_stats['platform_consistency_errors'])}")
            
            for platform, stats in platform_stats['platform_distribution'].items():
                print(f"   ✓ {platform}: {stats['entries']} entries, {stats['leagues']} leagues")
            
            return {
                'success': success,
                'platforms_found': platform_stats['platforms_found'],
                'cross_platform_players': platform_stats['cross_platform_players'],
                'platform_distribution': platform_stats['platform_distribution'],
                'platform_consistency_errors': len(platform_stats['platform_consistency_errors']),
                'sample_consistency_errors': platform_stats['platform_consistency_errors'][:2]
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Platform-specific data validation failed: {e}'
            }
    
    def validate_data_integrity(self) -> Dict[str, Any]:
        """
        Validate data integrity of roster snapshots
        
        Success Criteria:
        - No duplicate roster entries (same player, team, week, league)
        - Foreign key constraints are maintained
        - Data types are correct
        """
        try:
            print("   Testing data integrity...")
            
            integrity_stats = {
                'total_entries': 0,
                'duplicate_entries': 0,
                'orphaned_players': 0,
                'invalid_data_types': 0,
                'constraint_violations': []
            }
            
            # Count total entries
            integrity_stats['total_entries'] = self.db.query(RosterEntry).count()
            
            # Check for duplicate entries
            duplicate_query = self.db.query(
                RosterEntry.league_id,
                RosterEntry.user_id,
                RosterEntry.player_id,
                RosterEntry.week,
                func.count(RosterEntry.id).label('entry_count')
            ).group_by(
                RosterEntry.league_id,
                RosterEntry.user_id,
                RosterEntry.player_id,
                RosterEntry.week
            ).having(
                func.count(RosterEntry.id) > 1
            ).all()
            
            integrity_stats['duplicate_entries'] = len(duplicate_query)
            
            # Check for orphaned players (roster entries without corresponding player)
            orphaned_query = self.db.query(RosterEntry).outerjoin(Player).filter(
                Player.id.is_(None)
            ).all()
            
            integrity_stats['orphaned_players'] = len(orphaned_query)
            
            # Check data type validity (sample check)
            sample_entries = self.db.query(RosterEntry).limit(50).all()
            
            for entry in sample_entries:
                try:
                    # Check data types
                    if entry.league_id and not isinstance(entry.league_id, str):
                        integrity_stats['invalid_data_types'] += 1
                    if entry.week and not isinstance(entry.week, int):
                        integrity_stats['invalid_data_types'] += 1
                    if entry.player_id and not isinstance(entry.player_id, int):
                        integrity_stats['invalid_data_types'] += 1
                    
                    # Check constraint violations
                    if entry.week and (entry.week < 1 or entry.week > 18):
                        integrity_stats['constraint_violations'].append({
                            'entry_id': entry.id,
                            'violation': f'Invalid week: {entry.week}'
                        })
                    
                except Exception as e:
                    integrity_stats['constraint_violations'].append({
                        'entry_id': entry.id,
                        'violation': f'Data type error: {e}'
                    })
            
            # Success criteria
            duplicate_rate = integrity_stats['duplicate_entries'] / integrity_stats['total_entries'] if integrity_stats['total_entries'] > 0 else 0
            orphaned_rate = integrity_stats['orphaned_players'] / integrity_stats['total_entries'] if integrity_stats['total_entries'] > 0 else 0
            
            success = (
                duplicate_rate <= 0.01 and  # Less than 1% duplicates
                orphaned_rate <= 0.05 and  # Less than 5% orphaned
                integrity_stats['invalid_data_types'] <= 2 and  # Few data type errors
                len(integrity_stats['constraint_violations']) <= 3  # Few constraint violations
            )
            
            print(f"   ✓ Total entries: {integrity_stats['total_entries']}")
            print(f"   ✓ Duplicate rate: {duplicate_rate:.2%}")
            print(f"   ✓ Orphaned rate: {orphaned_rate:.2%}")
            print(f"   ✓ Data type errors: {integrity_stats['invalid_data_types']}")
            print(f"   ✓ Constraint violations: {len(integrity_stats['constraint_violations'])}")
            
            return {
                'success': success,
                'total_entries': integrity_stats['total_entries'],
                'duplicate_rate': duplicate_rate,
                'orphaned_rate': orphaned_rate,
                'data_type_errors': integrity_stats['invalid_data_types'],
                'constraint_violations': len(integrity_stats['constraint_violations']),
                'sample_violations': integrity_stats['constraint_violations'][:2]
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Data integrity validation failed: {e}'
            }
    
    def validate_query_performance(self) -> Dict[str, Any]:
        """
        Validate query performance for roster snapshots
        
        Success Criteria:
        - Common queries execute in reasonable time
        - Indexes are effective
        - Large result sets are handled efficiently
        """
        try:
            print("   Testing query performance...")
            
            import time
            
            performance_results = {
                'query_times': {},
                'total_queries_tested': 0
            }
            
            # Test common query patterns
            queries_to_test = [
                {
                    'name': 'league_roster_lookup',
                    'description': 'Get all rosters for a league',
                    'query': lambda: self.db.query(RosterEntry).filter(
                        RosterEntry.league_id == "1257071160403709954",
                        RosterEntry.is_active == True
                    ).all()
                },
                {
                    'name': 'team_roster_by_week',
                    'description': 'Get team roster for specific week',
                    'query': lambda: self.db.query(RosterEntry).filter(
                        RosterEntry.league_id == "1257071160403709954",
                        RosterEntry.week == 4,
                        RosterEntry.is_active == True
                    ).all()
                },
                {
                    'name': 'player_roster_history',
                    'description': 'Get roster history for a player',
                    'query': lambda: self.db.query(RosterEntry).join(Player).filter(
                        Player.name.like('%Josh Allen%'),
                        RosterEntry.is_active == True
                    ).all()
                },
                {
                    'name': 'weekly_roster_count',
                    'description': 'Count rosters by week',
                    'query': lambda: self.db.query(
                        RosterEntry.week,
                        func.count(RosterEntry.id)
                    ).group_by(RosterEntry.week).all()
                }
            ]
            
            for query_test in queries_to_test:
                try:
                    start_time = time.time()
                    results = query_test['query']()
                    end_time = time.time()
                    
                    query_time = end_time - start_time
                    result_count = len(results) if isinstance(results, list) else 1
                    
                    performance_results['query_times'][query_test['name']] = {
                        'time_seconds': query_time,
                        'result_count': result_count,
                        'description': query_test['description']
                    }
                    
                    performance_results['total_queries_tested'] += 1
                    
                except Exception as e:
                    performance_results['query_times'][query_test['name']] = {
                        'error': str(e),
                        'description': query_test['description']
                    }
            
            # Calculate success metrics
            successful_queries = [
                q for q in performance_results['query_times'].values()
                if 'time_seconds' in q
            ]
            
            if successful_queries:
                avg_query_time = sum(q['time_seconds'] for q in successful_queries) / len(successful_queries)
                max_query_time = max(q['time_seconds'] for q in successful_queries)
            else:
                avg_query_time = float('inf')
                max_query_time = float('inf')
            
            success = (
                len(successful_queries) >= 3 and  # At least 3 queries successful
                avg_query_time < 1.0 and  # Average < 1 second
                max_query_time < 5.0  # Max < 5 seconds
            )
            
            print(f"   ✓ Successful queries: {len(successful_queries)}/{performance_results['total_queries_tested']}")
            print(f"   ✓ Average query time: {avg_query_time:.3f}s")
            print(f"   ✓ Max query time: {max_query_time:.3f}s")
            
            for query_name, stats in performance_results['query_times'].items():
                if 'time_seconds' in stats:
                    print(f"   ✓ {query_name}: {stats['time_seconds']:.3f}s ({stats['result_count']} results)")
                else:
                    print(f"   ❌ {query_name}: {stats.get('error', 'Unknown error')}")
            
            return {
                'success': success,
                'successful_queries': len(successful_queries),
                'total_queries_tested': performance_results['total_queries_tested'],
                'avg_query_time_seconds': avg_query_time,
                'max_query_time_seconds': max_query_time,
                'query_details': performance_results['query_times']
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Query performance validation failed: {e}'
            }
    
    def _print_validation_summary(self, results: Dict[str, Any]):
        """Print comprehensive validation summary"""
        print("\n" + "=" * 80)
        print("ROSTER SNAPSHOTS PERSISTENCE VALIDATION SUMMARY")
        print("=" * 80)
        
        overall_success = results['overall_success']
        status_icon = "✅" if overall_success else "❌"
        
        print(f"\n{status_icon} OVERALL VALIDATION STATUS: {'PASS' if overall_success else 'FAIL'}")
        print(f"📅 Test Date: {results['timestamp']}")
        print(f"💾 Database: {results['database']}")
        print(f"🏈 Leagues Tested: {results['leagues_tested']}")
        
        print(f"\n📊 Individual Validation Results:")
        for validation_name, validation_result in results.get('validations', {}).items():
            success = validation_result.get('success', False)
            icon = "✅" if success else "❌"
            print(f"  {icon} {validation_name.replace('_', ' ').title()}: {'PASS' if success else 'FAIL'}")
            
            if not success and 'error' in validation_result:
                print(f"      Error: {validation_result['error']}")
        
        # Key metrics summary
        print(f"\n🎯 Key Roster Persistence Metrics:")
        
        if 'required_fields' in results.get('validations', {}):
            fields_test = results['validations']['required_fields']
            print(f"  • Field Population: {fields_test.get('avg_field_population', 0):.1%}")
        
        if 'data_completeness' in results.get('validations', {}):
            completeness_test = results['validations']['data_completeness']
            print(f"  • Leagues with Data: {completeness_test.get('leagues_with_data', 0)}")
            print(f"  • Weeks with Data: {completeness_test.get('weeks_with_data', 0)}")
            print(f"  • Teams with Data: {completeness_test.get('teams_with_data', 0)}")
        
        if 'data_integrity' in results.get('validations', {}):
            integrity_test = results['validations']['data_integrity']
            print(f"  • Duplicate Rate: {integrity_test.get('duplicate_rate', 0):.2%}")
            print(f"  • Orphaned Rate: {integrity_test.get('orphaned_rate', 0):.2%}")
        
        if 'query_performance' in results.get('validations', {}):
            perf_test = results['validations']['query_performance']
            print(f"  • Avg Query Time: {perf_test.get('avg_query_time_seconds', 0):.3f}s")
        
        print(f"\n{'🎉 Roster Snapshots Persistence is FULLY VALIDATED!' if overall_success else '⚠️  Roster Snapshots Persistence needs attention'}")
        print("=" * 80)

def main():
    """Run roster snapshots persistence validation tests"""
    print("Starting Roster Snapshots Persistence Validation...")
    
    try:
        validator = RosterSnapshotsPersistenceValidator()
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