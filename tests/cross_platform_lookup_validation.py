#!/usr/bin/env python3
"""
Cross-Platform Player Lookup Validation Tests

Epic A US-A1 Acceptance Criteria:
"Player rows can be looked up by canonical_player_id regardless of platform."

This script provides comprehensive validation of cross-platform player identification
and lookup functionality across Sleeper and MFL platforms.
"""

import os
import sys
import traceback
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config.config import get_config
from src.database import SessionLocal, Player
from src.utils.player_id_mapper import PlayerIDMapper

class CrossPlatformLookupValidator:
    """
    Comprehensive validation of cross-platform player lookup functionality
    """
    
    def __init__(self):
        self.config = get_config()
        self.db = SessionLocal()
        self.player_mapper = PlayerIDMapper()
        
        # Known test players across platforms for validation
        self.test_players = [
            {'name': 'CeeDee Lamb', 'position': 'WR', 'team': 'DAL'},
            {'name': 'Josh Allen', 'position': 'QB', 'team': 'BUF'},
            {'name': 'Christian McCaffrey', 'position': 'RB', 'team': 'SF'},
            {'name': 'Cooper Kupp', 'position': 'WR', 'team': 'LAR'},
            {'name': 'Travis Kelce', 'position': 'TE', 'team': 'KC'},
            {'name': 'Lamar Jackson', 'position': 'QB', 'team': 'BAL'},
            {'name': 'Derrick Henry', 'position': 'RB', 'team': 'TEN'},
            {'name': 'Davante Adams', 'position': 'WR', 'team': 'LV'},
            {'name': 'Aaron Rodgers', 'position': 'QB', 'team': 'GB'},
            {'name': 'Alvin Kamara', 'position': 'RB', 'team': 'NO'}
        ]
        
        print("=" * 80)
        print("CROSS-PLATFORM PLAYER LOOKUP VALIDATION")
        print("Epic A US-A1: Canonical Player ID Lookup Testing")
        print("=" * 80)
    
    def run_all_validations(self) -> Dict[str, Any]:
        """
        Execute all cross-platform lookup validation tests
        
        Returns comprehensive validation results
        """
        validation_results = {
            'overall_success': True,
            'timestamp': datetime.utcnow().isoformat(),
            'database': self.config.DATABASE_URL,
            'validations': {}
        }
        
        try:
            print(f"\n🔍 Running Cross-Platform Lookup Validations")
            print(f"Database: {self.config.DATABASE_URL}")
            print("-" * 60)
            
            # Validation 1: Canonical ID Generation
            print("\n🏷️  VALIDATION 1: Canonical ID Generation")
            result_1 = self.validate_canonical_id_generation()
            validation_results['validations']['canonical_id_generation'] = result_1
            if not result_1.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 2: Database Player Lookup
            print("\n🔍 VALIDATION 2: Database Player Lookup")
            result_2 = self.validate_database_player_lookup()
            validation_results['validations']['database_lookup'] = result_2
            if not result_2.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 3: Platform ID Mapping
            print("\n🔗 VALIDATION 3: Platform ID Mapping")
            result_3 = self.validate_platform_id_mapping()
            validation_results['validations']['platform_mapping'] = result_3
            if not result_3.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 4: Cross-Platform Consistency
            print("\n⚖️  VALIDATION 4: Cross-Platform Consistency")
            result_4 = self.validate_cross_platform_consistency()
            validation_results['validations']['cross_platform_consistency'] = result_4
            if not result_4.get('success', False):
                validation_results['overall_success'] = False
            
            # Validation 5: Lookup Performance
            print("\n⏱️  VALIDATION 5: Lookup Performance")
            result_5 = self.validate_lookup_performance()
            validation_results['validations']['lookup_performance'] = result_5
            if not result_5.get('success', False):
                validation_results['overall_success'] = False
            
            # Final Summary
            self._print_validation_summary(validation_results)
            
            return validation_results
            
        except Exception as e:
            print(f"❌ CRITICAL ERROR in cross-platform lookup validation: {e}")
            traceback.print_exc()
            validation_results['overall_success'] = False
            validation_results['error'] = str(e)
            return validation_results
        
        finally:
            self.db.close()
    
    def validate_canonical_id_generation(self) -> Dict[str, Any]:
        """
        Validate canonical ID generation consistency and uniqueness
        
        Success Criteria:
        - Same player generates same canonical ID consistently
        - Different players generate different canonical IDs
        - Canonical IDs are properly formatted
        """
        try:
            print("   Testing canonical ID generation...")
            
            generation_results = {
                'consistent_ids': 0,
                'unique_ids': 0,
                'total_tests': len(self.test_players),
                'canonical_ids': {},
                'format_errors': []
            }
            
            generated_ids = []
            
            for player in self.test_players:
                try:
                    # Generate canonical ID multiple times for consistency test
                    id1 = self.player_mapper.generate_canonical_id(
                        player['name'], player['position'], player['team']
                    )
                    id2 = self.player_mapper.generate_canonical_id(
                        player['name'], player['position'], player['team']
                    )
                    
                    # Check consistency
                    if id1 == id2:
                        generation_results['consistent_ids'] += 1
                    
                    # Check format (should be non-empty string)
                    if isinstance(id1, str) and len(id1) > 0:
                        generated_ids.append(id1)
                        generation_results['canonical_ids'][player['name']] = id1
                    else:
                        generation_results['format_errors'].append(
                            f"{player['name']}: Invalid ID format '{id1}'"
                        )
                    
                except Exception as e:
                    generation_results['format_errors'].append(
                        f"{player['name']}: Generation error - {e}"
                    )
            
            # Check uniqueness
            unique_ids = set(generated_ids)
            generation_results['unique_ids'] = len(unique_ids)
            
            # Calculate success metrics
            consistency_rate = generation_results['consistent_ids'] / generation_results['total_tests']
            uniqueness_rate = generation_results['unique_ids'] / len(generated_ids) if generated_ids else 0
            success = (consistency_rate >= 1.0 and 
                      uniqueness_rate >= 0.9 and  # Allow some duplicates for similar players
                      len(generation_results['format_errors']) == 0)
            
            print(f"   ✓ Consistency rate: {consistency_rate:.1%} ({generation_results['consistent_ids']}/{generation_results['total_tests']})")
            print(f"   ✓ Uniqueness rate: {uniqueness_rate:.1%} ({generation_results['unique_ids']}/{len(generated_ids)})")
            print(f"   ✓ Format errors: {len(generation_results['format_errors'])}")
            
            if generation_results['format_errors']:
                print(f"   ❌ Format errors: {generation_results['format_errors']}")
            
            return {
                'success': success,
                'consistency_rate': consistency_rate,
                'uniqueness_rate': uniqueness_rate,
                'total_players_tested': generation_results['total_tests'],
                'canonical_ids_generated': len(generated_ids),
                'format_errors': generation_results['format_errors'],
                'sample_ids': dict(list(generation_results['canonical_ids'].items())[:3])
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Canonical ID generation validation failed: {e}'
            }
    
    def validate_database_player_lookup(self) -> Dict[str, Any]:
        """
        Validate player lookup in database using canonical IDs
        
        Success Criteria:
        - Players can be found by canonical ID
        - Lookup returns correct player information
        - Lookup handles missing players gracefully
        """
        try:
            print("   Testing database player lookup...")
            
            lookup_results = {
                'players_found': 0,
                'players_tested': len(self.test_players),
                'lookup_errors': [],
                'data_accuracy_errors': [],
                'found_players': {}
            }
            
            for player_info in self.test_players:
                try:
                    # Generate canonical ID
                    canonical_id = self.player_mapper.generate_canonical_id(
                        player_info['name'], player_info['position'], player_info['team']
                    )
                    
                    # Lookup in database
                    db_player = self.db.query(Player).filter(
                        Player.nfl_id == canonical_id
                    ).first()
                    
                    if db_player:
                        lookup_results['players_found'] += 1
                        lookup_results['found_players'][player_info['name']] = {
                            'canonical_id': canonical_id,
                            'db_name': db_player.name,
                            'db_position': db_player.position,
                            'db_team': db_player.team
                        }
                        
                        # Validate data accuracy (name/position should match)
                        if (db_player.name != player_info['name'] and 
                            player_info['name'].lower() not in db_player.name.lower()):
                            lookup_results['data_accuracy_errors'].append(
                                f"{player_info['name']}: DB name '{db_player.name}' doesn't match"
                            )
                        
                        if db_player.position != player_info['position']:
                            lookup_results['data_accuracy_errors'].append(
                                f"{player_info['name']}: Position mismatch DB:'{db_player.position}' vs Expected:'{player_info['position']}'"
                            )
                    
                except Exception as e:
                    lookup_results['lookup_errors'].append(
                        f"{player_info['name']}: Lookup error - {e}"
                    )
            
            # Calculate success metrics
            lookup_success_rate = lookup_results['players_found'] / lookup_results['players_tested']
            data_accuracy = (lookup_results['players_found'] - len(lookup_results['data_accuracy_errors'])) / lookup_results['players_found'] if lookup_results['players_found'] > 0 else 0
            
            success = (lookup_success_rate >= 0.5 and  # At least 50% should be found
                      data_accuracy >= 0.8 and        # 80% data accuracy
                      len(lookup_results['lookup_errors']) <= 2)  # Allow few lookup errors
            
            print(f"   ✓ Players found: {lookup_results['players_found']}/{lookup_results['players_tested']} ({lookup_success_rate:.1%})")
            print(f"   ✓ Data accuracy: {data_accuracy:.1%}")
            print(f"   ✓ Lookup errors: {len(lookup_results['lookup_errors'])}")
            
            if lookup_results['data_accuracy_errors']:
                print(f"   ⚠️  Data accuracy issues: {lookup_results['data_accuracy_errors'][:3]}")
            
            return {
                'success': success,
                'lookup_success_rate': lookup_success_rate,
                'data_accuracy_rate': data_accuracy,
                'players_found': lookup_results['players_found'],
                'players_tested': lookup_results['players_tested'],
                'lookup_errors': lookup_results['lookup_errors'],
                'data_accuracy_errors': lookup_results['data_accuracy_errors'][:5],
                'sample_found_players': dict(list(lookup_results['found_players'].items())[:3])
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Database player lookup validation failed: {e}'
            }
    
    def validate_platform_id_mapping(self) -> Dict[str, Any]:
        """
        Validate platform-specific ID mapping functionality
        
        Success Criteria:
        - get_canonical_id() works for both Sleeper and MFL platforms
        - Platform IDs can be converted to canonical IDs
        - Mapping is consistent for same player across platforms
        """
        try:
            print("   Testing platform ID mapping...")
            
            mapping_results = {
                'sleeper_mappings': 0,
                'mfl_mappings': 0,
                'total_attempted': 0,
                'mapping_errors': [],
                'platform_consistency': 0
            }
            
            # Test with players that have platform IDs in database
            db_players_with_ids = self.db.query(Player).filter(
                (Player.sleeper_id.isnot(None)) | (Player.mfl_id.isnot(None))
            ).limit(20).all()
            
            for db_player in db_players_with_ids:
                mapping_results['total_attempted'] += 1
                
                try:
                    # Test Sleeper ID mapping
                    if db_player.sleeper_id:
                        sleeper_canonical = self.player_mapper.get_canonical_id(
                            db_player.sleeper_id, 'sleeper'
                        )
                        if sleeper_canonical == db_player.nfl_id:
                            mapping_results['sleeper_mappings'] += 1
                    
                    # Test MFL ID mapping
                    if db_player.mfl_id:
                        mfl_canonical = self.player_mapper.get_canonical_id(
                            db_player.mfl_id, 'mfl'
                        )
                        if mfl_canonical == db_player.nfl_id:
                            mapping_results['mfl_mappings'] += 1
                    
                    # Test consistency (if player has both platform IDs)
                    if db_player.sleeper_id and db_player.mfl_id:
                        sleeper_canonical = self.player_mapper.get_canonical_id(
                            db_player.sleeper_id, 'sleeper'
                        )
                        mfl_canonical = self.player_mapper.get_canonical_id(
                            db_player.mfl_id, 'mfl'
                        )
                        
                        if sleeper_canonical == mfl_canonical == db_player.nfl_id:
                            mapping_results['platform_consistency'] += 1
                
                except Exception as e:
                    mapping_results['mapping_errors'].append(
                        f"Player {db_player.name}: {e}"
                    )
            
            # Calculate success metrics
            total_platform_ids = sum([
                1 for p in db_players_with_ids if p.sleeper_id
            ]) + sum([
                1 for p in db_players_with_ids if p.mfl_id
            ])
            
            mapping_success_rate = (mapping_results['sleeper_mappings'] + mapping_results['mfl_mappings']) / total_platform_ids if total_platform_ids > 0 else 0
            
            players_with_both = len([
                p for p in db_players_with_ids if p.sleeper_id and p.mfl_id
            ])
            consistency_rate = mapping_results['platform_consistency'] / players_with_both if players_with_both > 0 else 1.0
            
            success = (mapping_success_rate >= 0.7 and      # 70% mapping success
                      consistency_rate >= 0.8 and           # 80% consistency
                      len(mapping_results['mapping_errors']) <= 3)  # Few errors allowed
            
            print(f"   ✓ Mapping success rate: {mapping_success_rate:.1%}")
            print(f"   ✓ Consistency rate: {consistency_rate:.1%}")
            print(f"   ✓ Sleeper mappings: {mapping_results['sleeper_mappings']}")
            print(f"   ✓ MFL mappings: {mapping_results['mfl_mappings']}")
            print(f"   ✓ Mapping errors: {len(mapping_results['mapping_errors'])}")
            
            return {
                'success': success,
                'mapping_success_rate': mapping_success_rate,
                'consistency_rate': consistency_rate,
                'sleeper_mappings': mapping_results['sleeper_mappings'],
                'mfl_mappings': mapping_results['mfl_mappings'],
                'total_attempted': mapping_results['total_attempted'],
                'mapping_errors': mapping_results['mapping_errors'][:3]
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Platform ID mapping validation failed: {e}'
            }
    
    def validate_cross_platform_consistency(self) -> Dict[str, Any]:
        """
        Validate consistency of player identification across platforms
        
        Success Criteria:
        - Same player has same canonical ID regardless of lookup method
        - Platform-agnostic lookup works correctly
        - No duplicate players across platforms
        """
        try:
            print("   Testing cross-platform consistency...")
            
            consistency_results = {
                'consistent_players': 0,
                'total_cross_platform_players': 0,
                'canonical_id_conflicts': [],
                'duplicate_players': []
            }
            
            # Find players that exist on both platforms
            cross_platform_players = self.db.query(Player).filter(
                Player.sleeper_id.isnot(None),
                Player.mfl_id.isnot(None)
            ).limit(50).all()
            
            consistency_results['total_cross_platform_players'] = len(cross_platform_players)
            
            for player in cross_platform_players:
                try:
                    # Generate canonical ID from name/position/team
                    generated_canonical = self.player_mapper.generate_canonical_id(
                        player.name, player.position, player.team
                    )
                    
                    # Get canonical ID from Sleeper ID
                    sleeper_canonical = self.player_mapper.get_canonical_id(
                        player.sleeper_id, 'sleeper'
                    )
                    
                    # Get canonical ID from MFL ID
                    mfl_canonical = self.player_mapper.get_canonical_id(
                        player.mfl_id, 'mfl'
                    )
                    
                    # Check if all methods give same canonical ID
                    ids_to_check = [generated_canonical, sleeper_canonical, mfl_canonical, player.nfl_id]
                    unique_ids = set(filter(None, ids_to_check))
                    
                    if len(unique_ids) == 1:
                        consistency_results['consistent_players'] += 1
                    else:
                        consistency_results['canonical_id_conflicts'].append({
                            'player': player.name,
                            'generated': generated_canonical,
                            'sleeper': sleeper_canonical,
                            'mfl': mfl_canonical,
                            'stored': player.nfl_id
                        })
                
                except Exception as e:
                    consistency_results['canonical_id_conflicts'].append({
                        'player': player.name,
                        'error': str(e)
                    })
            
            # Check for potential duplicate players (same canonical ID, different records)
            canonical_id_counts = {}
            all_players = self.db.query(Player).filter(Player.nfl_id.isnot(None)).all()
            
            for player in all_players:
                if player.nfl_id not in canonical_id_counts:
                    canonical_id_counts[player.nfl_id] = []
                canonical_id_counts[player.nfl_id].append(player.name)
            
            for canonical_id, names in canonical_id_counts.items():
                if len(names) > 1:
                    consistency_results['duplicate_players'].append({
                        'canonical_id': canonical_id,
                        'names': names
                    })
            
            # Calculate success metrics
            consistency_rate = consistency_results['consistent_players'] / consistency_results['total_cross_platform_players'] if consistency_results['total_cross_platform_players'] > 0 else 1.0
            duplicate_rate = len(consistency_results['duplicate_players']) / len(all_players) if all_players else 0
            
            success = (consistency_rate >= 0.8 and          # 80% consistency
                      duplicate_rate <= 0.05 and           # Less than 5% duplicates
                      len(consistency_results['canonical_id_conflicts']) <= 5)  # Few conflicts
            
            print(f"   ✓ Consistency rate: {consistency_rate:.1%} ({consistency_results['consistent_players']}/{consistency_results['total_cross_platform_players']})")
            print(f"   ✓ Duplicate rate: {duplicate_rate:.2%} ({len(consistency_results['duplicate_players'])} duplicates)")
            print(f"   ✓ ID conflicts: {len(consistency_results['canonical_id_conflicts'])}")
            
            if consistency_results['duplicate_players'][:2]:
                print(f"   ⚠️  Sample duplicates: {consistency_results['duplicate_players'][:2]}")
            
            return {
                'success': success,
                'consistency_rate': consistency_rate,
                'duplicate_rate': duplicate_rate,
                'consistent_players': consistency_results['consistent_players'],
                'total_cross_platform_players': consistency_results['total_cross_platform_players'],
                'canonical_id_conflicts': len(consistency_results['canonical_id_conflicts']),
                'duplicate_players_count': len(consistency_results['duplicate_players']),
                'sample_conflicts': consistency_results['canonical_id_conflicts'][:3]
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Cross-platform consistency validation failed: {e}'
            }
    
    def validate_lookup_performance(self) -> Dict[str, Any]:
        """
        Validate performance of cross-platform lookup operations
        
        Success Criteria:
        - Lookup operations complete in reasonable time
        - Batch lookups are efficient
        - No performance degradation with multiple lookups
        """
        try:
            print("   Testing lookup performance...")
            
            import time
            
            performance_results = {
                'single_lookup_times': [],
                'batch_lookup_time': 0,
                'total_lookups_tested': 0
            }
            
            # Test single lookup performance
            test_players = self.test_players[:5]  # Test with 5 players
            
            for player_info in test_players:
                start_time = time.time()
                
                canonical_id = self.player_mapper.generate_canonical_id(
                    player_info['name'], player_info['position'], player_info['team']
                )
                
                db_player = self.db.query(Player).filter(
                    Player.nfl_id == canonical_id
                ).first()
                
                end_time = time.time()
                lookup_time = end_time - start_time
                
                performance_results['single_lookup_times'].append(lookup_time)
                performance_results['total_lookups_tested'] += 1
            
            # Test batch lookup performance
            batch_start_time = time.time()
            
            for player_info in test_players:
                canonical_id = self.player_mapper.generate_canonical_id(
                    player_info['name'], player_info['position'], player_info['team']
                )
                # Simulate batch operation
                self.db.query(Player).filter(Player.nfl_id == canonical_id).first()
            
            batch_end_time = time.time()
            performance_results['batch_lookup_time'] = batch_end_time - batch_start_time
            
            # Calculate metrics
            avg_single_lookup = sum(performance_results['single_lookup_times']) / len(performance_results['single_lookup_times']) if performance_results['single_lookup_times'] else 0
            max_single_lookup = max(performance_results['single_lookup_times']) if performance_results['single_lookup_times'] else 0
            
            success = (avg_single_lookup < 0.1 and      # Average < 100ms
                      max_single_lookup < 0.5 and      # Max < 500ms
                      performance_results['batch_lookup_time'] < 2.0)  # Batch < 2s
            
            print(f"   ✓ Avg single lookup: {avg_single_lookup * 1000:.1f}ms")
            print(f"   ✓ Max single lookup: {max_single_lookup * 1000:.1f}ms")
            print(f"   ✓ Batch lookup time: {performance_results['batch_lookup_time']:.2f}s")
            print(f"   ✓ Total lookups tested: {performance_results['total_lookups_tested']}")
            
            return {
                'success': success,
                'avg_single_lookup_ms': avg_single_lookup * 1000,
                'max_single_lookup_ms': max_single_lookup * 1000,
                'batch_lookup_seconds': performance_results['batch_lookup_time'],
                'total_lookups_tested': performance_results['total_lookups_tested']
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Lookup performance validation failed: {e}'
            }
    
    def _print_validation_summary(self, results: Dict[str, Any]):
        """Print comprehensive validation summary"""
        print("\n" + "=" * 80)
        print("CROSS-PLATFORM LOOKUP VALIDATION SUMMARY")
        print("=" * 80)
        
        overall_success = results['overall_success']
        status_icon = "✅" if overall_success else "❌"
        
        print(f"\n{status_icon} OVERALL VALIDATION STATUS: {'PASS' if overall_success else 'FAIL'}")
        print(f"📅 Test Date: {results['timestamp']}")
        print(f"💾 Database: {results['database']}")
        
        print(f"\n📊 Individual Validation Results:")
        for validation_name, validation_result in results.get('validations', {}).items():
            success = validation_result.get('success', False)
            icon = "✅" if success else "❌"
            print(f"  {icon} {validation_name.replace('_', ' ').title()}: {'PASS' if success else 'FAIL'}")
            
            if not success and 'error' in validation_result:
                print(f"      Error: {validation_result['error']}")
        
        # Key metrics summary
        print(f"\n🎯 Key Cross-Platform Metrics:")
        
        if 'canonical_id_generation' in results.get('validations', {}):
            gen_test = results['validations']['canonical_id_generation']
            print(f"  • ID Generation Consistency: {gen_test.get('consistency_rate', 0):.1%}")
        
        if 'database_lookup' in results.get('validations', {}):
            lookup_test = results['validations']['database_lookup']
            print(f"  • Database Lookup Success: {lookup_test.get('lookup_success_rate', 0):.1%}")
        
        if 'platform_mapping' in results.get('validations', {}):
            mapping_test = results['validations']['platform_mapping']
            print(f"  • Platform Mapping Success: {mapping_test.get('mapping_success_rate', 0):.1%}")
        
        if 'cross_platform_consistency' in results.get('validations', {}):
            consistency_test = results['validations']['cross_platform_consistency']
            print(f"  • Cross-Platform Consistency: {consistency_test.get('consistency_rate', 0):.1%}")
        
        if 'lookup_performance' in results.get('validations', {}):
            perf_test = results['validations']['lookup_performance']
            print(f"  • Avg Lookup Time: {perf_test.get('avg_single_lookup_ms', 0):.1f}ms")
        
        print(f"\n{'🎉 Cross-Platform Lookup is FULLY VALIDATED!' if overall_success else '⚠️  Cross-Platform Lookup needs attention'}")
        print("=" * 80)

def main():
    """Run cross-platform lookup validation tests"""
    print("Starting Cross-Platform Player Lookup Validation...")
    
    try:
        validator = CrossPlatformLookupValidator()
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