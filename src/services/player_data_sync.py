import logging
import asyncio
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass

from src.config.config import get_config
from src.database import SessionLocal, Player
from src.utils.player_id_mapper import PlayerIDMapper
from src.services.roster_sync import SleeperAPIClient, MFLAPIClient
from src.utils.retry_handler import APIError

logger = logging.getLogger(__name__)

@dataclass
class PlayerSyncResult:
    """Container for player sync results"""
    start_time: datetime
    end_time: datetime
    sleeper_success: bool
    sleeper_players_count: int
    mfl_success: bool  
    mfl_players_count: int
    total_unified_players: int
    cross_platform_matches: int
    errors: List[str]
    duration: float
    
    @property
    def overall_success(self) -> bool:
        """Check if at least one platform succeeded"""
        return self.sleeper_success or self.mfl_success
    
    @property
    def full_success(self) -> bool:
        """Check if both platforms succeeded"""
        return self.sleeper_success and self.mfl_success

class PlayerDataSyncService:
    """Service to sync comprehensive player data from all platforms and unify them"""
    
    def __init__(self):
        self.config = get_config()
        self.sleeper_client = SleeperAPIClient()
        self.mfl_client = MFLAPIClient()
        self.player_mapper = PlayerIDMapper()
        self.current_season = 2025
        
    def sync_all_player_data(self) -> PlayerSyncResult:
        """
        Comprehensive player data sync from all platforms
        
        This is the missing piece that completes Epic A's cross-platform unification.
        Fetches all players from Sleeper and MFL APIs, then uses PlayerIDMapper 
        to create unified canonical database.
        """
        start_time = datetime.utcnow()
        logger.info("Starting comprehensive player data sync for Epic A cross-platform unification")
        
        sleeper_players = {}
        mfl_players = []
        sleeper_success = False
        mfl_success = False
        errors = []
        
        # Step 1: Fetch all players from Sleeper
        try:
            logger.info("Fetching all NFL players from Sleeper API...")
            sleeper_players = self.sleeper_client.get_players()
            sleeper_success = True
            logger.info(f"✓ Successfully fetched {len(sleeper_players)} players from Sleeper")
        except APIError as e:
            error_msg = f"Failed to fetch Sleeper players: {e}"
            logger.error(error_msg)
            errors.append(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error fetching Sleeper players: {e}"
            logger.error(error_msg)
            errors.append(error_msg)
        
        # Step 2: Fetch all players from MFL  
        try:
            logger.info("Fetching all NFL players from MFL API...")
            mfl_players = self.mfl_client.get_players()
            mfl_success = True
            logger.info(f"✓ Successfully fetched {len(mfl_players)} players from MFL")
        except APIError as e:
            error_msg = f"Failed to fetch MFL players: {e}"
            logger.error(error_msg)
            errors.append(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error fetching MFL players: {e}"
            logger.error(error_msg)
            errors.append(error_msg)
        
        # Step 3: Unify players using PlayerIDMapper (the Epic A magic happens here)
        total_unified_players = 0
        cross_platform_matches = 0
        
        if sleeper_success or mfl_success:
            try:
                logger.info("Unifying players across platforms using canonical ID mapping...")
                
                # This is the key Epic A method that was missing integration
                success = self.player_mapper.sync_players_to_database(
                    sleeper_players if sleeper_success else None,
                    mfl_players if mfl_success else None
                )
                
                if success:
                    # Get post-sync statistics
                    stats = self._get_unification_stats()
                    total_unified_players = stats['total_players']
                    cross_platform_matches = stats['cross_platform_matches']
                    
                    logger.info(f"✓ Player unification completed successfully")
                    logger.info(f"  Total unified players: {total_unified_players}")
                    logger.info(f"  Cross-platform matches: {cross_platform_matches}")
                else:
                    error_msg = "Player unification failed during database sync"
                    logger.error(error_msg)
                    errors.append(error_msg)
                    
            except Exception as e:
                error_msg = f"Player unification failed: {e}"
                logger.error(error_msg)
                errors.append(error_msg)
        else:
            error_msg = "Cannot proceed with unification - no platform data available"
            logger.error(error_msg)
            errors.append(error_msg)
        
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        
        result = PlayerSyncResult(
            start_time=start_time,
            end_time=end_time,
            sleeper_success=sleeper_success,
            sleeper_players_count=len(sleeper_players),
            mfl_success=mfl_success,
            mfl_players_count=len(mfl_players),
            total_unified_players=total_unified_players,
            cross_platform_matches=cross_platform_matches,
            errors=errors,
            duration=duration
        )
        
        # Log final results
        status = "SUCCESS" if result.overall_success else "FAILED"
        logger.info(f"Player data sync completed in {duration:.2f}s - {status}")
        
        if result.overall_success:
            platforms_synced = []
            if sleeper_success:
                platforms_synced.append(f"Sleeper ({len(sleeper_players)} players)")
            if mfl_success:
                platforms_synced.append(f"MFL ({len(mfl_players)} players)")
            
            logger.info(f"  Platforms synced: {', '.join(platforms_synced)}")
            logger.info(f"  Total unified: {total_unified_players} players")
            logger.info(f"  Cross-platform: {cross_platform_matches} matched players")
            
            if errors:
                logger.warning(f"  Completed with {len(errors)} warnings/errors")
        else:
            logger.error(f"  Failed to sync any platform data")
            for error in errors:
                logger.error(f"    - {error}")
        
        return result
    
    def _get_unification_stats(self) -> Dict[str, int]:
        """Get statistics about player unification after sync"""
        try:
            db = SessionLocal()
            try:
                total_players = db.query(Player).count()
                
                sleeper_mapped = db.query(Player).filter(
                    Player.sleeper_id.isnot(None)
                ).count()
                
                mfl_mapped = db.query(Player).filter(
                    Player.mfl_id.isnot(None)  
                ).count()
                
                # Cross-platform matches: players with both Sleeper AND MFL IDs
                cross_platform_matches = db.query(Player).filter(
                    Player.sleeper_id.isnot(None),
                    Player.mfl_id.isnot(None)
                ).count()
                
                return {
                    'total_players': total_players,
                    'sleeper_mapped': sleeper_mapped,
                    'mfl_mapped': mfl_mapped,
                    'cross_platform_matches': cross_platform_matches
                }
            finally:
                db.close()
        except Exception as e:
            logger.error(f"Failed to get unification stats: {e}")
            return {
                'total_players': 0,
                'sleeper_mapped': 0,
                'mfl_mapped': 0, 
                'cross_platform_matches': 0
            }
    
    def validate_cross_platform_unification(self) -> Dict[str, Any]:
        """Validate that Epic A cross-platform unification is working correctly"""
        try:
            logger.info("Validating Epic A cross-platform player unification...")
            
            db = SessionLocal()
            try:
                # Test 1: Check for players with multiple platform IDs
                unified_players = db.query(Player).filter(
                    Player.sleeper_id.isnot(None),
                    Player.mfl_id.isnot(None)
                ).limit(10).all()
                
                # Test 2: Verify canonical ID consistency
                canonical_ids = db.query(Player.nfl_id).distinct().count()
                total_players = db.query(Player).count()
                
                # Test 3: Check data quality
                complete_data_players = db.query(Player).filter(
                    Player.name != 'UNKNOWN',
                    Player.position != 'UNKNOWN',
                    Player.team != 'UNKNOWN'
                ).count()
                
                # Test 4: Verify PlayerIDMapper functionality
                self.player_mapper.load_from_database()
                mapper_stats = self.player_mapper.get_mapping_stats()
                
                validation_results = {
                    'cross_platform_unification': {
                        'unified_players_found': len(unified_players),
                        'sample_unified_players': [
                            {
                                'name': p.name,
                                'canonical_id': p.nfl_id,
                                'sleeper_id': p.sleeper_id,
                                'mfl_id': p.mfl_id,
                                'position': p.position,
                                'team': p.team
                            } for p in unified_players[:5]
                        ]
                    },
                    'data_integrity': {
                        'unique_canonical_ids': canonical_ids,
                        'total_player_records': total_players,
                        'canonical_id_integrity': canonical_ids == total_players,
                        'complete_data_percentage': (complete_data_players / total_players * 100) if total_players > 0 else 0
                    },
                    'mapper_functionality': mapper_stats,
                    'epic_a_readiness': {
                        'canonical_schema': True,
                        'cross_platform_ids': len(unified_players) > 0,
                        'waiver_candidates_ready': len(unified_players) > 0 and canonical_ids > 0
                    },
                    'validation_timestamp': datetime.utcnow().isoformat()
                }
                
                # Overall assessment
                epic_a_ready = (
                    validation_results['data_integrity']['canonical_id_integrity'] and
                    validation_results['epic_a_readiness']['cross_platform_ids'] and
                    validation_results['data_integrity']['complete_data_percentage'] > 50
                )
                
                validation_results['overall_status'] = 'EPIC_A_READY' if epic_a_ready else 'NEEDS_IMPROVEMENT'
                
                logger.info(f"Cross-platform unification validation completed")
                logger.info(f"  Status: {validation_results['overall_status']}")
                logger.info(f"  Unified players: {len(unified_players)}")
                logger.info(f"  Data quality: {validation_results['data_integrity']['complete_data_percentage']:.1f}%")
                
                return validation_results
                
            finally:
                db.close()
        except Exception as e:
            logger.error(f"Cross-platform unification validation failed: {e}")
            return {
                'error': str(e),
                'overall_status': 'VALIDATION_FAILED',
                'validation_timestamp': datetime.utcnow().isoformat()
            }
    
    def get_sync_status(self) -> Dict[str, Any]:
        """Get current status of player data synchronization"""
        try:
            stats = self._get_unification_stats()
            
            # Check last sync time from database
            db = SessionLocal()
            try:
                # Get most recently updated player as proxy for last sync
                latest_player = db.query(Player).order_by(Player.updated_at.desc()).first()
                last_sync_time = latest_player.updated_at if latest_player else None
                
                return {
                    'sync_statistics': stats,
                    'last_sync_time': last_sync_time.isoformat() if last_sync_time else None,
                    'epic_a_status': {
                        'canonical_schema_active': stats['total_players'] > 0,
                        'sleeper_integration': f"{stats['sleeper_mapped']} players",
                        'mfl_integration': f"{stats['mfl_mapped']} players", 
                        'cross_platform_unification': f"{stats['cross_platform_matches']} unified",
                        'ready_for_waiver_analysis': stats['cross_platform_matches'] > 0
                    },
                    'status_timestamp': datetime.utcnow().isoformat()
                }
            finally:
                db.close()
        except Exception as e:
            logger.error(f"Failed to get sync status: {e}")
            return {
                'error': str(e),
                'status_timestamp': datetime.utcnow().isoformat()
            }

# Test function
def test_player_data_sync():
    """Test the player data sync service"""
    print("Testing Epic A Player Data Sync Service")
    print("=" * 60)
    
    service = PlayerDataSyncService()
    
    try:
        print("\n1. Testing comprehensive player data sync...")
        result = service.sync_all_player_data()
        
        print(f"   Duration: {result.duration:.2f} seconds")
        print(f"   Overall success: {result.overall_success}")
        print(f"   Sleeper: {result.sleeper_success} ({result.sleeper_players_count} players)")
        print(f"   MFL: {result.mfl_success} ({result.mfl_players_count} players)")
        print(f"   Unified: {result.total_unified_players} total players")
        print(f"   Cross-platform: {result.cross_platform_matches} matched players")
        
        if result.errors:
            print(f"   Errors: {len(result.errors)}")
            for error in result.errors[:3]:  # Show first 3 errors
                print(f"     - {error}")
        
        print("\n2. Testing cross-platform unification validation...")
        validation = service.validate_cross_platform_unification()
        
        if 'error' not in validation:
            print(f"   Status: {validation['overall_status']}")
            print(f"   Unified players: {validation['cross_platform_unification']['unified_players_found']}")
            print(f"   Data quality: {validation['data_integrity']['complete_data_percentage']:.1f}%")
            print(f"   Epic A ready: {validation['epic_a_readiness']['waiver_candidates_ready']}")
        else:
            print(f"   Validation error: {validation['error']}")
        
        print("\n3. Testing sync status reporting...")
        status = service.get_sync_status()
        
        if 'error' not in status:
            epic_status = status['epic_a_status']
            print(f"   Canonical schema: {epic_status['canonical_schema_active']}")
            print(f"   Sleeper integration: {epic_status['sleeper_integration']}")
            print(f"   MFL integration: {epic_status['mfl_integration']}")
            print(f"   Cross-platform: {epic_status['cross_platform_unification']}")
            print(f"   Ready for waiver analysis: {epic_status['ready_for_waiver_analysis']}")
        else:
            print(f"   Status error: {status['error']}")
        
        return True
        
    except Exception as e:
        print(f"   ✗ Player data sync test failed: {e}")
        return False

if __name__ == "__main__":
    test_player_data_sync()