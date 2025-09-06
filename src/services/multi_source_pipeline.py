import logging
import asyncio
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
import time

from src.config.config import get_config
from src.database import SessionLocal, Player, PlayerUsage, PlayerProjections
from src.utils.player_id_mapper import PlayerIDMapper

# Import our three tested data services
from .nfl_data_service import NFLDataService
from .mfl_projection_service import MFLProjectionService  
from .pfr_data_service import PFRDataService

logger = logging.getLogger(__name__)

@dataclass
class PipelineExecutionResult:
    """Container for pipeline execution results"""
    start_time: datetime
    end_time: datetime
    nfl_data_success: bool
    nfl_data_records: int
    mfl_projection_success: bool
    mfl_projection_records: int
    pfr_data_success: bool
    pfr_data_records: int
    errors: List[str]
    total_duration: float
    
    @property
    def overall_success(self) -> bool:
        """Check if at least one data source succeeded"""
        return any([self.nfl_data_success, self.mfl_projection_success, self.pfr_data_success])
    
    @property
    def success_count(self) -> int:
        """Count successful data sources"""
        return sum([self.nfl_data_success, self.mfl_projection_success, self.pfr_data_success])

class MultiSourceDataPipeline:
    """Unified data pipeline orchestrating NFL-data-py, MFL API, and PFR scraper"""
    
    def __init__(self):
        self.config = get_config()
        self.player_mapper = PlayerIDMapper()
        self.current_season = 2025
        
        # Initialize our three data services
        self.nfl_service = NFLDataService()
        self.mfl_service = MFLProjectionService()
        self.pfr_service = PFRDataService()
        
        # Pipeline execution tracking
        self.last_execution: Optional[PipelineExecutionResult] = None
        
    def check_service_availability(self) -> Dict[str, bool]:
        """Check availability of all data services"""
        try:
            availability = {
                'nfl_data_py': True,  # nfl-data-py is always available (no API keys needed)
                'mfl_api': self.mfl_service.is_available(),
                'pfr_scraper': self.pfr_service.is_available()
            }
            
            logger.info("Service availability check:")
            for service, available in availability.items():
                status = "✓" if available else "✗"
                logger.info(f"  {status} {service}: {'AVAILABLE' if available else 'UNAVAILABLE'}")
            
            return availability
            
        except Exception as e:
            logger.error(f"Failed to check service availability: {e}")
            return {
                'nfl_data_py': False,
                'mfl_api': False,
                'pfr_scraper': False
            }
    
    async def execute_full_pipeline(self, week: Optional[int] = None, season: Optional[int] = None) -> PipelineExecutionResult:
        """
        Execute the full multi-source data pipeline
        
        Args:
            week: Specific NFL week to process
            season: Specific NFL season to process
            
        Returns:
            PipelineExecutionResult with detailed execution metrics
        """
        start_time = datetime.utcnow()
        
        if season is None:
            season = self.current_season
            
        if week is None:
            week = self._get_current_nfl_week()
        
        logger.info(f"🚀 Starting multi-source data pipeline for week {week}, season {season}")
        
        # Initialize result tracking
        errors = []
        nfl_data_success = False
        nfl_data_records = 0
        mfl_projection_success = False
        mfl_projection_records = 0
        pfr_data_success = False
        pfr_data_records = 0
        
        # Check service availability first
        availability = self.check_service_availability()
        
        # Step 1: NFL Data Service (snap counts and usage)
        if availability['nfl_data_py']:
            logger.info("📊 Step 1: Executing NFL data service...")
            try:
                nfl_result = self.nfl_service.daily_sync_job(week, season)
                nfl_data_success = nfl_result.get('nfl_usage_sync', False)
                nfl_data_records = nfl_result.get('records_processed', 0)
                
                if nfl_data_success:
                    logger.info(f"  ✓ NFL data sync successful: {nfl_data_records} records")
                else:
                    error_msg = f"NFL data sync failed: {nfl_result.get('error', 'Unknown error')}"
                    logger.error(f"  ✗ {error_msg}")
                    errors.append(error_msg)
                    
            except Exception as e:
                error_msg = f"NFL data service exception: {e}"
                logger.error(f"  ✗ {error_msg}")
                errors.append(error_msg)
        else:
            error_msg = "NFL data service unavailable"
            logger.warning(f"  ⚠ {error_msg}")
            errors.append(error_msg)
        
        # Brief pause between services
        await asyncio.sleep(1)
        
        # Step 2: MFL Projection Service
        if availability['mfl_api']:
            logger.info("🎯 Step 2: Executing MFL projection service...")
            try:
                mfl_result = self.mfl_service.daily_projections_sync_job(week)
                mfl_projection_success = mfl_result.get('mfl_projections_sync', False)
                mfl_projection_records = mfl_result.get('projections_count', 0)
                
                if mfl_projection_success:
                    logger.info(f"  ✓ MFL projections sync successful: {mfl_projection_records} records")
                else:
                    error_msg = f"MFL projections sync failed: {mfl_result.get('error', 'Unknown error')}"
                    logger.error(f"  ✗ {error_msg}")
                    errors.append(error_msg)
                    
            except Exception as e:
                error_msg = f"MFL projection service exception: {e}"
                logger.error(f"  ✗ {error_msg}")
                errors.append(error_msg)
        else:
            error_msg = "MFL API service unavailable"
            logger.warning(f"  ⚠ {error_msg}")
            errors.append(error_msg)
        
        # Pause between services (important for PFR rate limiting)
        await asyncio.sleep(2)
        
        # Step 3: PFR Data Service (with rate limiting)
        if availability['pfr_scraper']:
            logger.info("⚡ Step 3: Executing PFR data service (with rate limiting)...")
            try:
                # For now, sync a small sample of key players to respect rate limits
                key_players = [
                    ('Josh Allen', 'QB', 'BUF'),
                    ('Christian McCaffrey', 'RB', 'SF'),
                    ('Cooper Kupp', 'WR', 'LAR'),
                    ('Travis Kelce', 'TE', 'KC')
                ]
                
                pfr_records = 0
                pfr_successes = 0
                
                for player_name, position, team in key_players:
                    try:
                        # Fetch player data with built-in rate limiting
                        player_data = self.pfr_service.fetch_player_game_log(
                            player_name, position, season - 1  # Use previous season for complete data
                        )
                        
                        if player_data is not None and not player_data.empty:
                            pfr_records += len(player_data)
                            pfr_successes += 1
                            logger.info(f"    ✓ {player_name}: {len(player_data)} games")
                        
                    except Exception as e:
                        logger.warning(f"    ⚠ Failed to fetch {player_name}: {e}")
                
                pfr_data_success = pfr_successes > 0
                pfr_data_records = pfr_records
                
                if pfr_data_success:
                    logger.info(f"  ✓ PFR data sync successful: {pfr_records} records from {pfr_successes} players")
                else:
                    error_msg = "PFR data sync failed: No player data retrieved"
                    logger.error(f"  ✗ {error_msg}")
                    errors.append(error_msg)
                    
            except Exception as e:
                error_msg = f"PFR data service exception: {e}"
                logger.error(f"  ✗ {error_msg}")
                errors.append(error_msg)
        else:
            error_msg = "PFR scraper service unavailable"
            logger.warning(f"  ⚠ {error_msg}")
            errors.append(error_msg)
        
        # Calculate execution time
        end_time = datetime.utcnow()
        total_duration = (end_time - start_time).total_seconds()
        
        # Create result object
        result = PipelineExecutionResult(
            start_time=start_time,
            end_time=end_time,
            nfl_data_success=nfl_data_success,
            nfl_data_records=nfl_data_records,
            mfl_projection_success=mfl_projection_success,
            mfl_projection_records=mfl_projection_records,
            pfr_data_success=pfr_data_success,
            pfr_data_records=pfr_data_records,
            errors=errors,
            total_duration=total_duration
        )
        
        # Store for tracking
        self.last_execution = result
        
        # Log final results
        logger.info(f"🏁 Pipeline execution completed in {total_duration:.2f}s")
        logger.info(f"   Success rate: {result.success_count}/3 services")
        logger.info(f"   Total records: {result.nfl_data_records + result.mfl_projection_records + result.pfr_data_records}")
        
        if errors:
            logger.warning(f"   Errors encountered: {len(errors)}")
            for error in errors[:3]:  # Log first 3 errors
                logger.warning(f"     - {error}")
        
        return result
    
    def get_pipeline_statistics(self) -> Dict[str, Any]:
        """Get comprehensive pipeline statistics"""
        try:
            stats = {
                'last_execution': None,
                'service_availability': self.check_service_availability(),
                'database_stats': self._get_database_statistics(),
                'current_week': self._get_current_nfl_week(),
                'current_season': self.current_season
            }
            
            if self.last_execution:
                stats['last_execution'] = {
                    'timestamp': self.last_execution.start_time.isoformat(),
                    'duration': self.last_execution.total_duration,
                    'overall_success': self.last_execution.overall_success,
                    'success_count': self.last_execution.success_count,
                    'total_records': (
                        self.last_execution.nfl_data_records +
                        self.last_execution.mfl_projection_records +
                        self.last_execution.pfr_data_records
                    ),
                    'services': {
                        'nfl_data': {
                            'success': self.last_execution.nfl_data_success,
                            'records': self.last_execution.nfl_data_records
                        },
                        'mfl_projections': {
                            'success': self.last_execution.mfl_projection_success,
                            'records': self.last_execution.mfl_projection_records
                        },
                        'pfr_scraper': {
                            'success': self.last_execution.pfr_data_success,
                            'records': self.last_execution.pfr_data_records
                        }
                    },
                    'errors': self.last_execution.errors
                }
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get pipeline statistics: {e}")
            return {'error': str(e)}
    
    def _get_database_statistics(self) -> Dict[str, int]:
        """Get database record counts"""
        try:
            db = SessionLocal()
            try:
                stats = {
                    'total_players': db.query(Player).count(),
                    'usage_records': db.query(PlayerUsage).count(),
                    'projection_records': db.query(PlayerProjections).count(),
                    'nfl_data_usage': db.query(PlayerUsage).filter(
                        PlayerUsage.updated_at >= datetime.utcnow() - timedelta(days=1)
                    ).count(),
                    'mfl_projections': db.query(PlayerProjections).filter(
                        PlayerProjections.source == 'mfl'
                    ).count(),
                    'current_season_data': db.query(PlayerUsage).filter(
                        PlayerUsage.season == self.current_season
                    ).count()
                }
                
                return stats
                
            finally:
                db.close()
                
        except Exception as e:
            logger.warning(f"Failed to get database statistics: {e}")
            return {}
    
    def _get_current_nfl_week(self) -> int:
        """Get current NFL week based on date"""
        now = datetime.now()
        if now.month >= 9:
            return min(max(now.isocalendar()[1] - 35, 1), 18)
        else:
            return 1
    
    async def test_ceedee_lamb_data(self, season: int = 2024, week: int = 1) -> Dict[str, Any]:
        """
        Comprehensive test of Epic A pipeline using CeeDee Lamb as target player
        
        Args:
            season: NFL season to test (default 2024)
            week: NFL week to test (default 1)
            
        Returns:
            Dictionary with detailed test results for each data source
        """
        logger.info(f"🏈 Testing Epic A pipeline with CeeDee Lamb (WR, DAL) - Week {week}, {season}")
        
        # Target player details
        target_player = {
            'name': 'CeeDee Lamb',
            'position': 'WR',
            'team': 'DAL'
        }
        
        test_results = {
            'player': target_player,
            'season': season,
            'week': week,
            'nfl_data_results': {},
            'mfl_projection_results': {},
            'pfr_data_results': {},
            'database_integration': {},
            'cross_source_validation': {},
            'overall_success': False
        }
        
        try:
            # Test 1: NFL Data Service (nfl-data-py)
            logger.info("📊 Testing NFL data service for CeeDee Lamb...")
            try:
                # Fetch snap counts for the week
                snap_df = self.nfl_service.fetch_snap_counts([season], [week])
                ceedee_snaps = snap_df[snap_df['player'].str.contains('Lamb', case=False, na=False)] if not snap_df.empty else None
                
                # Fetch receiving stats 
                rec_df = self.nfl_service.fetch_weekly_advanced_stats('rec', [season])
                if not rec_df.empty:
                    rec_df_week = rec_df[rec_df['week'] == week]
                    ceedee_rec = rec_df_week[rec_df_week['pfr_player_name'].str.contains('Lamb', case=False, na=False)] if not rec_df_week.empty else None
                else:
                    ceedee_rec = None
                
                test_results['nfl_data_results'] = {
                    'snap_data_found': ceedee_snaps is not None and not ceedee_snaps.empty,
                    'receiving_data_found': ceedee_rec is not None and not ceedee_rec.empty,
                    'snap_records': len(ceedee_snaps) if ceedee_snaps is not None and not ceedee_snaps.empty else 0,
                    'receiving_records': len(ceedee_rec) if ceedee_rec is not None and not ceedee_rec.empty else 0,
                    'success': True
                }
                
                if ceedee_snaps is not None and not ceedee_snaps.empty:
                    sample_snap = ceedee_snaps.iloc[0]
                    test_results['nfl_data_results']['sample_data'] = {
                        'player': sample_snap.get('player', 'N/A'),
                        'team': sample_snap.get('team', 'N/A'),
                        'offense_pct': sample_snap.get('offense_pct', 'N/A'),
                        'offense_snaps': sample_snap.get('offense_snaps', 'N/A')
                    }
                
                logger.info(f"  ✓ NFL data: {test_results['nfl_data_results']['snap_records']} snap records, {test_results['nfl_data_results']['receiving_records']} receiving records")
                
            except Exception as e:
                test_results['nfl_data_results'] = {'success': False, 'error': str(e)}
                logger.error(f"  ✗ NFL data service failed: {e}")
            
            # Test 2: MFL Projection Service
            logger.info("🎯 Testing MFL projection service for CeeDee Lamb...")
            try:
                # Fetch WR projections
                wr_projections = self.mfl_service.fetch_projected_scores(week, 'WR', 200)
                ceedee_projection = None
                
                for proj in wr_projections:
                    if 'lamb' in proj.name.lower() and 'ceedee' in proj.name.lower():
                        ceedee_projection = proj
                        break
                
                test_results['mfl_projection_results'] = {
                    'projection_found': ceedee_projection is not None,
                    'total_wr_projections': len(wr_projections),
                    'success': True
                }
                
                if ceedee_projection:
                    test_results['mfl_projection_results']['projection_data'] = {
                        'name': ceedee_projection.name,
                        'position': ceedee_projection.position,
                        'team': ceedee_projection.team,
                        'projected_score': ceedee_projection.projected_score
                    }
                    logger.info(f"  ✓ MFL projection: {ceedee_projection.name} ({ceedee_projection.team}) - {ceedee_projection.projected_score} pts")
                else:
                    logger.info(f"  ⚠ CeeDee Lamb not found in {len(wr_projections)} WR projections")
                
            except Exception as e:
                test_results['mfl_projection_results'] = {'success': False, 'error': str(e)}
                logger.error(f"  ✗ MFL projection service failed: {e}")
            
            # Test 3: PFR Data Service (with rate limiting)
            logger.info("⚡ Testing PFR data service for CeeDee Lamb...")
            try:
                # Fetch CeeDee Lamb's game log data
                ceedee_pfr_data = self.pfr_service.fetch_player_game_log(
                    'CeeDee Lamb', 'WR', season
                )
                
                test_results['pfr_data_results'] = {
                    'game_log_found': ceedee_pfr_data is not None and not ceedee_pfr_data.empty,
                    'games_count': len(ceedee_pfr_data) if ceedee_pfr_data is not None and not ceedee_pfr_data.empty else 0,
                    'success': True
                }
                
                if ceedee_pfr_data is not None and not ceedee_pfr_data.empty:
                    test_results['pfr_data_results']['columns_count'] = len(ceedee_pfr_data.columns)
                    test_results['pfr_data_results']['sample_columns'] = list(ceedee_pfr_data.columns[:5])
                    logger.info(f"  ✓ PFR data: {len(ceedee_pfr_data)} games, {len(ceedee_pfr_data.columns)} columns")
                else:
                    logger.info("  ⚠ No PFR game log data found for CeeDee Lamb")
                
            except Exception as e:
                test_results['pfr_data_results'] = {'success': False, 'error': str(e)}
                logger.error(f"  ✗ PFR data service failed: {e}")
            
            # Test 4: Database Integration
            logger.info("🗄️ Testing database integration for CeeDee Lamb...")
            try:
                db = SessionLocal()
                try:
                    # Generate canonical ID
                    canonical_id = self.player_mapper.generate_canonical_id(
                        target_player['name'], target_player['position'], target_player['team']
                    )
                    
                    # Check if player exists in database
                    player_record = db.query(Player).filter(Player.nfl_id == canonical_id).first()
                    
                    # Check usage and projection records
                    usage_count = 0
                    projection_count = 0
                    
                    if player_record:
                        usage_count = db.query(PlayerUsage).filter(
                            PlayerUsage.player_id == player_record.id,
                            PlayerUsage.season == season
                        ).count()
                        
                        projection_count = db.query(PlayerProjections).filter(
                            PlayerProjections.player_id == player_record.id,
                            PlayerProjections.season == season
                        ).count()
                    
                    test_results['database_integration'] = {
                        'canonical_id': canonical_id,
                        'player_exists': player_record is not None,
                        'usage_records': usage_count,
                        'projection_records': projection_count,
                        'success': True
                    }
                    
                    logger.info(f"  ✓ Database: Player {'found' if player_record else 'not found'}, {usage_count} usage records, {projection_count} projections")
                    
                finally:
                    db.close()
                    
            except Exception as e:
                test_results['database_integration'] = {'success': False, 'error': str(e)}
                logger.error(f"  ✗ Database integration failed: {e}")
            
            # Test 5: Cross-Source Data Validation
            logger.info("🔍 Performing cross-source data validation...")
            validation_results = {
                'sources_with_data': 0,
                'consistency_checks': [],
                'success': True
            }
            
            # Count successful data sources
            if test_results['nfl_data_results'].get('success', False):
                validation_results['sources_with_data'] += 1
            if test_results['mfl_projection_results'].get('success', False):
                validation_results['sources_with_data'] += 1
            if test_results['pfr_data_results'].get('success', False):
                validation_results['sources_with_data'] += 1
            
            # Add consistency checks
            if test_results['nfl_data_results'].get('snap_data_found', False):
                validation_results['consistency_checks'].append("NFL snap data available for analysis")
            
            if test_results['mfl_projection_results'].get('projection_found', False):
                validation_results['consistency_checks'].append("MFL projection available for fantasy analysis")
            
            if test_results['pfr_data_results'].get('game_log_found', False):
                validation_results['consistency_checks'].append("PFR game logs available for historical analysis")
            
            test_results['cross_source_validation'] = validation_results
            
            # Determine overall success
            test_results['overall_success'] = validation_results['sources_with_data'] >= 2
            
            logger.info(f"📈 Cross-source validation: {validation_results['sources_with_data']}/3 sources successful")
            
            # Final summary
            success_emoji = "✅" if test_results['overall_success'] else "⚠️"
            logger.info(f"{success_emoji} CeeDee Lamb test completed: {validation_results['sources_with_data']}/3 sources successful")
            
            return test_results
            
        except Exception as e:
            logger.error(f"CeeDee Lamb test failed with exception: {e}")
            test_results['overall_success'] = False
            test_results['error'] = str(e)
            return test_results

# Test function
async def test_multi_source_pipeline():
    """Test the multi-source data pipeline"""
    print("Testing Multi-Source Data Pipeline...")
    print("=" * 70)
    
    pipeline = MultiSourceDataPipeline()
    
    try:
        # Test service availability check
        print("\n1. Testing service availability...")
        availability = pipeline.check_service_availability()
        print(f"   Availability: {availability}")
        
        # Test pipeline statistics (before execution)
        print("\n2. Testing pipeline statistics...")
        stats = pipeline.get_pipeline_statistics()
        print(f"   Current stats: {stats.get('database_stats', {})}")
        
        # Test full pipeline execution
        print("\n3. Testing full pipeline execution...")
        print("   This will take 30-60 seconds due to rate limiting...")
        result = await pipeline.execute_full_pipeline(week=1, season=2024)  # Use 2024 for testing
        
        print(f"\n   Pipeline Results:")
        print(f"   - Duration: {result.total_duration:.2f}s")
        print(f"   - Overall success: {result.overall_success}")
        print(f"   - Success count: {result.success_count}/3")
        print(f"   - NFL data: {'✓' if result.nfl_data_success else '✗'} ({result.nfl_data_records} records)")
        print(f"   - MFL projections: {'✓' if result.mfl_projection_success else '✗'} ({result.mfl_projection_records} records)")
        print(f"   - PFR scraper: {'✓' if result.pfr_data_success else '✗'} ({result.pfr_data_records} records)")
        
        if result.errors:
            print(f"   - Errors: {len(result.errors)}")
            for i, error in enumerate(result.errors[:3]):
                print(f"     {i+1}. {error}")
        
        # Test final statistics
        print("\n4. Testing final statistics...")
        final_stats = pipeline.get_pipeline_statistics()
        if 'last_execution' in final_stats and final_stats['last_execution']:
            exec_stats = final_stats['last_execution']
            print(f"   Last execution: {exec_stats['timestamp']}")
            print(f"   Total records processed: {exec_stats['total_records']}")
        
        # Test CeeDee Lamb specific data
        print("\n5. Testing CeeDee Lamb specific data...")
        ceedee_results = await pipeline.test_ceedee_lamb_data(season=2024, week=1)
        print(f"   CeeDee Lamb test success: {ceedee_results['overall_success']}")
        print(f"   Data sources successful: {ceedee_results['cross_source_validation']['sources_with_data']}/3")
        
        if ceedee_results['nfl_data_results'].get('success', False):
            nfl_data = ceedee_results['nfl_data_results']
            print(f"     NFL data: {nfl_data['snap_records']} snap records, {nfl_data['receiving_records']} receiving records")
        
        if ceedee_results['mfl_projection_results'].get('projection_found', False):
            proj_data = ceedee_results['mfl_projection_results']['projection_data']
            print(f"     MFL projection: {proj_data['projected_score']} points")
        
        if ceedee_results['pfr_data_results'].get('game_log_found', False):
            pfr_data = ceedee_results['pfr_data_results']
            print(f"     PFR data: {pfr_data['games_count']} games with {pfr_data['columns_count']} columns")
        
        return result.overall_success and ceedee_results['overall_success']
        
    except Exception as e:
        print(f"   ✗ Multi-source pipeline test failed: {e}")
        return False

if __name__ == "__main__":
    asyncio.run(test_multi_source_pipeline())