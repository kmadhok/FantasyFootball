import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.asyncio import AsyncIOExecutor

from ..config import get_config
from .roster_sync import RosterSyncService
from .waiver_tracker import WaiverTrackerService
from .usage_projections_service import UsageProjectionsService
from .nfl_data_service import NFLDataService
from .waiver_candidates_builder import WaiverCandidatesBuilder
from .player_data_sync import PlayerDataSyncService
from ..utils.player_id_mapper import PlayerIDMapper
from .rules_engine import evaluate_rules

logger = logging.getLogger(__name__)

class FantasyFootballScheduler:
    """Scheduler for fantasy football background tasks"""
    
    def __init__(self):
        self.config = get_config()
        self.scheduler = None
        self.roster_sync_service = RosterSyncService()
        self.waiver_tracker_service = WaiverTrackerService()
        self.usage_projections_service = UsageProjectionsService()
        self.waiver_candidates_builder = WaiverCandidatesBuilder()
        self.player_data_sync_service = PlayerDataSyncService()
        self.player_mapper = PlayerIDMapper()
        self.nfl_data_service = NFLDataService()
        self._setup_scheduler()
    
    def _setup_scheduler(self):
        """Setup the APScheduler instance"""
        jobstores = {
            'default': MemoryJobStore()
        }
        
        executors = {
            'default': AsyncIOExecutor()
        }
        
        job_defaults = {
            'coalesce': False,
            'max_instances': 1,
            'misfire_grace_time': 300  # 5 minutes
        }
        
        self.scheduler = AsyncIOScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone='UTC'
        )
    
    async def roster_sync_job(self):
        """Job to sync rosters from both platforms"""
        job_start_time = datetime.utcnow()
        logger.info(f"Starting scheduled roster sync at {job_start_time}")
        
        try:
            # First sync player mappings by loading from database
            logger.info("Loading player mappings from database...")
            self.player_mapper.load_from_database()
            
            # Then sync rosters
            results = await self.roster_sync_service.sync_all_rosters()
            
            job_end_time = datetime.utcnow()
            duration = (job_end_time - job_start_time).total_seconds()
            
            success_count = sum(results.values())
            logger.info(f"Roster sync completed in {duration:.2f}s - {success_count}/2 platforms successful")
            
            # Log individual platform results
            for platform, success in results.items():
                status = "✓" if success else "✗"
                logger.info(f"  {status} {platform.upper()} sync: {'SUCCESS' if success else 'FAILED'}")
            
            return results
            
        except Exception as e:
            logger.error(f"Roster sync job failed: {e}")
            raise
    
    async def player_mapping_job(self):
        """Job to update player mappings - now uses comprehensive player data sync"""
        job_start_time = datetime.utcnow()
        logger.info(f"Starting comprehensive player data sync at {job_start_time}")
        
        try:
            # Execute the comprehensive player data sync (Epic A completion)
            result = self.player_data_sync_service.sync_all_player_data()
            
            job_end_time = datetime.utcnow()
            duration = (job_end_time - job_start_time).total_seconds()
            
            status = "SUCCESS" if result.overall_success else "FAILED"
            logger.info(f"Player data sync completed in {duration:.2f}s - {status}")
            
            if result.overall_success:
                logger.info(f"  Sleeper: {result.sleeper_players_count} players")
                logger.info(f"  MFL: {result.mfl_players_count} players")
                logger.info(f"  Unified: {result.total_unified_players} total players")
                logger.info(f"  Cross-platform: {result.cross_platform_matches} matched")
            
            # Return structured results
            return {
                "overall_success": result.overall_success,
                "full_success": result.full_success,
                "sleeper_success": result.sleeper_success,
                "mfl_success": result.mfl_success,
                "total_unified_players": result.total_unified_players,
                "cross_platform_matches": result.cross_platform_matches,
                "duration": result.duration,
                "errors": result.errors
            }
            
        except Exception as e:
            logger.error(f"Player data sync job failed: {e}")
            raise
    
    async def player_data_sync_job(self):
        """Dedicated job for Epic A player data synchronization"""
        job_start_time = datetime.utcnow()
        logger.info(f"Starting Epic A player data synchronization at {job_start_time}")
        
        try:
            # Execute comprehensive player data sync
            result = self.player_data_sync_service.sync_all_player_data()
            
            job_end_time = datetime.utcnow()
            duration = (job_end_time - job_start_time).total_seconds()
            
            status = "SUCCESS" if result.overall_success else "FAILED"
            logger.info(f"Epic A player data sync completed in {duration:.2f}s - {status}")
            
            # Log detailed results
            platforms_synced = []
            if result.sleeper_success:
                platforms_synced.append(f"Sleeper ({result.sleeper_players_count})")
            if result.mfl_success:
                platforms_synced.append(f"MFL ({result.mfl_players_count})")
            
            if platforms_synced:
                logger.info(f"  Platforms: {', '.join(platforms_synced)} players synced")
                logger.info(f"  Unified database: {result.total_unified_players} total players")
                logger.info(f"  Cross-platform matches: {result.cross_platform_matches}")
                
                if result.cross_platform_matches > 0:
                    logger.info("  ✓ Epic A cross-platform unification ACTIVE")
                else:
                    logger.warning("  ⚠ Epic A cross-platform unification needs improvement")
            else:
                logger.error("  ✗ No platforms successfully synced")
            
            if result.errors:
                logger.warning(f"  Completed with {len(result.errors)} errors:")
                for error in result.errors[:3]:  # Show first 3 errors
                    logger.warning(f"    - {error}")
            
            return {
                "epic_a_player_sync": result.overall_success,
                "platforms_synced": len(platforms_synced),
                "total_unified_players": result.total_unified_players,
                "cross_platform_unification": result.cross_platform_matches,
                "duration": result.duration
            }
            
        except Exception as e:
            logger.error(f"Epic A player data sync job failed: {e}")
            raise
    
    async def waiver_sync_job(self):
        """Job to sync waiver states from both platforms"""
        job_start_time = datetime.utcnow()
        logger.info(f"Starting scheduled waiver sync at {job_start_time}")
        
        try:
            # Sync waiver states from both platforms
            results = await self.waiver_tracker_service.sync_all_waivers()
            
            job_end_time = datetime.utcnow()
            duration = (job_end_time - job_start_time).total_seconds()
            
            success_count = sum(results.values())
            logger.info(f"Waiver sync completed in {duration:.2f}s - {success_count}/2 platforms successful")
            
            # Log individual platform results
            for platform, success in results.items():
                status = "✓" if success else "✗"
                logger.info(f"  {status} {platform.upper()} waiver sync: {'SUCCESS' if success else 'FAILED'}")
            
            # Get statistics after sync
            stats = self.waiver_tracker_service.get_waiver_statistics()
            if 'error' not in stats:
                logger.info(f"  Sleeper waivers: {stats['waiver_data']['sleeper_count']}")
                logger.info(f"  MFL FAAB total: ${stats['mfl_faab_total']}")
            
            return results
            
        except Exception as e:
            logger.error(f"Waiver sync job failed: {e}")
            raise
    
    async def sleeper_waiver_job(self):
        """Job specifically for Sleeper waiver sync (6-hour intervals)"""
        job_start_time = datetime.utcnow()
        logger.info(f"Starting Sleeper waiver sync at {job_start_time}")
        
        try:
            success = self.waiver_tracker_service.sync_sleeper_waivers()
            
            job_end_time = datetime.utcnow()
            duration = (job_end_time - job_start_time).total_seconds()
            
            status = "SUCCESS" if success else "FAILED"
            logger.info(f"Sleeper waiver sync completed in {duration:.2f}s - {status}")
            
            return {"sleeper": success}
            
        except Exception as e:
            logger.error(f"Sleeper waiver sync job failed: {e}")
            raise
    
    async def mfl_faab_job(self):
        """Job specifically for MFL FAAB sync (daily intervals)"""
        job_start_time = datetime.utcnow()
        logger.info(f"Starting MFL FAAB sync at {job_start_time}")
        
        try:
            success = self.waiver_tracker_service.sync_mfl_faab()
            
            job_end_time = datetime.utcnow()
            duration = (job_end_time - job_start_time).total_seconds()
            
            status = "SUCCESS" if success else "FAILED"
            logger.info(f"MFL FAAB sync completed in {duration:.2f}s - {status}")
            
            return {"mfl": success}
            
        except Exception as e:
            logger.error(f"MFL FAAB sync job failed: {e}")
            raise
    
    async def usage_projections_sync_job(self):
        """Job to sync usage and projections data (Epic A)"""
        job_start_time = datetime.utcnow()
        logger.info(f"Starting usage and projections sync at {job_start_time}")
        
        try:
            results = self.usage_projections_service.daily_sync_job()
            
            job_end_time = datetime.utcnow()
            duration = (job_end_time - job_start_time).total_seconds()
            
            success_count = sum(results.values())
            logger.info(f"Usage/projections sync completed in {duration:.2f}s - {success_count}/3 operations successful")
            
            # Log individual results
            for operation, success in results.items():
                status = "✓" if success else "✗"
                logger.info(f"  {status} {operation}: {'SUCCESS' if success else 'FAILED'}")
            
            return results
            
        except Exception as e:
            logger.error(f"Usage/projections sync job failed: {e}")
            raise

    async def nightly_data_ingestion_job(self):
        """Nightly data ingestion for injuries, depth charts, betting lines, and defensive stats."""
        job_start_time = datetime.utcnow()
        logger.info(f"Starting nightly data ingestion at {job_start_time}")

        try:
            # Determine target week (current NFL week)
            week = self.waiver_candidates_builder._get_current_nfl_week()

            # Injuries
            injuries = self.nfl_data_service.build_injury_data_from_reports(week)
            inj_ok = self.nfl_data_service.sync_injury_data_to_database(injuries)

            # Depth charts
            depth = self.nfl_data_service.build_depth_chart_data(week)
            depth_ok = self.nfl_data_service.sync_depth_chart_data_to_database(depth)

            # Betting lines (for opp context)
            betting = self.nfl_data_service.build_betting_data_from_lines(week)
            bet_ok = self.nfl_data_service.sync_betting_data_to_database(betting)

            # Defensive stats for opponent filters
            def_stats = self.nfl_data_service.build_defensive_stats_from_pbp(week)
            def_ok = self.nfl_data_service.sync_defensive_stats_to_database(def_stats)

            duration = (datetime.utcnow() - job_start_time).total_seconds()
            logger.info(f"Nightly ingestion completed in {duration:.2f}s (injuries={inj_ok}, depth={depth_ok}, betting={bet_ok}, defense={def_ok})")
            return {
                'injuries': inj_ok,
                'depth': depth_ok,
                'betting': bet_ok,
                'defense': def_ok
            }
        except Exception as e:
            logger.error(f"Nightly data ingestion failed: {e}")
            raise

    async def weekly_schedule_refresh_job(self):
        """Weekly refresh of NFL schedule."""
        job_start_time = datetime.utcnow()
        logger.info(f"Starting weekly schedule refresh at {job_start_time}")
        try:
            schedule = self.nfl_data_service.build_schedule_data()
            ok = self.nfl_data_service.sync_schedule_data_to_database(schedule)
            logger.info(f"Weekly schedule refresh {'OK' if ok else 'FAILED'}")
            return {'schedule': ok}
        except Exception as e:
            logger.error(f"Weekly schedule refresh failed: {e}")
            raise
    
    
    async def waiver_candidates_refresh_job(self):
        """Job to refresh waiver candidates data (Epic A)"""
        job_start_time = datetime.utcnow()
        logger.info(f"Starting waiver candidates refresh at {job_start_time}")
        
        try:
            # For now, refresh for a demo league ID
            # In production, this would iterate through all active leagues
            demo_league_id = "demo_league_12345"
            
            success = self.waiver_candidates_builder.refresh_waiver_candidates_table(demo_league_id)
            
            job_end_time = datetime.utcnow()
            duration = (job_end_time - job_start_time).total_seconds()
            
            status = "SUCCESS" if success else "FAILED"
            logger.info(f"Waiver candidates refresh completed in {duration:.2f}s - {status}")
            
            return {"waiver_candidates_refresh": success}
        
        except Exception as e:
            logger.error(f"Waiver candidates refresh job failed: {e}")
            raise

    async def rules_evaluation_job(self):
        """Evaluate B1–B4 rules and persist alerts after candidates refresh."""
        job_start_time = datetime.utcnow()
        logger.info(f"Starting rules evaluation at {job_start_time}")
        try:
            league_id = "demo_league_12345"  # align with refresh job demo league
            stats = evaluate_rules(league_id)
            logger.info(f"Rules evaluated: {stats}")
            return stats
        except Exception as e:
            logger.error(f"Rules evaluation job failed: {e}")
            raise
    
    def schedule_roster_sync(self):
        """Schedule roster sync to run every 24 hours"""
        interval_hours = self.config.get_scheduler_config()['roster_sync_interval']
        
        self.scheduler.add_job(
            self.roster_sync_job,
            trigger=IntervalTrigger(hours=interval_hours),
            id='roster_sync',
            name='Roster Sync Job',
            replace_existing=True,
            next_run_time=datetime.utcnow() + timedelta(minutes=1)  # Start in 1 minute
        )
        
        logger.info(f"Scheduled roster sync to run every {interval_hours} hours")
    
    def schedule_player_mapping_update(self):
        """Schedule player mapping updates to run daily"""
        # Run player mapping update every 12 hours (twice daily)
        self.scheduler.add_job(
            self.player_mapping_job,
            trigger=IntervalTrigger(hours=12),
            id='player_mapping_update',
            name='Player Mapping Update Job',
            replace_existing=True,
            next_run_time=datetime.utcnow() + timedelta(minutes=5)  # Start in 5 minutes
        )
        
        logger.info("Scheduled player mapping update to run every 12 hours")
    
    def schedule_waiver_sync(self):
        """Schedule waiver sync jobs with different intervals for each platform"""
        # Schedule Sleeper waiver sync every 6 hours
        self.scheduler.add_job(
            self.sleeper_waiver_job,
            trigger=IntervalTrigger(hours=6),
            id='sleeper_waiver_sync',
            name='Sleeper Waiver Sync Job',
            replace_existing=True,
            next_run_time=datetime.utcnow() + timedelta(minutes=2)  # Start in 2 minutes
        )
        
        # Schedule MFL FAAB sync daily at 6 AM UTC (common league processing time)
        self.scheduler.add_job(
            self.mfl_faab_job,
            trigger=CronTrigger(hour=6, minute=0),
            id='mfl_faab_sync',
            name='MFL FAAB Sync Job',
            replace_existing=True
        )
        
        logger.info("Scheduled Sleeper waiver sync to run every 6 hours")
        logger.info("Scheduled MFL FAAB sync to run daily at 6:00 AM UTC")
    
    def schedule_combined_waiver_sync(self):
        """Schedule a combined waiver sync job (alternative to separate jobs)"""
        # Combined waiver sync every 6 hours (matches Sleeper frequency)
        self.scheduler.add_job(
            self.waiver_sync_job,
            trigger=IntervalTrigger(hours=6),
            id='combined_waiver_sync',
            name='Combined Waiver Sync Job',
            replace_existing=True,
            next_run_time=datetime.utcnow() + timedelta(minutes=3)  # Start in 3 minutes
        )
        
        logger.info("Scheduled combined waiver sync to run every 6 hours")
    
    def schedule_epic_a_jobs(self):
        """Schedule Epic A jobs (player data sync, multi-source pipeline and waiver candidates refresh)"""
        # Schedule Epic A player data sync weekly on Sundays at 6 AM UTC (before everything else)
        self.scheduler.add_job(
            self.player_data_sync_job,
            trigger=CronTrigger(day_of_week=6, hour=6, minute=0),  # Sunday 6 AM UTC
            id='epic_a_player_data_sync',
            name='Epic A Player Data Sync Job',
            replace_existing=True
        )
        
        
        # Keep original usage/projections sync as backup option
        self.scheduler.add_job(
            self.usage_projections_sync_job,
            trigger=CronTrigger(hour=7, minute=30),  # 30 minutes after main pipeline
            id='usage_projections_sync_backup',
            name='Usage/Projections Sync Backup Job',
            replace_existing=True
        )

        # Nightly ingestion for injuries/depth/betting/defense at 03:00 UTC
        self.scheduler.add_job(
            self.nightly_data_ingestion_job,
            trigger=CronTrigger(hour=3, minute=0),
            id='nightly_data_ingestion',
            name='Nightly Data Ingestion Job',
            replace_existing=True
        )

        # Weekly schedule refresh Sunday 05:00 UTC
        self.scheduler.add_job(
            self.weekly_schedule_refresh_job,
            trigger=CronTrigger(day_of_week=6, hour=5, minute=0),
            id='weekly_schedule_refresh',
            name='Weekly Schedule Refresh Job',
            replace_existing=True
        )

        # Schedule waiver candidates refresh twice daily (morning and evening)
        self.scheduler.add_job(
            self.waiver_candidates_refresh_job,
            trigger=CronTrigger(hour=8, minute=0),  # Morning refresh (after data sync)
            id='waiver_candidates_morning',
            name='Waiver Candidates Morning Refresh',
            replace_existing=True
        )
        
        self.scheduler.add_job(
            self.waiver_candidates_refresh_job,
            trigger=CronTrigger(hour=20, minute=0),  # Evening refresh
            id='waiver_candidates_evening',
            name='Waiver Candidates Evening Refresh',
            replace_existing=True
        )

        # Evaluate rules shortly after each refresh
        self.scheduler.add_job(
            self.rules_evaluation_job,
            trigger=CronTrigger(hour=8, minute=10),
            id='rules_eval_morning',
            name='Rules Evaluation Morning',
            replace_existing=True
        )
        self.scheduler.add_job(
            self.rules_evaluation_job,
            trigger=CronTrigger(hour=20, minute=10),
            id='rules_eval_evening',
            name='Rules Evaluation Evening',
            replace_existing=True
        )
        
        logger.info("Scheduled Epic A player data sync to run weekly on Sundays at 6:00 AM UTC")
        logger.info("Scheduled multi-source data pipeline to run daily at 7:00 AM UTC")
        logger.info("Scheduled usage/projections backup sync at 7:30 AM UTC")
        logger.info("Scheduled nightly data ingestion at 03:00 AM UTC")
        logger.info("Scheduled weekly schedule refresh Sunday 05:00 AM UTC")
        logger.info("Scheduled waiver candidates refresh twice daily (8:00 AM and 8:00 PM UTC)")
        logger.info("Scheduled rules evaluation at 8:10 AM and 8:10 PM UTC")
    
    def schedule_nfl_season_check(self):
        """Schedule job to check if we're in NFL season"""
        self.scheduler.add_job(
            self.nfl_season_check_job,
            trigger=CronTrigger(hour=0, minute=0),  # Run daily at midnight
            id='nfl_season_check',
            name='NFL Season Check Job',
            replace_existing=True
        )
        
        logger.info("Scheduled NFL season check to run daily at midnight")
    
    async def nfl_season_check_job(self):
        """Job to check if we're in NFL season and manage scheduler accordingly"""
        try:
            is_season = self.config.is_nfl_season()
            
            if is_season:
                logger.info("Currently in NFL season - scheduler will continue running")
                
                # Ensure roster sync job is scheduled
                if not self.scheduler.get_job('roster_sync'):
                    self.schedule_roster_sync()
                    logger.info("Re-enabled roster sync job for NFL season")
                
                # Ensure waiver sync jobs are scheduled
                if not self.scheduler.get_job('sleeper_waiver_sync') and not self.scheduler.get_job('combined_waiver_sync'):
                    self.schedule_waiver_sync()
                    logger.info("Re-enabled waiver sync jobs for NFL season")
                
            else:
                logger.info("Currently outside NFL season - pausing roster sync")
                
                # Remove roster sync job if it exists
                if self.scheduler.get_job('roster_sync'):
                    self.scheduler.remove_job('roster_sync')
                    logger.info("Disabled roster sync job for off-season")
                
                # Remove waiver sync jobs if they exist
                if self.scheduler.get_job('sleeper_waiver_sync'):
                    self.scheduler.remove_job('sleeper_waiver_sync')
                    logger.info("Disabled Sleeper waiver sync job for off-season")
                
                if self.scheduler.get_job('mfl_faab_sync'):
                    self.scheduler.remove_job('mfl_faab_sync')
                    logger.info("Disabled MFL FAAB sync job for off-season")
                
                if self.scheduler.get_job('combined_waiver_sync'):
                    self.scheduler.remove_job('combined_waiver_sync')
                    logger.info("Disabled combined waiver sync job for off-season")
            
        except Exception as e:
            logger.error(f"NFL season check job failed: {e}")
    
    async def manual_roster_sync(self) -> Dict[str, Any]:
        """Manually trigger roster sync (for testing or immediate needs)"""
        logger.info("Manual roster sync triggered")
        return await self.roster_sync_job()
    
    async def manual_player_mapping_update(self) -> Dict[str, Any]:
        """Manually trigger player mapping update"""
        logger.info("Manual player mapping update triggered")
        return await self.player_mapping_job()
    
    async def manual_waiver_sync(self) -> Dict[str, Any]:
        """Manually trigger waiver sync for both platforms"""
        logger.info("Manual waiver sync triggered")
        return await self.waiver_sync_job()
    
    async def manual_sleeper_waiver_sync(self) -> Dict[str, Any]:
        """Manually trigger Sleeper waiver sync"""
        logger.info("Manual Sleeper waiver sync triggered")
        return await self.sleeper_waiver_job()
    
    async def manual_mfl_faab_sync(self) -> Dict[str, Any]:
        """Manually trigger MFL FAAB sync"""
        logger.info("Manual MFL FAAB sync triggered")
        return await self.mfl_faab_job()
    
    async def manual_usage_projections_sync(self) -> Dict[str, Any]:
        """Manually trigger usage and projections sync"""
        logger.info("Manual usage/projections sync triggered")
        return await self.usage_projections_sync_job()
    
    async def manual_waiver_candidates_refresh(self) -> Dict[str, Any]:
        """Manually trigger waiver candidates refresh"""
        logger.info("Manual waiver candidates refresh triggered")
        return await self.waiver_candidates_refresh_job()
    
    
    async def manual_player_data_sync(self) -> Dict[str, Any]:
        """Manually trigger Epic A player data synchronization"""
        logger.info("Manual Epic A player data sync triggered")
        return await self.player_data_sync_job()
    
    def start(self):
        """Start the scheduler"""
        try:
            # Check if we're in NFL season before starting
            if not self.config.is_nfl_season():
                logger.warning("Starting scheduler outside NFL season - roster sync will be disabled")
            
            # Schedule all jobs
            self.schedule_roster_sync()
            self.schedule_player_mapping_update()
            self.schedule_waiver_sync()  # Add waiver sync scheduling
            self.schedule_epic_a_jobs()  # Add Epic A jobs
            self.schedule_nfl_season_check()
            
            # Start the scheduler
            self.scheduler.start()
            logger.info("Fantasy Football Scheduler started successfully")
            
            # Log scheduled jobs
            jobs = self.scheduler.get_jobs()
            logger.info(f"Scheduled jobs: {len(jobs)}")
            for job in jobs:
                next_run = job.next_run_time.strftime('%Y-%m-%d %H:%M:%S UTC') if job.next_run_time else 'N/A'
                logger.info(f"  - {job.name} (ID: {job.id}): Next run at {next_run}")
            
        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}")
            raise
    
    def stop(self):
        """Stop the scheduler"""
        try:
            if self.scheduler and self.scheduler.running:
                self.scheduler.shutdown()
                logger.info("Fantasy Football Scheduler stopped")
        except Exception as e:
            logger.error(f"Error stopping scheduler: {e}")
    
    def get_job_status(self) -> Dict[str, Any]:
        """Get status of scheduled jobs"""
        if not self.scheduler:
            return {"status": "not_initialized"}
        
        jobs = self.scheduler.get_jobs()
        job_status = {}
        
        for job in jobs:
            job_status[job.id] = {
                "name": job.name,
                "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
                "trigger": str(job.trigger)
            }
        
        return {
            "status": "running" if self.scheduler.running else "stopped",
            "jobs": job_status,
            "total_jobs": len(jobs)
        }
    
    def reschedule_job(self, job_id: str, **kwargs):
        """Reschedule a specific job"""
        try:
            job = self.scheduler.get_job(job_id)
            if job:
                self.scheduler.reschedule_job(job_id, **kwargs)
                logger.info(f"Rescheduled job {job_id}")
            else:
                logger.warning(f"Job {job_id} not found")
        except Exception as e:
            logger.error(f"Failed to reschedule job {job_id}: {e}")

# Global scheduler instance
scheduler_instance = None

def get_scheduler() -> FantasyFootballScheduler:
    """Get the global scheduler instance"""
    global scheduler_instance
    if scheduler_instance is None:
        scheduler_instance = FantasyFootballScheduler()
    return scheduler_instance

async def start_scheduler():
    """Start the global scheduler"""
    scheduler = get_scheduler()
    scheduler.start()
    return scheduler

def stop_scheduler():
    """Stop the global scheduler"""
    global scheduler_instance
    if scheduler_instance:
        scheduler_instance.stop()
        scheduler_instance = None

# Test function
async def test_scheduler():
    """Test the scheduler functionality"""
    print("Testing Fantasy Football Scheduler...")
    print("=" * 50)
    
    scheduler = FantasyFootballScheduler()
    
    try:
        # Test manual jobs
        print("\n1. Testing manual roster sync...")
        results = await scheduler.manual_roster_sync()
        print(f"   Results: {results}")
        
        print("\n2. Testing manual player mapping update...")
        stats = await scheduler.manual_player_mapping_update()
        print(f"   Stats: {stats}")
        
        print("\n3. Testing manual waiver sync...")
        waiver_results = await scheduler.manual_waiver_sync()
        print(f"   Waiver Results: {waiver_results}")
        
        print("\n4. Testing individual waiver syncs...")
        sleeper_result = await scheduler.manual_sleeper_waiver_sync()
        mfl_result = await scheduler.manual_mfl_faab_sync()
        print(f"   Sleeper: {sleeper_result}")
        print(f"   MFL: {mfl_result}")
        
        print("\n5. Testing Epic A player data sync...")
        player_sync_result = await scheduler.manual_player_data_sync()
        print(f"   Player sync result: {player_sync_result}")
        
        print("\n6. Testing scheduler start/stop...")
        scheduler.start()
        
        # Check job status
        status = scheduler.get_job_status()
        print(f"   Scheduler status: {status['status']}")
        print(f"   Total jobs: {status['total_jobs']}")
        
        # Wait a bit to see if jobs are scheduled
        await asyncio.sleep(2)
        
        # Stop scheduler
        scheduler.stop()
        print("   ✓ Scheduler stopped successfully")
        
    except Exception as e:
        print(f"   ✗ Scheduler test failed: {e}")

if __name__ == "__main__":
    asyncio.run(test_scheduler())
