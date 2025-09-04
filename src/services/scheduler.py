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
from ..utils.player_id_mapper import PlayerIDMapper

logger = logging.getLogger(__name__)

class FantasyFootballScheduler:
    """Scheduler for fantasy football background tasks"""
    
    def __init__(self):
        self.config = get_config()
        self.scheduler = None
        self.roster_sync_service = RosterSyncService()
        self.waiver_tracker_service = WaiverTrackerService()
        self.player_mapper = PlayerIDMapper()
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
        """Job to update player mappings"""
        job_start_time = datetime.utcnow()
        logger.info(f"Starting scheduled player mapping update at {job_start_time}")
        
        try:
            # For now, just load existing mappings from database
            # In a full implementation, you would fetch fresh data from APIs here
            self.player_mapper.load_from_database()
            
            # Get statistics
            stats = self.player_mapper.get_mapping_stats()
            
            job_end_time = datetime.utcnow()
            duration = (job_end_time - job_start_time).total_seconds()
            
            logger.info(f"Player mapping update completed in {duration:.2f}s")
            logger.info(f"Total players: {stats['total_players']}, Cross-platform: {stats['cross_platform_mappings']}")
            
            # Return stats as success indicator
            return stats
            
        except Exception as e:
            logger.error(f"Player mapping job failed: {e}")
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
        
        print("\n5. Testing scheduler start/stop...")
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