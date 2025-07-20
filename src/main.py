#!/usr/bin/env python3
"""
Fantasy Football Alert System - Main Application Entry Point
"""

import asyncio
import signal
import sys
import logging
from datetime import datetime

from .config import get_config
from .database import init_database
from .services.scheduler import get_scheduler, start_scheduler, stop_scheduler

logger = logging.getLogger(__name__)

class FantasyFootballApp:
    """Main application class for Fantasy Football Alert System"""
    
    def __init__(self):
        self.config = get_config()
        self.scheduler = None
        self.running = False
    
    async def initialize(self):
        """Initialize the application"""
        logger.info("Initializing Fantasy Football Alert System...")
        
        try:
            # Initialize database
            init_database()
            logger.info("Database initialized successfully")
            
            # Initialize scheduler
            self.scheduler = await start_scheduler()
            logger.info("Scheduler initialized successfully")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize application: {e}")
            return False
    
    async def start(self):
        """Start the application"""
        logger.info("Starting Fantasy Football Alert System...")
        
        # Initialize application
        if not await self.initialize():
            logger.error("Application initialization failed")
            return False
        
        self.running = True
        
        # Check if we're in NFL season
        if self.config.is_nfl_season():
            logger.info("✓ Currently in NFL season - full monitoring active")
        else:
            logger.warning("⚠ Currently outside NFL season - limited monitoring")
        
        # Print startup summary
        self._print_startup_summary()
        
        return True
    
    def _print_startup_summary(self):
        """Print startup summary"""
        print("\n" + "="*60)
        print("🏈 FANTASY FOOTBALL ALERT SYSTEM")
        print("="*60)
        print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⚙️  Config: {self.config.LOG_LEVEL} logging")
        print(f"🏟️  Season: {'ACTIVE' if self.config.is_nfl_season() else 'OFF-SEASON'}")
        print(f"📊 Sleeper League: {self.config.SLEEPER_LEAGUE_ID}")
        print(f"📊 MFL League: {self.config.MFL_LEAGUE_ID}")
        
        # Print scheduler status
        if self.scheduler:
            status = self.scheduler.get_job_status()
            print(f"⏰ Scheduler: {status['status'].upper()} ({status['total_jobs']} jobs)")
            
            for job_id, job_info in status['jobs'].items():
                next_run = job_info['next_run_time']
                if next_run:
                    next_run_str = datetime.fromisoformat(next_run).strftime('%H:%M:%S')
                    print(f"   • {job_info['name']}: Next run at {next_run_str}")
        
        print("="*60)
        print("✅ System ready - monitoring fantasy football news...")
        print("   Press Ctrl+C to stop")
        print("="*60)
    
    def stop(self):
        """Stop the application"""
        logger.info("Stopping Fantasy Football Alert System...")
        
        self.running = False
        
        # Stop scheduler
        if self.scheduler:
            stop_scheduler()
        
        logger.info("Application stopped")
    
    async def run(self):
        """Main application loop"""
        try:
            # Start the application
            if not await self.start():
                return False
            
            # Keep the application running
            while self.running:
                await asyncio.sleep(1)
            
            return True
            
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
            return True
        except Exception as e:
            logger.error(f"Application error: {e}")
            return False
        finally:
            self.stop()

def setup_signal_handlers(app: FantasyFootballApp):
    """Setup signal handlers for graceful shutdown"""
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}")
        app.stop()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

async def main():
    """Main entry point"""
    # Create application instance
    app = FantasyFootballApp()
    
    # Setup signal handlers
    setup_signal_handlers(app)
    
    # Run the application
    success = await app.run()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    asyncio.run(main())