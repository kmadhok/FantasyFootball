#!/usr/bin/env python3
"""
Test script for the roster sync scheduler
"""

import asyncio
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.services.scheduler import FantasyFootballScheduler
from src.config import get_config

async def test_scheduler():
    """Test the scheduler functionality"""
    print("🏈 Testing Fantasy Football Scheduler")
    print("=" * 50)
    
    config = get_config()
    print(f"Config loaded: {config.SLEEPER_LEAGUE_ID}, {config.MFL_LEAGUE_ID}")
    
    scheduler = FantasyFootballScheduler()
    
    try:
        print("\n1. Testing manual roster sync...")
        results = await scheduler.manual_roster_sync()
        print(f"   Results: {results}")
        
        print("\n2. Testing scheduler start...")
        scheduler.start()
        
        status = scheduler.get_job_status()
        print(f"   Status: {status['status']}")
        print(f"   Jobs: {status['total_jobs']}")
        
        for job_id, job_info in status['jobs'].items():
            print(f"   - {job_info['name']}: {job_info['next_run_time']}")
        
        print("\n3. Waiting 5 seconds...")
        await asyncio.sleep(5)
        
        print("\n4. Stopping scheduler...")
        scheduler.stop()
        print("   ✓ Scheduler stopped")
        
    except Exception as e:
        print(f"   ✗ Test failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = asyncio.run(test_scheduler())
    sys.exit(0 if success else 1)