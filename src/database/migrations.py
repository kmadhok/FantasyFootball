import os
import logging
from datetime import datetime, timedelta
from sqlalchemy import text
from .models import create_tables, drop_tables, engine, SessionLocal, DeduplicationLog

logger = logging.getLogger(__name__)

def init_database():
    """Initialize database with tables and initial data"""
    logger.info("Initializing database...")
    
    # Create data directory if it doesn't exist
    data_dir = os.path.dirname(os.path.abspath('data/fantasy_football.db'))
    os.makedirs(data_dir, exist_ok=True)
    
    # Create all tables
    create_tables()
    logger.info("Database tables created successfully")
    
    # Run initial migrations
    run_migrations()
    logger.info("Database initialization complete")

def run_migrations():
    """Run database migrations"""
    logger.info("Running database migrations...")
    
    # Create indexes for better performance
    create_indexes()
    
    # Clean up old deduplication logs (older than 24 hours)
    cleanup_old_dedup_logs()
    
    logger.info("Database migrations completed")

def create_indexes():
    """Create database indexes for better performance"""
    with engine.connect() as conn:
        try:
            # Index on player platform IDs for faster lookups
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_players_sleeper_id ON players (sleeper_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_players_mfl_id ON players (mfl_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_players_nfl_id ON players (nfl_id)"))
            
            # Index on roster entries for faster roster lookups
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_roster_platform_league ON roster_entries (platform, league_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_roster_player_active ON roster_entries (player_id, is_active)"))
            
            # Index on news items for deduplication
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_news_headline_hash ON news_items (headline_hash)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_news_player_created ON news_items (player_id, created_at)"))
            
            # Index on alerts for faster queries
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts (delivery_status)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_alerts_created ON alerts (created_at)"))
            
            # Index on deduplication logs
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_dedup_player_hash ON deduplication_logs (player_id, headline_hash)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_dedup_expires ON deduplication_logs (expires_at)"))
            
            # Index on waiver states
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_waiver_platform_league ON waiver_states (platform, league_id)"))

            # Indexes on roster snapshots
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_roster_snap_league_week ON roster_snapshots (league_id, week)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_roster_snap_team_week ON roster_snapshots (team_id, week)"))
            
            conn.commit()
            logger.info("Database indexes created successfully")
            
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")
            conn.rollback()

def cleanup_old_dedup_logs():
    """Remove deduplication logs older than 24 hours"""
    db = SessionLocal()
    try:
        cutoff_time = datetime.utcnow() - timedelta(hours=24)
        deleted_count = db.query(DeduplicationLog).filter(
            DeduplicationLog.expires_at < cutoff_time
        ).delete()
        
        db.commit()
        logger.info(f"Cleaned up {deleted_count} old deduplication logs")
        
    except Exception as e:
        logger.error(f"Error cleaning up deduplication logs: {e}")
        db.rollback()
    finally:
        db.close()

def reset_database():
    """Reset database (drop and recreate all tables) - USE WITH CAUTION"""
    logger.warning("Resetting database - all data will be lost!")
    drop_tables()
    create_tables()
    run_migrations()
    logger.info("Database reset complete")

def backup_database():
    """Create a backup of the database"""
    import shutil
    from datetime import datetime
    
    db_path = 'data/fantasy_football.db'
    if os.path.exists(db_path):
        backup_path = f'data/fantasy_football_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.db'
        shutil.copy2(db_path, backup_path)
        logger.info(f"Database backup created: {backup_path}")
        return backup_path
    else:
        logger.warning("Database file not found, no backup created")
        return None

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Initialize database
    init_database()
