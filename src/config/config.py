import os
import logging
from typing import Optional, List
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Detect project root directory
def get_project_root() -> Path:
    """Get the project root directory by looking for key files"""
    current_path = Path(__file__).resolve()
    
    # Look for project markers (requirements.txt, .git, etc.)
    for parent in current_path.parents:
        if (parent / "requirements.txt").exists() or (parent / ".git").exists():
            return parent
    
    # Fallback: assume we're in src/config/ and go up 2 levels
    return current_path.parent.parent

PROJECT_ROOT = get_project_root()
DATA_DIR = PROJECT_ROOT / "data"

logger = logging.getLogger(__name__)

class Config:
    """Configuration management for fantasy football application"""
    
    def __init__(self):
        self.load_config()
        self.validate_config()
    
    def load_config(self):
        """Load configuration from environment variables"""
        
        # Fantasy Football League Configuration
        self.SLEEPER_LEAGUE_ID: str = os.getenv('SLEEPER_LEAGUE_ID', '')
        self.MFL_LEAGUE_ID: str = os.getenv('MFL_LEAGUE_ID', '')
        self.MFL_LEAGUE_API_KEY: str = os.getenv('MFL_LEAGUE_API_KEY', '')
        
        # Database Configuration
        default_db_path = f'sqlite:///{DATA_DIR / "fantasy_football.db"}'
        self.DATABASE_URL: str = os.getenv('DATABASE_URL', default_db_path)
        
        # Redis Configuration
        self.REDIS_URL: str = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
        
        # Slack Integration
        self.SLACK_WEBHOOK_URL: str = os.getenv('SLACK_WEBHOOK_URL', '')
        
        # Reddit API Configuration
        self.REDDIT_CLIENT_ID: str = os.getenv('REDDIT_CLIENT_ID', '')
        self.REDDIT_CLIENT_SECRET: str = os.getenv('REDDIT_CLIENT_SECRET', '')
        self.REDDIT_USER_AGENT: str = os.getenv('REDDIT_USER_AGENT', 'FantasyFootballBot/1.0')
        
        # Twitter API Configuration
        self.TWITTER_API_KEY: str = os.getenv('TWITTER_API_KEY', '')
        self.TWITTER_API_SECRET: str = os.getenv('TWITTER_API_SECRET', '')
        self.TWITTER_ACCESS_TOKEN: str = os.getenv('TWITTER_ACCESS_TOKEN', '')
        self.TWITTER_ACCESS_TOKEN_SECRET: str = os.getenv('TWITTER_ACCESS_TOKEN_SECRET', '')
        self.TWITTER_BEARER_TOKEN: str = os.getenv('TWITTER_BEARER_TOKEN', '')
        
        # Application Configuration
        self.LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')
        self.DEBUG: bool = os.getenv('DEBUG', 'false').lower() == 'true'
        self.NFL_SEASON_START_DATE: str = os.getenv('NFL_SEASON_START_DATE', '2024-09-05')
        self.NFL_SEASON_END_DATE: str = os.getenv('NFL_SEASON_END_DATE', '2025-02-16')
        
        # Rate Limiting Configuration
        self.REDDIT_RATE_LIMIT: int = int(os.getenv('REDDIT_RATE_LIMIT', '100'))
        self.TWITTER_RATE_LIMIT: int = int(os.getenv('TWITTER_RATE_LIMIT', '50'))
        
        # Alert Configuration
        self.ALERT_LATENCY_THRESHOLD: int = int(os.getenv('ALERT_LATENCY_THRESHOLD', '300'))
        self.CONFIDENCE_THRESHOLD: float = float(os.getenv('CONFIDENCE_THRESHOLD', '0.7'))
        
        # Scheduler Configuration
        self.ROSTER_SYNC_INTERVAL: int = int(os.getenv('ROSTER_SYNC_INTERVAL', '24'))
        self.SLEEPER_WAIVER_SYNC_INTERVAL: int = int(os.getenv('SLEEPER_WAIVER_SYNC_INTERVAL', '6'))
        self.MFL_WAIVER_SYNC_INTERVAL: int = int(os.getenv('MFL_WAIVER_SYNC_INTERVAL', '24'))
        self.NEWS_POLLING_INTERVAL: int = int(os.getenv('NEWS_POLLING_INTERVAL', '2'))
        
        # Logging configuration
        self.setup_logging()
    
    def validate_config(self):
        """Validate required configuration values"""
        required_configs = []
        
        # Check required league configurations
        if not self.SLEEPER_LEAGUE_ID:
            required_configs.append('SLEEPER_LEAGUE_ID')
        if not self.MFL_LEAGUE_ID:
            required_configs.append('MFL_LEAGUE_ID')
        if not self.MFL_LEAGUE_API_KEY:
            required_configs.append('MFL_LEAGUE_API_KEY')
        
        # Check required alert configuration
        if not self.SLACK_WEBHOOK_URL or self.SLACK_WEBHOOK_URL == 'your_slack_webhook_url_here':
            logger.warning("SLACK_WEBHOOK_URL not configured - alerts will not be sent")
        
        # Check API configurations (optional for now)
        if not self.REDDIT_CLIENT_ID or self.REDDIT_CLIENT_ID == 'your_reddit_client_id_here':
            logger.warning("Reddit API not configured - Reddit news monitoring disabled")
        
        if not self.TWITTER_API_KEY or self.TWITTER_API_KEY == 'your_twitter_api_key_here':
            logger.warning("Twitter API not configured - Twitter news monitoring disabled")
        
        if required_configs:
            raise ValueError(f"Missing required configuration: {', '.join(required_configs)}")
        
        logger.info("Configuration validation completed successfully")
    
    def setup_logging(self):
        """Setup logging configuration"""
        # Ensure data directory exists
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        
        log_file_path = DATA_DIR / "fantasy_football.log"
        
        logging.basicConfig(
            level=getattr(logging, self.LOG_LEVEL.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(str(log_file_path))
            ]
        )
    
    def is_nfl_season(self) -> bool:
        """Check if current date is within NFL season"""
        try:
            start_date = datetime.strptime(self.NFL_SEASON_START_DATE, '%Y-%m-%d')
            end_date = datetime.strptime(self.NFL_SEASON_END_DATE, '%Y-%m-%d')
            current_date = datetime.now()
            
            return start_date <= current_date <= end_date
        except ValueError as e:
            logger.error(f"Invalid date format in NFL season configuration: {e}")
            return True  # Default to active if parsing fails
    
    def get_api_keys(self) -> dict:
        """Get all API keys for external services"""
        return {
            'sleeper': {
                'league_id': self.SLEEPER_LEAGUE_ID
            },
            'mfl': {
                'league_id': self.MFL_LEAGUE_ID,
                'api_key': self.MFL_LEAGUE_API_KEY
            },
            'reddit': {
                'client_id': self.REDDIT_CLIENT_ID,
                'client_secret': self.REDDIT_CLIENT_SECRET,
                'user_agent': self.REDDIT_USER_AGENT
            },
            'twitter': {
                'api_key': self.TWITTER_API_KEY,
                'api_secret': self.TWITTER_API_SECRET,
                'access_token': self.TWITTER_ACCESS_TOKEN,
                'access_token_secret': self.TWITTER_ACCESS_TOKEN_SECRET,
                'bearer_token': self.TWITTER_BEARER_TOKEN
            },
            'slack': {
                'webhook_url': self.SLACK_WEBHOOK_URL
            }
        }
    
    def get_rate_limits(self) -> dict:
        """Get rate limiting configuration"""
        return {
            'reddit': self.REDDIT_RATE_LIMIT,
            'twitter': self.TWITTER_RATE_LIMIT
        }
    
    def get_scheduler_config(self) -> dict:
        """Get scheduler configuration"""
        return {
            'roster_sync_interval': self.ROSTER_SYNC_INTERVAL,
            'sleeper_waiver_sync_interval': self.SLEEPER_WAIVER_SYNC_INTERVAL,
            'mfl_waiver_sync_interval': self.MFL_WAIVER_SYNC_INTERVAL,
            'news_polling_interval': self.NEWS_POLLING_INTERVAL
        }
    
    def __str__(self) -> str:
        """String representation of config (without sensitive data)"""
        return f"""
Fantasy Football Configuration:
- Sleeper League ID: {self.SLEEPER_LEAGUE_ID}
- MFL League ID: {self.MFL_LEAGUE_ID}
- Database URL: {self.DATABASE_URL}
- Redis URL: {self.REDIS_URL}
- Log Level: {self.LOG_LEVEL}
- Debug Mode: {self.DEBUG}
- NFL Season: {self.NFL_SEASON_START_DATE} to {self.NFL_SEASON_END_DATE}
- Reddit Rate Limit: {self.REDDIT_RATE_LIMIT} req/min
- Twitter Rate Limit: {self.TWITTER_RATE_LIMIT} rules
- Alert Threshold: {self.ALERT_LATENCY_THRESHOLD}s
- Confidence Threshold: {self.CONFIDENCE_THRESHOLD}
"""

# Global configuration instance
config = Config()

# Helper functions for easy access
def get_config() -> Config:
    """Get the global configuration instance"""
    return config

def is_development() -> bool:
    """Check if running in development mode"""
    return config.DEBUG

def get_database_url() -> str:
    """Get database URL"""
    return config.DATABASE_URL

def get_redis_url() -> str:
    """Get Redis URL"""
    return config.REDIS_URL

def get_sleeper_league_id() -> str:
    """Get Sleeper league ID"""
    return config.SLEEPER_LEAGUE_ID

def get_mfl_config() -> dict:
    """Get MFL configuration"""
    return {
        'league_id': config.MFL_LEAGUE_ID,
        'api_key': config.MFL_LEAGUE_API_KEY
    }

def get_project_root_path() -> Path:
    """Get the project root directory path"""
    return PROJECT_ROOT

def get_data_directory() -> Path:
    """Get the data directory path"""
    return DATA_DIR

def ensure_data_directory():
    """Ensure the data directory exists"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

# Validate configuration on import
if __name__ == "__main__":
    print(config)