## Relevant Files

- `.env` - Environment configuration containing sleeper_league_id, mfl_league_id, and mfl_league_api_key
- `src/services/roster_sync.py` - Service to sync rosters from Sleeper and MFL APIs
- `src/services/test_roster_sync.py` - Unit tests for roster sync service
- `src/services/waiver_tracker.py` - Service to track waiver orders and FAAB balances from both platforms
- `src/services/test_waiver_tracker.py` - Unit tests for waiver tracker service
- `src/services/alert_context.py` - Service to enhance alerts with waiver and roster context
- `src/services/scheduler.py` - Enhanced scheduler with waiver sync jobs (6h Sleeper, daily MFL)
- `src/services/news_ingestion.py` - Service to ingest news from Reddit, Twitter, and APIs
- `src/services/test_news_ingestion.py` - Unit tests for news ingestion service
- `src/services/alert_delivery.py` - Service to deliver alerts via Slack webhook
- `src/services/test_alert_delivery.py` - Unit tests for alert delivery service
- `src/workers/ingest_worker.py` - Background worker for news ingestion
- `src/workers/dedup_worker.py` - Background worker for deduplication
- `src/workers/notify_worker.py` - Background worker for alert notifications
- `src/utils/player_id_mapper.py` - Utility to map platform-specific player IDs to canonical NFL IDs
- `src/utils/test_player_id_mapper.py` - Unit tests for player ID mapper
- `src/utils/keyword_filter.py` - Utility to filter news items by keywords
- `src/utils/test_keyword_filter.py` - Unit tests for keyword filter
- `src/config/config.py` - Configuration management for API keys and settings (reads from .env)
- `src/database/models.py` - Database models for players, news, alerts, waiver states, and waiver claims
- `src/database/migrations.py` - Database migration scripts
- `docker-compose.yml` - Docker configuration for Redis and application
- `Dockerfile` - Docker container configuration
- `requirements.txt` - Python dependencies
- `pyproject.toml` - Python project configuration

### Notes

- Unit tests should typically be placed alongside the code files they are testing (e.g., `service.py` and `test_service.py` in the same directory).
- Use `pytest [optional/path/to/test/file]` to run tests. Running without a path executes all tests found by the pytest configuration.

## Tasks

- [ ] 1.0 Project Setup and Infrastructure
  - [x] 1.1 Initialize Python project with virtual environment and requirements.txt
  - [x] 1.2 Set up Docker container with Redis for message passing
  - [x] 1.3 Configure SQLite database with initial schema using SQLAlchemy
  - [x] 1.4 Create .env file with sleeper_league_id, mfl_league_id, and mfl_league_api_key
  - [x] 1.5 Set up environment configuration loading from .env file using python-dotenv
  - [ ] 1.6 Create basic project structure with src/, tests/, and config/ directories
  - [ ] 1.7 Set up pytest testing framework and test scripts
  - [ ] 1.8 Configure deployment for Fly.io or GitHub Actions

- [x] 2.0 Roster Synchronization System
  - [x] 2.1 Implement Sleeper API client for roster fetching (/league/{league_id}/rosters)
  - [x] 2.2 Implement MFL API client for roster fetching (export?TYPE=rosters)
  - [x] 2.3 Create player ID mapping system to canonical NFL IDs
  - [x] 2.4 Build roster sync scheduler to run every 24 hours
  - [x] 2.5 Implement database storage for roster data
  - [x] 2.6 Add error handling and retry logic for API failures
  - [x] 2.7 Create unit tests for roster sync functionality

- [x] 3.0 Waiver State Tracking
  - [x] 3.1 Implement Sleeper waiver order fetching (/league/{id}/waivers)
  - [x] 3.2 Implement MFL FAAB balance fetching (export?TYPE=blindBidSummary)
  - [x] 3.3 Create waiver state scheduler (6h for Sleeper, daily for MFL)
  - [x] 3.4 Build database models for waiver state storage
  - [x] 3.5 Add waiver state to alert context for notifications
  - [x] 3.6 Create unit tests for waiver tracking functionality

- [ ] 4.0 News Ingestion and Processing
  - [ ] 4.1 Implement Reddit API client for r/fantasyfootball streaming
  - [ ] 4.2 Implement Twitter/X filtered stream for beat reporter handles
  - [ ] 4.3 Create polling system for ESPN player news and Sleeper trending (2min intervals)
  - [ ] 4.4 Build keyword filtering system for roster-movement terms
  - [ ] 4.5 Implement confidence scoring based on source weight and keyword match
  - [ ] 4.6 Create deduplication system using (player_id, headline_hash) within 24h window
  - [ ] 4.7 Set up Redis-based message queue for worker communication
  - [ ] 4.8 Create ingest, dedup, and notify async workers
  - [ ] 4.9 Add rate limiting (< 100 req/min Reddit, < 50 filtered rules Twitter)
  - [ ] 4.10 Create unit tests for news ingestion and processing

- [ ] 5.0 Alert Delivery System
  - [ ] 5.1 Implement Slack webhook integration for alert delivery
  - [ ] 5.2 Create alert formatting with player name, team, event type, source link, timestamp
  - [ ] 5.3 Add waiver information (order/FAAB) and rostered-by-user flag to alerts
  - [ ] 5.4 Implement alert filtering (rostered players + unrostered starter-level players)
  - [ ] 5.5 Create player classification system for starter-level determination (RB1, WR1-2, QB, TE1)
  - [ ] 5.6 Add service lifecycle management (auto-disable after Super Bowl + 7 days)
  - [ ] 5.7 Implement preseason restart automation
  - [ ] 5.8 Create unit tests for alert delivery functionality