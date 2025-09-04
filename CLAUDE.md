# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Fantasy Football Alert System

This is a comprehensive Fantasy Football monitoring system that integrates with Sleeper and MyFantasyLeague (MFL) platforms. The system tracks rosters, waiver states, and delivers intelligent alerts through a multi-layered architecture.

## Virtual Environment Setup

**Always use the existing virtual environment:**
```bash
source venv/bin/activate  # Required for all Python operations
```

## Core Architecture

The system follows a **Service-Oriented Architecture** with clear separation of concerns:

```
APIs (Sleeper/MFL) → Services → Database → Alert Context → Notifications
```

### Key Components:

1. **Multi-Platform Integration**: Handles both Sleeper and MFL APIs with unified data models
2. **Cross-Platform Player Mapping**: `PlayerIDMapper` creates canonical NFL IDs to unify players across platforms
3. **Scheduled Background Processing**: APScheduler coordinates periodic data synchronization
4. **Database-Backed Persistence**: SQLAlchemy models with SQLite for roster tracking and waiver states

## Essential Commands

### Testing and Development
```bash
# Run all tests
pytest -v

# Run specific service tests
python -m pytest src/services/test_roster_sync.py -v
python -m pytest src/services/test_waiver_tracker.py -v

# Test individual services directly
python src/services/roster_sync.py
python src/services/waiver_tracker.py
python src/services/reddit_client.py

# Manual testing framework
python manual_test/comprehensive_test.py
python manual_test/comprehensive_test.py --interactive
```

### User-Specific Operations
```bash
# Get specific user's roster (requires Sleeper league setup)
python get_kanum_roster.py

# Expected fantasy points lookup (requires additional dependencies)
python fantasy_xfp_lookup.py
```

### Database Operations
```bash
# Run database migrations
python src/database/migrations.py

# Test database storage
python src/database/test_roster_storage.py
```

## Configuration Management

**Environment Variables** (`.env` file):
- `SLEEPER_LEAGUE_ID`, `MFL_LEAGUE_ID`, `MFL_LEAGUE_API_KEY` - Primary league integration
- `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET` - Required for news monitoring
- `DATABASE_URL`, `REDIS_URL` - Data persistence and message queuing
- Rate limiting and scheduling intervals

The system uses absolute path resolution to work from any directory via `src.config.config.get_project_root()`.

## Data Flow Architecture

### Core Service Layer (`src/services/`)

**Roster Synchronization** (`roster_sync.py`):
- `SleeperAPIClient` and `MFLAPIClient` - Platform-specific API wrappers
- `RosterSyncService` - Orchestrates cross-platform sync with error handling
- Scheduled every 24 hours, stores in `RosterEntry` models

**Waiver Tracking** (`waiver_tracker.py`):
- `SleeperWaiverClient` - Tracks waiver order priority
- `MFLWaiverClient` - Tracks FAAB (Free Agent Acquisition Budget) balances  
- `WaiverTrackerService` - Coordinates both platforms, stores in `WaiverState` models
- Different sync intervals: 6h (Sleeper) vs 24h (MFL)

**Alert Enhancement** (`alert_context.py`):
- `AlertContextService` - Enriches alerts with waiver recommendations and FAAB bid suggestions
- Cross-references roster ownership with available players

### Database Models (`src/database/models.py`)

**Core Tables:**
- `Player` - Unified NFL players with cross-platform IDs (sleeper_id, mfl_id, nfl_id)
- `RosterEntry` - Team roster assignments across platforms
- `WaiverState` - Current waiver positions and FAAB balances
- `WaiverClaim` - Historical waiver transactions

**Key Relationship**: All tables link through canonical `nfl_id` via `PlayerIDMapper` for cross-platform data consistency.

### Utilities (`src/utils/`)

**Player ID Mapping** (`player_id_mapper.py`):
- **Critical Component**: Creates unified player identification across Sleeper/MFL platforms
- `generate_canonical_id()` - Creates hash-based NFL IDs from player attributes
- `get_canonical_id()` - Maps platform-specific IDs to canonical IDs
- Uses dependency injection pattern to avoid circular imports

**Retry Logic** (`retry_handler.py`):
- `@handle_api_request` decorator for automatic retry with backoff
- `APIError` custom exception with platform context
- Rate limiting and failure statistics tracking

## Manual Testing Framework (`manual_test/`)

**Purpose**: Direct API validation without database dependencies, designed to complement the production system.

**Key Files:**
- `comprehensive_test.py` - Interactive testing with both platforms
- `sleeper_client.py`, `mfl_client.py` - Direct API clients using official endpoints
- `data_models.py` - Simplified data structures for testing

**Usage Pattern**: Use for API credential verification, debugging, and understanding data structures before implementing production features.

## Development Patterns

### Error Handling Philosophy
The system prioritizes **real data over fake data** - services stop execution and request user input when encountering data issues rather than generating placeholder content.

### Cross-Platform Consistency  
All services use the `PlayerIDMapper` to maintain data consistency between Sleeper and MFL platforms. When adding new features, ensure they support both platforms through the unified player identification system.

### Dependency Injection
Services use dependency injection (especially `PlayerIDMapper`) to break circular import dependencies. Follow this pattern when adding new cross-service dependencies.

### Scheduling Architecture
Background jobs use APScheduler with platform-specific intervals reflecting each API's data update patterns. Sleeper data changes more frequently than MFL, hence different sync schedules.

### Task Management Workflow

This project uses a specific task management workflow defined in `claude_instructions/process-task-list.md`:

1. **Work one sub-task at a time** - Do NOT start the next sub-task until you ask the user for permission
2. **Completion protocol** when finishing a sub-task:
   - Mark sub-task as completed `[x]`
   - If all subtasks under a parent are complete:
     - Run full test suite first (`pytest`)
     - Only if tests pass: stage changes (`git add .`)
     - Clean up temporary files
     - Commit with conventional commit format and descriptive message
     - Mark parent task as completed
3. **Update task list files** regularly as work progresses

## Path Resolution
The system uses absolute path resolution through `get_project_root()` to work reliably from any directory. All file operations should use this pattern for cross-directory compatibility.