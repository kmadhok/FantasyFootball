# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Fantasy Football Real-Time Alert & Waiver Monitor system. The goal is to build a background service that monitors fantasy football news from multiple sources (Reddit, Twitter, APIs) and delivers actionable alerts within 5 minutes for players on user rosters across Sleeper and MyFantasyLeague (MFL) platforms.

## Architecture

The system follows a microservices architecture with three main async workers:
- **Ingest Worker**: Monitors Reddit, Twitter, and APIs for fantasy football news
- **Dedup Worker**: Deduplicates news items using (player_id, headline_hash) within 24h windows
- **Notify Worker**: Delivers alerts via Slack webhook with waiver state information

**Tech Stack:**
- Python with asyncio for async operations
- SQLite database + Redis cache for message passing
- Docker containerization
- Scheduled jobs for roster sync (24h) and waiver tracking (6h for Sleeper, daily for MFL)

## Development Commands

Based on Python development standards:
- `pytest` - Run all tests
- `pytest [path/to/test/file]` - Run specific test file
- `python -m pytest` - Alternative test runner

## Key Workflows

### Task List Management
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

### PRD-Based Development
Tasks are generated from PRDs using the workflow in `claude_instructions/generate-tasks.md`:
- Phase 1: Generate high-level parent tasks, wait for "Go" confirmation
- Phase 2: Break down into detailed sub-tasks
- Output format: `tasks-[prd-file-name].md` in `/tasks/` directory

## API Integration Requirements

The system integrates with multiple external APIs:
- **Sleeper API**: `/league/{league_id}/rosters` (daily), `/league/{id}/waivers` (6h)
- **MFL API**: `export?TYPE=rosters` (daily), `export?TYPE=blindBidSummary` (daily)
- **Reddit API**: Stream r/fantasyfootball with rate limit < 100 req/min
- **Twitter API**: Filtered stream for beat reporters with < 50 filtered rules

## Environment Configuration

The project requires environment variables in `.env`:
- `sleeper_league_id`
- `mfl_league_id`
- `mfl_league_api_key`

## Testing Strategy

- Unit tests alongside source files (e.g., `service.py` and `test_service.py` in same directory)
- Full test suite must pass before committing completed parent tasks
- Coverage expected for all services, workers, and utilities

## Alert System

Alerts are delivered via Slack webhook and include:
- Player name, team, event type, source link, timestamp
- Current waiver info (order for Sleeper, FAAB for MFL)
- Rostered-by-user flag
- Only triggered for rostered players or unrostered starter-level players (RB1, WR1-2, QB, TE1)

## Lifecycle Management

- Service automatically disables after Super Bowl + 7 days
- Restarts the week before NFL preseason
- Operates only during NFL season to minimize costs