# Fantasy Football Manual Testing Framework

This directory contains a comprehensive manual testing framework for Fantasy Football APIs using only official documentation from Sleeper and MFL.

## Overview

The manual testing framework allows you to:
- **Get team names** from both Sleeper and MFL platforms
- **Get roster information** for all teams
- **Get waiver/transaction data** including FAAB balances
- **Get league information** and settings

## Files

- **`data_models.py`** - Data models for standardized API responses
- **`sleeper_client.py`** - Sleeper API client using official endpoints
- **`mfl_client.py`** - MFL API client using official endpoints  
- **`comprehensive_test.py`** - Main testing script with interactive menu
- **`test_structure.py`** - Structure validation script

## Setup

1. **Install Dependencies**:
   ```bash
   pip install requests python-dotenv
   ```

2. **Configure Environment**:
   Ensure your `.env` file in the project root contains:
   ```
   SLEEPER_LEAGUE_ID=your_sleeper_league_id
   MFL_LEAGUE_ID=your_mfl_league_id
   ```

3. **Verify Structure**:
   ```bash
   python3 manual_test/test_structure.py
   ```

## Usage

### Run Comprehensive Test
```bash
python3 manual_test/comprehensive_test.py
```

### Interactive Menu
```bash
python3 manual_test/comprehensive_test.py --interactive
```

### Test Individual Clients
```bash
# Test Sleeper only
python3 manual_test/sleeper_client.py

# Test MFL only
python3 manual_test/mfl_client.py
```

## API Endpoints Used

### Sleeper API (https://docs.sleeper.com/)
- `GET /v1/league/{league_id}` - League information
- `GET /v1/league/{league_id}/users` - Team names/users
- `GET /v1/league/{league_id}/rosters` - Roster information
- `GET /v1/league/{league_id}/waivers` - Waiver orders
- `GET /v1/league/{league_id}/transactions/{round}` - Transactions
- `GET /v1/players/nfl` - All NFL players

### MFL API (https://api.myfantasyleague.com/2022/api_info?STATE=details)
- `export?TYPE=league` - League information
- `export?TYPE=franchises` - Team names/franchises
- `export?TYPE=rosters` - Roster information
- `export?TYPE=blindBidSummary` - FAAB balances
- `export?TYPE=transactions` - Transaction history
- `export?TYPE=players` - All NFL players

## Features

### Data Models
- **LeagueInfo** - Standardized league information
- **TeamInfo** - Team/franchise information  
- **RosterInfo** - Roster with starters and bench
- **WaiverInfo** - Waiver claims and FAAB balances
- **APIResponse** - Standardized API response wrapper
- **TestResults** - Container for all test results

### Interactive Menu Options
1. Run Comprehensive Test (Both Platforms)
2. Test Sleeper Only
3. Test MFL Only
4. Show Current Configuration
5. Export Results to File

### Error Handling
- Graceful handling of API failures
- Detailed error reporting
- Platform-specific retry logic
- Comprehensive test summaries

## Output Example

```
COMPREHENSIVE FANTASY FOOTBALL API TESTING
============================================================
Testing core functionality using official API documentation:
- League information
- Team names
- Roster information
- Waiver/transaction data

============================================================
TESTING SLEEPER FUNCTIONALITY
============================================================

1. Testing Sleeper League Information...
   ✓ Success: SLEEPER League: My Fantasy League (2024) - 12 teams

2. Testing Sleeper Team Names...
   ✓ Success: Found 12 teams
     - SLEEPER Team: Team1 (ID: 123456)
     - SLEEPER Team: Team2 (ID: 234567)
     ...

3. Testing Sleeper Roster Information...
   ✓ Success: Found 12 rosters
     - SLEEPER Roster - Team1: 9 starters, 6 bench (15 total)
     ...
```

## Next Steps

This manual testing framework provides the foundation for:
1. **API Integration Validation** - Verify your API credentials work
2. **Data Structure Understanding** - See exactly what data is available
3. **Development Planning** - Understand API capabilities before building features
4. **Debugging** - Isolated testing of specific API endpoints

The framework uses only official API documentation and provides a clean, standardized interface for both Sleeper and MFL platforms.