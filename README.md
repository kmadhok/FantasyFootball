# Fantasy Football Alert System

A comprehensive automated Fantasy Football system that monitors rosters, tracks waiver states, and delivers intelligent alerts across Sleeper and MyFantasyLeague (MFL) platforms.

## 🏈 System Overview

This system provides **automated Fantasy Football intelligence** by:

1. **🔄 Roster Synchronization** - Tracks all player rosters across platforms
2. **📊 Waiver Monitoring** - Monitors waiver orders, FAAB balances, and claims
3. **🚨 Intelligent Alerts** - Delivers contextual alerts with waiver recommendations
4. **🔗 Cross-Platform Integration** - Seamlessly handles both Sleeper and MFL leagues

### Architecture Flow
```
APIs (Sleeper/MFL) → Services → Database → Alert Context → Notifications
     ↓                ↓          ↓           ↓              ↓
  Raw Data      Processing   Storage    Enhancement     Delivery
```

## 📁 Codebase Structure

### 🚀 Core Services (`src/services/`)

#### **`roster_sync.py`** - Roster Synchronization Engine
**Purpose**: Fetches and synchronizes roster data from both platforms every 24 hours

**Key Classes**:
- **`SleeperAPIClient`** - Handles all Sleeper API interactions
  - `get_league_info()` - League details and settings
  - `get_rosters()` - All team rosters with player IDs
  - `get_users()` - Team owners and usernames
  - `get_players()` - Complete NFL player database
  
- **`MFLAPIClient`** - Handles all MFL API interactions
  - `get_league_info()` - League configuration
  - `get_rosters()` - Franchise rosters with player data
  - `get_detailed_roster(franchise_id)` - Specific team roster
  - `get_players()` - MFL player database
  
- **`RosterSyncService`** - Coordinating service
  - `sync_sleeper_rosters()` - Process Sleeper data
  - `sync_mfl_rosters()` - Process MFL data
  - `sync_all_rosters()` - Sync both platforms
  - `get_sync_statistics()` - Performance metrics
  - `validate_sync_data()` - Data integrity checks

**How to Use**:
```python
from src.services.roster_sync import RosterSyncService

# Initialize service
sync_service = RosterSyncService()

# Sync both platforms
results = await sync_service.sync_all_rosters()
print(f"Sleeper: {'SUCCESS' if results['sleeper'] else 'FAILED'}")
print(f"MFL: {'SUCCESS' if results['mfl'] else 'FAILED'}")

# Get statistics
stats = sync_service.get_sync_statistics()
print(f"Total roster entries: {stats['roster_data']['total_roster_entries']}")
```

**How to Test**:
```bash
# Test API connections
python3 src/services/roster_sync.py

# Run specific tests
python3 -m pytest src/services/test_roster_sync.py -v

# Test individual functions
python3 -c "
from src.services.roster_sync import test_api_connections
test_api_connections()
"
```

---

#### **`waiver_tracker.py`** - Waiver State Monitoring System
**Purpose**: Tracks waiver orders, FAAB balances, and claim activity

**Key Classes**:
- **`SleeperWaiverClient`** - Sleeper waiver operations
  - `get_waiver_orders()` - Current waiver claims (`/league/{id}/waivers`)
  - `get_league_settings()` - Waiver configuration
  - `get_users()` - User context for waiver orders
  - `process_waiver_data()` - Transform to standard format

- **`MFLWaiverClient`** - MFL FAAB operations
  - `get_faab_balances()` - FAAB budget tracking (`export?TYPE=blindBidSummary`)
  - `get_league_settings()` - FAAB configuration
  - `process_faab_data()` - Standardize FAAB data

- **`WaiverTrackerService`** - Master coordinator
  - `sync_sleeper_waivers()` - Process Sleeper waivers
  - `sync_mfl_faab()` - Process MFL FAAB data
  - `sync_all_waivers()` - Sync both platforms
  - `get_waiver_statistics()` - Analytics and reporting
  - `get_user_waiver_info(platform, user_id)` - Individual user data

**How to Use**:
```python
from src.services.waiver_tracker import WaiverTrackerService

# Initialize tracker
tracker = WaiverTrackerService()

# Sync all waiver data
results = await tracker.sync_all_waivers()

# Get user's waiver position
sleeper_info = tracker.get_user_waiver_info("sleeper", "user_123")
print(f"Waiver order: {sleeper_info['waiver_order']}")

# Get FAAB balance
mfl_info = tracker.get_user_waiver_info("mfl", "franchise_01")
print(f"FAAB balance: ${mfl_info['faab_balance']}")
```

**How to Test**:
```bash
# Test waiver APIs
python3 src/services/waiver_tracker.py

# Run waiver tests
python3 -m pytest src/services/test_waiver_tracker.py -v

# Test individual APIs
python3 -c "
from src.services.waiver_tracker import test_sleeper_waiver_api, test_mfl_faab_api
test_sleeper_waiver_api()
test_mfl_faab_api()
"
```

---

#### **`alert_context.py`** - Alert Enhancement Engine
**Purpose**: Enhances news alerts with waiver context and recommendations

**Key Classes**:
- **`AlertContextService`** - Alert enhancement coordinator
  - `add_waiver_context_to_alert(alert_id)` - Add waiver data to alerts
  - `_generate_waiver_context()` - Create waiver recommendations
  - `_generate_roster_context()` - Add roster ownership data
  - `_suggest_faab_bid()` - Calculate FAAB bid suggestions
  - `_calculate_waiver_urgency()` - Determine claim priority

**Key Functions**:
- **Waiver Recommendations**: Analyzes player value vs waiver position
- **FAAB Suggestions**: Recommends bid amounts based on league data
- **Roster Context**: Shows which teams own/need the player
- **Urgency Scoring**: Prioritizes time-sensitive waiver claims

**How to Use**:
```python
from src.services.alert_context import AlertContextService

# Initialize service
alert_service = AlertContextService()

# Enhance an alert with waiver context
success = alert_service.add_waiver_context_to_alert(alert_id=123)

# The alert now contains:
# - waiver_info: JSON with recommendations
# - roster_context: Ownership information
# - waiver_recommendation: Text suggestion
# - faab_suggestion: Suggested bid amount
# - waiver_urgency: Priority level
```

**How to Test**:
```bash
# Test alert context service
python3 -m pytest src/services/test_alert_context.py -v
```

---

#### **`scheduler.py`** - Background Job Coordinator
**Purpose**: Orchestrates automated data synchronization

**Key Features**:
- **Roster Sync**: Every 24 hours
- **Sleeper Waivers**: Every 6 hours
- **MFL FAAB**: Every 24 hours
- **Error Recovery**: Automatic retry logic

**How to Use**:
```python
from src.services.scheduler import start_scheduler

# Start background jobs
scheduler = start_scheduler()

# Jobs will run automatically based on schedule
```

### 🗄️ Database Layer (`src/database/`)

#### **`models.py`** - Data Models
**Core Tables**:

- **`Player`** - NFL player master data
  ```python
  id, nfl_id, sleeper_id, mfl_id, name, position, team, is_starter
  ```

- **`RosterEntry`** - Team roster assignments
  ```python
  player_id, platform, league_id, user_id, roster_slot, is_active
  ```

- **`WaiverState`** - Current waiver positions and FAAB
  ```python
  platform, league_id, user_id, waiver_order, faab_balance, total_claims
  ```

- **`WaiverClaim`** - Individual waiver transactions
  ```python
  platform, league_id, user_id, player_id, claim_type, bid_amount, status
  ```

#### **`roster_storage.py`** - Database Operations
**Key Functions**:
- `store_roster_entries()` - Save roster data
- `get_roster_statistics()` - Analytics queries
- `get_roster_changes()` - Track roster moves
- `validate_data_integrity()` - Data quality checks

**How to Test**:
```bash
# Test database operations
python3 src/database/test_roster_storage.py
```

### 🛠️ Utilities (`src/utils/`)

#### **`player_id_mapper.py`** - Cross-Platform Player Mapping
**Purpose**: Maps player IDs between Sleeper, MFL, and canonical NFL IDs

**Key Classes**:
- **`PlayerIDMapper`** - ID translation service
  - `get_canonical_id(sleeper_id=None, mfl_id=None)` - Get unified ID
  - `get_player_info(canonical_id)` - Get player details
  - `create_player_mapping()` - Build ID mappings
  - `update_mappings()` - Refresh mapping data

**How to Use**:
```python
from src.utils.player_id_mapper import PlayerIDMapper

mapper = PlayerIDMapper()

# Get canonical ID from platform-specific ID
canonical_id = mapper.get_canonical_id(sleeper_id="4035")
player_info = mapper.get_player_info(canonical_id)
print(f"Player: {player_info.name} ({player_info.position}, {player_info.team})")
```

#### **`retry_handler.py`** - API Error Handling
**Purpose**: Provides robust retry logic and error handling for API calls

**Key Functions**:
- `@handle_api_request` - Decorator for automatic retries
- `safe_api_call()` - Wrapper for safe API execution
- `get_retry_statistics()` - Performance monitoring

### ⚙️ Configuration (`src/config/`)

#### **`config.py`** - Environment Management
**Purpose**: Centralized configuration and environment variable handling

**Key Functions**:
- `get_config()` - Load configuration
- `get_database_url()` - Database connection
- `ensure_data_directory()` - Create data directories

## 🎯 Core Functionality Analysis

### ✅ **Check Rosters** (Both Sleeper & MFL)

**Sleeper Roster Checking**:
```python
# What happens:
# 1. API call to /league/{league_id}/rosters
# 2. Get user data from /league/{league_id}/users  
# 3. Map player IDs to canonical NFL IDs
# 4. Store in RosterEntry table with platform="sleeper"

from src.services.roster_sync import SleeperAPIClient
client = SleeperAPIClient()
rosters = client.get_rosters()  # Returns list of all team rosters
users = client.get_users()      # Returns team owner information
```

**MFL Roster Checking**:
```python
# What happens:
# 1. API call to export?TYPE=rosters&L={league_id}
# 2. Parse franchise data structure
# 3. Map MFL player IDs to canonical IDs
# 4. Store in RosterEntry table with platform="mfl"

from src.services.roster_sync import MFLAPIClient
client = MFLAPIClient()
rosters = client.get_rosters()  # Returns franchise roster data
```

### ✅ **Check Waivers for Players** (Pending Claims & Transactions)

**Sleeper Waiver Checking**:
```python
# What happens:
# 1. API call to /league/{league_id}/waivers
# 2. Get pending waiver claims
# 3. API call to /league/{league_id}/transactions/{round}
# 4. Get processed transactions

from src.services.waiver_tracker import SleeperWaiverClient
client = SleeperWaiverClient()
waivers = client.get_waiver_orders()  # Current pending claims
```

**MFL Transaction Checking**:
```python
# What happens:
# 1. API call to export?TYPE=transactions&W=*
# 2. Filter for waiver-related transactions
# 3. Process blind bid (FAAB) data

from src.services.waiver_tracker import MFLWaiverClient
client = MFLWaiverClient()
# Note: MFL shows completed transactions, not pending claims
```

### ✅ **Check Waiver List Position** (Priority Order)

**Sleeper Waiver Position**:
```python
# What happens:
# 1. Waiver order embedded in league settings
# 2. Current position tracked in waiver claims
# 3. Stored in WaiverState.waiver_order

from src.services.waiver_tracker import WaiverTrackerService
tracker = WaiverTrackerService()
user_info = tracker.get_user_waiver_info("sleeper", "user_id")
position = user_info['waiver_order']  # 1 = first priority
```

**MFL Waiver Position**:
```python
# MFL uses FAAB (Free Agent Acquisition Budget) instead of waiver order
# Position determined by bid amount, not priority order
```

### ✅ **Check Waiver Budget** (FAAB Balances for MFL)

**MFL FAAB Checking**:
```python
# What happens:
# 1. API call to export?TYPE=blindBidSummary
# 2. Get current FAAB balances for all franchises
# 3. Track spending history
# 4. Stored in WaiverState.faab_balance

from src.services.waiver_tracker import MFLWaiverClient
client = MFLWaiverClient()
faab_data = client.get_faab_balances()  # All franchise balances

# Get individual balance
tracker = WaiverTrackerService()
user_info = tracker.get_user_waiver_info("mfl", "franchise_id")
balance = user_info['faab_balance']  # Remaining FAAB budget
```

## 🧪 Manual Test vs Production Comparison

### **`manual_test/` Folder - Immediate API Validation**

**Purpose**: Quick, direct API testing without database dependencies

**Files**:
- `sleeper_client.py` - Direct Sleeper API calls
- `mfl_client.py` - Direct MFL API calls  
- `comprehensive_test.py` - Interactive testing framework
- `data_models.py` - Simple data structures

**When to Use**:
- ✅ Verify API credentials work
- ✅ Test new league IDs
- ✅ Debug API responses
- ✅ Understand data structures
- ✅ Quick manual verification

**How to Run**:
```bash
# Test everything
python3 manual_test/comprehensive_test.py

# Interactive menu
python3 manual_test/comprehensive_test.py --interactive

# Test individual platforms
python3 manual_test/sleeper_client.py
python3 manual_test/mfl_client.py
```

### **`src/` Folder - Production System**

**Purpose**: Automated, scheduled, database-backed production system

**Features**:
- ✅ Automated scheduling
- ✅ Database persistence
- ✅ Error handling & retry logic
- ✅ Cross-platform data mapping
- ✅ Alert context enhancement
- ✅ Historical tracking

**When to Use**:
- ✅ Production deployment
- ✅ Automated monitoring
- ✅ Historical analysis
- ✅ Alert generation
- ✅ Cross-platform integration

## 🚀 Usage Instructions & Testing Guide

### **Initial Setup**

1. **Install Dependencies**:
```bash
pip install -r requirements.txt
```

2. **Configure Environment**:
```bash
# Create .env file with:
SLEEPER_LEAGUE_ID=your_sleeper_league_id
MFL_LEAGUE_ID=your_mfl_league_id
MFL_LEAGUE_API_KEY=your_mfl_api_key
DATABASE_URL=sqlite:///data/fantasy_football.db
```

3. **Initialize Database**:
```bash
python3 src/database/migrations.py
```

### **Testing Each Component**

#### **1. Test API Connections**
```bash
# Test manual framework
python3 manual_test/test_structure.py

# Test production APIs
python3 src/services/roster_sync.py
python3 src/services/waiver_tracker.py
```

#### **2. Test Database Operations**
```bash
# Test database models
python3 src/database/test_roster_storage.py

# Run all database tests
python3 -m pytest src/database/ -v
```

#### **3. Test Individual Services**
```bash
# Test roster synchronization
python3 -m pytest src/services/test_roster_sync.py -v

# Test waiver tracking
python3 -m pytest src/services/test_waiver_tracker.py -v

# Test player ID mapping
python3 -m pytest src/utils/test_player_id_mapper.py -v
```

#### **4. Test Full System Integration**
```bash
# Run all tests
python3 -m pytest -v

# Test specific functionality
python3 -c "
from src.services.roster_sync import RosterSyncService
from src.services.waiver_tracker import WaiverTrackerService

# Test roster sync
sync_service = RosterSyncService()
roster_stats = sync_service.get_sync_statistics()
print('Roster Stats:', roster_stats)

# Test waiver tracking  
waiver_service = WaiverTrackerService()
waiver_stats = waiver_service.get_waiver_statistics()
print('Waiver Stats:', waiver_stats)
"
```

### **Production Usage**

#### **Start the Complete System**:
```bash
python3 src/main.py
```

#### **Run Individual Operations**:
```python
# Sync rosters manually
from src.services.roster_sync import RosterSyncService
sync_service = RosterSyncService()
results = await sync_service.sync_all_rosters()

# Track waivers manually  
from src.services.waiver_tracker import WaiverTrackerService
tracker = WaiverTrackerService()
results = await tracker.sync_all_waivers()

# Check user's waiver status
user_waiver_info = tracker.get_user_waiver_info("sleeper", "user_123")
print(f"Waiver position: {user_waiver_info['waiver_order']}")

# Check FAAB balance
faab_info = tracker.get_user_waiver_info("mfl", "franchise_01") 
print(f"FAAB balance: ${faab_info['faab_balance']}")
```

## 🔧 Troubleshooting Common Issues

### **API Connection Problems**
```bash
# Test basic connectivity
python3 manual_test/comprehensive_test.py

# Check environment variables
python3 -c "
import os
print('Sleeper ID:', os.getenv('SLEEPER_LEAGUE_ID'))
print('MFL ID:', os.getenv('MFL_LEAGUE_ID'))
"
```

### **Database Issues**
```bash
# Recreate database
rm data/fantasy_football.db
python3 src/database/migrations.py

# Check database integrity
python3 -c "
from src.services.roster_sync import RosterSyncService
sync = RosterSyncService()
validation = sync.validate_sync_data()
print('Validation:', validation)
"
```

### **Import Errors**
```bash
# Fix Python path
export PYTHONPATH=/Users/kanumadhok/FantasyFootball:$PYTHONPATH

# Or run from project root
cd /Users/kanumadhok/FantasyFootball
python3 -m src.services.roster_sync
```

## 📊 Data Flow Summary

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Sleeper API   │    │    MFL API       │    │  Manual Test    │
│                 │    │                  │    │                 │
│ • Rosters       │    │ • Rosters        │    │ • Direct API    │
│ • Waivers       │    │ • FAAB Balances  │    │ • Quick Test    │
│ • Users         │    │ • Transactions   │    │ • Validation    │
│ • Players       │    │ • Franchises     │    │ • Debug         │
└─────────┬───────┘    └────────┬─────────┘    └─────────────────┘
          │                     │                        │
          │                     │                        │
          ▼                     ▼                        ▼
┌─────────────────────────────────────────────────────────────────┐
│                     PRODUCTION SYSTEM                           │
│                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌────────────────┐  │
│  │ Roster Sync     │  │ Waiver Tracker  │  │ Alert Context  │  │
│  │ Service         │  │ Service         │  │ Service        │  │
│  └─────────┬───────┘  └─────────┬───────┘  └────────┬───────┘  │
│            │                    │                   │          │
│            ▼                    ▼                   ▼          │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                   DATABASE                              │   │
│  │                                                         │   │
│  │ • Players (unified IDs)                                 │   │
│  │ • RosterEntries (ownership)                             │   │
│  │ • WaiverStates (positions/FAAB)                         │   │
│  │ • WaiverClaims (transactions)                           │   │
│  └─────────────────────┬───────────────────────────────────┘   │
│                        │                                       │
│                        ▼                                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                ENHANCED ALERTS                          │   │
│  │                                                         │   │
│  │ • Waiver Recommendations                                │   │
│  │ • FAAB Bid Suggestions                                  │   │
│  │ • Roster Context                                        │   │
│  │ • Urgency Scoring                                       │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## 🎯 Key Capabilities Summary

| Capability | Sleeper | MFL | Implementation |
|------------|---------|-----|----------------|
| **Check Rosters** | ✅ | ✅ | `roster_sync.py` → `RosterEntry` table |
| **Check Waivers** | ✅ | ✅ | `waiver_tracker.py` → `WaiverState` table |
| **Waiver Position** | ✅ | N/A* | Stored in `waiver_order` field |
| **FAAB Budget** | N/A* | ✅ | Stored in `faab_balance` field |

*Sleeper uses traditional waiver order (1st, 2nd, 3rd priority)  
*MFL uses FAAB bidding system instead of waiver priority order

This system provides complete Fantasy Football intelligence across both major platforms with automated monitoring, intelligent alerts, and comprehensive waiver management.