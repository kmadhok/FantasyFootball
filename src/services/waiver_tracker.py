import requests
import logging
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass

from src.config.config import get_config
from src.database import SessionLocal, WaiverState, get_storage_service
from src.utils.retry_handler import handle_api_request, safe_api_call, APIError

logger = logging.getLogger(__name__)

@dataclass
class WaiverInfo:
    """Data class for waiver information"""
    user_id: str
    waiver_order: Optional[int] = None
    faab_balance: Optional[int] = None
    platform: str = "unknown"
    league_id: str = ""
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()

class SleeperWaiverClient:
    """Client for Sleeper waiver API interactions"""
    
    def __init__(self):
        self.config = get_config()
        self.base_url = "https://api.sleeper.app/v1"
        self.league_id = self.config.SLEEPER_LEAGUE_ID
        self.timeout = 10
    
    @handle_api_request
    def get_waiver_orders(self, platform: str = "sleeper") -> List[Dict[str, Any]]:
        """Fetch waiver orders for the league (/league/{id}/waivers)"""
        url = f"{self.base_url}/league/{self.league_id}/waivers"
        
        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            waivers = response.json()
            
            logger.info(f"Successfully fetched {len(waivers)} waiver claims from Sleeper")
            return waivers
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch Sleeper waiver orders: {e}")
            raise APIError(f"Sleeper waiver orders request failed: {e}", 
                         status_code=getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None,
                         platform="sleeper")
    
    @handle_api_request
    def get_league_settings(self, platform: str = "sleeper") -> Dict[str, Any]:
        """Get league settings including waiver settings"""
        url = f"{self.base_url}/league/{self.league_id}"
        
        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            league_info = response.json()
            
            logger.info("Successfully fetched Sleeper league settings")
            return league_info
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch Sleeper league settings: {e}")
            raise APIError(f"Sleeper league settings request failed: {e}", 
                         status_code=getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None,
                         platform="sleeper")
    
    @handle_api_request
    def get_users(self, platform: str = "sleeper") -> List[Dict[str, Any]]:
        """Get all users in the league for waiver order context"""
        url = f"{self.base_url}/league/{self.league_id}/users"
        
        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            users = response.json()
            
            logger.info(f"Successfully fetched {len(users)} users from Sleeper for waiver context")
            return users
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch Sleeper users for waiver context: {e}")
            raise APIError(f"Sleeper users request failed: {e}", 
                         status_code=getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None,
                         platform="sleeper")
    
    def process_waiver_data(self, waivers: List[Dict[str, Any]], users: List[Dict[str, Any]] = None) -> List[WaiverInfo]:
        """Process raw waiver data into WaiverInfo objects"""
        processed_waivers = []
        
        # Create user lookup if provided
        user_lookup = {}
        if users:
            user_lookup = {user["user_id"]: user for user in users}
        
        for waiver in waivers:
            waiver_info = WaiverInfo(
                user_id=waiver.get("roster_id", waiver.get("user_id", "unknown")),
                waiver_order=waiver.get("waiver_priority"),
                platform="sleeper",
                league_id=self.league_id,
                timestamp=datetime.utcnow()
            )
            
            # Add user context if available
            if waiver_info.user_id in user_lookup:
                user_info = user_lookup[waiver_info.user_id]
                logger.debug(f"Waiver info for user {user_info.get('username', 'Unknown')}: order {waiver_info.waiver_order}")
            
            processed_waivers.append(waiver_info)
        
        return processed_waivers

class MFLWaiverClient:
    """Client for MyFantasyLeague FAAB balance interactions"""
    
    def __init__(self):
        self.config = get_config()
        self.season = 2025  # Current season
        self.league_id = self.config.MFL_LEAGUE_ID
        self.api_key = self.config.MFL_LEAGUE_API_KEY
        self.base_url = f"https://api.myfantasyleague.com/{self.season}/export"
        self.timeout = 10
    
    @handle_api_request
    def get_faab_balances(self, platform: str = "mfl") -> List[Dict[str, Any]]:
        """Fetch FAAB balances using export?TYPE=blindBidSummary"""
        params = {
            "TYPE": "blindBidSummary",
            "L": self.league_id,
            "JSON": "1"
        }
        
        try:
            logger.info(f"Fetching MFL FAAB balances for league {self.league_id}")
            response = requests.get(self.base_url, params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            
            # MFL response structure: {"blindBidSummary": {"franchise": [...]}}
            faab_data = data.get("blindBidSummary", {})
            
            if "franchise" in faab_data:
                franchises = faab_data["franchise"]
                if not isinstance(franchises, list):
                    franchises = [franchises]  # Handle single franchise case
                
                logger.info(f"Successfully fetched FAAB data for {len(franchises)} franchises from MFL")
                return franchises
            else:
                logger.warning("No franchise FAAB data found in MFL response")
                logger.debug(f"MFL response data: {data}")
                return []
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch MFL FAAB balances: {e}")
            raise APIError(f"MFL FAAB balances request failed: {e}", 
                         status_code=getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None,
                         platform="mfl")
    
    @handle_api_request
    def get_league_settings(self, platform: str = "mfl") -> Dict[str, Any]:
        """Get MFL league settings including FAAB settings"""
        params = {
            "TYPE": "league",
            "L": self.league_id,
            "JSON": "1"
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            
            league_info = data.get("league", {})
            logger.info("Successfully fetched MFL league settings")
            return league_info
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch MFL league settings: {e}")
            raise APIError(f"MFL league settings request failed: {e}", 
                         status_code=getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None,
                         platform="mfl")
    
    def process_faab_data(self, faab_data: List[Dict[str, Any]]) -> List[WaiverInfo]:
        """Process raw FAAB data into WaiverInfo objects"""
        processed_faab = []
        
        for franchise in faab_data:
            franchise_id = franchise.get("id")
            faab_balance = franchise.get("balance", franchise.get("faabBalance"))
            
            # Convert string balance to int if needed
            if isinstance(faab_balance, str):
                try:
                    faab_balance = int(faab_balance)
                except (ValueError, TypeError):
                    faab_balance = 0
                    logger.warning(f"Could not parse FAAB balance for franchise {franchise_id}")
            
            waiver_info = WaiverInfo(
                user_id=franchise_id,
                faab_balance=faab_balance,
                platform="mfl",
                league_id=self.league_id,
                timestamp=datetime.utcnow()
            )
            
            processed_faab.append(waiver_info)
            logger.debug(f"FAAB info for franchise {franchise_id}: ${faab_balance}")
        
        return processed_faab

class WaiverTrackerService:
    """Service to track waiver states from both platforms"""
    
    def __init__(self):
        self.sleeper_client = SleeperWaiverClient()
        self.mfl_client = MFLWaiverClient()
        self.storage_service = get_storage_service()
    
    def sync_sleeper_waivers(self) -> bool:
        """Sync waiver orders from Sleeper platform"""
        try:
            logger.info("Starting Sleeper waiver order sync...")
            
            # Fetch waiver orders and users
            try:
                waivers = self.sleeper_client.get_waiver_orders()
                users = self.sleeper_client.get_users()
            except APIError as e:
                logger.error(f"Failed to fetch Sleeper waiver data: {e}")
                return False
            
            # Process waiver data
            waiver_infos = self.sleeper_client.process_waiver_data(waivers, users)
            
            # Store in database
            db = SessionLocal()
            try:
                # Clear existing Sleeper waiver states
                db.query(WaiverState).filter(WaiverState.platform == "sleeper").delete()
                
                for waiver_info in waiver_infos:
                    waiver_state = WaiverState(
                        platform=waiver_info.platform,
                        league_id=waiver_info.league_id,
                        user_id=waiver_info.user_id,
                        waiver_order=waiver_info.waiver_order,
                        faab_balance=waiver_info.faab_balance,
                        timestamp=waiver_info.timestamp
                    )
                    db.add(waiver_state)
                
                db.commit()
                logger.info(f"Successfully synced {len(waiver_infos)} Sleeper waiver states")
                return True
                
            except Exception as e:
                db.rollback()
                logger.error(f"Database error during Sleeper waiver sync: {e}")
                raise
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to sync Sleeper waivers: {e}")
            return False
    
    def sync_mfl_faab(self) -> bool:
        """Sync FAAB balances from MFL platform"""
        try:
            logger.info("Starting MFL FAAB balance sync...")
            
            # Fetch FAAB data
            try:
                faab_data = self.mfl_client.get_faab_balances()
                if not faab_data:
                    logger.warning("No FAAB data returned from MFL API")
                    return False
            except APIError as e:
                logger.error(f"Failed to fetch MFL FAAB data: {e}")
                return False
            
            # Process FAAB data
            waiver_infos = self.mfl_client.process_faab_data(faab_data)
            
            # Store in database
            db = SessionLocal()
            try:
                # Clear existing MFL waiver states
                db.query(WaiverState).filter(WaiverState.platform == "mfl").delete()
                
                for waiver_info in waiver_infos:
                    waiver_state = WaiverState(
                        platform=waiver_info.platform,
                        league_id=waiver_info.league_id,
                        user_id=waiver_info.user_id,
                        waiver_order=waiver_info.waiver_order,
                        faab_balance=waiver_info.faab_balance,
                        timestamp=waiver_info.timestamp
                    )
                    db.add(waiver_state)
                
                db.commit()
                logger.info(f"Successfully synced {len(waiver_infos)} MFL FAAB states")
                return True
                
            except Exception as e:
                db.rollback()
                logger.error(f"Database error during MFL FAAB sync: {e}")
                raise
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to sync MFL FAAB: {e}")
            return False
    
    async def sync_all_waivers(self) -> Dict[str, bool]:
        """Sync waiver states from both platforms"""
        logger.info("Starting full waiver state sync from both platforms...")
        
        results = {
            "sleeper": False,
            "mfl": False
        }
        
        # Sync Sleeper waivers
        try:
            results["sleeper"] = self.sync_sleeper_waivers()
        except Exception as e:
            logger.error(f"Sleeper waiver sync failed: {e}")
        
        # Sync MFL FAAB
        try:
            results["mfl"] = self.sync_mfl_faab()
        except Exception as e:
            logger.error(f"MFL FAAB sync failed: {e}")
        
        success_count = sum(results.values())
        logger.info(f"Waiver sync completed: {success_count}/2 platforms successful")
        
        return results
    
    def get_waiver_statistics(self) -> Dict[str, Any]:
        """Get comprehensive waiver statistics"""
        try:
            # Get waiver states from storage service
            sleeper_waivers = self.storage_service.get_waiver_states(platform='sleeper')
            mfl_waivers = self.storage_service.get_waiver_states(platform='mfl')
            
            # Calculate statistics
            stats = {
                'waiver_data': {
                    'sleeper_count': len(sleeper_waivers),
                    'mfl_count': len(mfl_waivers),
                    'total_tracked_users': len(sleeper_waivers) + len(mfl_waivers)
                },
                'sleeper_orders': [w.get('waiver_order') for w in sleeper_waivers if w.get('waiver_order')],
                'mfl_faab_total': sum(w.get('faab_balance', 0) for w in mfl_waivers),
                'last_updated': datetime.utcnow().isoformat()
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get waiver statistics: {e}")
            return {'error': str(e)}
    
    def get_user_waiver_info(self, platform: str, user_id: str) -> Optional[Dict[str, Any]]:
        """Get waiver information for a specific user"""
        try:
            db = SessionLocal()
            try:
                waiver_state = db.query(WaiverState).filter(
                    WaiverState.platform == platform,
                    WaiverState.user_id == user_id
                ).order_by(WaiverState.timestamp.desc()).first()
                
                if waiver_state:
                    return {
                        'user_id': waiver_state.user_id,
                        'platform': waiver_state.platform,
                        'waiver_order': waiver_state.waiver_order,
                        'faab_balance': waiver_state.faab_balance,
                        'timestamp': waiver_state.timestamp.isoformat()
                    }
                else:
                    return None
                    
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to get user waiver info: {e}")
            return None

# Convenience functions for testing
def test_sleeper_waiver_api():
    """Test Sleeper waiver API connection"""
    print("Testing Sleeper Waiver API connection...")
    
    sleeper_client = SleeperWaiverClient()
    
    try:
        # Test league settings
        league_settings = sleeper_client.get_league_settings()
        print(f"✓ Sleeper league settings fetched: {league_settings.get('name', 'Unknown')}")
        
        # Test waiver orders
        waivers = sleeper_client.get_waiver_orders()
        print(f"✓ Sleeper waiver orders fetched: {len(waivers)} claims")
        
        # Test users
        users = sleeper_client.get_users()
        print(f"✓ Sleeper users fetched: {len(users)} users")
        
        # Test processing
        waiver_infos = sleeper_client.process_waiver_data(waivers, users)
        print(f"✓ Processed {len(waiver_infos)} waiver info objects")
        
        return True
    except Exception as e:
        print(f"✗ Sleeper waiver API test failed: {e}")
        return False

def test_mfl_faab_api():
    """Test MFL FAAB API connection"""
    print("Testing MFL FAAB API connection...")
    
    mfl_client = MFLWaiverClient()
    
    try:
        # Test league settings
        league_settings = mfl_client.get_league_settings()
        print(f"✓ MFL league settings fetched: {league_settings.get('name', 'Unknown')}")
        
        # Test FAAB balances
        faab_data = mfl_client.get_faab_balances()
        print(f"✓ MFL FAAB balances fetched: {len(faab_data)} franchises")
        
        # Test processing
        waiver_infos = mfl_client.process_faab_data(faab_data)
        print(f"✓ Processed {len(waiver_infos)} FAAB info objects")
        
        # Show sample FAAB data
        for info in waiver_infos[:3]:  # Show first 3
            print(f"   - Franchise {info.user_id}: ${info.faab_balance}")
        
        return True
    except Exception as e:
        print(f"✗ MFL FAAB API test failed: {e}")
        return False

def test_waiver_tracker_integration():
    """Test waiver tracker integration"""
    print("Testing Waiver Tracker Integration...")
    print("=" * 60)
    
    tracker = WaiverTrackerService()
    
    try:
        # Test statistics
        print("\n1. Testing waiver statistics...")
        stats = tracker.get_waiver_statistics()
        if 'error' not in stats:
            print(f"   ✓ Sleeper waiver count: {stats['waiver_data']['sleeper_count']}")
            print(f"   ✓ MFL FAAB count: {stats['waiver_data']['mfl_count']}")
            print(f"   ✓ Total MFL FAAB: ${stats['mfl_faab_total']}")
        else:
            print(f"   ✗ Error getting statistics: {stats['error']}")
        
        # Test individual sync operations
        print("\n2. Testing individual sync operations...")
        sleeper_result = tracker.sync_sleeper_waivers()
        mfl_result = tracker.sync_mfl_faab()
        
        print(f"   Sleeper sync: {'SUCCESS' if sleeper_result else 'FAILED'}")
        print(f"   MFL sync: {'SUCCESS' if mfl_result else 'FAILED'}")
        
        return True
        
    except Exception as e:
        print(f"   ✗ Waiver tracker integration test failed: {e}")
        return False

if __name__ == "__main__":
    # Test Sleeper waiver API
    test_sleeper_waiver_api()
    
    print("\n" + "="*60)
    
    # Test MFL FAAB API
    test_mfl_faab_api()
    
    print("\n" + "="*60)
    
    # Test waiver tracker integration
    test_waiver_tracker_integration()