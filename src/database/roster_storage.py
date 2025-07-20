import logging
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime, timedelta
from sqlalchemy import and_, or_, func
from sqlalchemy.orm import Session

from .models import SessionLocal, Player, RosterEntry, WaiverState
from ..config import get_config

logger = logging.getLogger(__name__)

class RosterStorageService:
    """Service for managing roster data storage and retrieval"""
    
    def __init__(self):
        self.config = get_config()
    
    def get_db_session(self) -> Session:
        """Get a database session"""
        return SessionLocal()
    
    def store_roster_entries(self, roster_entries: List[Dict[str, Any]], platform: str) -> bool:
        """Store roster entries for a specific platform"""
        db = self.get_db_session()
        
        try:
            logger.info(f"Storing roster entries for {platform} platform")
            
            # Clear existing entries for this platform
            deleted_count = db.query(RosterEntry).filter(
                RosterEntry.platform == platform
            ).delete()
            
            logger.info(f"Cleared {deleted_count} existing {platform} roster entries")
            
            # Add new entries
            added_count = 0
            for entry_data in roster_entries:
                roster_entry = RosterEntry(
                    player_id=entry_data['player_id'],
                    platform=platform,
                    league_id=entry_data['league_id'],
                    user_id=entry_data['user_id'],
                    roster_slot=entry_data.get('roster_slot', 'active'),
                    is_active=entry_data.get('is_active', True)
                )
                db.add(roster_entry)
                added_count += 1
            
            db.commit()
            logger.info(f"Successfully stored {added_count} roster entries for {platform}")
            return True
            
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to store roster entries for {platform}: {e}")
            return False
        finally:
            db.close()
    
    def get_roster_entries(self, platform: str = None, user_id: str = None, 
                          league_id: str = None, is_active: bool = None) -> List[RosterEntry]:
        """Get roster entries with optional filters"""
        db = self.get_db_session()
        
        try:
            query = db.query(RosterEntry).join(Player)
            
            # Apply filters
            if platform:
                query = query.filter(RosterEntry.platform == platform)
            if user_id:
                query = query.filter(RosterEntry.user_id == user_id)
            if league_id:
                query = query.filter(RosterEntry.league_id == league_id)
            if is_active is not None:
                query = query.filter(RosterEntry.is_active == is_active)
            
            # Order by creation date
            query = query.order_by(RosterEntry.created_at.desc())
            
            entries = query.all()
            logger.info(f"Retrieved {len(entries)} roster entries")
            return entries
            
        except Exception as e:
            logger.error(f"Failed to get roster entries: {e}")
            return []
        finally:
            db.close()
    
    def get_user_roster(self, user_id: str, platform: str) -> List[Dict[str, Any]]:
        """Get complete roster for a specific user on a platform"""
        db = self.get_db_session()
        
        try:
            query = db.query(RosterEntry, Player).join(Player).filter(
                and_(
                    RosterEntry.user_id == user_id,
                    RosterEntry.platform == platform,
                    RosterEntry.is_active == True
                )
            ).order_by(Player.position, Player.name)
            
            results = query.all()
            
            roster = []
            for roster_entry, player in results:
                roster.append({
                    'player_id': player.id,
                    'nfl_id': player.nfl_id,
                    'sleeper_id': player.sleeper_id,
                    'mfl_id': player.mfl_id,
                    'name': player.name,
                    'position': player.position,
                    'team': player.team,
                    'roster_slot': roster_entry.roster_slot,
                    'added_at': roster_entry.created_at
                })
            
            logger.info(f"Retrieved roster for user {user_id} on {platform}: {len(roster)} players")
            return roster
            
        except Exception as e:
            logger.error(f"Failed to get user roster: {e}")
            return []
        finally:
            db.close()
    
    def get_player_ownership(self, player_id: int) -> Dict[str, Any]:
        """Get ownership information for a specific player"""
        db = self.get_db_session()
        
        try:
            query = db.query(RosterEntry).filter(
                and_(
                    RosterEntry.player_id == player_id,
                    RosterEntry.is_active == True
                )
            )
            
            entries = query.all()
            
            ownership = {
                'player_id': player_id,
                'total_owners': len(entries),
                'sleeper_owners': [],
                'mfl_owners': [],
                'platforms': []
            }
            
            for entry in entries:
                if entry.platform == 'sleeper':
                    ownership['sleeper_owners'].append(entry.user_id)
                elif entry.platform == 'mfl':
                    ownership['mfl_owners'].append(entry.user_id)
                
                ownership['platforms'].append(entry.platform)
            
            # Remove duplicates
            ownership['platforms'] = list(set(ownership['platforms']))
            
            return ownership
            
        except Exception as e:
            logger.error(f"Failed to get player ownership: {e}")
            return {}
        finally:
            db.close()
    
    def get_roster_changes(self, hours_back: int = 24) -> List[Dict[str, Any]]:
        """Get roster changes within the specified time period"""
        db = self.get_db_session()
        
        try:
            cutoff_time = datetime.utcnow() - timedelta(hours=hours_back)
            
            query = db.query(RosterEntry, Player).join(Player).filter(
                RosterEntry.created_at >= cutoff_time
            ).order_by(RosterEntry.created_at.desc())
            
            results = query.all()
            
            changes = []
            for roster_entry, player in results:
                changes.append({
                    'player_name': player.name,
                    'player_position': player.position,
                    'player_team': player.team,
                    'platform': roster_entry.platform,
                    'user_id': roster_entry.user_id,
                    'roster_slot': roster_entry.roster_slot,
                    'change_time': roster_entry.created_at,
                    'change_type': 'added'  # Since we clear and re-add, these are all additions
                })
            
            logger.info(f"Retrieved {len(changes)} roster changes from last {hours_back} hours")
            return changes
            
        except Exception as e:
            logger.error(f"Failed to get roster changes: {e}")
            return []
        finally:
            db.close()
    
    def get_roster_statistics(self) -> Dict[str, Any]:
        """Get overall roster statistics"""
        db = self.get_db_session()
        
        try:
            # Basic counts
            total_entries = db.query(RosterEntry).filter(RosterEntry.is_active == True).count()
            sleeper_entries = db.query(RosterEntry).filter(
                and_(RosterEntry.platform == 'sleeper', RosterEntry.is_active == True)
            ).count()
            mfl_entries = db.query(RosterEntry).filter(
                and_(RosterEntry.platform == 'mfl', RosterEntry.is_active == True)
            ).count()
            
            # Unique users
            unique_sleeper_users = db.query(RosterEntry.user_id).filter(
                and_(RosterEntry.platform == 'sleeper', RosterEntry.is_active == True)
            ).distinct().count()
            unique_mfl_users = db.query(RosterEntry.user_id).filter(
                and_(RosterEntry.platform == 'mfl', RosterEntry.is_active == True)
            ).distinct().count()
            
            # Players with roster spots
            rostered_players = db.query(RosterEntry.player_id).filter(
                RosterEntry.is_active == True
            ).distinct().count()
            
            # Position breakdown
            position_breakdown = db.query(
                Player.position,
                func.count(RosterEntry.id).label('count')
            ).join(RosterEntry).filter(
                RosterEntry.is_active == True
            ).group_by(Player.position).all()
            
            position_stats = {pos: count for pos, count in position_breakdown}
            
            stats = {
                'total_roster_entries': total_entries,
                'sleeper_entries': sleeper_entries,
                'mfl_entries': mfl_entries,
                'unique_sleeper_users': unique_sleeper_users,
                'unique_mfl_users': unique_mfl_users,
                'rostered_players': rostered_players,
                'position_breakdown': position_stats,
                'last_updated': datetime.utcnow().isoformat()
            }
            
            logger.info(f"Generated roster statistics: {total_entries} total entries")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get roster statistics: {e}")
            return {}
        finally:
            db.close()
    
    def store_waiver_state(self, platform: str, league_id: str, user_id: str, 
                          waiver_order: int = None, faab_balance: float = None) -> bool:
        """Store waiver state information"""
        db = self.get_db_session()
        
        try:
            # Check if waiver state already exists
            waiver_state = db.query(WaiverState).filter(
                and_(
                    WaiverState.platform == platform,
                    WaiverState.league_id == league_id,
                    WaiverState.user_id == user_id
                )
            ).first()
            
            if waiver_state:
                # Update existing record
                waiver_state.waiver_order = waiver_order
                waiver_state.faab_balance = faab_balance
                waiver_state.last_updated = datetime.utcnow()
            else:
                # Create new record
                waiver_state = WaiverState(
                    platform=platform,
                    league_id=league_id,
                    user_id=user_id,
                    waiver_order=waiver_order,
                    faab_balance=faab_balance
                )
                db.add(waiver_state)
            
            db.commit()
            logger.info(f"Updated waiver state for {platform} user {user_id}")
            return True
            
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to store waiver state: {e}")
            return False
        finally:
            db.close()
    
    def get_waiver_states(self, platform: str = None, league_id: str = None) -> List[WaiverState]:
        """Get waiver states with optional filters"""
        db = self.get_db_session()
        
        try:
            query = db.query(WaiverState)
            
            if platform:
                query = query.filter(WaiverState.platform == platform)
            if league_id:
                query = query.filter(WaiverState.league_id == league_id)
            
            waiver_states = query.order_by(WaiverState.last_updated.desc()).all()
            logger.info(f"Retrieved {len(waiver_states)} waiver states")
            return waiver_states
            
        except Exception as e:
            logger.error(f"Failed to get waiver states: {e}")
            return []
        finally:
            db.close()
    
    def cleanup_old_data(self, days_old: int = 30) -> Dict[str, int]:
        """Clean up old roster data beyond specified days"""
        db = self.get_db_session()
        
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days_old)
            
            # Clean up old inactive roster entries
            deleted_roster_entries = db.query(RosterEntry).filter(
                and_(
                    RosterEntry.created_at < cutoff_date,
                    RosterEntry.is_active == False
                )
            ).delete()
            
            # Clean up old waiver states
            deleted_waiver_states = db.query(WaiverState).filter(
                WaiverState.last_updated < cutoff_date
            ).delete()
            
            db.commit()
            
            cleanup_stats = {
                'deleted_roster_entries': deleted_roster_entries,
                'deleted_waiver_states': deleted_waiver_states,
                'cutoff_date': cutoff_date.isoformat()
            }
            
            logger.info(f"Cleaned up old data: {deleted_roster_entries} roster entries, "
                       f"{deleted_waiver_states} waiver states")
            return cleanup_stats
            
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to cleanup old data: {e}")
            return {}
        finally:
            db.close()
    
    def validate_data_integrity(self) -> Dict[str, Any]:
        """Validate database data integrity"""
        db = self.get_db_session()
        
        try:
            issues = []
            
            # Check for roster entries without valid players
            orphaned_entries = db.query(RosterEntry).filter(
                RosterEntry.player_id.notin_(
                    db.query(Player.id)
                )
            ).count()
            
            if orphaned_entries > 0:
                issues.append(f"Found {orphaned_entries} roster entries without valid players")
            
            # Check for duplicate active roster entries
            duplicate_entries = db.query(RosterEntry).filter(
                RosterEntry.is_active == True
            ).group_by(
                RosterEntry.player_id,
                RosterEntry.platform,
                RosterEntry.user_id
            ).having(func.count(RosterEntry.id) > 1).count()
            
            if duplicate_entries > 0:
                issues.append(f"Found {duplicate_entries} duplicate active roster entries")
            
            # Check for players without platform IDs
            players_without_ids = db.query(Player).filter(
                and_(
                    Player.sleeper_id.is_(None),
                    Player.mfl_id.is_(None)
                )
            ).count()
            
            if players_without_ids > 0:
                issues.append(f"Found {players_without_ids} players without platform IDs")
            
            integrity_report = {
                'issues_found': len(issues),
                'issues': issues,
                'check_timestamp': datetime.utcnow().isoformat()
            }
            
            logger.info(f"Data integrity check completed: {len(issues)} issues found")
            return integrity_report
            
        except Exception as e:
            logger.error(f"Failed to validate data integrity: {e}")
            return {'error': str(e)}
        finally:
            db.close()

# Global storage service instance
storage_service = RosterStorageService()

def get_storage_service() -> RosterStorageService:
    """Get the global storage service instance"""
    return storage_service

# Convenience functions
def store_roster_data(roster_entries: List[Dict[str, Any]], platform: str) -> bool:
    """Store roster data for a platform"""
    return storage_service.store_roster_entries(roster_entries, platform)

def get_user_roster(user_id: str, platform: str) -> List[Dict[str, Any]]:
    """Get user's roster for a platform"""
    return storage_service.get_user_roster(user_id, platform)

def get_roster_stats() -> Dict[str, Any]:
    """Get roster statistics"""
    return storage_service.get_roster_statistics()

# Test function
def test_roster_storage():
    """Test the roster storage functionality"""
    print("Testing Roster Storage Service...")
    print("=" * 50)
    
    service = RosterStorageService()
    
    try:
        # Test getting roster statistics
        print("\n1. Testing roster statistics...")
        stats = service.get_roster_statistics()
        print(f"   Total entries: {stats.get('total_roster_entries', 0)}")
        print(f"   Sleeper entries: {stats.get('sleeper_entries', 0)}")
        print(f"   MFL entries: {stats.get('mfl_entries', 0)}")
        print(f"   Rostered players: {stats.get('rostered_players', 0)}")
        
        # Test data integrity
        print("\n2. Testing data integrity...")
        integrity = service.validate_data_integrity()
        print(f"   Issues found: {integrity.get('issues_found', 0)}")
        for issue in integrity.get('issues', []):
            print(f"   - {issue}")
        
        # Test recent roster changes
        print("\n3. Testing recent roster changes...")
        changes = service.get_roster_changes(hours_back=24)
        print(f"   Recent changes: {len(changes)}")
        
        if changes:
            for change in changes[:3]:  # Show first 3
                print(f"   - {change['player_name']} ({change['platform']})")
        
        return True
        
    except Exception as e:
        print(f"   ✗ Storage test failed: {e}")
        return False

if __name__ == "__main__":
    test_roster_storage()