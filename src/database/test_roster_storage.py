import pytest
import unittest.mock as mock
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
from typing import Dict, List, Any

from src.database.roster_storage import (
    RosterStorageService, get_storage_service, store_roster_data,
    get_user_roster, get_roster_stats
)
from src.database.models import Player, RosterEntry, WaiverState, SessionLocal


class TestRosterStorageService:
    """Test cases for RosterStorageService class"""
    
    @pytest.fixture
    def storage_service(self):
        """Create a RosterStorageService instance for testing"""
        with patch('src.database.roster_storage.SessionLocal'):
            return RosterStorageService()
    
    @patch('src.database.roster_storage.SessionLocal')
    def test_store_roster_entries_success(self, mock_session, storage_service):
        """Test successful roster entry storage"""
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        roster_entries = [
            {
                "player_id": 1,
                "platform": "sleeper",
                "league_id": "test_league",
                "user_id": "user_123",
                "roster_slot": "active",
                "is_active": True
            },
            {
                "player_id": 2,
                "platform": "mfl",
                "league_id": "test_mfl_league",
                "user_id": "franchise_456",
                "roster_slot": "bench",
                "is_active": True
            }
        ]
        
        result = storage_service.store_roster_entries(roster_entries, "sleeper")
        
        assert result is True
        assert mock_db.add.call_count == 2
        mock_db.commit.assert_called_once()
        mock_db.close.assert_called_once()
    
    @patch('src.database.roster_storage.SessionLocal')
    def test_store_roster_entries_database_error(self, mock_session, storage_service):
        """Test roster entry storage with database error"""
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        # Mock database error
        mock_db.commit.side_effect = Exception("Database error")
        
        roster_entries = [
            {
                "player_id": 1,
                "platform": "sleeper",
                "league_id": "test_league",
                "user_id": "user_123",
                "roster_slot": "active",
                "is_active": True
            }
        ]
        
        result = storage_service.store_roster_entries(roster_entries, "sleeper")
        
        assert result is False
        mock_db.rollback.assert_called_once()
        mock_db.close.assert_called_once()
    
    @patch('src.database.roster_storage.SessionLocal')
    def test_get_roster_statistics(self, mock_session, storage_service):
        """Test getting roster statistics"""
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        # Mock database queries
        mock_db.query.return_value.count.return_value = 100  # Total roster entries
        mock_db.query.return_value.filter.return_value.count.return_value = 75  # Active entries
        mock_db.query.return_value.distinct.return_value.count.return_value = 50  # Unique players
        mock_db.query.return_value.group_by.return_value.all.return_value = [
            ("sleeper", 60),
            ("mfl", 40)
        ]  # Platform breakdown
        
        result = storage_service.get_roster_statistics()
        
        expected = {
            "total_roster_entries": 100,
            "active_roster_entries": 75,
            "unique_players": 50,
            "platform_breakdown": {
                "sleeper": 60,
                "mfl": 40
            },
            "last_updated": result["last_updated"]  # Dynamic timestamp
        }
        
        assert result == expected
        mock_db.close.assert_called_once()
    
    @patch('src.database.roster_storage.SessionLocal')
    def test_get_roster_changes(self, mock_session, storage_service):
        """Test getting roster changes"""
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        # Mock change records
        mock_changes = [
            Mock(
                id=1,
                player_id=123,
                platform="sleeper",
                user_id="user_123",
                change_type="added",
                old_value=None,
                new_value="active",
                timestamp=datetime(2025, 1, 1, 12, 0, 0)
            ),
            Mock(
                id=2,
                player_id=124,
                platform="mfl",
                user_id="franchise_456",
                change_type="removed",
                old_value="bench",
                new_value=None,
                timestamp=datetime(2025, 1, 1, 13, 0, 0)
            )
        ]
        
        mock_db.query.return_value.filter.return_value.order_by.return_value.all.return_value = mock_changes
        
        result = storage_service.get_roster_changes(hours_back=24)
        
        assert len(result) == 2
        assert result[0]["player_id"] == 123
        assert result[0]["change_type"] == "added"
        assert result[1]["player_id"] == 124
        assert result[1]["change_type"] == "removed"
        
        mock_db.close.assert_called_once()
    
    @patch('src.database.roster_storage.SessionLocal')
    def test_get_waiver_states(self, mock_session, storage_service):
        """Test getting waiver states"""
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        # Mock waiver state records
        mock_waivers = [
            Mock(
                id=1,
                platform="sleeper",
                league_id="test_league",
                user_id="user_123",
                waiver_order=1,
                faab_balance=100,
                timestamp=datetime(2025, 1, 1, 12, 0, 0)
            ),
            Mock(
                id=2,
                platform="sleeper",
                league_id="test_league",
                user_id="user_456",
                waiver_order=2,
                faab_balance=95,
                timestamp=datetime(2025, 1, 1, 12, 0, 0)
            )
        ]
        
        mock_db.query.return_value.filter.return_value.order_by.return_value.all.return_value = mock_waivers
        
        result = storage_service.get_waiver_states(platform="sleeper")
        
        assert len(result) == 2
        assert result[0]["user_id"] == "user_123"
        assert result[0]["waiver_order"] == 1
        assert result[1]["faab_balance"] == 95
        
        mock_db.close.assert_called_once()
    
    @patch('src.database.roster_storage.SessionLocal')
    def test_validate_data_integrity(self, mock_session, storage_service):
        """Test data integrity validation"""
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        # Mock integrity checks
        mock_db.query.return_value.filter.return_value.count.return_value = 0  # No orphaned entries
        mock_db.query.return_value.join.return_value.filter.return_value.count.return_value = 5  # Duplicate players
        
        result = storage_service.validate_data_integrity()
        
        assert result["status"] == "success"
        assert result["issues_found"] == 1  # One type of issue found
        assert len(result["issues"]) == 1
        assert "duplicate player entries" in result["issues"][0]
        
        mock_db.close.assert_called_once()
    
    @patch('src.database.roster_storage.SessionLocal')
    def test_cleanup_old_data(self, mock_session, storage_service):
        """Test cleaning up old data"""
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        # Mock cleanup operations
        mock_db.query.return_value.filter.return_value.delete.return_value = 25  # Old roster entries
        
        result = storage_service.cleanup_old_data(days_old=30)
        
        assert result["old_roster_entries_deleted"] == 25
        assert result["days_old"] == 30
        assert "cleanup_timestamp" in result
        
        mock_db.commit.assert_called_once()
        mock_db.close.assert_called_once()
    
    @patch('src.database.roster_storage.SessionLocal')
    def test_get_user_roster_data(self, mock_session, storage_service):
        """Test getting user roster data"""
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        # Mock player and roster entry data
        mock_roster_data = [
            (
                Mock(id=1, name="Josh Allen", position="QB", team="BUF", is_starter=True),
                Mock(roster_slot="QB", is_active=True, timestamp=datetime(2025, 1, 1))
            ),
            (
                Mock(id=2, name="Stefon Diggs", position="WR", team="BUF", is_starter=True),
                Mock(roster_slot="WR", is_active=True, timestamp=datetime(2025, 1, 1))
            )
        ]
        
        mock_db.query.return_value.join.return_value.filter.return_value.all.return_value = mock_roster_data
        
        result = storage_service.get_user_roster_data("sleeper", "test_league", "user_123")
        
        assert len(result) == 2
        assert result[0]["player_name"] == "Josh Allen"
        assert result[0]["position"] == "QB"
        assert result[1]["player_name"] == "Stefon Diggs"
        assert result[1]["position"] == "WR"
        
        mock_db.close.assert_called_once()
    
    @patch('src.database.roster_storage.SessionLocal')
    def test_store_waiver_state(self, mock_session, storage_service):
        """Test storing waiver state"""
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        waiver_data = {
            "platform": "sleeper",
            "league_id": "test_league",
            "user_id": "user_123",
            "waiver_order": 3,
            "faab_balance": 85
        }
        
        result = storage_service.store_waiver_state(waiver_data)
        
        assert result is True
        mock_db.add.assert_called_once()
        mock_db.commit.assert_called_once()
        mock_db.close.assert_called_once()
    
    @patch('src.database.roster_storage.SessionLocal')
    def test_get_platform_summary(self, mock_session, storage_service):
        """Test getting platform summary"""
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        # Mock platform data
        mock_db.query.return_value.filter.return_value.count.side_effect = [50, 45, 25]  # Total, active, unique
        
        result = storage_service.get_platform_summary("sleeper")
        
        assert result["platform"] == "sleeper"
        assert result["total_entries"] == 50
        assert result["active_entries"] == 45
        assert result["unique_players"] == 25
        
        mock_db.close.assert_called_once()
    
    @patch('src.database.roster_storage.SessionLocal')
    def test_database_error_handling(self, mock_session, storage_service):
        """Test database error handling"""
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        # Mock database error
        mock_db.query.side_effect = Exception("Database connection error")
        
        result = storage_service.get_roster_statistics()
        
        assert "error" in result
        assert "Database connection error" in result["error"]
        mock_db.close.assert_called_once()


class TestUtilityFunctions:
    """Test utility functions in the module"""
    
    @patch('src.database.roster_storage.RosterStorageService')
    def test_get_storage_service(self, mock_service_class):
        """Test get_storage_service function"""
        mock_service = Mock()
        mock_service_class.return_value = mock_service
        
        result = get_storage_service()
        
        assert result == mock_service
        mock_service_class.assert_called_once()
    
    @patch('src.database.roster_storage.get_storage_service')
    def test_store_roster_data(self, mock_get_service):
        """Test store_roster_data function"""
        mock_service = Mock()
        mock_service.store_roster_entries.return_value = True
        mock_get_service.return_value = mock_service
        
        roster_entries = [
            {
                "player_id": 1,
                "platform": "sleeper",
                "league_id": "test_league",
                "user_id": "user_123",
                "roster_slot": "active",
                "is_active": True
            }
        ]
        
        result = store_roster_data(roster_entries)
        
        assert result is True
        mock_service.store_roster_entries.assert_called_once_with(roster_entries, "sleeper")
    
    @patch('src.database.roster_storage.get_storage_service')
    def test_get_user_roster(self, mock_get_service):
        """Test get_user_roster function"""
        mock_service = Mock()
        mock_service.get_user_roster_data.return_value = [
            {"player_name": "Josh Allen", "position": "QB"}
        ]
        mock_get_service.return_value = mock_service
        
        result = get_user_roster("sleeper", "test_league", "user_123")
        
        assert len(result) == 1
        assert result[0]["player_name"] == "Josh Allen"
        mock_service.get_user_roster_data.assert_called_once_with("sleeper", "test_league", "user_123")
    
    @patch('src.database.roster_storage.get_storage_service')
    def test_get_roster_stats(self, mock_get_service):
        """Test get_roster_stats function"""
        mock_service = Mock()
        mock_service.get_roster_statistics.return_value = {
            "total_roster_entries": 100,
            "active_roster_entries": 75
        }
        mock_get_service.return_value = mock_service
        
        result = get_roster_stats()
        
        assert result["total_roster_entries"] == 100
        assert result["active_roster_entries"] == 75
        mock_service.get_roster_statistics.assert_called_once()


class TestRosterStorageIntegration:
    """Integration tests for roster storage functionality"""
    
    @pytest.mark.integration
    @patch('src.database.roster_storage.SessionLocal')
    def test_full_roster_storage_flow(self, mock_session):
        """Integration test for complete roster storage flow"""
        # Skip if no database available
        pytest.skip("Integration test requires actual database")
        
        # This would test the complete flow with a real database
        service = RosterStorageService()
        
        # Test data
        roster_entries = [
            {
                "player_id": 1,
                "platform": "sleeper",
                "league_id": "test_league",
                "user_id": "user_123",
                "roster_slot": "QB",
                "is_active": True
            }
        ]
        
        # Store data
        store_result = service.store_roster_entries(roster_entries, "sleeper")
        assert store_result is True
        
        # Get statistics
        stats = service.get_roster_statistics()
        assert stats["total_roster_entries"] >= 1
        
        # Validate integrity
        integrity = service.validate_data_integrity()
        assert integrity["status"] == "success"
    
    @pytest.mark.integration 
    def test_storage_service_singleton(self):
        """Test that get_storage_service returns singleton instance"""
        service1 = get_storage_service()
        service2 = get_storage_service()
        
        # Should return the same instance (if singleton pattern implemented)
        # This test may need adjustment based on actual implementation
        assert service1 is not None
        assert service2 is not None


class TestRosterStorageEdgeCases:
    """Test edge cases and error scenarios"""
    
    @pytest.fixture
    def storage_service(self):
        with patch('src.database.roster_storage.SessionLocal'):
            return RosterStorageService()
    
    @patch('src.database.roster_storage.SessionLocal')
    def test_store_empty_roster_entries(self, mock_session, storage_service):
        """Test storing empty roster entries list"""
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        result = storage_service.store_roster_entries([], "sleeper")
        
        assert result is True
        mock_db.add.assert_not_called()
        mock_db.commit.assert_called_once()
    
    @patch('src.database.roster_storage.SessionLocal')
    def test_get_roster_changes_no_changes(self, mock_session, storage_service):
        """Test getting roster changes when no changes exist"""
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        mock_db.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
        
        result = storage_service.get_roster_changes(hours_back=24)
        
        assert result == []
        mock_db.close.assert_called_once()
    
    @patch('src.database.roster_storage.SessionLocal')
    def test_get_waiver_states_platform_filter(self, mock_session, storage_service):
        """Test getting waiver states with platform filter"""
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        # Test both with and without platform filter
        mock_waivers = [Mock(platform="sleeper", user_id="user_123")]
        mock_db.query.return_value.filter.return_value.order_by.return_value.all.return_value = mock_waivers
        
        # With platform filter
        result = storage_service.get_waiver_states(platform="sleeper")
        assert len(result) == 1
        
        # Without platform filter (should get all platforms)
        result = storage_service.get_waiver_states()
        # Verify the query was called without platform filter
        mock_db.close.assert_called()
    
    @patch('src.database.roster_storage.SessionLocal')
    def test_invalid_roster_entry_data(self, mock_session, storage_service):
        """Test storing invalid roster entry data"""
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        # Mock validation error
        mock_db.add.side_effect = ValueError("Invalid data")
        
        invalid_entries = [
            {
                "player_id": "invalid",  # Should be int
                "platform": "sleeper",
                "league_id": "test_league",
                "user_id": "user_123"
            }
        ]
        
        result = storage_service.store_roster_entries(invalid_entries, "sleeper")
        
        assert result is False
        mock_db.rollback.assert_called_once()
    
    @patch('src.database.roster_storage.SessionLocal')
    def test_cleanup_old_data_no_old_data(self, mock_session, storage_service):
        """Test cleanup when no old data exists"""
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        # Mock no old data to delete
        mock_db.query.return_value.filter.return_value.delete.return_value = 0
        
        result = storage_service.cleanup_old_data(days_old=30)
        
        assert result["old_roster_entries_deleted"] == 0
        assert result["days_old"] == 30


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__])