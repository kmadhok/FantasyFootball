import pytest
import unittest.mock as mock
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import json

from src.services.waiver_tracker import (
    SleeperWaiverClient, MFLWaiverClient, WaiverTrackerService, WaiverInfo,
    test_sleeper_waiver_api, test_mfl_faab_api
)
from src.services.alert_context import AlertContextService, enhance_alert_with_waiver_context
from src.utils.retry_handler import APIError
from src.database.models import WaiverState, Player, Alert, NewsItem


class TestWaiverInfo:
    """Test cases for WaiverInfo dataclass"""
    
    def test_waiver_info_creation(self):
        """Test WaiverInfo creation and attributes"""
        waiver = WaiverInfo(
            user_id="user123",
            waiver_order=3,
            faab_balance=75.0,
            platform="sleeper",
            league_id="league456"
        )
        
        assert waiver.user_id == "user123"
        assert waiver.waiver_order == 3
        assert waiver.faab_balance == 75.0
        assert waiver.platform == "sleeper"
        assert waiver.league_id == "league456"
        assert isinstance(waiver.timestamp, datetime)
    
    def test_waiver_info_defaults(self):
        """Test WaiverInfo with default values"""
        waiver = WaiverInfo(user_id="user123")
        
        assert waiver.user_id == "user123"
        assert waiver.waiver_order is None
        assert waiver.faab_balance is None
        assert waiver.platform == "unknown"
        assert waiver.league_id == ""
        assert isinstance(waiver.timestamp, datetime)


class TestSleeperWaiverClient:
    """Test cases for SleeperWaiverClient"""
    
    @pytest.fixture
    def sleeper_client(self):
        """Create a SleeperWaiverClient instance for testing"""
        with patch('src.services.waiver_tracker.get_config') as mock_config:
            mock_config.return_value.SLEEPER_LEAGUE_ID = "test_league_123"
            return SleeperWaiverClient()
    
    @patch('src.services.waiver_tracker.requests.get')
    def test_get_waiver_orders_success(self, mock_get, sleeper_client):
        """Test successful waiver orders retrieval"""
        mock_response = Mock()
        mock_response.json.return_value = [
            {
                "waiver_id": 1,
                "roster_id": 1,
                "waiver_priority": 3,
                "status": "complete",
                "type": "waiver"
            },
            {
                "waiver_id": 2,
                "roster_id": 2,
                "waiver_priority": 1,
                "status": "pending",
                "type": "waiver"
            }
        ]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = sleeper_client.get_waiver_orders()
        
        assert len(result) == 2
        assert result[0]["waiver_priority"] == 3
        assert result[1]["roster_id"] == 2
        mock_get.assert_called_once_with(
            "https://api.sleeper.app/v1/league/test_league_123/waivers",
            timeout=10
        )
    
    @patch('src.services.waiver_tracker.requests.get')
    def test_get_league_settings_success(self, mock_get, sleeper_client):
        """Test successful league settings retrieval"""
        mock_response = Mock()
        mock_response.json.return_value = {
            "league_id": "test_league_123",
            "name": "Test League",
            "settings": {
                "waiver_type": 1,
                "waiver_clear_days": 1
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = sleeper_client.get_league_settings()
        
        assert result["league_id"] == "test_league_123"
        assert "settings" in result
        mock_get.assert_called_once_with(
            "https://api.sleeper.app/v1/league/test_league_123",
            timeout=10
        )
    
    @patch('src.services.waiver_tracker.requests.get')
    def test_api_error_handling(self, mock_get, sleeper_client):
        """Test API error handling"""
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection failed")
        
        with pytest.raises(APIError) as exc_info:
            sleeper_client.get_waiver_orders()
        
        assert "Sleeper waiver orders request failed" in str(exc_info.value)
        assert exc_info.value.platform == "sleeper"
    
    def test_process_waiver_data(self, sleeper_client):
        """Test processing waiver data into WaiverInfo objects"""
        waivers = [
            {"roster_id": "1", "waiver_priority": 3},
            {"roster_id": "2", "waiver_priority": 1}
        ]
        
        users = [
            {"user_id": "1", "username": "User1"},
            {"user_id": "2", "username": "User2"}
        ]
        
        result = sleeper_client.process_waiver_data(waivers, users)
        
        assert len(result) == 2
        assert result[0].user_id == "1"
        assert result[0].waiver_order == 3
        assert result[0].platform == "sleeper"
        assert result[1].waiver_order == 1


class TestMFLWaiverClient:
    """Test cases for MFLWaiverClient"""
    
    @pytest.fixture
    def mfl_client(self):
        """Create an MFLWaiverClient instance for testing"""
        with patch('src.services.waiver_tracker.get_config') as mock_config:
            mock_config.return_value.MFL_LEAGUE_ID = "test_mfl_123"
            mock_config.return_value.MFL_LEAGUE_API_KEY = "test_api_key"
            return MFLWaiverClient()
    
    @patch('src.services.waiver_tracker.requests.get')
    def test_get_faab_balances_success(self, mock_get, mfl_client):
        """Test successful FAAB balances retrieval"""
        mock_response = Mock()
        mock_response.json.return_value = {
            "blindBidSummary": {
                "franchise": [
                    {"id": "0001", "balance": "85"},
                    {"id": "0002", "balance": "92"},
                    {"id": "0003", "balance": "78"}
                ]
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = mfl_client.get_faab_balances()
        
        assert len(result) == 3
        assert result[0]["id"] == "0001"
        assert result[0]["balance"] == "85"
        
        # Verify request parameters
        call_args = mock_get.call_args
        assert call_args[1]["params"]["TYPE"] == "blindBidSummary"
        assert call_args[1]["params"]["L"] == "test_mfl_123"
    
    @patch('src.services.waiver_tracker.requests.get')
    def test_get_faab_balances_single_franchise(self, mock_get, mfl_client):
        """Test FAAB balances with single franchise"""
        mock_response = Mock()
        mock_response.json.return_value = {
            "blindBidSummary": {
                "franchise": {"id": "0001", "balance": "100"}
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = mfl_client.get_faab_balances()
        
        assert len(result) == 1
        assert result[0]["id"] == "0001"
        assert result[0]["balance"] == "100"
    
    @patch('src.services.waiver_tracker.requests.get')
    def test_get_faab_balances_empty_response(self, mock_get, mfl_client):
        """Test FAAB balances with empty response"""
        mock_response = Mock()
        mock_response.json.return_value = {"blindBidSummary": {}}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = mfl_client.get_faab_balances()
        
        assert result == []
    
    def test_process_faab_data(self, mfl_client):
        """Test processing FAAB data into WaiverInfo objects"""
        faab_data = [
            {"id": "0001", "balance": "85"},
            {"id": "0002", "balance": "92.5"},
            {"id": "0003", "faabBalance": 78}  # Different field name
        ]
        
        result = mfl_client.process_faab_data(faab_data)
        
        assert len(result) == 3
        assert result[0].user_id == "0001"
        assert result[0].faab_balance == 85
        assert result[0].platform == "mfl"
        assert result[1].faab_balance == 92.5
        assert result[2].faab_balance == 78
    
    def test_process_faab_data_invalid_balance(self, mfl_client):
        """Test processing FAAB data with invalid balance"""
        faab_data = [
            {"id": "0001", "balance": "invalid"},
            {"id": "0002", "balance": "75"}
        ]
        
        result = mfl_client.process_faab_data(faab_data)
        
        assert len(result) == 2
        assert result[0].faab_balance == 0  # Invalid balance converted to 0
        assert result[1].faab_balance == 75


class TestWaiverTrackerService:
    """Test cases for WaiverTrackerService"""
    
    @pytest.fixture
    def tracker_service(self):
        """Create a WaiverTrackerService instance for testing"""
        with patch('src.services.waiver_tracker.get_storage_service'), \
             patch('src.services.waiver_tracker.SessionLocal'):
            return WaiverTrackerService()
    
    @patch('src.services.waiver_tracker.SessionLocal')
    def test_sync_sleeper_waivers_success(self, mock_session, tracker_service):
        """Test successful Sleeper waiver sync"""
        # Mock database session
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        # Mock API responses
        tracker_service.sleeper_client.get_waiver_orders = Mock(return_value=[
            {"roster_id": "1", "waiver_priority": 3}
        ])
        tracker_service.sleeper_client.get_users = Mock(return_value=[
            {"user_id": "1", "username": "TestUser"}
        ])
        
        # Mock database operations
        mock_db.query.return_value.filter.return_value.delete.return_value = None
        
        result = tracker_service.sync_sleeper_waivers()
        
        assert result is True
        mock_db.add.assert_called()
        mock_db.commit.assert_called_once()
        mock_db.close.assert_called_once()
    
    @patch('src.services.waiver_tracker.SessionLocal')
    def test_sync_mfl_faab_success(self, mock_session, tracker_service):
        """Test successful MFL FAAB sync"""
        # Mock database session
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        # Mock API response
        tracker_service.mfl_client.get_faab_balances = Mock(return_value=[
            {"id": "0001", "balance": "85"}
        ])
        
        # Mock database operations
        mock_db.query.return_value.filter.return_value.delete.return_value = None
        
        result = tracker_service.sync_mfl_faab()
        
        assert result is True
        mock_db.add.assert_called()
        mock_db.commit.assert_called_once()
        mock_db.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_sync_all_waivers(self, tracker_service):
        """Test syncing all waivers from both platforms"""
        tracker_service.sync_sleeper_waivers = Mock(return_value=True)
        tracker_service.sync_mfl_faab = Mock(return_value=True)
        
        result = await tracker_service.sync_all_waivers()
        
        assert result["sleeper"] is True
        assert result["mfl"] is True
        tracker_service.sync_sleeper_waivers.assert_called_once()
        tracker_service.sync_mfl_faab.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_sync_all_waivers_partial_failure(self, tracker_service):
        """Test syncing with one platform failing"""
        tracker_service.sync_sleeper_waivers = Mock(return_value=True)
        tracker_service.sync_mfl_faab = Mock(side_effect=Exception("MFL Error"))
        
        result = await tracker_service.sync_all_waivers()
        
        assert result["sleeper"] is True
        assert result["mfl"] is False
    
    def test_get_waiver_statistics(self, tracker_service):
        """Test getting waiver statistics"""
        # Mock storage service responses
        tracker_service.storage_service.get_waiver_states = Mock(side_effect=[
            [{"user_id": "1", "waiver_order": 1}],  # Sleeper
            [{"user_id": "1", "faab_balance": 85}, {"user_id": "2", "faab_balance": 92}]  # MFL
        ])
        
        result = tracker_service.get_waiver_statistics()
        
        assert "waiver_data" in result
        assert result["waiver_data"]["sleeper_count"] == 1
        assert result["waiver_data"]["mfl_count"] == 2
        assert result["mfl_faab_total"] == 177  # 85 + 92
        assert "last_updated" in result
    
    @patch('src.services.waiver_tracker.SessionLocal')
    def test_get_user_waiver_info(self, mock_session, tracker_service):
        """Test getting waiver info for specific user"""
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        # Mock waiver state
        mock_waiver = Mock()
        mock_waiver.user_id = "user123"
        mock_waiver.platform = "sleeper"
        mock_waiver.waiver_order = 3
        mock_waiver.faab_balance = None
        mock_waiver.timestamp = datetime(2025, 1, 1, 12, 0, 0)
        
        mock_db.query.return_value.filter.return_value.order_by.return_value.first.return_value = mock_waiver
        
        result = tracker_service.get_user_waiver_info("sleeper", "user123")
        
        assert result is not None
        assert result["user_id"] == "user123"
        assert result["platform"] == "sleeper"
        assert result["waiver_order"] == 3
        mock_db.close.assert_called_once()


class TestAlertContextService:
    """Test cases for AlertContextService"""
    
    @pytest.fixture
    def context_service(self):
        """Create an AlertContextService instance for testing"""
        with patch('src.services.alert_context.WaiverTrackerService'), \
             patch('src.services.alert_context.RosterStorageService'):
            return AlertContextService()
    
    @patch('src.services.alert_context.SessionLocal')
    def test_add_waiver_context_to_alert(self, mock_session, context_service):
        """Test adding waiver context to an alert"""
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        # Mock alert, player, and news item
        mock_alert = Mock()
        mock_alert.id = 1
        mock_alert.player_id = 1
        mock_alert.news_item_id = 1
        
        mock_player = Mock()
        mock_player.id = 1
        mock_player.name = "Test Player"
        mock_player.position = "RB"
        mock_player.team = "KC"
        mock_player.is_starter = True
        
        mock_news = Mock()
        mock_news.id = 1
        mock_news.event_type = "injury"
        mock_news.confidence_score = 0.8
        mock_news.source = "twitter"
        
        # Mock database queries
        mock_db.query.return_value.filter.return_value.first.side_effect = [
            mock_alert, mock_player, mock_news
        ]
        mock_db.query.return_value.filter.return_value.all.return_value = []
        
        result = context_service.add_waiver_context_to_alert(1)
        
        assert result is True
        mock_db.commit.assert_called_once()
        mock_db.close.assert_called_once()
    
    def test_generate_waiver_recommendation(self, context_service):
        """Test waiver recommendation generation"""
        # Mock player and news item
        mock_player = Mock()
        mock_player.is_starter = True
        mock_player.position = "RB"
        
        mock_news = Mock()
        mock_news.confidence_score = 0.8
        mock_news.event_type = "injury"
        
        # Create mock context
        context = {"test": "context"}
        
        result = context_service._generate_waiver_recommendation(mock_player, mock_news, context)
        
        # Starter with high confidence injury news should be high priority
        assert result == "high_priority"
    
    def test_determine_waiver_urgency(self, context_service):
        """Test waiver urgency determination"""
        # Mock news item
        mock_news = Mock()
        mock_news.event_type = "injury"
        mock_news.confidence_score = 0.9
        
        context = {}
        
        result = context_service._determine_waiver_urgency(mock_news, context)
        
        # High confidence injury should be immediate
        assert result == "immediate"
    
    def test_suggest_faab_bid(self, context_service):
        """Test FAAB bid suggestion"""
        # Mock player and news item
        mock_player = Mock()
        mock_player.is_starter = True
        mock_player.position = "RB"
        
        mock_news = Mock()
        mock_news.confidence_score = 0.8
        mock_news.event_type = "injury"
        
        # Mock context with MFL data
        context = {
            "mfl": {
                "average_faab_remaining": 100.0
            }
        }
        
        result = context_service._suggest_faab_bid(mock_player, mock_news, context)
        
        assert result is not None
        assert isinstance(result, (int, float))
        assert result > 0
    
    def test_suggest_faab_bid_no_mfl(self, context_service):
        """Test FAAB bid suggestion with no MFL context"""
        mock_player = Mock()
        mock_news = Mock()
        context = {}  # No MFL context
        
        result = context_service._suggest_faab_bid(mock_player, mock_news, context)
        
        assert result is None


class TestConvenienceFunctions:
    """Test convenience functions"""
    
    @patch('src.services.waiver_tracker.SleeperWaiverClient')
    def test_test_sleeper_waiver_api(self, mock_client_class):
        """Test the test_sleeper_waiver_api function"""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock successful API calls
        mock_client.get_league_settings.return_value = {"name": "Test League"}
        mock_client.get_waiver_orders.return_value = [{"waiver_id": 1}]
        mock_client.get_users.return_value = [{"user_id": "123"}]
        mock_client.process_waiver_data.return_value = [Mock()]
        
        # This should run without exceptions
        result = test_sleeper_waiver_api()
        
        assert result is True
        mock_client.get_league_settings.assert_called_once()
        mock_client.get_waiver_orders.assert_called_once()
        mock_client.get_users.assert_called_once()
    
    @patch('src.services.waiver_tracker.MFLWaiverClient')
    def test_test_mfl_faab_api(self, mock_client_class):
        """Test the test_mfl_faab_api function"""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock successful API calls
        mock_client.get_league_settings.return_value = {"name": "Test MFL League"}
        mock_client.get_faab_balances.return_value = [{"id": "0001", "balance": "85"}]
        mock_client.process_faab_data.return_value = [Mock(user_id="0001", faab_balance=85)]
        
        # This should run without exceptions
        result = test_mfl_faab_api()
        
        assert result is True
        mock_client.get_league_settings.assert_called_once()
        mock_client.get_faab_balances.assert_called_once()
    
    @patch('src.services.alert_context.AlertContextService')
    def test_enhance_alert_with_waiver_context(self, mock_service_class):
        """Test enhance_alert_with_waiver_context function"""
        mock_service = Mock()
        mock_service_class.return_value = mock_service
        mock_service.add_waiver_context_to_alert.return_value = True
        
        result = enhance_alert_with_waiver_context(1)
        
        assert result is True
        mock_service.add_waiver_context_to_alert.assert_called_once_with(1)


class TestWaiverTrackerIntegration:
    """Integration tests for waiver tracker functionality"""
    
    @pytest.mark.integration
    def test_full_waiver_sync_flow(self):
        """Integration test for complete waiver sync flow"""
        # Skip if no valid config available
        pytest.skip("Integration test requires valid API configuration")
        
        # This would test the complete flow with real APIs
        tracker = WaiverTrackerService()
        
        try:
            # Test individual syncs
            sleeper_result = tracker.sync_sleeper_waivers()
            mfl_result = tracker.sync_mfl_faab()
            
            # At least one should work in a real environment
            assert sleeper_result or mfl_result
            
        except Exception:
            pytest.skip("Real APIs not available for integration test")
    
    @pytest.mark.integration
    def test_waiver_context_integration(self):
        """Integration test for waiver context service"""
        # Skip if no valid data available
        pytest.skip("Integration test requires valid database data")
        
        context_service = AlertContextService()
        
        try:
            # Test getting waiver context
            context = context_service.get_waiver_context_for_player(1)
            
            if context:
                assert "waiver_context" in context
                assert "roster_context" in context
                
        except Exception:
            pytest.skip("Real database not available for integration test")


class TestWaiverTrackerEdgeCases:
    """Test edge cases and error handling"""
    
    @pytest.fixture
    def tracker_service(self):
        with patch('src.services.waiver_tracker.get_storage_service'), \
             patch('src.services.waiver_tracker.SessionLocal'):
            return WaiverTrackerService()
    
    def test_sync_sleeper_waivers_api_failure(self, tracker_service):
        """Test Sleeper sync with API failure"""
        tracker_service.sleeper_client.get_waiver_orders = Mock(side_effect=APIError("API Failed"))
        
        result = tracker_service.sync_sleeper_waivers()
        
        assert result is False
    
    def test_sync_mfl_faab_empty_data(self, tracker_service):
        """Test MFL sync with empty data"""
        tracker_service.mfl_client.get_faab_balances = Mock(return_value=[])
        
        result = tracker_service.sync_mfl_faab()
        
        assert result is False
    
    @patch('src.services.waiver_tracker.SessionLocal')
    def test_database_error_handling(self, mock_session, tracker_service):
        """Test database error handling"""
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        # Mock database error
        mock_db.commit.side_effect = Exception("Database error")
        
        # Mock successful API call
        tracker_service.sleeper_client.get_waiver_orders = Mock(return_value=[])
        tracker_service.sleeper_client.get_users = Mock(return_value=[])
        
        result = tracker_service.sync_sleeper_waivers()
        
        assert result is False
        mock_db.rollback.assert_called_once()
        mock_db.close.assert_called_once()


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__])