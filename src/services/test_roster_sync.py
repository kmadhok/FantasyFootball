import pytest
import unittest.mock as mock
from unittest.mock import Mock, patch, MagicMock
import requests
from typing import Dict, List, Any

from src.services.roster_sync import (
    SleeperAPIClient, MFLAPIClient, RosterSyncService,
    test_api_connections, test_roster_fetching
)
from src.utils.retry_handler import APIError
from src.database.models import Player, RosterEntry, SessionLocal


class TestSleeperAPIClient:
    """Test cases for SleeperAPIClient"""
    
    @pytest.fixture
    def sleeper_client(self):
        """Create a SleeperAPIClient instance for testing"""
        with patch('src.services.roster_sync.get_config') as mock_config:
            mock_config.return_value.SLEEPER_LEAGUE_ID = "test_league_123"
            return SleeperAPIClient()
    
    @patch('src.services.roster_sync.requests.get')
    def test_get_league_info_success(self, mock_get, sleeper_client):
        """Test successful league info retrieval"""
        # Mock successful response
        mock_response = Mock()
        mock_response.json.return_value = {
            "league_id": "test_league_123",
            "name": "Test League",
            "status": "in_season"
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = sleeper_client.get_league_info()
        
        assert result["league_id"] == "test_league_123"
        assert result["name"] == "Test League"
        mock_get.assert_called_once_with(
            "https://api.sleeper.app/v1/league/test_league_123", 
            timeout=10
        )
    
    @patch('src.services.roster_sync.requests.get')
    def test_get_rosters_success(self, mock_get, sleeper_client):
        """Test successful roster retrieval"""
        mock_response = Mock()
        mock_response.json.return_value = [
            {
                "roster_id": 1,
                "owner_id": "user_123",
                "players": ["player_1", "player_2", "player_3"]
            },
            {
                "roster_id": 2,
                "owner_id": "user_456",
                "players": ["player_4", "player_5"]
            }
        ]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = sleeper_client.get_rosters()
        
        assert len(result) == 2
        assert result[0]["roster_id"] == 1
        assert len(result[0]["players"]) == 3
        mock_get.assert_called_once_with(
            "https://api.sleeper.app/v1/league/test_league_123/rosters",
            timeout=10
        )
    
    @patch('src.services.roster_sync.requests.get')
    def test_get_users_success(self, mock_get, sleeper_client):
        """Test successful users retrieval"""
        mock_response = Mock()
        mock_response.json.return_value = [
            {
                "user_id": "user_123",
                "username": "TestUser1",
                "display_name": "Test User 1"
            },
            {
                "user_id": "user_456", 
                "username": "TestUser2",
                "display_name": "Test User 2"
            }
        ]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = sleeper_client.get_users()
        
        assert len(result) == 2
        assert result[0]["username"] == "TestUser1"
        mock_get.assert_called_once_with(
            "https://api.sleeper.app/v1/league/test_league_123/users",
            timeout=10
        )
    
    @patch('src.services.roster_sync.requests.get')
    def test_api_error_handling(self, mock_get, sleeper_client):
        """Test API error handling"""
        # Mock request exception
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection failed")
        
        with pytest.raises(APIError) as exc_info:
            sleeper_client.get_league_info()
        
        assert "Sleeper league info request failed" in str(exc_info.value)
        assert exc_info.value.platform == "sleeper"
    
    @patch('src.services.roster_sync.requests.get')
    def test_http_error_handling(self, mock_get, sleeper_client):
        """Test HTTP error handling"""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        
        with pytest.raises(APIError) as exc_info:
            sleeper_client.get_rosters()
        
        assert "Sleeper rosters request failed" in str(exc_info.value)


class TestMFLAPIClient:
    """Test cases for MFLAPIClient"""
    
    @pytest.fixture
    def mfl_client(self):
        """Create an MFLAPIClient instance for testing"""
        with patch('src.services.roster_sync.get_config') as mock_config:
            mock_config.return_value.MFL_LEAGUE_ID = "test_mfl_123"
            mock_config.return_value.MFL_LEAGUE_API_KEY = "test_api_key"
            return MFLAPIClient()
    
    @patch('src.services.roster_sync.requests.get')
    def test_get_league_info_success(self, mock_get, mfl_client):
        """Test successful MFL league info retrieval"""
        mock_response = Mock()
        mock_response.json.return_value = {
            "league": {
                "id": "test_mfl_123",
                "name": "Test MFL League",
                "lastRegularSeasonWeek": "17"
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = mfl_client.get_league_info()
        
        assert result["id"] == "test_mfl_123"
        assert result["name"] == "Test MFL League"
        
        # Verify request parameters
        call_args = mock_get.call_args
        assert call_args[1]["params"]["TYPE"] == "league"
        assert call_args[1]["params"]["L"] == "test_mfl_123"
        assert call_args[1]["params"]["JSON"] == "1"
    
    @patch('src.services.roster_sync.requests.get')
    def test_get_rosters_success(self, mock_get, mfl_client):
        """Test successful MFL roster retrieval"""
        mock_response = Mock()
        mock_response.json.return_value = {
            "rosters": {
                "franchise": [
                    {
                        "id": "0001",
                        "name": "Team 1",
                        "player": [
                            {"id": "12345", "status": "starter"},
                            {"id": "12346", "status": "bench"}
                        ]
                    },
                    {
                        "id": "0002", 
                        "name": "Team 2",
                        "player": [
                            {"id": "12347", "status": "starter"}
                        ]
                    }
                ]
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = mfl_client.get_rosters()
        
        assert len(result) == 2
        assert result[0]["id"] == "0001"
        assert result[0]["name"] == "Team 1"
        assert len(result[0]["player"]) == 2
        
        # Verify request parameters
        call_args = mock_get.call_args
        assert call_args[1]["params"]["TYPE"] == "rosters"
        assert call_args[1]["params"]["L"] == "test_mfl_123"
    
    @patch('src.services.roster_sync.requests.get')
    def test_get_rosters_single_franchise(self, mock_get, mfl_client):
        """Test MFL roster retrieval with single franchise"""
        mock_response = Mock()
        mock_response.json.return_value = {
            "rosters": {
                "franchise": {
                    "id": "0001",
                    "name": "Solo Team",
                    "player": {"id": "12345", "status": "starter"}
                }
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = mfl_client.get_rosters()
        
        assert len(result) == 1
        assert result[0]["id"] == "0001"
        assert result[0]["name"] == "Solo Team"
    
    @patch('src.services.roster_sync.requests.get')
    def test_get_rosters_empty_response(self, mock_get, mfl_client):
        """Test MFL roster retrieval with empty response"""
        mock_response = Mock()
        mock_response.json.return_value = {"rosters": {}}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = mfl_client.get_rosters()
        
        assert result == []
    
    @patch('src.services.roster_sync.requests.get')
    def test_get_players_success(self, mock_get, mfl_client):
        """Test successful MFL players retrieval"""
        mock_response = Mock()
        mock_response.json.return_value = {
            "players": {
                "player": [
                    {
                        "id": "12345",
                        "name": "John Doe",
                        "team": "KC",
                        "position": "QB"
                    },
                    {
                        "id": "12346",
                        "name": "Jane Smith", 
                        "team": "SF",
                        "position": "RB"
                    }
                ]
            }
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = mfl_client.get_players()
        
        assert len(result) == 2
        assert result[0]["name"] == "John Doe"
        assert result[1]["position"] == "RB"


class TestRosterSyncService:
    """Test cases for RosterSyncService"""
    
    @pytest.fixture
    def sync_service(self):
        """Create a RosterSyncService instance for testing"""
        with patch('src.services.roster_sync.get_config'), \
             patch('src.services.roster_sync.get_storage_service'), \
             patch('src.services.roster_sync.PlayerIDMapper'), \
             patch('src.services.roster_sync.SessionLocal'):
            return RosterSyncService()
    
    @patch('src.services.roster_sync.SessionLocal')
    def test_sync_sleeper_rosters_success(self, mock_session, sync_service):
        """Test successful Sleeper roster sync"""
        # Mock database session
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        # Mock API responses
        sync_service.sleeper_client.get_rosters = Mock(return_value=[
            {
                "roster_id": 1,
                "owner_id": "user_123",
                "players": ["player_1", "player_2"]
            }
        ])
        
        sync_service.sleeper_client.get_users = Mock(return_value=[
            {
                "user_id": "user_123",
                "username": "TestUser"
            }
        ])
        
        # Mock player mapper
        sync_service.player_mapper.get_canonical_id = Mock(return_value="nfl_123")
        sync_service.player_mapper.get_player_info = Mock(return_value=Mock(
            name="Test Player",
            position="QB",
            team="KC"
        ))
        sync_service.player_mapper._is_starter_position = Mock(return_value=True)
        
        # Mock database queries
        mock_db.query.return_value.filter.return_value.first.return_value = None
        mock_db.query.return_value.filter.return_value.delete.return_value = None
        
        result = sync_service.sync_sleeper_rosters()
        
        assert result is True
        mock_db.commit.assert_called_once()
        mock_db.close.assert_called_once()
    
    @patch('src.services.roster_sync.SessionLocal')
    def test_sync_sleeper_rosters_api_failure(self, mock_session, sync_service):
        """Test Sleeper roster sync with API failure"""
        sync_service.sleeper_client.get_rosters = Mock(side_effect=APIError("API Failed"))
        
        result = sync_service.sync_sleeper_rosters()
        
        assert result is False
    
    @patch('src.services.roster_sync.SessionLocal')
    def test_sync_mfl_rosters_success(self, mock_session, sync_service):
        """Test successful MFL roster sync"""
        # Mock database session
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        # Mock API response
        sync_service.mfl_client.get_rosters = Mock(return_value=[
            {
                "id": "0001",
                "name": "Test Team",
                "player": [
                    {"id": "12345", "status": "starter"}
                ]
            }
        ])
        
        # Mock player mapper
        sync_service.player_mapper.get_canonical_id = Mock(return_value="nfl_456")
        sync_service.player_mapper.get_player_info = Mock(return_value=Mock(
            name="MFL Player",
            position="RB", 
            team="SF"
        ))
        sync_service.player_mapper._is_starter_position = Mock(return_value=True)
        
        # Mock database queries
        mock_db.query.return_value.filter.return_value.first.return_value = None
        mock_db.query.return_value.filter.return_value.delete.return_value = None
        
        result = sync_service.sync_mfl_rosters()
        
        assert result is True
        mock_db.commit.assert_called_once()
        mock_db.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_sync_all_rosters(self, sync_service):
        """Test syncing all rosters from both platforms"""
        sync_service.sync_sleeper_rosters = Mock(return_value=True)
        sync_service.sync_mfl_rosters = Mock(return_value=True)
        
        result = await sync_service.sync_all_rosters()
        
        assert result["sleeper"] is True
        assert result["mfl"] is True
        sync_service.sync_sleeper_rosters.assert_called_once()
        sync_service.sync_mfl_rosters.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_sync_all_rosters_partial_failure(self, sync_service):
        """Test syncing with one platform failing"""
        sync_service.sync_sleeper_rosters = Mock(return_value=True)
        sync_service.sync_mfl_rosters = Mock(side_effect=Exception("MFL Error"))
        
        result = await sync_service.sync_all_rosters()
        
        assert result["sleeper"] is True
        assert result["mfl"] is False
    
    def test_get_sync_statistics(self, sync_service):
        """Test getting sync statistics"""
        # Mock storage service responses
        sync_service.storage_service.get_roster_statistics = Mock(return_value={
            "total_roster_entries": 50,
            "active_players": 25
        })
        
        sync_service.player_mapper.get_mapping_stats = Mock(return_value={
            "total_players": 100,
            "mapped_players": 95
        })
        
        sync_service.storage_service.get_roster_changes = Mock(return_value=[
            {"change": "player_added", "timestamp": "2025-01-01T12:00:00"}
        ])
        
        sync_service.storage_service.get_waiver_states = Mock(return_value=[])
        
        result = sync_service.get_sync_statistics()
        
        assert "roster_data" in result
        assert "player_mapping" in result
        assert "recent_changes" in result
        assert "waiver_states" in result
        assert "last_updated" in result
        assert result["roster_data"]["total_roster_entries"] == 50
    
    def test_validate_sync_data(self, sync_service):
        """Test sync data validation"""
        # Mock storage service response
        sync_service.storage_service.validate_data_integrity = Mock(return_value={
            "issues_found": 0,
            "issues": []
        })
        
        # Mock database session
        with patch('src.services.roster_sync.SessionLocal') as mock_session:
            mock_db = Mock()
            mock_session.return_value = mock_db
            
            # Mock database queries for validation
            mock_db.query.return_value.filter.return_value.count.return_value = 0
            mock_db.query.return_value.join.return_value.filter.return_value.distinct.return_value.filter.return_value.count.return_value = 0
            
            result = sync_service.validate_sync_data()
            
            assert "issues_found" in result
            assert "issues" in result
            mock_db.close.assert_called_once()


class TestConvenienceFunctions:
    """Test convenience and utility functions"""
    
    @patch('src.services.roster_sync.SleeperAPIClient')
    @patch('src.services.roster_sync.MFLAPIClient')
    def test_test_api_connections(self, mock_mfl_class, mock_sleeper_class):
        """Test the test_api_connections function"""
        # Mock API clients
        mock_sleeper = Mock()
        mock_mfl = Mock()
        mock_sleeper_class.return_value = mock_sleeper
        mock_mfl_class.return_value = mock_mfl
        
        # Mock successful API calls
        mock_sleeper.get_league_info.return_value = {"name": "Test Sleeper League"}
        mock_mfl.get_league_info.return_value = {"name": "Test MFL League"}
        
        # This should run without exceptions
        test_api_connections()
        
        mock_sleeper.get_league_info.assert_called_once()
        mock_mfl.get_league_info.assert_called_once()
    
    @patch('src.services.roster_sync.SleeperAPIClient')
    @patch('src.services.roster_sync.MFLAPIClient')
    def test_test_roster_fetching(self, mock_mfl_class, mock_sleeper_class):
        """Test the test_roster_fetching function"""
        # Mock API clients
        mock_sleeper = Mock()
        mock_mfl = Mock()
        mock_sleeper_class.return_value = mock_sleeper
        mock_mfl_class.return_value = mock_mfl
        
        # Mock API responses
        mock_sleeper.get_rosters.return_value = [{"roster_id": 1}]
        mock_sleeper.get_users.return_value = [{"user_id": "123", "username": "test"}]
        mock_mfl.get_rosters.return_value = [{"id": "0001", "name": "Team 1"}]
        
        # This should run without exceptions
        test_roster_fetching()
        
        mock_sleeper.get_rosters.assert_called_once()
        mock_sleeper.get_users.assert_called_once()
        mock_mfl.get_rosters.assert_called_once()


# Integration tests
class TestRosterSyncIntegration:
    """Integration tests for roster sync functionality"""
    
    @pytest.mark.integration
    @patch('src.services.roster_sync.get_config')
    def test_sleeper_api_integration(self, mock_config):
        """Integration test for Sleeper API (requires valid config)"""
        # Skip if no valid config available
        pytest.skip("Integration test requires valid API configuration")
        
        mock_config.return_value.SLEEPER_LEAGUE_ID = "real_league_id"
        
        client = SleeperAPIClient()
        
        # This would make real API calls in a full integration test
        try:
            league_info = client.get_league_info()
            assert "league_id" in league_info
        except APIError:
            pytest.skip("Real API not available for integration test")
    
    @pytest.mark.integration
    @patch('src.services.roster_sync.get_config')
    def test_mfl_api_integration(self, mock_config):
        """Integration test for MFL API (requires valid config)"""
        # Skip if no valid config available
        pytest.skip("Integration test requires valid API configuration")
        
        mock_config.return_value.MFL_LEAGUE_ID = "real_league_id"
        mock_config.return_value.MFL_LEAGUE_API_KEY = "real_api_key"
        
        client = MFLAPIClient()
        
        # This would make real API calls in a full integration test
        try:
            league_info = client.get_league_info()
            assert "id" in league_info
        except APIError:
            pytest.skip("Real API not available for integration test")


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__])