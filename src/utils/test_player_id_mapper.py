import pytest
import unittest.mock as mock
from unittest.mock import Mock, patch, MagicMock
import hashlib
from typing import Dict, Any

from src.utils.player_id_mapper import (
    PlayerIDMapper, PlayerInfo, create_player_mapping,
    get_canonical_id, sync_players_to_database
)
from src.database.models import Player, SessionLocal


class TestPlayerInfo:
    """Test cases for PlayerInfo dataclass"""
    
    def test_player_info_creation(self):
        """Test PlayerInfo creation and attributes"""
        player = PlayerInfo(
            name="Josh Allen",
            position="QB",
            team="BUF",
            sleeper_id="4881",
            mfl_id="14228"
        )
        
        assert player.name == "Josh Allen"
        assert player.position == "QB"
        assert player.team == "BUF"
        assert player.sleeper_id == "4881"
        assert player.mfl_id == "14228"
    
    def test_player_info_defaults(self):
        """Test PlayerInfo with default values"""
        player = PlayerInfo(
            name="Unknown Player",
            position="UNKNOWN",
            team="UNKNOWN"
        )
        
        assert player.sleeper_id is None
        assert player.mfl_id is None
        assert player.name == "Unknown Player"


class TestPlayerIDMapper:
    """Test cases for PlayerIDMapper class"""
    
    @pytest.fixture
    def mapper(self):
        """Create a PlayerIDMapper instance for testing"""
        with patch('src.utils.player_id_mapper.SessionLocal'):
            return PlayerIDMapper()
    
    def test_normalize_player_name(self, mapper):
        """Test player name normalization"""
        test_cases = [
            ("Josh Allen", "josh allen"),
            ("D'Andre Swift", "dandre swift"),
            ("DeAndre Hopkins", "deandre hopkins"),
            ("A.J. Brown", "aj brown"),
            ("Geno Smith", "geno smith"),
            ("  Josh  Allen  ", "josh allen"),
            ("DE'VON ACHANE", "devon achane")
        ]
        
        for input_name, expected in test_cases:
            result = mapper.normalize_player_name(input_name)
            assert result == expected, f"Failed for {input_name}: got {result}, expected {expected}"
    
    def test_normalize_position(self, mapper):
        """Test position normalization"""
        test_cases = [
            ("QB", "QB"),
            ("RB", "RB"), 
            ("WR", "WR"),
            ("TE", "TE"),
            ("K", "K"),
            ("DEF", "DEF"),
            ("qb", "QB"),
            ("rb", "RB"),
            ("Running Back", "RB"),
            ("Wide Receiver", "WR"),
            ("Tight End", "TE"),
            ("Quarterback", "QB"),
            ("Kicker", "K"),
            ("Defense", "DEF"),
            ("DST", "DEF"),
            ("D/ST", "DEF"),
            ("unknown", "UNKNOWN"),
            ("", "UNKNOWN")
        ]
        
        for input_pos, expected in test_cases:
            result = mapper.normalize_position(input_pos)
            assert result == expected, f"Failed for {input_pos}: got {result}, expected {expected}"
    
    def test_normalize_team(self, mapper):
        """Test team normalization"""
        test_cases = [
            ("BUF", "BUF"),
            ("buf", "BUF"),
            ("Buffalo", "BUF"),
            ("Buffalo Bills", "BUF"),
            ("KC", "KC"),
            ("Kansas City", "KC"),
            ("Kansas City Chiefs", "KC"),
            ("SF", "SF"),
            ("San Francisco", "SF"),
            ("San Francisco 49ers", "SF"),
            ("49ers", "SF"),
            ("LAR", "LAR"),
            ("Los Angeles Rams", "LAR"),
            ("Rams", "LAR"),
            ("NE", "NE"),
            ("New England", "NE"),
            ("New England Patriots", "NE"),
            ("Patriots", "NE"),
            ("unknown", "UNKNOWN"),
            ("", "UNKNOWN")
        ]
        
        for input_team, expected in test_cases:
            result = mapper.normalize_team(input_team)
            assert result == expected, f"Failed for {input_team}: got {result}, expected {expected}"
    
    def test_generate_canonical_id(self, mapper):
        """Test canonical ID generation"""
        # Test with consistent inputs
        name = "Josh Allen"
        position = "QB"
        team = "BUF"
        
        id1 = mapper.generate_canonical_id(name, position, team)
        id2 = mapper.generate_canonical_id(name, position, team)
        
        # Should be consistent
        assert id1 == id2
        assert len(id1) == 32  # MD5 hash length
        
        # Different inputs should produce different IDs
        id3 = mapper.generate_canonical_id("Patrick Mahomes", "QB", "KC")
        assert id1 != id3
    
    def test_generate_canonical_id_normalization(self, mapper):
        """Test that canonical ID generation normalizes inputs"""
        # These should produce the same ID due to normalization
        id1 = mapper.generate_canonical_id("Josh Allen", "QB", "BUF")
        id2 = mapper.generate_canonical_id("josh allen", "qb", "buf")
        id3 = mapper.generate_canonical_id("  Josh  Allen  ", "Quarterback", "Buffalo")
        
        assert id1 == id2 == id3
    
    def test_is_starter_position(self, mapper):
        """Test starter position determination"""
        starter_positions = ["QB", "RB", "WR", "TE", "K"]
        non_starter_positions = ["DEF", "UNKNOWN", "BENCH", ""]
        
        for pos in starter_positions:
            assert mapper._is_starter_position(pos) is True
        
        for pos in non_starter_positions:
            assert mapper._is_starter_position(pos) is False
    
    @patch('src.utils.player_id_mapper.SessionLocal')
    def test_add_player_mapping(self, mock_session, mapper):
        """Test adding player mapping"""
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        # Mock database query to return no existing player
        mock_db.query.return_value.filter.return_value.first.return_value = None
        
        player_info = PlayerInfo(
            name="Josh Allen",
            position="QB", 
            team="BUF",
            sleeper_id="4881",
            mfl_id="14228"
        )
        
        canonical_id = mapper.add_player_mapping(player_info)
        
        # Verify canonical ID was generated
        assert canonical_id is not None
        assert len(canonical_id) == 32
        
        # Verify player was added to internal mapping
        assert canonical_id in mapper.id_mappings
        assert mapper.id_mappings[canonical_id] == player_info
        
        # Verify database operations
        mock_db.add.assert_called_once()
        mock_db.commit.assert_called_once()
        mock_db.close.assert_called_once()
    
    @patch('src.utils.player_id_mapper.SessionLocal')
    def test_add_player_mapping_existing_player(self, mock_session, mapper):
        """Test adding mapping for existing player"""
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        # Mock existing player in database
        existing_player = Mock()
        existing_player.nfl_id = "existing_canonical_id"
        mock_db.query.return_value.filter.return_value.first.return_value = existing_player
        
        player_info = PlayerInfo(
            name="Josh Allen",
            position="QB",
            team="BUF",
            sleeper_id="4881"
        )
        
        canonical_id = mapper.add_player_mapping(player_info)
        
        # Should return existing canonical ID
        assert canonical_id == "existing_canonical_id"
        
        # Should not add new player to database
        mock_db.add.assert_not_called()
        mock_db.commit.assert_called_once()
    
    def test_get_canonical_id_from_mapping(self, mapper):
        """Test getting canonical ID from internal mapping"""
        # Add a player to internal mapping
        player_info = PlayerInfo(
            name="Josh Allen",
            position="QB",
            team="BUF",
            sleeper_id="4881",
            mfl_id="14228"
        )
        
        canonical_id = mapper.generate_canonical_id("Josh Allen", "QB", "BUF")
        mapper.id_mappings[canonical_id] = player_info
        mapper.sleeper_to_canonical["4881"] = canonical_id
        mapper.mfl_to_canonical["14228"] = canonical_id
        
        # Test retrieval by Sleeper ID
        result = mapper.get_canonical_id(sleeper_id="4881")
        assert result == canonical_id
        
        # Test retrieval by MFL ID
        result = mapper.get_canonical_id(mfl_id="14228")
        assert result == canonical_id
        
        # Test retrieval by name/position/team
        result = mapper.get_canonical_id(name="Josh Allen", position="QB", team="BUF")
        assert result == canonical_id
    
    @patch('src.utils.player_id_mapper.SessionLocal')
    def test_get_canonical_id_from_database(self, mock_session, mapper):
        """Test getting canonical ID from database"""
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        # Mock player in database
        mock_player = Mock()
        mock_player.nfl_id = "db_canonical_id"
        mock_player.name = "Josh Allen"
        mock_player.position = "QB"
        mock_player.team = "BUF"
        mock_player.sleeper_id = "4881"
        mock_player.mfl_id = "14228"
        
        mock_db.query.return_value.filter.return_value.first.return_value = mock_player
        
        # Test retrieval by Sleeper ID
        result = mapper.get_canonical_id(sleeper_id="4881")
        assert result == "db_canonical_id"
        
        # Verify internal mappings were updated
        assert "4881" in mapper.sleeper_to_canonical
        assert "14228" in mapper.mfl_to_canonical
        assert "db_canonical_id" in mapper.id_mappings
    
    def test_get_player_info(self, mapper):
        """Test getting player info"""
        # Add a player to internal mapping
        player_info = PlayerInfo(
            name="Josh Allen",
            position="QB",
            team="BUF",
            sleeper_id="4881",
            mfl_id="14228"
        )
        
        canonical_id = "test_canonical_id"
        mapper.id_mappings[canonical_id] = player_info
        
        result = mapper.get_player_info(canonical_id)
        assert result == player_info
        
        # Test non-existent ID
        result = mapper.get_player_info("non_existent_id")
        assert result is None
    
    @patch('src.utils.player_id_mapper.SessionLocal')
    def test_load_from_database(self, mock_session, mapper):
        """Test loading mappings from database"""
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        # Mock players in database
        mock_players = [
            Mock(
                nfl_id="canonical_1",
                name="Josh Allen",
                position="QB",
                team="BUF",
                sleeper_id="4881",
                mfl_id="14228"
            ),
            Mock(
                nfl_id="canonical_2", 
                name="Patrick Mahomes",
                position="QB",
                team="KC",
                sleeper_id="4046",
                mfl_id="12971"
            )
        ]
        
        mock_db.query.return_value.all.return_value = mock_players
        
        mapper.load_from_database()
        
        # Verify mappings were loaded
        assert len(mapper.id_mappings) == 2
        assert "4881" in mapper.sleeper_to_canonical
        assert "14228" in mapper.mfl_to_canonical
        assert "4046" in mapper.sleeper_to_canonical
        assert "12971" in mapper.mfl_to_canonical
        
        # Verify player info
        player_info = mapper.id_mappings["canonical_1"]
        assert player_info.name == "Josh Allen"
        assert player_info.position == "QB"
    
    def test_get_mapping_stats(self, mapper):
        """Test getting mapping statistics"""
        # Add some test mappings
        mapper.id_mappings = {
            "id1": PlayerInfo("Player 1", "QB", "BUF"),
            "id2": PlayerInfo("Player 2", "RB", "KC"),
            "id3": PlayerInfo("Player 3", "WR", "SF")
        }
        
        mapper.sleeper_to_canonical = {"s1": "id1", "s2": "id2"}
        mapper.mfl_to_canonical = {"m1": "id1", "m2": "id2", "m3": "id3"}
        
        stats = mapper.get_mapping_stats()
        
        assert stats["total_players"] == 3
        assert stats["sleeper_mappings"] == 2
        assert stats["mfl_mappings"] == 3
        assert stats["cross_platform_mappings"] == 2  # id1 and id2 have both


class TestUtilityFunctions:
    """Test utility functions in the module"""
    
    @patch('src.utils.player_id_mapper.PlayerIDMapper')
    def test_create_player_mapping(self, mock_mapper_class):
        """Test create_player_mapping function"""
        mock_mapper = Mock()
        mock_mapper_class.return_value = mock_mapper
        mock_mapper.add_player_mapping.return_value = "test_canonical_id"
        
        player_info = PlayerInfo(
            name="Josh Allen",
            position="QB",
            team="BUF"
        )
        
        result = create_player_mapping(player_info)
        
        assert result == "test_canonical_id"
        mock_mapper.add_player_mapping.assert_called_once_with(player_info)
    
    @patch('src.utils.player_id_mapper.PlayerIDMapper')
    def test_get_canonical_id_function(self, mock_mapper_class):
        """Test get_canonical_id utility function"""
        mock_mapper = Mock()
        mock_mapper_class.return_value = mock_mapper
        mock_mapper.get_canonical_id.return_value = "test_canonical_id"
        
        result = get_canonical_id(sleeper_id="4881")
        
        assert result == "test_canonical_id"
        mock_mapper.get_canonical_id.assert_called_once_with(
            sleeper_id="4881", mfl_id=None, name=None, position=None, team=None
        )
    
    @patch('src.utils.player_id_mapper.PlayerIDMapper')
    def test_sync_players_to_database(self, mock_mapper_class):
        """Test sync_players_to_database function"""
        mock_mapper = Mock()
        mock_mapper_class.return_value = mock_mapper
        
        # Mock player data
        sleeper_players = {
            "4881": {
                "first_name": "Josh",
                "last_name": "Allen", 
                "position": "QB",
                "team": "BUF"
            }
        }
        
        mfl_players = [
            {
                "id": "14228",
                "name": "Allen, Josh",
                "position": "QB",
                "team": "BUF"
            }
        ]
        
        result = sync_players_to_database(sleeper_players, mfl_players)
        
        # Should return number of players synced
        assert isinstance(result, int)
        
        # Verify mapper methods were called
        mock_mapper.add_player_mapping.assert_called()
        

class TestPlayerIDMapperEdgeCases:
    """Test edge cases and error handling"""
    
    @pytest.fixture
    def mapper(self):
        with patch('src.utils.player_id_mapper.SessionLocal'):
            return PlayerIDMapper()
    
    def test_normalize_name_edge_cases(self, mapper):
        """Test name normalization edge cases"""
        edge_cases = [
            ("", ""),
            ("   ", ""),
            ("A", "a"),
            ("A'B", "ab"),
            ("A.B.C", "abc"),
            ("St. Brown", "st brown"),
            ("O'Dell", "odell"),
            ("D'Andre", "dandre")
        ]
        
        for input_name, expected in edge_cases:
            result = mapper.normalize_player_name(input_name)
            assert result == expected
    
    def test_get_canonical_id_no_match(self, mapper):
        """Test getting canonical ID when no match exists"""
        result = mapper.get_canonical_id(sleeper_id="nonexistent")
        assert result is None
        
        result = mapper.get_canonical_id(name="Nonexistent Player", position="QB", team="NONE")
        assert result is None
    
    @patch('src.utils.player_id_mapper.SessionLocal')
    def test_database_error_handling(self, mock_session, mapper):
        """Test database error handling"""
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        # Mock database error
        mock_db.query.side_effect = Exception("Database error")
        
        player_info = PlayerInfo("Test Player", "QB", "BUF")
        
        # Should handle database errors gracefully
        result = mapper.add_player_mapping(player_info)
        assert result is None
        
        # Database should be closed even on error
        mock_db.close.assert_called()
    
    def test_empty_player_info(self, mapper):
        """Test with empty/minimal player info"""
        player_info = PlayerInfo("", "UNKNOWN", "UNKNOWN")
        
        # Should still generate a canonical ID
        canonical_id = mapper.generate_canonical_id(
            player_info.name, 
            player_info.position, 
            player_info.team
        )
        
        assert canonical_id is not None
        assert len(canonical_id) == 32


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__])