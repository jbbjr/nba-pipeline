# lineups/tests/test_lineup_states.py

import pytest
import pandas as pd
import numpy as np

from ..transformers.lineup_states import (
    extract_lineup_states,
    _extract_starting_lineups,
    _parse_substitutions,
    _game_clock_to_seconds
)


@pytest.fixture
def valid_box_score_data():
    """Valid box score data with starting lineups."""
    return pd.DataFrame({
        'nbaId': [101, 102, 103, 104, 105, 201, 202, 203, 204, 205],
        'team': ['HOU', 'HOU', 'HOU', 'HOU', 'HOU', 'DAL', 'DAL', 'DAL', 'DAL', 'DAL'],
        'nbaTeamId': [1610612745] * 5 + [1610612742] * 5,
        'startPos': ['PG', 'SG', 'SF', 'PF', 'C', 'PG', 'SG', 'SF', 'PF', 'C'],
        'name': [f'Player{i}' for i in range(1, 11)]
    })


@pytest.fixture
def valid_pbp_data():
    """Valid PBP data with substitutions."""
    return pd.DataFrame({
        'msgType': [10, 8, 8],  # Jump ball, then 2 substitutions
        'period': [1, 1, 1],
        'gameClock': ['12:00', '9:30', '8:15'],
        'team': ['HOU', 'HOU', 'DAL'],
        'playerId1': [106.0, 106.0, 206.0],  # Players coming in
        'playerId2': [101.0, 102.0, 201.0],  # Players going out
        'pbpOrder': [1, 2, 3]
    })


class TestExtractLineupStates:
    """Test main extract_lineup_states function."""
    
    def test_valid_data_produces_lineup_states(self, valid_box_score_data, valid_pbp_data):
        """Test that valid data produces lineup states."""
        result = extract_lineup_states(valid_box_score_data, valid_pbp_data)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert 'lineup_id' in result.columns
        assert 'team' in result.columns
        assert 'period' in result.columns
        assert all(f'player_{i}' in result.columns for i in range(1, 6))
    
    def test_teams_have_starting_lineups(self, valid_box_score_data, valid_pbp_data):
        """Test that both teams have starting lineups."""
        result = extract_lineup_states(valid_box_score_data, valid_pbp_data)
        
        teams = result['team'].unique()
        assert len(teams) == 2
        assert 'HOU' in teams
        assert 'DAL' in teams


class TestExtractStartingLineups:
    """Test starting lineup extraction."""
    
    def test_extract_starting_lineups_success(self, valid_box_score_data):
        """Test successful extraction of starting lineups."""
        result = _extract_starting_lineups(valid_box_score_data)
        
        assert len(result) == 2  # Two teams
        assert 'HOU' in result
        assert 'DAL' in result
        
        # Each team should have 5 players
        assert len(result['HOU']['players']) == 5
        assert len(result['DAL']['players']) == 5
    
    def test_incorrect_starter_count_raises_error(self):
        """Test that incorrect number of starters raises error."""
        invalid_data = pd.DataFrame({
            'nbaId': [101, 102, 103],  # Only 3 starters
            'team': ['HOU', 'HOU', 'HOU'],
            'nbaTeamId': [1610612745, 1610612745, 1610612745],
            'startPos': ['PG', 'SG', 'SF']
        })
        
        with pytest.raises(ValueError, match="has 3 starters, expected 5"):
            _extract_starting_lineups(invalid_data)


class TestParseSubstitutions:
    """Test substitution parsing."""
    
    def test_parse_substitutions_success(self, valid_pbp_data):
        """Test successful parsing of substitutions."""
        result = _parse_substitutions(valid_pbp_data)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2  # 2 substitutions in test data
        
        expected_cols = ['period', 'game_clock', 'game_clock_seconds', 'team', 'player_in', 'player_out']
        for col in expected_cols:
            assert col in result.columns


class TestHelperFunctions:
    """Test helper functions."""
    
    def test_game_clock_to_seconds(self):
        """Test game clock conversion to seconds."""
        assert _game_clock_to_seconds('12:00') == 720
        assert _game_clock_to_seconds('9:30') == 570
        assert _game_clock_to_seconds('0:45') == 45
        assert _game_clock_to_seconds('invalid') == 0
        assert _game_clock_to_seconds('') == 0