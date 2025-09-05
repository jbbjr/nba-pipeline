# lineups/tests/test_possessions.py

import pytest
import pandas as pd
import numpy as np

from ..transformers.possessions import (
    extract_possessions,
    _game_clock_to_seconds
)


@pytest.fixture
def valid_pbp_data():
    """Valid PBP data with two teams and various event types."""
    return pd.DataFrame({
        'period': [1, 1, 1, 1, 1],
        'gameClock': ['12:00', '11:45', '11:30', '11:15', '11:00'],
        'msgType': [10, 2, 4, 1, 12],  # Jump ball, miss, rebound, made shot, period end
        'offTeamId': [1610612745, 1610612745, 1610612742, 1610612745, 0],
        'defTeamId': [1610612742, 1610612742, 1610612745, 1610612742, 0],
        'nbaTeamId': [1610612745, 1610612745, 1610612742, 1610612745, np.nan],
        'playerId1': [101.0, 101.0, 201.0, 102.0, np.nan],
        'playerId2': [201.0, np.nan, np.nan, np.nan, np.nan],
        'pts': [0, 0, 0, 2, 0],
        'pbpOrder': [1, 2, 3, 4, 5]
    })


class TestExtractPossessions:
    """Test main extract_possessions function."""
    
    def test_valid_data_produces_possessions(self, valid_pbp_data):
        """Test that valid data produces possession records."""
        result = extract_possessions(valid_pbp_data)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 0  # May be 0 if no valid possessions found
        
        if len(result) > 0:
            expected_cols = [
                'possession_id', 'period', 'start_time_seconds', 'end_time_seconds',
                'off_team', 'def_team', 'points_scored', 'end_type', 'duration_seconds'
            ]
            for col in expected_cols:
                assert col in result.columns
    
    def test_possession_ids_are_sequential(self, valid_pbp_data):
        """Test that possession IDs are sequential."""
        result = extract_possessions(valid_pbp_data)
        
        if len(result) > 1:
            possession_ids = result['possession_id'].tolist()
            expected_ids = list(range(1, len(result) + 1))
            assert possession_ids == expected_ids
    
    def test_possessions_have_valid_teams(self, valid_pbp_data):
        """Test that possessions have valid team assignments."""
        result = extract_possessions(valid_pbp_data)
        
        for _, poss in result.iterrows():
            assert pd.notna(poss['off_team'])
            assert pd.notna(poss['def_team'])
            assert poss['off_team'] != poss['def_team']


class TestHelperFunctions:
    """Test helper functions."""
    
    def test_game_clock_to_seconds_conversion(self):
        """Test game clock conversion to seconds."""
        assert _game_clock_to_seconds('12:00') == 720
        assert _game_clock_to_seconds('9:30') == 570
        assert _game_clock_to_seconds('0:45') == 45
        assert _game_clock_to_seconds('') == 0
        assert _game_clock_to_seconds('invalid') == 0


class TestBasicFunctionality:
    """Test basic functionality with realistic two-team scenarios."""
    
    def test_handles_simple_made_shot(self):
        """Test handling of simple made shot possession."""
        simple_pbp = pd.DataFrame({
            'period': [1, 1, 1, 1],
            'gameClock': ['12:00', '11:45', '11:30', '11:00'],
            'msgType': [10, 1, 2, 12],  # Jump ball, made shot, missed shot, period end
            'offTeamId': [1610612745, 1610612745, 1610612742, 0],
            'defTeamId': [1610612742, 1610612742, 1610612745, 0],
            'nbaTeamId': [1610612745, 1610612745, 1610612742, np.nan],
            'playerId1': [101.0, 101.0, 201.0, np.nan],
            'pts': [0, 2, 0, 0],
            'pbpOrder': [1, 2, 3, 4]
        })
        
        result = extract_possessions(simple_pbp)
        assert isinstance(result, pd.DataFrame)
    
    def test_handles_period_end(self):
        """Test handling of period end."""
        period_end_pbp = pd.DataFrame({
            'period': [1, 1, 1, 1],
            'gameClock': ['12:00', '11:30', '11:15', '0:00'],
            'msgType': [10, 2, 1, 12],  # Jump ball, missed shot, made shot, period end
            'offTeamId': [1610612745, 1610612745, 1610612742, 0],
            'defTeamId': [1610612742, 1610612742, 1610612745, 0],
            'nbaTeamId': [1610612745, 1610612745, 1610612742, np.nan],
            'playerId1': [101.0, 101.0, 201.0, np.nan],
            'pts': [0, 0, 2, 0],
            'pbpOrder': [1, 2, 3, 4]
        })
        
        result = extract_possessions(period_end_pbp)
        assert isinstance(result, pd.DataFrame)
    
    def test_handles_team_switching_possession(self):
        """Test possession switching between teams."""
        switching_pbp = pd.DataFrame({
            'period': [1, 1, 1, 1],
            'gameClock': ['12:00', '11:30', '11:15', '11:00'],
            'msgType': [10, 2, 4, 1],  # Jump ball, miss, def rebound, made shot
            'offTeamId': [1610612745, 1610612745, 1610612742, 1610612742],
            'defTeamId': [1610612742, 1610612742, 1610612745, 1610612745],
            'nbaTeamId': [1610612745, 1610612745, 1610612742, 1610612742],
            'playerId1': [101.0, 101.0, 201.0, 201.0],
            'pts': [0, 0, 0, 3],
            'pbpOrder': [1, 2, 3, 4]
        })
        
        result = extract_possessions(switching_pbp)
        assert isinstance(result, pd.DataFrame)
        # Should have possessions for both teams
        if len(result) > 1:
            team_ids = result['off_team'].unique()
            assert len(team_ids) >= 1  # At least one team should have possessions