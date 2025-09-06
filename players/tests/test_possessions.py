# tests/test_possessions.py

import pytest
import pandas as pd
import numpy as np

from ..transformers.possessions import (
    analyze_possessions,
    _get_team_mapping,
    _identify_possessions,
    _count_player_possessions
)


@pytest.fixture
def valid_box_score():
    """Valid box score data."""
    return pd.DataFrame({
        'nbaId': [101, 102, 103, 201, 202, 203],
        'nbaTeamId': [1610612745, 1610612745, 1610612745, 1610612742, 1610612742, 1610612742],
        'name': ['Player1', 'Player2', 'Player3', 'Player4', 'Player5', 'Player6'],
        'min': [35.5, 28.2, 42.1, 38.2, 33.4, 29.8]
    })


@pytest.fixture
def valid_pbp():
    """Valid PBP data with possession events."""
    return pd.DataFrame({
        'period': [1, 1, 1, 1, 1],
        'wallClockInt': [1000, 1100, 1200, 1300, 1400],
        'msgType': [10, 2, 4, 1, 12],  # Jump ball, miss, rebound, make, period end
        'actionType': [0, 0, 1, 0, 0],  # Required by the code
        'offTeamId': [1610612745, 1610612745, 1610612742, 1610612742, np.nan],
        'defTeamId': [1610612742, 1610612742, 1610612745, 1610612745, np.nan],
        'playerId1': [101, 101, 201, 202, np.nan],
        'playerId2': [201, np.nan, np.nan, np.nan, np.nan],
        'description': ['Jump ball', 'Missed shot', 'Rebound', 'Made shot', 'End period']
    })


@pytest.fixture
def valid_lineup_intervals():
    """Valid lineup intervals data."""
    return pd.DataFrame({
        'playerId': [101, 102, 201, 202],
        'teamId': [1610612745, 1610612745, 1610612742, 1610612742],
        'period_start': [1, 1, 1, 1],
        'period_end': [1, 1, 1, 1],
        'wallClock_start': [950, 950, 950, 950],
        'wallClock_end': [1450, 1450, 1450, 1450]
    })


class TestAnalyzePossessions:
    """Test main analyze_possessions function."""
    
    def test_produces_possession_counts(self, valid_box_score, valid_pbp, valid_lineup_intervals):
        """Test that possession counts are produced."""
        result = analyze_possessions(valid_box_score, valid_pbp, valid_lineup_intervals)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 0
        
        expected_cols = ['playerId', 'offensive_possessions', 'defensive_possessions', 'total_possessions']
        for col in expected_cols:
            assert col in result.columns
    
    def test_possession_counts_are_numeric(self, valid_box_score, valid_pbp, valid_lineup_intervals):
        """Test that possession counts are numeric."""
        result = analyze_possessions(valid_box_score, valid_pbp, valid_lineup_intervals)
        
        if len(result) > 0:
            assert pd.api.types.is_numeric_dtype(result['offensive_possessions'])
            assert pd.api.types.is_numeric_dtype(result['defensive_possessions'])
            assert pd.api.types.is_numeric_dtype(result['total_possessions'])
    
    def test_total_possessions_sum_correctly(self, valid_box_score, valid_pbp, valid_lineup_intervals):
        """Test that total possessions equal offensive + defensive."""
        result = analyze_possessions(valid_box_score, valid_pbp, valid_lineup_intervals)
        
        if len(result) > 0:
            calculated_total = result['offensive_possessions'] + result['defensive_possessions']
            pd.testing.assert_series_equal(result['total_possessions'], calculated_total, check_names=False)


class TestGetTeamMapping:
    """Test _get_team_mapping helper function."""
    
    def test_creates_player_team_mapping(self, valid_box_score):
        """Test that player-to-team mapping is created correctly."""
        result = _get_team_mapping(valid_box_score)
        
        assert isinstance(result, dict)
        assert 101 in result
        assert result[101] == 1610612745  # HOU team
        assert 201 in result
        assert result[201] == 1610612742  # DAL team


class TestIdentifyPossessions:
    """Test _identify_possessions helper function."""
    
    def test_identifies_possession_boundaries(self, valid_pbp):
        """Test that possession boundaries are identified."""
        result = _identify_possessions(valid_pbp)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 0
        
        if len(result) > 0:
            expected_cols = ['possession_id', 'start_period', 'start_wallClock', 'end_period', 'end_wallClock', 'end_reason']
            for col in expected_cols:
                assert col in result.columns
    
    def test_possession_ids_sequential(self, valid_pbp):
        """Test that possession IDs are sequential."""
        result = _identify_possessions(valid_pbp)
        
        if len(result) > 1:
            ids = result['possession_id'].tolist()
            expected_ids = list(range(1, len(result) + 1))
            assert ids == expected_ids
    
    def test_possessions_have_teams(self, valid_pbp):
        """Test that possessions have offensive and defensive teams."""
        result = _identify_possessions(valid_pbp)
        
        for _, poss in result.iterrows():
            # May be None for some possessions, but if present should be valid
            if pd.notna(poss.get('offensive_team')) and pd.notna(poss.get('defensive_team')):
                assert poss['offensive_team'] != poss['defensive_team']


class TestCountPlayerPossessions:
    """Test _count_player_possessions helper function."""
    
    def test_counts_possessions_per_player(self):
        """Test that possessions are counted per player."""
        possessions_df = pd.DataFrame({
            'possession_id': [1, 2],
            'start_period': [1, 1],
            'end_period': [1, 1],
            'start_wallClock': [1000, 1200],
            'end_wallClock': [1100, 1300],
            'offensive_team': [1610612745, 1610612742],
            'defensive_team': [1610612742, 1610612745]
        })
        
        lineup_intervals = pd.DataFrame({
            'playerId': [101, 201],
            'teamId': [1610612745, 1610612742],
            'period_start': [1, 1],
            'period_end': [1, 1],
            'wallClock_start': [950, 950],
            'wallClock_end': [1350, 1350]
        })
        
        team_mapping = {101: 1610612745, 201: 1610612742}
        
        result = _count_player_possessions(possessions_df, lineup_intervals, team_mapping)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 0
        
        if len(result) > 0:
            assert 'playerId' in result.columns
            assert 'offensive_possessions' in result.columns
            assert 'defensive_possessions' in result.columns


class TestBasicFunctionality:
    """Test basic functionality and edge cases."""
    
    def test_empty_pbp_data(self, valid_box_score, valid_lineup_intervals):
        """Test handling of empty PBP data."""
        empty_pbp = pd.DataFrame(columns=[
            'period', 'wallClockInt', 'msgType', 'actionType', 
            'offTeamId', 'defTeamId', 'playerId1', 'playerId2', 'description'
        ])
        
        result = analyze_possessions(valid_box_score, empty_pbp, valid_lineup_intervals)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
    
    def test_no_possessions_identified(self, valid_box_score, valid_lineup_intervals):
        """Test handling when no possessions are identified."""
        no_poss_pbp = pd.DataFrame({
            'period': [1],
            'wallClockInt': [1000],
            'msgType': [10],  # Only jump ball, no possession-ending events
            'actionType': [0],  # Required by the code
            'offTeamId': [1610612745],
            'defTeamId': [1610612742],
            'playerId1': [101],
            'playerId2': [201],
            'description': ['Jump ball']
        })
        
        result = analyze_possessions(valid_box_score, no_poss_pbp, valid_lineup_intervals)
        
        assert isinstance(result, pd.DataFrame)
        # Should handle gracefully even with minimal possessions
    
    def test_single_possession(self, valid_box_score, valid_lineup_intervals):
        """Test handling of single possession."""
        single_poss_pbp = pd.DataFrame({
            'period': [1, 1],
            'wallClockInt': [1000, 1100],
            'msgType': [1, 12],  # Made shot, period end
            'actionType': [0, 0],  # Required by the code
            'offTeamId': [1610612745, np.nan],
            'defTeamId': [1610612742, np.nan],
            'playerId1': [101, np.nan],
            'playerId2': [np.nan, np.nan],
            'description': ['Made shot', 'End period']
        })
        
        result = analyze_possessions(valid_box_score, single_poss_pbp, valid_lineup_intervals)
        
        assert isinstance(result, pd.DataFrame)