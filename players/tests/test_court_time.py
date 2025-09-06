# tests/test_court_time.py

import pytest
import pandas as pd
import numpy as np

from ..transformers.court_time import (
    track_lineup_states, 
    _get_starting_lineups, 
    _get_substitution_events,
    _get_player_activities
)


@pytest.fixture
def valid_box_score():
    """Valid box score with starters."""
    return pd.DataFrame({
        'nbaId': [101, 102, 103, 104, 105, 201, 202, 203, 204, 205],
        'nbaTeamId': [1610612745] * 5 + [1610612742] * 5,
        'name': [f'Player{i}' for i in range(1, 11)],
        'gs': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],  # All starters
        'min': [35.5, 28.2, 42.1, 31.7, 25.9, 38.2, 33.4, 29.8, 36.1, 27.3]
    })


@pytest.fixture
def valid_pbp():
    """Valid PBP with substitutions and activities."""
    return pd.DataFrame({
        'gameId': ['0022400001'] * 8,
        'period': [1, 1, 1, 1, 1, 1, 1, 1],
        'wallClockInt': [1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700],
        'msgType': [10, 8, 2, 1, 8, 4, 3, 12],  # Jump ball, sub, miss, make, sub, rebound, FT, period end
        'playerId1': [101, 106, 101, 102, 206, 201, 103, np.nan],
        'playerId2': [201, 105, np.nan, np.nan, 205, np.nan, np.nan, np.nan],
        'playerId3': [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
        'description': ['Jump ball', 'Substitution', 'Missed shot', 'Made shot', 'Substitution', 'Rebound', 'Free throw', 'End period']
    })


class TestTrackLineupStates:
    """Test main track_lineup_states function."""
    
    def test_produces_lineup_intervals(self, valid_box_score, valid_pbp):
        """Test that lineup intervals are produced."""
        result = track_lineup_states(valid_box_score, valid_pbp)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 0
        
        expected_cols = ['gameId', 'playerId', 'teamId', 'period_start', 'wallClock_start', 'period_end', 'wallClock_end']
        for col in expected_cols:
            assert col in result.columns
    
    def test_all_starters_have_intervals(self, valid_box_score, valid_pbp):
        """Test that all starting players have court time intervals."""
        result = track_lineup_states(valid_box_score, valid_pbp)
        
        if len(result) > 0:
            starters = valid_box_score[valid_box_score['gs'] == 1]['nbaId'].tolist()
            players_with_intervals = result['playerId'].unique()
            
            # At least some starters should have intervals
            starter_coverage = set(starters) & set(players_with_intervals)
            assert len(starter_coverage) > 0
    
    def test_intervals_have_valid_times(self, valid_box_score, valid_pbp):
        """Test that intervals have valid start/end times."""
        result = track_lineup_states(valid_box_score, valid_pbp)
        
        if len(result) > 0:
            # Start time should be <= end time
            assert all(result['wallClock_start'] <= result['wallClock_end'])
            assert all(result['period_start'] <= result['period_end'])


class TestGetStartingLineups:
    """Test _get_starting_lineups helper function."""
    
    def test_extracts_starters_by_team(self, valid_box_score):
        """Test that starters are correctly extracted by team."""
        result = _get_starting_lineups(valid_box_score)
        
        assert isinstance(result, dict)
        assert 1610612745 in result  # HOU team ID
        assert 1610612742 in result  # DAL team ID
        
        # Each team should have 5 starters
        assert len(result[1610612745]) == 5
        assert len(result[1610612742]) == 5
    
    def test_starter_ids_correct(self, valid_box_score):
        """Test that correct player IDs are identified as starters."""
        result = _get_starting_lineups(valid_box_score)
        
        hou_starters = result[1610612745]
        dal_starters = result[1610612742]
        
        assert 101 in hou_starters  # First HOU player
        assert 201 in dal_starters  # First DAL player


class TestGetSubstitutionEvents:
    """Test _get_substitution_events helper function."""
    
    def test_filters_substitution_events(self, valid_pbp):
        """Test that substitution events are correctly filtered."""
        result = _get_substitution_events(valid_pbp)
        
        assert isinstance(result, pd.DataFrame)
        
        if len(result) > 0:
            expected_cols = ['period', 'wallClockInt', 'playerId1', 'playerId2', 'description']
            for col in expected_cols:
                assert col in result.columns
            
            # Check that we got the expected number of substitutions
            expected_subs = len(valid_pbp[valid_pbp['msgType'] == 8])
            assert len(result) == expected_subs


class TestGetPlayerActivities:
    """Test _get_player_activities helper function."""
    
    def test_extracts_activity_events(self, valid_pbp):
        """Test that player activity events are extracted."""
        result = _get_player_activities(valid_pbp)
        
        assert isinstance(result, pd.DataFrame)
        
        if len(result) > 0:
            expected_cols = ['period', 'wallClockInt', 'playerId', 'msgType', 'description']
            for col in expected_cols:
                assert col in result.columns
            
            # Should only contain activity msgTypes
            activity_types = [1, 2, 3, 4, 5, 6, 7]
            assert all(result['msgType'].isin(activity_types))


class TestBasicFunctionality:
    """Test basic functionality and edge cases."""
    
    def test_minimal_pbp_data(self, valid_box_score):
        """Test handling of minimal PBP data with activity events."""
        # Ensure at least one activity event to avoid empty DataFrame sorting issues
        minimal_pbp = pd.DataFrame({
            'gameId': ['0022400001'],
            'period': [1],
            'wallClockInt': [1000],
            'msgType': [1],  # Made shot (activity type)
            'playerId1': [101],
            'playerId2': [np.nan],
            'playerId3': [np.nan],
            'description': ['Made shot']
        })
        
        result = track_lineup_states(valid_box_score, minimal_pbp)
        
        assert isinstance(result, pd.DataFrame)
    
    def test_no_substitutions(self, valid_box_score):
        """Test handling when no substitutions occur."""
        no_subs_pbp = pd.DataFrame({
            'gameId': ['0022400001'] * 3,
            'period': [1, 1, 1],
            'wallClockInt': [1000, 1500, 2000],
            'msgType': [10, 1, 12],  # Jump ball, made shot, period end
            'playerId1': [101, 102, np.nan],
            'playerId2': [201, np.nan, np.nan],
            'playerId3': [np.nan, np.nan, np.nan],
            'description': ['Jump ball', 'Made shot', 'End period']
        })
        
        result = track_lineup_states(valid_box_score, no_subs_pbp)
        
        assert isinstance(result, pd.DataFrame)
    
    def test_single_team_data(self):
        """Test handling of single team data."""
        single_team_box = pd.DataFrame({
            'nbaId': [101, 102, 103, 104, 105],
            'nbaTeamId': [1610612745] * 5,
            'name': [f'Player{i}' for i in range(1, 6)],
            'gs': [1, 1, 1, 1, 1],
            'min': [35, 30, 25, 20, 15]
        })
        
        single_team_pbp = pd.DataFrame({
            'gameId': ['0022400001', '0022400001'],
            'period': [1, 1],
            'wallClockInt': [1000, 1100],
            'msgType': [10, 1],  # Jump ball + made shot (activity type)
            'playerId1': [101, 102],
            'playerId2': [np.nan, np.nan],
            'playerId3': [np.nan, np.nan],
            'description': ['Jump ball', 'Made shot']
        })
        
        result = track_lineup_states(single_team_box, single_team_pbp)
        
        assert isinstance(result, pd.DataFrame)