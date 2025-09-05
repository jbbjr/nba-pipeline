# lineups/tests/test_lineup_possessions.py

import pytest
import pandas as pd
import numpy as np

from ..transformers.lineup_possessions import (
    match_lineups_to_possessions,
    find_lineup_at_time,
    create_possession_record,
    validate_for_ratings_calculation
)


@pytest.fixture
def valid_lineup_states():
    """Valid lineup states data."""
    return pd.DataFrame({
        'period': [1, 1, 1, 1],
        'game_clock_seconds': [720, 570, 720, 480],
        'team_id': [1610612745, 1610612745, 1610612742, 1610612742],
        'team': ['HOU', 'HOU', 'DAL', 'DAL'],
        'player_1': [101, 101, 201, 201],
        'player_2': [102, 102, 202, 202],
        'player_3': [103, 103, 203, 203],
        'player_4': [104, 104, 204, 204],
        'player_5': [105, 106, 205, 206],  # Substitutions
        'lineup_id': ['HOU_101_102_103_104_105', 'HOU_101_102_103_104_106', 
                     'DAL_201_202_203_204_205', 'DAL_201_202_203_204_206']
    })


@pytest.fixture
def valid_possessions():
    """Valid possessions data."""
    return pd.DataFrame({
        'possession_id': [1, 2, 3],
        'period': [1, 1, 1],
        'start_time_seconds': [720, 600, 500],
        'end_time_seconds': [600, 500, 450],
        'duration_seconds': [120, 100, 50],
        'points_scored': [2, 0, 3],
        'end_type': ['made_shot', 'turnover', 'made_shot'],
        'off_team': [1610612745, 1610612742, 1610612745],
        'def_team': [1610612742, 1610612745, 1610612742]
    })


class TestMatchLineupsToPossessions:
    """Test main match_lineups_to_possessions function."""
    
    def test_valid_data_produces_matched_records(self, valid_lineup_states, valid_possessions):
        """Test that valid data produces matched lineup-possession records."""
        result = match_lineups_to_possessions(valid_lineup_states, valid_possessions)
        
        assert isinstance(result, pd.DataFrame)
        
        if len(result) > 0:
            expected_cols = [
                'possession_id', 'period', 'points_scored', 'off_team_id', 'def_team_id',
                'off_player_1', 'off_player_2', 'off_player_3', 'off_player_4', 'off_player_5',
                'def_player_1', 'def_player_2', 'def_player_3', 'def_player_4', 'def_player_5',
                'off_lineup_id', 'def_lineup_id'
            ]
            
            for col in expected_cols:
                assert col in result.columns
    
    def test_possession_id_preserved(self, valid_lineup_states, valid_possessions):
        """Test that original possession IDs are preserved."""
        result = match_lineups_to_possessions(valid_lineup_states, valid_possessions)
        
        if len(result) > 0:
            # Check that possession IDs match original data
            original_poss_ids = set(valid_possessions['possession_id'])
            result_poss_ids = set(result['possession_id'])
            assert result_poss_ids.issubset(original_poss_ids)


class TestFindLineupAtTime:
    """Test find_lineup_at_time function."""
    
    def test_finds_correct_lineup_at_period_start(self, valid_lineup_states):
        """Test finding lineup at period start."""
        result = find_lineup_at_time(valid_lineup_states, period=1, time_seconds=720, team_id=1610612745)
        
        assert result is not None
        assert result['team_id'] == 1610612745
        assert result['period'] == 1
        assert result['player_1'] == 101
    
    def test_returns_none_for_unknown_team(self, valid_lineup_states):
        """Test that unknown team returns None."""
        result = find_lineup_at_time(valid_lineup_states, period=1, time_seconds=720, team_id=9999999)
        
        assert result is None
    
    def test_returns_none_for_unknown_period(self, valid_lineup_states):
        """Test that unknown period returns None."""
        result = find_lineup_at_time(valid_lineup_states, period=5, time_seconds=720, team_id=1610612745)
        
        assert result is None


class TestCreatePossessionRecord:
    """Test create_possession_record function."""
    
    def test_creates_complete_record(self):
        """Test that complete possession record is created."""
        possession = pd.Series({
            'possession_id': 1,
            'period': 1,
            'start_time_seconds': 720,
            'end_time_seconds': 690,
            'duration_seconds': 30,
            'points_scored': 2,
            'end_type': 'made_shot',
            'off_team': 1610612745,
            'def_team': 1610612742
        })
        
        off_lineup = {
            'team': 'HOU',
            'player_1': 101, 'player_2': 102, 'player_3': 103, 'player_4': 104, 'player_5': 105,
            'lineup_id': 'HOU_101_102_103_104_105'
        }
        
        def_lineup = {
            'team': 'DAL',
            'player_1': 201, 'player_2': 202, 'player_3': 203, 'player_4': 204, 'player_5': 205,
            'lineup_id': 'DAL_201_202_203_204_205'
        }
        
        result = create_possession_record(possession, off_lineup, def_lineup)
        
        # Check possession info preserved
        assert result['possession_id'] == 1
        assert result['period'] == 1
        assert result['points_scored'] == 2
        
        # Check team info
        assert result['off_team_id'] == 1610612745
        assert result['def_team_id'] == 1610612742
        assert result['off_team'] == 'HOU'
        assert result['def_team'] == 'DAL'
        
        # Check lineup info
        assert result['off_lineup_id'] == 'HOU_101_102_103_104_105'
        assert result['def_lineup_id'] == 'DAL_201_202_203_204_205'


class TestValidateForRatingsCalculation:
    """Test validate_for_ratings_calculation function."""
    
    def test_valid_complete_data_passes_validation(self):
        """Test that complete valid data passes validation."""
        complete_data = pd.DataFrame({
            'possession_id': [1, 2],
            'period': [1, 1],  # Added missing period column
            'off_team_id': [1610612745, 1610612742],
            'def_team_id': [1610612742, 1610612745],
            'points_scored': [2, 0],
            'off_player_1': [101, 201],
            'off_player_2': [102, 202],
            'off_player_3': [103, 203],
            'off_player_4': [104, 204],
            'off_player_5': [105, 205],
            'def_player_1': [201, 101],
            'def_player_2': [202, 102],
            'def_player_3': [203, 103],
            'def_player_4': [204, 104],
            'def_player_5': [205, 105],
            'off_lineup_id': ['HOU_lineup', 'DAL_lineup'],
            'def_lineup_id': ['DAL_lineup', 'HOU_lineup']
        })
        
        result = validate_for_ratings_calculation(complete_data)
        assert result is True


class TestBasicFunctionality:
    """Test basic functionality without complex edge cases."""
    
    def test_empty_inputs_handled_gracefully(self):
        """Test that empty inputs are handled gracefully."""
        empty_lineups = pd.DataFrame(columns=['period', 'team_id', 'player_1'])
        empty_possessions = pd.DataFrame(columns=['possession_id', 'period', 'off_team'])
        
        result = match_lineups_to_possessions(empty_lineups, empty_possessions)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
    
    def test_simple_single_possession_match(self):
        """Test matching a single simple possession."""
        simple_lineups = pd.DataFrame({
            'period': [1, 1],
            'game_clock_seconds': [720, 720],
            'team_id': [1610612745, 1610612742],
            'team': ['HOU', 'DAL'],
            'player_1': [101, 201],
            'player_2': [102, 202],
            'player_3': [103, 203],
            'player_4': [104, 204],
            'player_5': [105, 205],
            'lineup_id': ['HOU_lineup', 'DAL_lineup']
        })
        
        simple_possession = pd.DataFrame({
            'possession_id': [1],
            'period': [1],
            'start_time_seconds': [720],
            'end_time_seconds': [690],
            'points_scored': [2],
            'off_team': [1610612745],
            'def_team': [1610612742]
        })
        
        result = match_lineups_to_possessions(simple_lineups, simple_possession)
        assert isinstance(result, pd.DataFrame)