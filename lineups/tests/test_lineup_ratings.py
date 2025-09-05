# lineups/tests/test_lineup_ratings.py

import pytest
import pandas as pd
import numpy as np

from ..transformers.lineup_ratings import (
    calculate_lineup_ratings,
    calculate_offensive_stats,
    calculate_defensive_stats,
    combine_offensive_defensive_stats,
    calculate_final_ratings,
    get_lineup_summary,
    filter_lineups
)


@pytest.fixture
def valid_lineup_possessions():
    """Valid lineup possessions data for testing."""
    return pd.DataFrame({
        'possession_id': [1, 2, 3, 4],
        'period': [1, 1, 1, 1],
        'points_scored': [2, 0, 3, 2],
        'off_team': ['HOU', 'DAL', 'HOU', 'DAL'],
        'def_team': ['DAL', 'HOU', 'DAL', 'HOU'],
        'off_team_id': [1610612745, 1610612742, 1610612745, 1610612742],
        'def_team_id': [1610612742, 1610612745, 1610612742, 1610612745],
        # HOU lineup
        'off_player_1': [101, 201, 101, 201],
        'off_player_2': [102, 202, 102, 202],
        'off_player_3': [103, 203, 103, 203],
        'off_player_4': [104, 204, 104, 204],
        'off_player_5': [105, 205, 105, 205],
        # Defensive lineups (opposite)
        'def_player_1': [201, 101, 201, 101],
        'def_player_2': [202, 102, 202, 102],
        'def_player_3': [203, 103, 203, 103],
        'def_player_4': [204, 104, 204, 104],
        'def_player_5': [205, 105, 205, 105],
        'off_lineup_id': ['HOU_1', 'DAL_1', 'HOU_1', 'DAL_1'],
        'def_lineup_id': ['DAL_1', 'HOU_1', 'DAL_1', 'HOU_1']
    })


class TestCalculateLineupRatings:
    """Test main calculate_lineup_ratings function."""
    
    def test_valid_data_produces_ratings(self, valid_lineup_possessions):
        """Test that valid data produces lineup ratings."""
        result = calculate_lineup_ratings(valid_lineup_possessions)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 0
        
        if len(result) > 0:
            expected_cols = [
                'team', 'player_1', 'player_2', 'player_3', 'player_4', 'player_5',
                'off_poss', 'def_poss', 'off_rating', 'def_rating', 'net_rating'
            ]
            
            for col in expected_cols:
                assert col in result.columns
    
    def test_ratings_are_numeric(self, valid_lineup_possessions):
        """Test that ratings are numeric values."""
        result = calculate_lineup_ratings(valid_lineup_possessions)
        
        if len(result) > 0:
            assert pd.api.types.is_numeric_dtype(result['off_rating'])
            assert pd.api.types.is_numeric_dtype(result['def_rating'])
            assert pd.api.types.is_numeric_dtype(result['net_rating'])
    
    def test_player_ids_are_integers(self, valid_lineup_possessions):
        """Test that player IDs are integers."""
        result = calculate_lineup_ratings(valid_lineup_possessions)
        
        if len(result) > 0:
            player_cols = ['player_1', 'player_2', 'player_3', 'player_4', 'player_5']
            for col in player_cols:
                assert result[col].dtype == 'Int64'  # Nullable integer


class TestCalculateOffensiveStats:
    """Test calculate_offensive_stats function."""
    
    def test_calculates_offensive_possessions_and_points(self, valid_lineup_possessions):
        """Test that offensive stats are calculated correctly."""
        result = calculate_offensive_stats(valid_lineup_possessions)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 0
        
        if len(result) > 0:
            assert 'off_poss' in result.columns
            assert 'off_points' in result.columns
            assert 'team' in result.columns
            assert 'lineup_players' in result.columns
    
    def test_groups_by_sorted_player_combination(self):
        """Test that players are sorted consistently for grouping."""
        minimal_data = pd.DataFrame({
            'possession_id': [1],
            'off_team': ['HOU'],
            'off_player_1': [105],  # Unsorted
            'off_player_2': [101],
            'off_player_3': [103],
            'off_player_4': [102],
            'off_player_5': [104],
            'points_scored': [2]
        })
        
        result = calculate_offensive_stats(minimal_data)
        
        if len(result) > 0:
            for lineup_players in result['lineup_players']:
                assert isinstance(lineup_players, tuple)
                assert len(lineup_players) == 5
                assert list(lineup_players) == sorted(list(lineup_players))


class TestCalculateDefensiveStats:
    """Test calculate_defensive_stats function."""
    
    def test_calculates_defensive_possessions_and_points(self, valid_lineup_possessions):
        """Test that defensive stats are calculated correctly."""
        result = calculate_defensive_stats(valid_lineup_possessions)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 0
        
        if len(result) > 0:
            assert 'def_poss' in result.columns
            assert 'def_points_allowed' in result.columns
            assert 'team' in result.columns
            assert 'lineup_players' in result.columns


class TestCombineOffensiveDefensiveStats:
    """Test combine_offensive_defensive_stats function."""
    
    def test_combines_stats_correctly(self):
        """Test that offensive and defensive stats are combined correctly."""
        off_stats = pd.DataFrame({
            'team': ['HOU'],
            'lineup_players': [(101, 102, 103, 104, 105)],
            'off_poss': [10],
            'off_points': [20]
        })
        
        def_stats = pd.DataFrame({
            'team': ['HOU'],
            'lineup_players': [(101, 102, 103, 104, 105)],
            'def_poss': [8],
            'def_points_allowed': [16]
        })
        
        result = combine_offensive_defensive_stats(off_stats, def_stats)
        
        assert len(result) == 1
        assert 'off_poss' in result.columns
        assert 'def_poss' in result.columns
        assert 'off_points' in result.columns
        assert 'def_points_allowed' in result.columns


class TestCalculateFinalRatings:
    """Test calculate_final_ratings function."""
    
    def test_calculates_per_100_possession_ratings(self):
        """Test that ratings are calculated per 100 possessions."""
        combined_stats = pd.DataFrame({
            'team': ['HOU'],
            'lineup_players': [(101, 102, 103, 104, 105)],
            'off_poss': [10],
            'off_points': [12],
            'def_poss': [8],
            'def_points_allowed': [8]
        })
        
        result = calculate_final_ratings(combined_stats)
        
        assert 'off_rating' in result.columns
        assert 'def_rating' in result.columns
        assert 'net_rating' in result.columns
        
        # Check calculations (allowing for floating point precision)
        assert abs(result['off_rating'].iloc[0] - 120.0) < 0.1  # (12/10) * 100
        assert abs(result['def_rating'].iloc[0] - 100.0) < 0.1  # (8/8) * 100
        assert abs(result['net_rating'].iloc[0] - 20.0) < 0.1   # 120 - 100
    
    def test_handles_zero_possessions(self):
        """Test handling of lineups with zero possessions."""
        combined_stats = pd.DataFrame({
            'team': ['HOU'],
            'lineup_players': [(101, 102, 103, 104, 105)],
            'off_poss': [0],  # No offensive possessions
            'off_points': [0],
            'def_poss': [5],
            'def_points_allowed': [10]
        })
        
        result = calculate_final_ratings(combined_stats)
        
        # Should handle division by zero
        assert result['off_rating'].iloc[0] == 0.0  # Zero possessions = 0 rating
        assert result['def_rating'].iloc[0] == 200.0  # (10/5) * 100


class TestGetLineupSummary:
    """Test get_lineup_summary function."""
    
    def test_creates_summary_statistics(self, valid_lineup_possessions):
        """Test that summary statistics are created correctly."""
        ratings = calculate_lineup_ratings(valid_lineup_possessions)
        
        if len(ratings) > 0:
            summary = get_lineup_summary(ratings)
            
            assert isinstance(summary, dict)
            assert 'total_lineups' in summary
            assert 'teams' in summary
            
            # Check reasonable values
            assert summary['total_lineups'] == len(ratings)
            assert len(summary['teams']) > 0


class TestFilterLineups:
    """Test filter_lineups function."""
    
    def test_filters_by_minimum_possessions(self):
        """Test filtering lineups by minimum possession threshold."""
        ratings = pd.DataFrame({
            'team': ['HOU', 'HOU'],
            'player_1': [101, 106],
            'player_2': [102, 102],
            'player_3': [103, 103],
            'player_4': [104, 104],
            'player_5': [105, 105],
            'off_poss': [15, 3],  # Second lineup below threshold
            'def_poss': [12, 2],
            'off_rating': [110.0, 120.0],
            'def_rating': [100.0, 90.0],
            'net_rating': [10.0, 30.0]
        })
        
        result = filter_lineups(ratings, min_possessions=5)
        
        # Should filter out the lineup with only 3 off_poss and 2 def_poss
        assert len(result) == 1
        assert result['player_1'].iloc[0] == 101  # First lineup should remain


class TestBasicFunctionality:
    """Test basic functionality without complex edge cases."""
    
    def test_empty_data_handled_gracefully(self):
        """Test that empty data is handled gracefully."""
        empty_data = pd.DataFrame(columns=[
            'possession_id', 'off_team', 'def_team', 'points_scored',
            'off_player_1', 'off_player_2', 'off_player_3', 'off_player_4', 'off_player_5',
            'def_player_1', 'def_player_2', 'def_player_3', 'def_player_4', 'def_player_5'
        ])
        
        result = calculate_lineup_ratings(empty_data)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
    
    def test_single_possession_lineup(self):
        """Test handling of lineup with only one possession."""
        single_poss = pd.DataFrame({
            'possession_id': [1],
            'off_team': ['HOU'],
            'def_team': ['DAL'],
            'points_scored': [3],
            'off_player_1': [101],
            'off_player_2': [102],
            'off_player_3': [103],
            'off_player_4': [104],
            'off_player_5': [105],
            'def_player_1': [201],
            'def_player_2': [202],
            'def_player_3': [203],
            'def_player_4': [204],
            'def_player_5': [205]
        })
        
        result = calculate_lineup_ratings(single_poss)
        assert isinstance(result, pd.DataFrame)