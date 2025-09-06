# tests/test_shot_distance.py

import pytest
import pandas as pd
import numpy as np

from ..transformers.shot_distance import calculate_shot_distances, _is_shot_attempt, _calculate_distance_from_basket


@pytest.fixture
def valid_pbp_data():
    """Valid PBP data with shot attempts."""
    return pd.DataFrame({
        'msgType': [1, 2, 1, 4, 5],  # Made shot, missed shot, made shot, rebound, turnover
        'locX': [0, 50, 100, 0, 0],  # Shot locations in tenths of feet
        'locY': [0, 30, 200, 0, 0],
        'description': ['Made shot', 'Missed 3PT', 'Made layup', 'Rebound', 'Turnover'],
        'period': [1, 1, 1, 1, 1],
        'wallClockInt': [1000, 1100, 1200, 1300, 1400]
    })


class TestCalculateShotDistances:
    """Test main calculate_shot_distances function."""
    
    def test_adds_required_columns(self, valid_pbp_data):
        """Test that shot_distance and is_rim_shot columns are added."""
        result = calculate_shot_distances(valid_pbp_data)
        
        assert 'shot_distance' in result.columns
        assert 'is_rim_shot' in result.columns
        assert len(result) == len(valid_pbp_data)
    
    def test_only_shots_have_distances(self, valid_pbp_data):
        """Test that only shot attempts get distance calculations."""
        result = calculate_shot_distances(valid_pbp_data)
        
        # Shots should have positive distances
        shot_mask = result['msgType'].isin([1, 2])
        shot_distances = result.loc[shot_mask, 'shot_distance']
        assert all(shot_distances >= 0)
        
        # Non-shots should have -1
        non_shot_mask = ~result['msgType'].isin([1, 2])
        non_shot_distances = result.loc[non_shot_mask, 'shot_distance']
        assert all(non_shot_distances == -1)
    
    def test_rim_shots_identified_correctly(self, valid_pbp_data):
        """Test that rim shots (≤4 feet) are identified correctly."""
        result = calculate_shot_distances(valid_pbp_data)
        
        # Shot at (0,0) should be rim shot (0 feet)
        rim_shot_mask = (result['locX'] == 0) & (result['locY'] == 0) & (result['msgType'].isin([1, 2]))
        assert result.loc[rim_shot_mask, 'is_rim_shot'].iloc[0] == True
        assert result.loc[rim_shot_mask, 'shot_distance'].iloc[0] == 0.0


class TestIsShotAttempt:
    """Test _is_shot_attempt helper function."""
    
    def test_identifies_shot_attempts(self, valid_pbp_data):
        """Test that shot attempts are correctly identified."""
        result = _is_shot_attempt(valid_pbp_data)
        
        expected_shots = valid_pbp_data['msgType'].isin([1, 2])
        pd.testing.assert_series_equal(result, expected_shots)


class TestCalculateDistanceFromBasket:
    """Test _calculate_distance_from_basket helper function."""
    
    def test_distance_calculations(self):
        """Test distance calculations with known coordinates."""
        # Test coordinates (in tenths of feet)
        loc_x = np.array([0, 30, 40])  # 0, 3, 4 feet from basket
        loc_y = np.array([0, 40, 30])  # 0, 4, 3 feet from basket
        
        distances = _calculate_distance_from_basket(loc_x, loc_y)
        
        # Expected distances: 0, 5, 5 feet (Pythagorean theorem)
        expected = np.array([0.0, 5.0, 5.0])
        np.testing.assert_array_almost_equal(distances, expected, decimal=1)
    
    def test_basket_at_origin(self):
        """Test that basket is correctly positioned at origin."""
        loc_x = np.array([0])
        loc_y = np.array([0])
        
        distance = _calculate_distance_from_basket(loc_x, loc_y)
        
        assert distance[0] == 0.0


class TestBasicFunctionality:
    """Test basic functionality and edge cases."""
    
    def test_empty_dataframe(self):
        """Test handling of empty dataframe."""
        empty_df = pd.DataFrame(columns=['msgType', 'locX', 'locY'])
        
        result = calculate_shot_distances(empty_df)
        
        assert 'shot_distance' in result.columns
        assert 'is_rim_shot' in result.columns
        assert len(result) == 0
    
    def test_no_shots_in_data(self):
        """Test handling when no shot attempts exist."""
        no_shots_df = pd.DataFrame({
            'msgType': [4, 5, 6],  # Rebound, turnover, foul
            'locX': [0, 0, 0],
            'locY': [0, 0, 0]
        })
        
        result = calculate_shot_distances(no_shots_df)
        
        assert all(result['shot_distance'] == -1)
        assert all(result['is_rim_shot'] == False)
    
    def test_missing_coordinates(self):
        """Test handling of missing coordinate data."""
        missing_coords_df = pd.DataFrame({
            'msgType': [1, 2],
            'locX': [np.nan, 50],
            'locY': [30, np.nan]
        })
        
        result = calculate_shot_distances(missing_coords_df)
        
        # Should handle NaN coordinates gracefully
        assert 'shot_distance' in result.columns
        assert 'is_rim_shot' in result.columns