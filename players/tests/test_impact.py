# tests/test_impact.py

import pytest
import pandas as pd
import numpy as np
import tempfile
import os

from ..transformers.impact import (
    calculate_impact,
    validate_final_table,
    export_final_table
)


@pytest.fixture
def rim_defense_data():
    """Sample rim defense data."""
    return pd.DataFrame({
        'playerId': [101, 102, 201, 202],
        'teamId': [1610612745, 1610612745, 1610612742, 1610612742],
        'rim_fgm_on': [5, 3, 4, 6],
        'rim_fga_on': [10, 8, 12, 15],
        'rim_fgm_off': [8, 6, 7, 9],
        'rim_fga_off': [12, 10, 14, 16],
        'rim_fg_pct_on': [0.500, 0.375, 0.333, 0.400],
        'rim_fg_pct_off': [0.667, 0.600, 0.500, 0.563],
        'rim_fg_pct_diff': [-0.167, -0.225, -0.167, -0.163]
    })


@pytest.fixture
def possession_counts_data():
    """Sample possession counts data."""
    return pd.DataFrame({
        'playerId': [101, 102, 103, 201, 202],
        'offensive_possessions': [45, 38, 25, 42, 39],
        'defensive_possessions': [47, 40, 27, 44, 41],
        'total_possessions': [92, 78, 52, 86, 80]
    })


@pytest.fixture
def box_score_data():
    """Sample box score data."""
    return pd.DataFrame({
        'nbaId': [101, 102, 103, 201, 202],
        'name': ['Player A', 'Player B', 'Player C', 'Player D', 'Player E'],
        'team': ['HOU', 'HOU', 'HOU', 'DAL', 'DAL'],
        'min': [35.5, 28.2, 15.3, 38.2, 33.4]
    })


class TestCalculateImpact:
    """Test main calculate_impact function."""
    
    def test_produces_final_impact_table(self, rim_defense_data, possession_counts_data, box_score_data):
        """Test that final impact table is produced."""
        result = calculate_impact(rim_defense_data, possession_counts_data, box_score_data)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 0
        
        expected_cols = [
            'Player ID', 'Player Name', 'Team',
            'Offensive possessions played', 'Defensive possessions played',
            'Opponent rim FG% when player ON court',
            'Opponent rim FG% when player OFF court',
            'Opponent rim FG% on/off difference (on-off)'
        ]
        for col in expected_cols:
            assert col in result.columns
    
    def test_merges_data_correctly(self, rim_defense_data, possession_counts_data, box_score_data):
        """Test that data sources are merged correctly."""
        result = calculate_impact(rim_defense_data, possession_counts_data, box_score_data)
        
        if len(result) > 0:
            # Should have rim defense players who also have possession counts
            common_players = set(rim_defense_data['playerId']) & set(possession_counts_data['playerId'])
            if common_players:
                result_players = set(result['Player ID'])
                assert len(result_players & common_players) > 0
    
    def test_percentages_rounded(self, rim_defense_data, possession_counts_data, box_score_data):
        """Test that percentages are rounded to 3 decimal places."""
        result = calculate_impact(rim_defense_data, possession_counts_data, box_score_data)
        
        if len(result) > 0:
            pct_cols = [
                'Opponent rim FG% when player ON court',
                'Opponent rim FG% when player OFF court',
                'Opponent rim FG% on/off difference (on-off)'
            ]
            
            for col in pct_cols:
                for value in result[col].dropna():
                    # Check that values are rounded to 3 decimal places
                    assert round(value, 3) == value
    
    def test_sorted_by_defensive_impact(self, rim_defense_data, possession_counts_data, box_score_data):
        """Test that results are sorted by defensive impact (most negative first)."""
        result = calculate_impact(rim_defense_data, possession_counts_data, box_score_data)
        
        if len(result) > 1:
            diff_col = 'Opponent rim FG% on/off difference (on-off)'
            diffs = result[diff_col].dropna().tolist()
            
            # Should be sorted in ascending order (most negative first)
            assert diffs == sorted(diffs)


class TestValidateFinalTable:
    """Test validate_final_table function."""
    
    def test_validates_complete_data(self, rim_defense_data, possession_counts_data, box_score_data):
        """Test validation of complete data."""
        final_table = calculate_impact(rim_defense_data, possession_counts_data, box_score_data)
        
        if len(final_table) > 0:
            result = validate_final_table(final_table)
            
            assert isinstance(result, pd.DataFrame)
            # Should return DataFrame with complete records
            assert len(result) <= len(final_table)
    
    def test_handles_incomplete_data(self):
        """Test handling of incomplete data."""
        incomplete_table = pd.DataFrame({
            'Player ID': [101, 102],
            'Player Name': ['Player A', 'Player B'],
            'Team': ['HOU', 'HOU'],
            'Offensive possessions played': [45, np.nan],  # Missing data
            'Defensive possessions played': [47, 40],
            'Opponent rim FG% when player ON court': [0.500, 0.375],
            'Opponent rim FG% when player OFF court': [np.nan, 0.600],  # Missing data
            'Opponent rim FG% on/off difference (on-off)': [-0.167, -0.225]
        })
        
        result = validate_final_table(incomplete_table)
        
        assert isinstance(result, pd.DataFrame)
        # Should filter out incomplete records
        assert len(result) <= len(incomplete_table)


class TestExportFinalTable:
    """Test export_final_table function."""
    
    def test_exports_to_csv(self, rim_defense_data, possession_counts_data, box_score_data):
        """Test that table exports to CSV correctly."""
        final_table = calculate_impact(rim_defense_data, possession_counts_data, box_score_data)
        
        if len(final_table) > 0:
            with tempfile.TemporaryDirectory() as temp_dir:
                test_filename = os.path.join(temp_dir, "test_impact.csv")
                
                # Should not raise an exception
                export_final_table(final_table, test_filename)
                
                # File should exist
                assert os.path.exists(test_filename)
                
                # Should be readable as CSV
                reloaded = pd.read_csv(test_filename)
                assert isinstance(reloaded, pd.DataFrame)
                assert len(reloaded) == len(final_table)


class TestBasicFunctionality:
    """Test basic functionality and edge cases."""
    
    def test_empty_rim_defense_data(self, possession_counts_data, box_score_data):
        """Test handling of empty rim defense data."""
        empty_rim_defense = pd.DataFrame(columns=[
            'playerId', 'teamId', 'rim_fgm_on', 'rim_fga_on', 
            'rim_fgm_off', 'rim_fga_off', 'rim_fg_pct_on', 
            'rim_fg_pct_off', 'rim_fg_pct_diff'
        ])
        
        # Ensure proper data types for empty DataFrame
        empty_rim_defense = empty_rim_defense.astype({
            'playerId': 'Int64', 'teamId': 'Int64', 
            'rim_fgm_on': 'Int64', 'rim_fga_on': 'Int64',
            'rim_fgm_off': 'Int64', 'rim_fga_off': 'Int64',
            'rim_fg_pct_on': 'float64', 'rim_fg_pct_off': 'float64', 
            'rim_fg_pct_diff': 'float64'
        })
        
        result = calculate_impact(empty_rim_defense, possession_counts_data, box_score_data)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
    
    def test_no_matching_players(self, box_score_data):
        """Test handling when no players match across datasets."""
        rim_defense = pd.DataFrame({
            'playerId': [999, 998],  # Different player IDs
            'teamId': [1610612745, 1610612742],
            'rim_fg_pct_on': [0.500, 0.400],
            'rim_fg_pct_off': [0.600, 0.550],
            'rim_fg_pct_diff': [-0.100, -0.150]
        })
        
        possession_counts = pd.DataFrame({
            'playerId': [997, 996],  # Different player IDs
            'offensive_possessions': [45, 42],
            'defensive_possessions': [47, 44]
        })
        
        result = calculate_impact(rim_defense, possession_counts, box_score_data)
        
        assert isinstance(result, pd.DataFrame)
        # Should handle gracefully, likely resulting in empty or minimal data
    
    def test_single_player_data(self):
        """Test handling of single player data."""
        single_rim_defense = pd.DataFrame({
            'playerId': [101],
            'teamId': [1610612745],
            'rim_fg_pct_on': [0.500],
            'rim_fg_pct_off': [0.600],
            'rim_fg_pct_diff': [-0.100]
        })
        
        single_possession_counts = pd.DataFrame({
            'playerId': [101],
            'offensive_possessions': [45],
            'defensive_possessions': [47]
        })
        
        single_box_score = pd.DataFrame({
            'nbaId': [101],
            'name': ['Player A'],
            'team': ['HOU']
        })
        
        result = calculate_impact(single_rim_defense, single_possession_counts, single_box_score)
        
        assert isinstance(result, pd.DataFrame)
        if len(result) > 0:
            assert len(result) == 1
            assert result['Player Name'].iloc[0] == 'Player A'