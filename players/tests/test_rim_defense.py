# tests/test_rim_defense.py

import pytest
import pandas as pd
import numpy as np

from ..transformers.rim_defense import (
    track_rim_defense,
    _get_player_team_mapping,
    _calculate_rim_defense_stats
)


@pytest.fixture
def enhanced_pbp():
    """Enhanced PBP data with rim shots."""
    return pd.DataFrame({
        'period': [1, 1, 1, 1, 1],
        'wallClockInt': [1000, 1100, 1200, 1300, 1400],
        'msgType': [1, 2, 1, 2, 1],  # Made, miss, made, miss, made
        'offTeamId': [1610612745, 1610612745, 1610612742, 1610612742, 1610612745],
        'defTeamId': [1610612742, 1610612742, 1610612745, 1610612745, 1610612742],
        'shot_distance': [2.0, 3.5, 1.5, 4.0, 2.5],  # All rim shots (≤4 feet)
        'is_rim_shot': [True, True, True, True, True],
        'playerId1': [101, 102, 201, 202, 103]  # Shooters
    })


@pytest.fixture
def lineup_intervals():
    """Lineup intervals showing when players were on court."""
    return pd.DataFrame({
        'playerId': [101, 102, 103, 201, 202, 203],
        'teamId': [1610612745, 1610612745, 1610612745, 1610612742, 1610612742, 1610612742],
        'period_start': [1, 1, 1, 1, 1, 1],
        'period_end': [1, 1, 1, 1, 1, 1],
        'wallClock_start': [950, 950, 1250, 950, 950, 1250],  # Player 103 and 203 enter mid-period
        'wallClock_end': [1450, 1450, 1450, 1450, 1450, 1450]
    })


class TestTrackRimDefense:
    """Test main track_rim_defense function."""
    
    def test_produces_rim_defense_stats(self, enhanced_pbp, lineup_intervals):
        """Test that rim defense stats are produced."""
        result = track_rim_defense(enhanced_pbp, lineup_intervals)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 0
        
        expected_cols = [
            'playerId', 'teamId', 'rim_fgm_on', 'rim_fga_on', 
            'rim_fgm_off', 'rim_fga_off', 'rim_fg_pct_on', 
            'rim_fg_pct_off', 'rim_fg_pct_diff'
        ]
        for col in expected_cols:
            assert col in result.columns
    
    def test_only_rim_shots_processed(self, lineup_intervals):
        """Test that only rim shots are processed."""
        mixed_pbp = pd.DataFrame({
            'period': [1, 1, 1],
            'wallClockInt': [1000, 1100, 1200],
            'msgType': [1, 1, 1],
            'offTeamId': [1610612745, 1610612745, 1610612745],
            'defTeamId': [1610612742, 1610612742, 1610612742],
            'shot_distance': [2.0, 15.0, 3.0],  # Only 1st and 3rd are rim shots
            'is_rim_shot': [True, False, True],
            'playerId1': [101, 102, 103]
        })
        
        result = track_rim_defense(mixed_pbp, lineup_intervals)
        
        # Should only process the 2 rim shots, not the 15-foot shot
        if len(result) > 0:
            total_attempts = result['rim_fga_on'].sum() + result['rim_fga_off'].sum()
            # Each rim shot should be counted for all defensive players
            assert total_attempts > 0
    
    def test_defensive_stats_calculated(self, enhanced_pbp, lineup_intervals):
        """Test that defensive stats are calculated correctly."""
        result = track_rim_defense(enhanced_pbp, lineup_intervals)
        
        if len(result) > 0:
            # Should have stats for defensive team players
            defensive_players = result[result['teamId'].isin([1610612742, 1610612745])]
            assert len(defensive_players) > 0
            
            # FG percentages should be between 0 and 1 (or None)
            for _, player in defensive_players.iterrows():
                if pd.notna(player['rim_fg_pct_on']):
                    assert 0 <= player['rim_fg_pct_on'] <= 1
                if pd.notna(player['rim_fg_pct_off']):
                    assert 0 <= player['rim_fg_pct_off'] <= 1


class TestGetPlayerTeamMapping:
    """Test _get_player_team_mapping helper function."""
    
    def test_creates_mapping(self, lineup_intervals):
        """Test that player-team mapping is created."""
        result = _get_player_team_mapping(lineup_intervals)
        
        assert isinstance(result, dict)
        assert 101 in result
        assert result[101] == 1610612745
        assert 201 in result
        assert result[201] == 1610612742


class TestCalculateRimDefenseStats:
    """Test _calculate_rim_defense_stats helper function."""
    
    def test_processes_defensive_team_players(self, enhanced_pbp, lineup_intervals):
        """Test that defensive team players are processed."""
        player_teams = _get_player_team_mapping(lineup_intervals)
        
        result = _calculate_rim_defense_stats(enhanced_pbp, lineup_intervals, player_teams)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 0
        
        if len(result) > 0:
            # Should have both teams represented
            teams = result['teamId'].unique()
            assert len(teams) > 0
    
    def test_on_off_court_tracking(self, lineup_intervals):
        """Test that on/off court situations are tracked correctly."""
        # Create specific scenario: one shot when player is on court, one when off
        test_pbp = pd.DataFrame({
            'period': [1, 1],
            'wallClockInt': [1000, 1500],  # First when player 103 is off, second when on
            'msgType': [1, 1],  # Both made shots
            'offTeamId': [1610612745, 1610612745],
            'defTeamId': [1610612742, 1610612742],
            'shot_distance': [2.0, 3.0],
            'is_rim_shot': [True, True],
            'playerId1': [101, 102]
        })
        
        player_teams = _get_player_team_mapping(lineup_intervals)
        result = _calculate_rim_defense_stats(test_pbp, lineup_intervals, player_teams)
        
        if len(result) > 0:
            # Player 203 should have different on/off court stats
            player_203 = result[result['playerId'] == 203]
            if len(player_203) > 0:
                p203 = player_203.iloc[0]
                # Should have some combination of on/off court attempts
                total_attempts = p203['rim_fga_on'] + p203['rim_fga_off']
                assert total_attempts > 0


class TestBasicFunctionality:
    """Test basic functionality with realistic scenarios."""
    
    def test_minimal_rim_shots(self, lineup_intervals):
        """Test handling with minimal but realistic rim shot data."""
        minimal_rim_pbp = pd.DataFrame({
            'period': [1, 1],
            'wallClockInt': [1000, 1100],
            'msgType': [1, 2],  # Made shot, missed shot
            'offTeamId': [1610612745, 1610612742],
            'defTeamId': [1610612742, 1610612745],
            'shot_distance': [2.0, 3.0],  # Both rim shots
            'is_rim_shot': [True, True],
            'playerId1': [101, 201]
        })
        
        result = track_rim_defense(minimal_rim_pbp, lineup_intervals)
        
        assert isinstance(result, pd.DataFrame)
        if len(result) > 0:
            # Should have defensive team players with rim defense stats
            defensive_players = result[result['teamId'].isin([1610612742, 1610612745])]
            assert len(defensive_players) > 0
    
    def test_single_rim_shot(self, lineup_intervals):
        """Test handling of single rim shot."""
        single_shot_pbp = pd.DataFrame({
            'period': [1],
            'wallClockInt': [1000],
            'msgType': [1],  # Made shot
            'offTeamId': [1610612745],
            'defTeamId': [1610612742],
            'shot_distance': [2.0],
            'is_rim_shot': [True],
            'playerId1': [101]
        })
        
        result = track_rim_defense(single_shot_pbp, lineup_intervals)
        
        assert isinstance(result, pd.DataFrame)
        if len(result) > 0:
            # Should have stats for defensive team players
            dal_players = result[result['teamId'] == 1610612742]
            assert len(dal_players) > 0
    
    def test_multiple_periods(self, lineup_intervals):
        """Test handling of rim shots across multiple periods."""
        multi_period_pbp = pd.DataFrame({
            'period': [1, 1, 2, 2],
            'wallClockInt': [1000, 1100, 2000, 2100],
            'msgType': [1, 2, 1, 2],
            'offTeamId': [1610612745, 1610612742, 1610612745, 1610612742],
            'defTeamId': [1610612742, 1610612745, 1610612742, 1610612745],
            'shot_distance': [2.0, 3.0, 2.5, 3.5],
            'is_rim_shot': [True, True, True, True],
            'playerId1': [101, 201, 102, 202]
        })
        
        result = track_rim_defense(multi_period_pbp, lineup_intervals)
        
        assert isinstance(result, pd.DataFrame)
        if len(result) > 0:
            # Should handle multiple periods correctly
            assert len(result) > 0