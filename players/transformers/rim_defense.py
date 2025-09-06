# rim_defense.py

"""
Track rim defense statistics for each player (on court vs off court performance).
"""

import pandas as pd
from typing import Dict, List, Set


def track_rim_defense(enhanced_pbp_df: pd.DataFrame, 
                     lineup_intervals: pd.DataFrame) -> pd.DataFrame:
    """
    Track rim defense statistics for each player.
    
    Args:
        enhanced_pbp_df: PBP data with shot_distance and is_rim_shot columns
        lineup_intervals: Player court time intervals from lineup tracker
        
    Returns:
        DataFrame with columns: [playerId, teamId, rim_fgm_on, rim_fga_on, rim_fgm_off, rim_fga_off]
    """
    # Filter for rim shots only
    rim_shots = enhanced_pbp_df[enhanced_pbp_df['is_rim_shot'] == True].copy()
    
    print(f"RIM DEFENSE DEBUG: Processing {len(rim_shots)} rim shots")
    
    # Get team assignments for all players
    player_teams = _get_player_team_mapping(lineup_intervals)
    
    # Track rim defense stats for each player
    rim_defense_stats = _calculate_rim_defense_stats(rim_shots, lineup_intervals, player_teams)
    
    return rim_defense_stats


def _get_player_team_mapping(lineup_intervals: pd.DataFrame) -> Dict[int, int]:
    """Create mapping of player ID to team ID from lineup intervals."""
    player_teams = {}
    for _, interval in lineup_intervals.iterrows():
        player_teams[interval['playerId']] = interval['teamId']
    
    return player_teams


def _calculate_rim_defense_stats(rim_shots: pd.DataFrame, 
                                lineup_intervals: pd.DataFrame,
                                player_teams: Dict[int, int]) -> pd.DataFrame:
    """Calculate rim defense statistics for each player."""
    
    # Initialize stats tracking
    player_stats = {}  # player_id -> {'on': {'makes': 0, 'attempts': 0}, 'off': {'makes': 0, 'attempts': 0}}
    
    print(f"RIM DEFENSE DEBUG: Calculating stats for {len(player_teams)} players")
    
    for _, shot in rim_shots.iterrows():
        # Determine shot details
        shot_made = (shot['msgType'] == 1)  # msgType 1 = made shot
        shot_period = shot['period']
        shot_wallClock = shot['wallClockInt']
        
        # Determine defending team (opposite of offensive team)
        offensive_team = shot['offTeamId']
        defensive_team = shot['defTeamId']
        
        if pd.isna(offensive_team) or pd.isna(defensive_team):
            continue
            
        offensive_team = int(offensive_team)
        defensive_team = int(defensive_team)
        
        # Find players on court during this shot
        players_on_court = lineup_intervals[
            (lineup_intervals['period_start'] <= shot_period) &
            (lineup_intervals['period_end'] >= shot_period) &
            (lineup_intervals['wallClock_start'] <= shot_wallClock) &
            (lineup_intervals['wallClock_end'] >= shot_wallClock)
        ]
        
        # Get defending players (players on court for defensive team)
        defending_players = players_on_court[
            players_on_court['teamId'] == defensive_team
        ]['playerId'].tolist()
        
        # Track stats for all defensive team players
        defensive_team_players = [pid for pid, tid in player_teams.items() if tid == defensive_team]
        
        for player_id in defensive_team_players:
            # Initialize player stats if not seen before
            if player_id not in player_stats:
                player_stats[player_id] = {
                    'on': {'makes': 0, 'attempts': 0},
                    'off': {'makes': 0, 'attempts': 0},
                    'teamId': defensive_team
                }
            
            # Determine if player was on court or off court
            if player_id in defending_players:
                # Player was on court defending
                player_stats[player_id]['on']['attempts'] += 1
                if shot_made:
                    player_stats[player_id]['on']['makes'] += 1
            else:
                # Player was off court
                player_stats[player_id]['off']['attempts'] += 1
                if shot_made:
                    player_stats[player_id]['off']['makes'] += 1
    
    # Convert to DataFrame
    result_data = []
    for player_id, stats in player_stats.items():
        result_data.append({
            'playerId': player_id,
            'teamId': stats['teamId'],
            'rim_fgm_on': stats['on']['makes'],
            'rim_fga_on': stats['on']['attempts'],
            'rim_fgm_off': stats['off']['makes'],
            'rim_fga_off': stats['off']['attempts']
        })
    
    result_df = pd.DataFrame(result_data)
    
    # Calculate rim FG% on and off court
    result_df['rim_fg_pct_on'] = result_df['rim_fgm_on'] / result_df['rim_fga_on'].replace(0, 1)
    result_df['rim_fg_pct_off'] = result_df['rim_fgm_off'] / result_df['rim_fga_off'].replace(0, 1)
    result_df['rim_fg_pct_diff'] = result_df['rim_fg_pct_on'] - result_df['rim_fg_pct_off']
    
    # Handle cases where players have 0 attempts
    result_df['rim_fg_pct_on'] = result_df['rim_fg_pct_on'].where(result_df['rim_fga_on'] > 0, None)
    result_df['rim_fg_pct_off'] = result_df['rim_fg_pct_off'].where(result_df['rim_fga_off'] > 0, None)
    
    print(f"RIM DEFENSE DEBUG: Calculated rim defense stats for {len(result_df)} players")
    
    return result_df


def validate_rim_defense_stats(rim_defense_df: pd.DataFrame, 
                              enhanced_pbp_df: pd.DataFrame,
                              box_score_df: pd.DataFrame):
    """Validate rim defense statistics."""
    
    # Merge with player names for readability
    validation = rim_defense_df.merge(
        box_score_df[['nbaId', 'name', 'nbaTeamId']], 
        left_on='playerId', 
        right_on='nbaId',
        how='inner'
    )
    
    print(f"\n=== RIM DEFENSE VALIDATION ===")
    
    # Check total rim shot attempts
    total_rim_shots = len(enhanced_pbp_df[enhanced_pbp_df['is_rim_shot'] == True])
    total_on_attempts = rim_defense_df['rim_fga_on'].sum()
    total_off_attempts = rim_defense_df['rim_fga_off'].sum()
    
    print(f"Total rim shots in PBP: {total_rim_shots}")
    print(f"Total on-court attempts tracked: {total_on_attempts}")
    print(f"Total off-court attempts tracked: {total_off_attempts}")
    print(f"Expected ratio: ~5:1 (5 defenders on court vs rest off court)")
    
    # Show rim defense leaders (best on-court defensive performance)
    print(f"\n=== TOP RIM DEFENDERS (Lowest FG% allowed on court) ===")
    qualified_defenders = validation[
        (validation['rim_fga_on'] >= 10) &  # Minimum attempts
        (validation['rim_fg_pct_on'].notna())
    ].copy()
    
    if len(qualified_defenders) > 0:
        top_defenders = qualified_defenders.nsmallest(10, 'rim_fg_pct_on')
        for _, defender in top_defenders.iterrows():
            print(f"  {defender['name']}: {defender['rim_fg_pct_on']:.3f} FG% allowed ({defender['rim_fgm_on']}/{defender['rim_fga_on']})")
    
    # Show biggest on/off impact (negative diff = better when on court)
    print(f"\n=== BIGGEST DEFENSIVE IMPACT (On/Off Difference) ===")
    qualified_impact = validation[
        (validation['rim_fga_on'] >= 5) & 
        (validation['rim_fga_off'] >= 5) &
        (validation['rim_fg_pct_diff'].notna())
    ].copy()
    
    if len(qualified_impact) > 0:
        biggest_impact = qualified_impact.nsmallest(10, 'rim_fg_pct_diff')  # Most negative = best impact
        for _, player in biggest_impact.iterrows():
            on_pct = player['rim_fg_pct_on']
            off_pct = player['rim_fg_pct_off']
            diff = player['rim_fg_pct_diff']
            print(f"  {player['name']}: {on_pct:.3f} on vs {off_pct:.3f} off (diff: {diff:+.3f})")
    
    # Team-level validation
    print(f"\n=== TEAM TOTALS ===")
    team_totals = validation.groupby('nbaTeamId').agg({
        'rim_fga_on': 'sum',
        'rim_fga_off': 'sum',
        'rim_fgm_on': 'sum',
        'rim_fgm_off': 'sum'
    }).reset_index()
    
    for _, team in team_totals.iterrows():
        team_fg_pct_on = team['rim_fgm_on'] / team['rim_fga_on'] if team['rim_fga_on'] > 0 else 0
        team_fg_pct_off = team['rim_fgm_off'] / team['rim_fga_off'] if team['rim_fga_off'] > 0 else 0
        print(f"  Team {team['nbaTeamId']}: {team_fg_pct_on:.3f} FG% on court, {team_fg_pct_off:.3f} FG% off court")
    
    return validation


if __name__ == "__main__":
    # Load test data
    box_score_df = pd.read_csv("../../data/box_HOU-DAL.csv")
    pbp_df = pd.read_csv("../../data/pbp_HOU-DAL.csv")
    
    print("Tracking rim defense...")
    
    # Get required inputs from previous modules
    from shot_distance import calculate_shot_distances
    from court_time import track_lineup_states
    
    # Calculate shot distances (enhanced PBP)
    enhanced_pbp = calculate_shot_distances(pbp_df)
    
    # Get lineup intervals
    lineup_intervals = track_lineup_states(box_score_df, pbp_df)
    
    # Track rim defense
    rim_defense_stats = track_rim_defense(enhanced_pbp, lineup_intervals)
    
    print(f"\nGenerated rim defense stats for {len(rim_defense_stats)} players")
    
    # Validate results
    validation_results = validate_rim_defense_stats(rim_defense_stats, enhanced_pbp, box_score_df)
    
    # Show sample results
    print(f"\nSample rim defense stats:")
    print(rim_defense_stats[['playerId', 'rim_fga_on', 'rim_fga_off', 'rim_fg_pct_on', 'rim_fg_pct_off', 'rim_fg_pct_diff']].head(10).to_string(index=False))