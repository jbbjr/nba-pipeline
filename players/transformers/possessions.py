# possessions.py

"""
Analyze possessions from play-by-play data and count possessions per player.
Clean modular design - takes lineup intervals as input parameter.
"""

import pandas as pd
from typing import Dict, List, Tuple


def analyze_possessions(box_score_df: pd.DataFrame, 
                      pbp_df: pd.DataFrame, 
                      lineup_intervals: pd.DataFrame) -> pd.DataFrame:
    """
    Analyze possessions and count offensive/defensive possessions per player.
    
    Args:
        box_score_df: Box score data (for team mapping)
        pbp_df: Play-by-play data with possession events
        lineup_intervals: Player court time intervals from lineup tracker
        
    Returns:
        DataFrame with columns: [playerId, offensive_possessions, defensive_possessions]
    """
    # Get team mapping
    team_mapping = _get_team_mapping(box_score_df)
    
    # Identify possession boundaries
    possessions = _identify_possessions(pbp_df)
    
    # Count possessions per player using provided lineup intervals
    player_possessions = _count_player_possessions(possessions, lineup_intervals, team_mapping)
    
    return player_possessions


def _get_team_mapping(box_score_df: pd.DataFrame) -> Dict[int, int]:
    """Map player ID to team ID."""
    return dict(zip(box_score_df['nbaId'], box_score_df['nbaTeamId']))


def _identify_possessions(pbp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Identify possession boundaries from PBP events.
    
    A possession ends when:
    - Made shot (unless followed by offensive rebound)
    - Defensive rebound
    - Turnover
    - Steal
    - Certain fouls
    - End of period
    """
    
    # Sort PBP chronologically
    pbp_sorted = pbp_df.sort_values(['period', 'wallClockInt']).reset_index(drop=True)
    
    possessions = []
    current_possession = None
    
    print(f"POSSESSION DEBUG: Processing {len(pbp_sorted)} PBP events")
    
    for i, (_, event) in enumerate(pbp_sorted.iterrows()):
        
        # Start new possession if needed
        if current_possession is None:
            current_possession = {
                'possession_id': len(possessions) + 1,
                'start_period': event['period'],
                'start_wallClock': event['wallClockInt'],
                'offensive_team': event['offTeamId'] if pd.notna(event['offTeamId']) else None,
                'defensive_team': event['defTeamId'] if pd.notna(event['defTeamId']) else None,
                'events': []
            }
        
        # Add event to current possession
        current_possession['events'].append({
            'period': event['period'],
            'wallClockInt': event['wallClockInt'],
            'msgType': event['msgType'],
            'actionType': event['actionType'],
            'description': event.get('description', ''),
            'playerId1': event.get('playerId1'),
            'playerId2': event.get('playerId2')
        })
        
        # Check if this event ends the possession
        possession_ended = False
        end_reason = None
        
        # Made shot ends possession (offense scores)
        if event['msgType'] == 1:  # Made shot
            # Check if next event is offensive rebound (possession continues)
            next_is_oreb = False
            if i + 1 < len(pbp_sorted):
                next_event = pbp_sorted.iloc[i + 1]
                if (next_event['msgType'] == 4 and  # Rebound
                    next_event['actionType'] == 0):  # Need to check if offensive
                    # This is a simplification - would need more logic to determine offensive vs defensive rebound
                    pass
            
            if not next_is_oreb:
                possession_ended = True
                end_reason = "made_shot"
        
        # Missed shot - check next event for rebound
        elif event['msgType'] == 2:  # Missed shot
            if i + 1 < len(pbp_sorted):
                next_event = pbp_sorted.iloc[i + 1]
                if next_event['msgType'] == 4:  # Rebound
                    # Determine if defensive rebound (possession change)
                    # This is simplified - in reality need to check team of rebounder
                    possession_ended = True
                    end_reason = "defensive_rebound"
        
        # Turnover ends possession
        elif event['msgType'] == 5:  # Turnover
            possession_ended = True
            end_reason = "turnover"
        
        # End of period ends possession
        elif event['msgType'] == 13:  # End period
            possession_ended = True
            end_reason = "end_period"
        
        # Foul can end possession (simplified)
        elif event['msgType'] == 6:  # Foul
            # Some fouls end possessions, others don't - simplified logic
            if 'FLAGRANT' in str(event.get('description', '')).upper():
                possession_ended = True
                end_reason = "foul"
        
        # End current possession if criteria met
        if possession_ended:
            current_possession['end_period'] = event['period']
            current_possession['end_wallClock'] = event['wallClockInt']
            current_possession['end_reason'] = end_reason
            current_possession['event_count'] = len(current_possession['events'])
            
            possessions.append(current_possession)
            current_possession = None
    
    # Handle final possession if game ended without explicit end
    if current_possession is not None:
        final_event = pbp_sorted.iloc[-1]
        current_possession['end_period'] = final_event['period']
        current_possession['end_wallClock'] = final_event['wallClockInt']
        current_possession['end_reason'] = "game_end"
        current_possession['event_count'] = len(current_possession['events'])
        possessions.append(current_possession)
    
    print(f"POSSESSION DEBUG: Identified {len(possessions)} possessions")
    
    return pd.DataFrame(possessions)


def _count_player_possessions(possessions_df: pd.DataFrame, 
                             lineup_intervals: pd.DataFrame,
                             team_mapping: Dict[int, int]) -> pd.DataFrame:
    """Count offensive and defensive possessions for each player."""
    
    player_possession_counts = {}  # player_id -> {'offensive': count, 'defensive': count}
    
    print(f"POSSESSION DEBUG: Counting possessions for {len(possessions_df)} possessions")
    
    for _, possession in possessions_df.iterrows():
        if pd.isna(possession['offensive_team']) or pd.isna(possession['defensive_team']):
            continue
            
        offensive_team = int(possession['offensive_team'])
        defensive_team = int(possession['defensive_team'])
        
        # Find players on court during this possession
        # Use middle of possession for lookup
        mid_period = possession['start_period']
        mid_wallClock = (possession['start_wallClock'] + possession['end_wallClock']) / 2
        
        # Find players on court at this time
        players_on_court = lineup_intervals[
            (lineup_intervals['period_start'] <= mid_period) &
            (lineup_intervals['period_end'] >= mid_period) &
            (lineup_intervals['wallClock_start'] <= mid_wallClock) &
            (lineup_intervals['wallClock_end'] >= mid_wallClock)
        ]
        
        # Count possessions for each player
        for _, player_interval in players_on_court.iterrows():
            player_id = player_interval['playerId']
            player_team = player_interval['teamId']
            
            # Initialize player if not seen before
            if player_id not in player_possession_counts:
                player_possession_counts[player_id] = {'offensive': 0, 'defensive': 0}
            
            # Increment appropriate possession count
            if player_team == offensive_team:
                player_possession_counts[player_id]['offensive'] += 1
            elif player_team == defensive_team:
                player_possession_counts[player_id]['defensive'] += 1
    
    # Convert to DataFrame
    result_data = []
    for player_id, counts in player_possession_counts.items():
        result_data.append({
            'playerId': player_id,
            'offensive_possessions': counts['offensive'],
            'defensive_possessions': counts['defensive'],
            'total_possessions': counts['offensive'] + counts['defensive']
        })
    
    result_df = pd.DataFrame(result_data)
    
    print(f"POSSESSION DEBUG: Calculated possessions for {len(result_df)} players")
    
    return result_df


def validate_possession_counts(possession_counts: pd.DataFrame, box_score_df: pd.DataFrame):
    """Validate possession counts against expected game patterns."""
    
    # Merge with player names for readability
    validation = possession_counts.merge(
        box_score_df[['nbaId', 'name', 'min', 'nbaTeamId']], 
        left_on='playerId', 
        right_on='nbaId',
        how='inner'
    )
    
    print(f"\n=== POSSESSION VALIDATION ===")
    
    # Show possession counts by team
    team_totals = validation.groupby('nbaTeamId').agg({
        'offensive_possessions': 'sum',
        'defensive_possessions': 'sum',
        'total_possessions': 'sum'
    }).reset_index()
    
    print("Team possession totals:")
    for _, team in team_totals.iterrows():
        print(f"  Team {team['nbaTeamId']}: {team['offensive_possessions']} offensive, {team['defensive_possessions']} defensive")
    
    # Show top players by possession count
    print(f"\nTop 10 players by total possessions:")
    top_players = validation.nlargest(10, 'total_possessions')
    for _, player in top_players.iterrows():
        print(f"  {player['name']}: {player['offensive_possessions']} off, {player['defensive_possessions']} def, {player['total_possessions']} total ({player['min']:.1f} min)")
    
    # Sanity checks
    total_offensive = validation['offensive_possessions'].sum()
    total_defensive = validation['defensive_possessions'].sum()
    
    print(f"\nSanity checks:")
    print(f"  Total offensive possessions: {total_offensive}")
    print(f"  Total defensive possessions: {total_defensive}")
    print(f"  Difference: {abs(total_offensive - total_defensive)} (should be small)")
    
    # Check possession rate vs minutes
    validation['possessions_per_minute'] = validation['total_possessions'] / validation['min']
    avg_poss_per_min = validation['possessions_per_minute'].mean()
    
    print(f"  Average possessions per minute: {avg_poss_per_min:.2f}")
    print(f"  Range: {validation['possessions_per_minute'].min():.2f} to {validation['possessions_per_minute'].max():.2f}")
    
    return validation


if __name__ == "__main__":
    # Load test data
    box_score_df = pd.read_csv("../../data/box_HOU-DAL.csv")
    pbp_df = pd.read_csv("../../data/pbp_HOU-DAL.csv")
    
    print("Analyzing possessions...")
    
    # Get lineup intervals from hybrid tracker
    from court_time import track_lineup_states
    lineup_intervals = track_lineup_states(box_score_df, pbp_df)
    
    # Run possession analysis with clean interface
    possession_counts = analyze_possessions(box_score_df, pbp_df, lineup_intervals)
    
    print(f"\nGenerated possession counts for {len(possession_counts)} players")
    
    # Validate results
    validation_results = validate_possession_counts(possession_counts, box_score_df)
    
    # Show sample results
    print(f"\nSample possession counts:")
    print(possession_counts.head(10).to_string(index=False))