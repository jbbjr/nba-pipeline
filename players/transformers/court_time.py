# court_time.py

"""
Hybrid lineup tracker that combines explicit substitutions with activity inference.
"""

import pandas as pd
from typing import Dict, List, Set, Tuple


def track_lineup_states(box_score_df: pd.DataFrame, pbp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Track when each player was on court using hybrid approach:
    - Explicit substitutions (msgType 8) within periods
    - Player activity inference to detect re-entries across period boundaries
    
    Args:
        box_score_df: Box score data with starting lineup info
        pbp_df: Play-by-play data with substitution and activity events
        
    Returns:
        DataFrame with player court time intervals:
        [gameId, playerId, teamId, period_start, wallClock_start, period_end, wallClock_end]
    """
    # Get starting lineups and team mapping
    starters = _get_starting_lineups(box_score_df)
    team_mapping = _get_team_mapping(box_score_df)
    
    # Get all player activities to detect re-entries
    activities = _get_player_activities(pbp_df)
    
    # Get explicit substitutions
    substitutions = _get_substitution_events(pbp_df)
    
    # Build intervals using hybrid approach
    intervals = _build_hybrid_intervals(starters, team_mapping, substitutions, activities, pbp_df)
    
    return intervals


def _get_starting_lineups(box_score_df: pd.DataFrame) -> Dict[int, Set[int]]:
    """Extract starting lineups by team from box score."""
    starters_df = box_score_df[box_score_df['gs'] == 1]
    
    starters = {}
    for team_id in starters_df['nbaTeamId'].unique():
        team_starters = set(starters_df[starters_df['nbaTeamId'] == team_id]['nbaId'].tolist())
        starters[team_id] = team_starters
    
    return starters


def _get_team_mapping(box_score_df: pd.DataFrame) -> Dict[int, int]:
    """Map player ID to team ID."""
    return dict(zip(box_score_df['nbaId'], box_score_df['nbaTeamId']))


def _get_substitution_events(pbp_df: pd.DataFrame) -> pd.DataFrame:
    """Extract and sort substitution events."""
    subs = pbp_df[pbp_df['msgType'] == 8].copy()
    subs = subs.sort_values(['period', 'wallClockInt'], ascending=[True, True])
    
    return subs[['period', 'wallClockInt', 'playerId1', 'playerId2', 'description']].reset_index(drop=True)


def _get_player_activities(pbp_df: pd.DataFrame) -> pd.DataFrame:
    """Get all player activity events to detect when players are on court."""
    # Activity types that indicate player is on court
    activity_msg_types = [1, 2, 3, 4, 5, 6, 7]  # Made shot, missed shot, free throw, rebound, turnover, foul, violation
    
    activities = pbp_df[pbp_df['msgType'].isin(activity_msg_types)].copy()
    
    # Expand to track all players involved in each activity
    activity_list = []
    
    for _, row in activities.iterrows():
        for player_col in ['playerId1', 'playerId2', 'playerId3']:
            if pd.notna(row[player_col]):
                activity_list.append({
                    'period': row['period'],
                    'wallClockInt': row['wallClockInt'],
                    'playerId': int(row[player_col]),
                    'msgType': row['msgType'],
                    'description': row['description']
                })
    
    return pd.DataFrame(activity_list).sort_values(['period', 'wallClockInt'])


def _build_hybrid_intervals(starters: Dict[int, Set[int]], 
                           team_mapping: Dict[int, int],
                           substitutions: pd.DataFrame,
                           activities: pd.DataFrame,
                           pbp_df: pd.DataFrame) -> pd.DataFrame:
    """Build intervals using hybrid substitution + activity approach."""
    
    game_id = pbp_df['gameId'].iloc[0]
    max_period = pbp_df['period'].max()
    game_start_wallClock = pbp_df[pbp_df['period'] == 1]['wallClockInt'].min()
    game_end_wallClock = pbp_df[pbp_df['period'] == max_period]['wallClockInt'].max()
    
    print(f"HYBRID DEBUG: Processing {len(substitutions)} substitutions and {len(activities)} activities")
    
    # Create timeline of all player status changes
    status_changes = []
    
    # Add game start for starters
    for team_id, players in starters.items():
        for player_id in players:
            status_changes.append({
                'period': 1,
                'wallClockInt': game_start_wallClock,
                'playerId': player_id,
                'teamId': team_id,
                'action': 'START',
                'source': 'starter'
            })
    
    # Add explicit substitutions
    for _, sub in substitutions.iterrows():
        player_out = int(sub['playerId1'])
        player_in = int(sub['playerId2'])
        
        # Player going out
        team_id = team_mapping.get(player_out)
        if team_id:
            status_changes.append({
                'period': sub['period'],
                'wallClockInt': sub['wallClockInt'],
                'playerId': player_out,
                'teamId': team_id,
                'action': 'OUT',
                'source': 'substitution'
            })
        
        # Player coming in
        team_id = team_mapping.get(player_in)
        if team_id:
            status_changes.append({
                'period': sub['period'],
                'wallClockInt': sub['wallClockInt'],
                'playerId': player_in,
                'teamId': team_id,
                'action': 'IN',
                'source': 'substitution'
            })
    
    # Add inferred re-entries from activity
    inferred_entries = _infer_reentries_from_activity(activities, substitutions, team_mapping)
    status_changes.extend(inferred_entries)
    
    # Sort all status changes chronologically
    status_df = pd.DataFrame(status_changes).sort_values(['period', 'wallClockInt', 'action'])
    
    print(f"HYBRID DEBUG: Total status changes: {len(status_df)}")
    
    # Build intervals from status changes
    intervals = _build_intervals_from_status_changes(status_df, game_id, game_end_wallClock)
    
    return intervals


def _infer_reentries_from_activity(activities: pd.DataFrame, 
                                 substitutions: pd.DataFrame,
                                 team_mapping: Dict[int, int]) -> List[Dict]:
    """Infer when players re-enter the game based on their activity."""
    
    inferred_entries = []
    
    # Combine all events (substitutions + activities) and sort chronologically
    all_events = []
    
    # Add substitution events
    for _, sub in substitutions.iterrows():
        all_events.append({
            'period': sub['period'],
            'wallClockInt': sub['wallClockInt'],
            'event_type': 'substitution',
            'player_out': int(sub['playerId1']),
            'player_in': int(sub['playerId2'])
        })
    
    # Add activity events
    for _, activity in activities.iterrows():
        all_events.append({
            'period': activity['period'],
            'wallClockInt': activity['wallClockInt'],
            'event_type': 'activity',
            'player_id': activity['playerId'],
            'msgType': activity['msgType']
        })
    
    # Sort all events chronologically
    all_events_df = pd.DataFrame(all_events).sort_values(['period', 'wallClockInt'])
    
    # Track current status for each player
    player_status = {}  # player_id -> 'IN'/'OUT'
    
    print(f"FIXED INFERENCE DEBUG: Processing {len(all_events_df)} chronological events")
    
    # Process events chronologically
    for i, (_, event) in enumerate(all_events_df.iterrows()):
        
        if event['event_type'] == 'substitution':
            # Update player statuses
            player_out = event['player_out']
            player_in = event['player_in']
            
            player_status[player_out] = 'OUT'
            player_status[player_in] = 'IN'
            
            if i < 10:  # Debug first few
                print(f"  Sub: Player {player_out} OUT, Player {player_in} IN at period {event['period']}")
        
        elif event['event_type'] == 'activity':
            player_id = event['player_id']
            current_status = player_status.get(player_id, 'UNKNOWN')
            
            # If player has activity but status shows OUT, they must have re-entered
            if current_status == 'OUT':
                team_id = team_mapping.get(player_id)
                if team_id:
                    inferred_entries.append({
                        'period': event['period'],
                        'wallClockInt': event['wallClockInt'],
                        'playerId': player_id,
                        'teamId': team_id,
                        'action': 'IN',
                        'source': 'inferred_from_activity'
                    })
                    
                    # Update status
                    player_status[player_id] = 'IN'
                    
                    if len(inferred_entries) <= 5:  # Debug first few inferences
                        print(f"  INFERRED: Player {player_id} re-entered at period {event['period']}, wallClock {event['wallClockInt']}")
    
    print(f"FIXED INFERENCE DEBUG: Inferred {len(inferred_entries)} re-entries")
    
    return inferred_entries


def _build_intervals_from_status_changes(status_df: pd.DataFrame, 
                                       game_id: str,
                                       game_end_wallClock: int) -> pd.DataFrame:
    """Build final intervals from all status changes."""
    
    intervals = []
    player_entry_times = {}  # player_id -> (period, wallClock, team_id)
    
    for _, change in status_df.iterrows():
        player_id = change['playerId']
        
        if change['action'] in ['START', 'IN']:
            # Player enters court
            player_entry_times[player_id] = (
                change['period'], 
                change['wallClockInt'], 
                change['teamId']
            )
            
        elif change['action'] == 'OUT' and player_id in player_entry_times:
            # Player exits court - create interval
            entry_period, entry_wallClock, team_id = player_entry_times[player_id]
            
            intervals.append({
                'gameId': game_id,
                'playerId': player_id,
                'teamId': team_id,
                'period_start': entry_period,
                'wallClock_start': entry_wallClock,
                'period_end': change['period'],
                'wallClock_end': change['wallClockInt']
            })
            
            del player_entry_times[player_id]
    
    # Handle players still on court at game end
    max_period = status_df['period'].max()
    for player_id, (entry_period, entry_wallClock, team_id) in player_entry_times.items():
        intervals.append({
            'gameId': game_id,
            'playerId': player_id,
            'teamId': team_id,
            'period_start': entry_period,
            'wallClock_start': entry_wallClock,
            'period_end': max_period,
            'wallClock_end': game_end_wallClock
        })
    
    return pd.DataFrame(intervals)


def validate_against_box_score(intervals_df: pd.DataFrame, box_score_df: pd.DataFrame):
    """Validate calculated intervals against box score minutes."""
    
    # Calculate total court time per player
    player_totals = intervals_df.groupby('playerId').apply(
        lambda group: (group['wallClock_end'] - group['wallClock_start']).sum(),
        include_groups=False
    ).reset_index()
    player_totals.columns = ['playerId', 'total_wallClock_units']
    
    # Merge with box score
    comparison = player_totals.merge(
        box_score_df[['nbaId', 'name', 'min', 'gs']], 
        left_on='playerId', 
        right_on='nbaId'
    )
    
    # Estimate conversion factor
    stable_players = comparison[comparison['min'] > 10]
    if len(stable_players) > 0:
        stable_players = stable_players.copy()  # Avoid warning
        stable_players.loc[:, 'wallClock_per_second'] = stable_players['total_wallClock_units'] / (stable_players['min'] * 60)
        conversion_factor = stable_players['wallClock_per_second'].median()
    else:
        conversion_factor = 30.0
    
    # Calculate accuracy
    comparison['calculated_minutes'] = comparison['total_wallClock_units'] / conversion_factor / 60
    comparison['minutes_diff'] = abs(comparison['calculated_minutes'] - comparison['min'])
    
    print(f"\n=== HYBRID VALIDATION RESULTS ===")
    print(f"Conversion factor: {conversion_factor:.2f} wallClock units per second")
    
    # Show detailed comparison table for all players
    print(f"\n=== ALL PLAYERS COMPARISON ===")
    comparison_sorted = comparison.sort_values('minutes_diff', ascending=True)
    
    print("Player                    Actual  Calculated  Diff   Status")
    print("-" * 60)
    for _, row in comparison_sorted.iterrows():
        status = "STARTER" if row['gs'] == 1 else "BENCH  "
        diff_indicator = "✅" if row['minutes_diff'] <= 2.0 else "❌"
        print(f"{row['name']:<20} {row['min']:>6.1f}  {row['calculated_minutes']:>10.1f}  {row['minutes_diff']:>4.1f}  {status}  {diff_indicator}")
    
    # Show key players separately for emphasis
    key_players = [202691, 203957]  # Klay, Exum
    print(f"\n=== KEY PROBLEM PLAYERS ===")
    for player_id in key_players:
        player_data = comparison[comparison['playerId'] == player_id]
        if len(player_data) > 0:
            row = player_data.iloc[0]
            status = "SOLVED" if row['minutes_diff'] <= 3.0 else "PROBLEM"
            print(f"{row['name']}: {row['min']:.1f} actual vs {row['calculated_minutes']:.1f} calculated (diff: {row['minutes_diff']:.1f}) - {status}")
    
    # Summary statistics
    avg_diff = comparison['minutes_diff'].mean()
    within_2min = (comparison['minutes_diff'] <= 2.0).sum()
    within_5min = (comparison['minutes_diff'] <= 5.0).sum()
    total_players = len(comparison)
    
    print(f"\n=== SUMMARY STATISTICS ===")
    print(f"Average difference: {avg_diff:.2f} minutes")
    print(f"Players within 2 minutes: {within_2min}/{total_players} ({within_2min/total_players*100:.1f}%)")
    print(f"Players within 5 minutes: {within_5min}/{total_players} ({within_5min/total_players*100:.1f}%)")
    
    if avg_diff <= 1.5:
        print("✅ EXCELLENT: Average difference ≤ 1.5 minutes")
    elif avg_diff <= 3.0:
        print("✅ GOOD: Average difference ≤ 3 minutes")
    elif avg_diff <= 5.0:
        print("⚠️  ACCEPTABLE: Average difference ≤ 5 minutes")
    else:
        print("❌ NEEDS WORK: Average difference > 5 minutes")
    
    return comparison


if __name__ == "__main__":
    # Load test data
    box_score_df = pd.read_csv("../../data/box_HOU-DAL.csv")
    pbp_df = pd.read_csv("../../data/pbp_HOU-DAL.csv")
    
    print("Processing hybrid lineup tracking...")
    lineup_intervals = track_lineup_states(box_score_df, pbp_df)
    
    print(f"\nGenerated {len(lineup_intervals)} intervals for {lineup_intervals['playerId'].nunique()} players")
    
    # Validate against box score
    validation_results = validate_against_box_score(lineup_intervals, box_score_df)
    
    # Test specific problem players
    print(f"\n=== KEY PLAYER ANALYSIS ===")
    klay_intervals = lineup_intervals[lineup_intervals['playerId'] == 202691]  # Klay Thompson
    exum_intervals = lineup_intervals[lineup_intervals['playerId'] == 203957]  # Dante Exum
    
    print(f"Klay Thompson intervals: {len(klay_intervals)}")
    if len(klay_intervals) > 0:
        total_time = (klay_intervals['wallClock_end'] - klay_intervals['wallClock_start']).sum()
        print(f"Total wallClock units: {total_time}")
        print(klay_intervals[['period_start', 'wallClock_start', 'period_end', 'wallClock_end']].to_string(index=False))
    
    print(f"\nDante Exum intervals: {len(exum_intervals)}")
    if len(exum_intervals) > 0:
        total_time = (exum_intervals['wallClock_end'] - exum_intervals['wallClock_start']).sum()
        print(f"Total wallClock units: {total_time}")
        print(exum_intervals[['period_start', 'wallClock_start', 'period_end', 'wallClock_end']].to_string(index=False))