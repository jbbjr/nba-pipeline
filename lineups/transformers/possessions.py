# lineups/transformers/possessions.py

"""
Parse basketball possessions from play-by-play data.
A possession ends with: made shot, defensive rebound, turnover, or period end.
Handles edge cases like and-1 situations, technical fouls, and period transitions.
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Tuple, Optional


def extract_possessions(pbp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract possession-level data from play-by-play events.
    
    Args:
        pbp_df: Play-by-play DataFrame with NBA event data
        
    Returns:
        DataFrame with columns:
        - possession_id: Unique identifier for each possession
        - period: Game period
        - start_time: Possession start (game clock seconds)
        - end_time: Possession end (game clock seconds)
        - off_team: Team with possession
        - def_team: Defending team
        - points_scored: Points scored during possession
        - possession_end_type: How possession ended
        - duration_seconds: Length of possession
    """
    
    # Sort PBP chronologically and clean
    pbp_clean = _prepare_pbp_data(pbp_df)
    
    # Identify possession-ending events
    possession_endings = _identify_possession_endings(pbp_clean)
    
    # Build possession timeline
    possessions = _build_possession_timeline(pbp_clean, possession_endings)
    
    # Calculate possession metrics
    possessions = _calculate_possession_metrics(possessions, pbp_clean)
    
    return possessions.reset_index(drop=True)


def _prepare_pbp_data(pbp_df: pd.DataFrame) -> pd.DataFrame:
    """Clean and prepare PBP data for possession parsing."""
    pbp = pbp_df.copy()
    
    # Convert game clock to seconds for easier calculation
    pbp['game_clock_seconds'] = pbp['gameClock'].apply(_game_clock_to_seconds)
    
    # Calculate actual time elapsed (higher seconds = earlier in period)
    max_period_time = pbp.groupby('period')['game_clock_seconds'].max()
    pbp['time_elapsed'] = pbp.apply(
        lambda row: max_period_time[row['period']] - row['game_clock_seconds'], 
        axis=1
    )
    
    # Sort chronologically: period ASC, time_elapsed ASC
    pbp = pbp.sort_values(['period', 'time_elapsed', 'pbpOrder']).reset_index(drop=True)
    
    # Handle team ID inconsistencies
    pbp = _clean_team_ids(pbp)
    
    return pbp


def _clean_team_ids(pbp_df: pd.DataFrame) -> pd.DataFrame:
    """Clean team ID inconsistencies in PBP data."""
    pbp = pbp_df.copy()
    
    # Get valid team IDs from non-zero entries
    valid_teams = set(pbp[pbp['offTeamId'] > 0]['offTeamId'].unique())
    if len(valid_teams) != 2:
        # Fallback: use nbaTeamId where available
        valid_teams = set(pbp[pbp['nbaTeamId'].notna()]['nbaTeamId'].unique())
    
    valid_teams = sorted(list(valid_teams))
    
    # Fill missing team IDs where possible
    pbp['offTeamId_clean'] = pbp['offTeamId'].where(pbp['offTeamId'] > 0, np.nan)
    pbp['defTeamId_clean'] = pbp['offTeamId_clean'].apply(
        lambda x: valid_teams[1] if x == valid_teams[0] else valid_teams[0] if pd.notna(x) else np.nan
    )
    
    return pbp


def _identify_possession_endings(pbp_df: pd.DataFrame) -> pd.DataFrame:
    """Identify events that end basketball possessions."""
    
    # Define possession-ending event types
    possession_end_events = {
        1: 'made_shot',      # Made field goal
        3: 'free_throw',     # Free throw (check if last in sequence)
        5: 'turnover',       # Turnover
        4: 'rebound',        # Rebound (check if defensive)
        12: 'period_end',    # End of period
        13: 'period_end'     # End of game
    }
    
    endings = []
    
    for idx, play in pbp_df.iterrows():
        msg_type = play['msgType']
        
        if msg_type in possession_end_events:
            end_type = possession_end_events[msg_type]
            
            # Special handling for specific event types
            if msg_type == 1:  # Made shots
                # Skip and-1 free throws (they don't end possession immediately)
                if not _is_and_one_freethrow(play, pbp_df, idx):
                    endings.append(_create_possession_ending(play, end_type, idx))
                    
            elif msg_type == 3:  # Free throws
                # Only count if it's the last free throw in sequence
                if _is_last_free_throw(play, pbp_df, idx):
                    endings.append(_create_possession_ending(play, end_type, idx))
                    
            elif msg_type == 4:  # Rebounds
                # Only defensive rebounds end possessions
                if _is_defensive_rebound(play, pbp_df, idx):
                    endings.append(_create_possession_ending(play, 'defensive_rebound', idx))
                    
            elif msg_type == 5:  # Turnovers
                endings.append(_create_possession_ending(play, end_type, idx))
                
            elif msg_type in [12, 13]:  # Period/game end
                endings.append(_create_possession_ending(play, end_type, idx))
    
    return pd.DataFrame(endings) if endings else pd.DataFrame()


def _is_and_one_freethrow(play: pd.Series, pbp_df: pd.DataFrame, idx: int) -> bool:
    """Check if this is an and-1 free throw that shouldn't end possession."""
    # Look ahead for immediate free throw attempts
    next_plays = pbp_df.iloc[idx+1:idx+3] if idx < len(pbp_df)-2 else pd.DataFrame()
    
    # If next play is free throw by same player, this was likely and-1
    for _, next_play in next_plays.iterrows():
        if (next_play['msgType'] == 3 and  # Free throw
            next_play['playerId1'] == play['playerId1'] and  # Same player
            abs(next_play['time_elapsed'] - play['time_elapsed']) < 5):  # Within 5 seconds
            return True
    
    return False


def _is_last_free_throw(play: pd.Series, pbp_df: pd.DataFrame, idx: int) -> bool:
    """Check if this is the last free throw in a sequence."""
    # Look ahead for more free throws by same player
    next_plays = pbp_df.iloc[idx+1:idx+3] if idx < len(pbp_df)-2 else pd.DataFrame()
    
    for _, next_play in next_plays.iterrows():
        if (next_play['msgType'] == 3 and  # Another free throw
            next_play['playerId1'] == play['playerId1'] and  # Same player
            abs(next_play['time_elapsed'] - play['time_elapsed']) < 10):  # Within 10 seconds
            return False  # Not the last one
    
    return True


def _is_defensive_rebound(play: pd.Series, pbp_df: pd.DataFrame, idx: int) -> bool:
    """Check if rebound is defensive (different team than previous shot)."""
    # Look back for recent missed shot
    prev_plays = pbp_df.iloc[max(0, idx-5):idx][::-1]  # Last 5 plays, reversed
    
    for _, prev_play in prev_plays.iterrows():
        if prev_play['msgType'] == 2:  # Missed shot
            # If rebounding team different from shooting team, it's defensive
            if (pd.notna(play['offTeamId_clean']) and 
                pd.notna(prev_play['offTeamId_clean']) and
                play['offTeamId_clean'] != prev_play['offTeamId_clean']):
                return True
            break  # Stop at first shot found
    
    # Default assumption for unclear cases
    return True


def _create_possession_ending(play: pd.Series, end_type: str, idx: int) -> Dict:
    """Create possession ending record."""
    return {
        'period': play['period'],
        'end_time_seconds': play['game_clock_seconds'],
        'time_elapsed': play['time_elapsed'],
        'end_type': end_type,
        'ending_team': play['offTeamId_clean'],
        'pbp_idx': idx
    }


def _build_possession_timeline(pbp_df: pd.DataFrame, endings_df: pd.DataFrame) -> pd.DataFrame:
    """Build timeline of possessions from ending events."""
    if endings_df.empty:
        return pd.DataFrame()
    
    possessions = []
    possession_id = 1
    
    # Group endings by period
    for period in sorted(endings_df['period'].unique()):
        period_endings = endings_df[endings_df['period'] == period].sort_values('time_elapsed')
        
        # Start of period
        period_start_time = pbp_df[pbp_df['period'] == period]['game_clock_seconds'].max()
        possession_start = period_start_time
        
        # Determine starting team (team with first non-neutral event)
        period_pbp = pbp_df[pbp_df['period'] == period]
        first_team_event = period_pbp[period_pbp['offTeamId_clean'].notna()].iloc[0]
        current_off_team = first_team_event['offTeamId_clean']
        
        # Get all valid teams
        valid_teams = sorted(pbp_df[pbp_df['offTeamId_clean'].notna()]['offTeamId_clean'].unique())
        other_team = valid_teams[1] if current_off_team == valid_teams[0] else valid_teams[0]
        
        for _, ending in period_endings.iterrows():
            possession = {
                'possession_id': possession_id,
                'period': period,
                'start_time_seconds': possession_start,
                'end_time_seconds': ending['end_time_seconds'],
                'off_team': current_off_team,
                'def_team': other_team,
                'end_type': ending['end_type'],
                'pbp_start_idx': None,  # Will fill later if needed
                'pbp_end_idx': ending['pbp_idx']
            }
            
            possessions.append(possession)
            possession_id += 1
            
            # Next possession starts where this ended, teams may switch
            possession_start = ending['end_time_seconds']
            if ending['end_type'] in ['defensive_rebound', 'turnover']:
                # Possession changes teams
                current_off_team, other_team = other_team, current_off_team
    
    return pd.DataFrame(possessions)


def _calculate_possession_metrics(possessions_df: pd.DataFrame, pbp_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate possession-level metrics like points scored and duration."""
    if possessions_df.empty:
        return possessions_df
    
    possessions = possessions_df.copy()
    
    # Calculate possession duration
    possessions['duration_seconds'] = (
        possessions['start_time_seconds'] - possessions['end_time_seconds']
    ).abs()
    
    # Calculate points scored during each possession
    possessions['points_scored'] = 0
    
    for idx, poss in possessions.iterrows():
        # Find scoring events during this possession
        poss_events = pbp_df[
            (pbp_df['period'] == poss['period']) &
            (pbp_df['game_clock_seconds'] <= poss['start_time_seconds']) &
            (pbp_df['game_clock_seconds'] >= poss['end_time_seconds']) &
            (pbp_df['offTeamId_clean'] == poss['off_team']) &
            (pbp_df['pts'] > 0)
        ]
        
        possessions.at[idx, 'points_scored'] = poss_events['pts'].sum()
    
    # Convert team IDs back to integers for clean output
    possessions['off_team'] = possessions['off_team'].astype('int64')
    possessions['def_team'] = possessions['def_team'].astype('int64')
    
    return possessions


def _game_clock_to_seconds(game_clock: str) -> int:
    """Convert MM:SS game clock to total seconds remaining."""
    try:
        if pd.isna(game_clock) or game_clock == '':
            return 0
        minutes, seconds = map(int, str(game_clock).split(':'))
        return minutes * 60 + seconds
    except (ValueError, AttributeError):
        return 0


if __name__ == '__main__':
    # Test the transformation
    print("Testing possessions transformation...")
    
    # Load test data
    pbp_df = pd.read_csv("../../data/pbp_HOU-DAL.csv")
    print(f"Loaded {len(pbp_df)} PBP records")
    
    # Run transformation
    possessions_df = extract_possessions(pbp_df)
    
    print(f"\nGenerated {len(possessions_df)} possessions")
    print(f"Periods: {sorted(possessions_df['period'].unique())}")
    print(f"Average possession duration: {possessions_df['duration_seconds'].mean():.1f} seconds")
    
    # Show possession summary by team
    print(f"\nPossession summary:")
    for team in possessions_df['off_team'].unique():
        team_poss = possessions_df[possessions_df['off_team'] == team]
        total_points = team_poss['points_scored'].sum()
        print(f"Team {int(team)}: {len(team_poss)} possessions, {total_points} points")
    
    # Show sample possessions
    print(f"\nSample possessions:")
    sample_cols = ['possession_id', 'period', 'off_team', 'duration_seconds', 'points_scored', 'end_type']
    print(possessions_df[sample_cols].head(10))
    
    # Show possession ending types
    print(f"\nPossession ending types:")
    print(possessions_df['end_type'].value_counts())
    
    print(f"\nOutput columns: {list(possessions_df.columns)}")