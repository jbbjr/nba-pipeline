# lineups/transformers/lineup_states.py

"""
Transform box score and PBP data into a timeline of 5-man lineups.
Tracks who's on court for each team at every substitution moment.
"""
import pandas as pd
from typing import Dict, List, Set
import numpy as np


def extract_lineup_states(box_score_df: pd.DataFrame, pbp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract timeline of 5-man lineups for each team throughout the game.

    Args:
        box_score_df: Box score data with starting positions
        pbp_df: Play-by-play data with substitutions

    Returns:
        DataFrame with columns:
        - period: Game period (1-4, OT periods)
        - game_clock: Time remaining in period (MM:SS format)
        - team: Team abbreviation
        - team_id: Numeric team ID for joining with possessions
        - player_1, player_2, player_3, player_4, player_5: Player IDs in lineup
        - lineup_id: Unique identifier for this 5-man combination
    """
    # Extract starting lineups from box score
    starting_lineups = _extract_starting_lineups(box_score_df)

    # Parse substitutions from PBP
    substitutions = _parse_substitutions(pbp_df)

    # Build chronological lineup states
    lineup_states = _build_lineup_timeline(starting_lineups, substitutions, pbp_df)

    # Add lineup_id for easier grouping later
    lineup_states['lineup_id'] = lineup_states.apply(
        lambda row: _generate_lineup_id(row), axis=1
    )

    return lineup_states.sort_values(['team', 'period', 'game_clock_seconds']).reset_index(drop=True)


def _extract_starting_lineups(box_score_df: pd.DataFrame) -> Dict[str, Dict]:
    """Extract starting 5 for each team from box score."""
    starters = box_score_df[
        (box_score_df['startPos'].notna()) & 
        (box_score_df['startPos'] != '')
    ].copy()

    starting_lineups = {}
    for team in starters['team'].unique():
        team_starters = starters[starters['team'] == team]
        player_ids = team_starters['nbaId'].tolist()
        team_id = team_starters['nbaTeamId'].iloc[0]  # Read from CSV but rename internally

        if len(player_ids) != 5:
            raise ValueError(f"Team {team} has {len(player_ids)} starters, expected 5")

        starting_lineups[team] = {
            'players': set(player_ids),
            'team_id': int(team_id)
        }

    return starting_lineups


def _parse_substitutions(pbp_df: pd.DataFrame) -> pd.DataFrame:
    """Parse substitution events from PBP data."""
    # Substitutions are typically msgType 8
    subs = pbp_df[pbp_df['msgType'] == 8].copy()

    if subs.empty:
        return pd.DataFrame(columns=['period', 'game_clock', 'team', 'player_in', 'player_out'])

    # Clean substitution data
    subs = subs[
        (subs['playerId1'].notna()) & 
        (subs['playerId2'].notna())
    ].copy()

    # Convert game clock to seconds for sorting
    subs['game_clock_seconds'] = subs['gameClock'].apply(_game_clock_to_seconds)

    # playerId1 is typically player coming in, playerId2 is going out
    substitutions = pd.DataFrame({
        'period': subs['period'],
        'game_clock': subs['gameClock'],
        'game_clock_seconds': subs['game_clock_seconds'],
        'team': subs['team'],
        'player_in': subs['playerId1'].astype(int),
        'player_out': subs['playerId2'].astype(int)
    })

    return substitutions.sort_values(['period', 'game_clock_seconds'], ascending=[True, False])


def _build_lineup_timeline(starting_lineups: Dict[str, Dict], 
                          substitutions: pd.DataFrame,
                          pbp_df: pd.DataFrame) -> pd.DataFrame:
    """Build chronological timeline of lineup states."""
    # Get unique teams
    teams = list(starting_lineups.keys())

    # Initialize lineup states with starting lineups
    current_lineups = {team: starting_lineups[team]['players'].copy() for team in teams}
    team_ids = {team: starting_lineups[team]['team_id'] for team in teams}

    # Get all periods in the game
    periods = sorted(pbp_df['period'].unique())

    lineup_timeline = []

    for period in periods:
        # Add starting lineup for each period
        for team in teams:
            lineup_timeline.append({
                'period': period,
                'game_clock': '12:00' if period <= 4 else '05:00',  # Regular vs OT
                'game_clock_seconds': 720 if period <= 4 else 300,
                'team': team,
                'team_id': team_ids[team],
                **_lineup_to_dict(current_lineups[team])
            })

        # Process substitutions for this period
        period_subs = substitutions[substitutions['period'] == period]

        for _, sub in period_subs.iterrows():
            team = sub['team']

            # Validate substitution
            if sub['player_out'] not in current_lineups[team]:
                continue  # Skip invalid substitutions

            if sub['player_in'] in current_lineups[team]:
                continue  # Skip if player already on court

            # Make the substitution
            current_lineups[team].remove(sub['player_out'])
            current_lineups[team].add(sub['player_in'])

            # Record new lineup state
            lineup_timeline.append({
                'period': sub['period'],
                'game_clock': sub['game_clock'],
                'game_clock_seconds': sub['game_clock_seconds'],
                'team': team,
                'team_id': team_ids[team],
                **_lineup_to_dict(current_lineups[team])
            })

    return pd.DataFrame(lineup_timeline)


def _lineup_to_dict(lineup_set: Set[int]) -> Dict[str, int]:
    """Convert lineup set to ordered dictionary for DataFrame."""
    sorted_lineup = sorted(list(lineup_set))
    return {f'player_{i+1}': player_id for i, player_id in enumerate(sorted_lineup)}


def _generate_lineup_id(row: pd.Series) -> str:
    """Generate unique ID for a 5-man lineup."""
    players = sorted([
        row['player_1'], row['player_2'], row['player_3'], 
        row['player_4'], row['player_5']
    ])
    return f"{row['team']}_{'_'.join(map(str, players))}"


def _game_clock_to_seconds(game_clock: str) -> int:
    """Convert MM:SS game clock to total seconds remaining."""
    try:
        minutes, seconds = map(int, game_clock.split(':'))
        return minutes * 60 + seconds
    except (ValueError, AttributeError):
        return 0


if __name__ == '__main__':
    # Test the transformation with sample data
    print("Testing lineup_states transformation...")

    # Load test data
    box_score_df = pd.read_csv("../../data/box_HOU-DAL.csv")
    pbp_df = pd.read_csv("../../data/pbp_HOU-DAL.csv")

    print(f"Loaded {len(box_score_df)} box score records")
    print(f"Loaded {len(pbp_df)} PBP records")

    # Run transformation
    lineup_states_df = extract_lineup_states(box_score_df, pbp_df)

    print(f"\nGenerated {len(lineup_states_df)} lineup state records")
    print(f"Teams: {lineup_states_df['team'].unique()}")
    print(f"Team IDs: {lineup_states_df['team_id'].unique()}")
    print(f"Periods: {sorted(lineup_states_df['period'].unique())}")
    print(f"Unique lineups: {lineup_states_df['lineup_id'].nunique()}")

    # Show sample output
    print("\nSample lineup states:")
    print(lineup_states_df.head(10)[['period', 'game_clock', 'team', 'team_id', 'player_1', 'player_2', 'lineup_id']])

    # Show substitution summary
    print(f"\nLineup changes by team:")
    for team in lineup_states_df['team'].unique():
        team_states = lineup_states_df[lineup_states_df['team'] == team]
        team_id = team_states['team_id'].iloc[0]
        print(f"{team} (ID: {team_id}): {len(team_states)} lineup states")

    print(f"\nOutput columns: {list(lineup_states_df.columns)}")
