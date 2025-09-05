# lineups/transformers/lineup_possessions.py

"""
Match 5-man lineups to basketball possessions using time-based joins.
Creates clean data ready for lineup ratings calculation.
"""
import pandas as pd
import numpy as np


def match_lineups_to_possessions(lineup_states_df: pd.DataFrame, 
                                possessions_df: pd.DataFrame) -> pd.DataFrame:
    """
    Match lineup states to possessions based on game time.
    
    Returns clean DataFrame ready for lineup ratings calculation with:
    - possession_id, period, points_scored, duration_seconds
    - off_team_id, def_team_id (numeric team IDs)
    - off_player_1-5, def_player_1-5 (player IDs for each lineup)
    - off_lineup_id, def_lineup_id (for grouping)
    """
    
    if possessions_df.empty or lineup_states_df.empty:
        return _create_empty_output()
    
    print(f"Processing {len(possessions_df)} possessions with {len(lineup_states_df)} lineup states")
    
    # Prepare data
    lineups = lineup_states_df.copy()
    possessions = possessions_df.copy()
    
    # Match each possession to lineups
    results = []
    
    for idx, poss in possessions.iterrows():
        # Find offensive lineup (team with possession)
        off_lineup = find_lineup_at_time(
            lineups, poss['period'], poss['start_time_seconds'], poss['off_team']
        )
        
        # Find defensive lineup (other team)
        def_lineup = find_lineup_at_time(
            lineups, poss['period'], poss['start_time_seconds'], poss['def_team']
        )
        
        # Only include if we found both lineups
        if off_lineup is not None and def_lineup is not None:
            result = create_possession_record(poss, off_lineup, def_lineup)
            results.append(result)
    
    if not results:
        return _create_empty_output()
    
    # Convert to DataFrame and clean
    df = pd.DataFrame(results)
    df = clean_output_data(df)
    
    print(f"Successfully matched {len(df)} possessions to lineups")
    return df


def find_lineup_at_time(lineups_df: pd.DataFrame, period: int, time_seconds: float, team_id: int):
    """Find which lineup was active for a team at a specific time."""
    
    # Filter to team and period
    team_lineups = lineups_df[
        (lineups_df['team_id'] == team_id) & 
        (lineups_df['period'] == period)
    ].copy()
    
    if team_lineups.empty:
        return None
    
    # Sort by game clock (descending - higher seconds = earlier in period)
    team_lineups = team_lineups.sort_values('game_clock_seconds', ascending=False)
    
    # Find the lineup that was active at possession start
    # We want the most recent lineup change before or at the possession time
    active_lineups = team_lineups[team_lineups['game_clock_seconds'] >= time_seconds]
    
    if active_lineups.empty:
        # No lineup changes after possession start - use last known lineup
        return team_lineups.iloc[-1].to_dict()
    else:
        # Use the most recent lineup (lowest game_clock_seconds that's >= time_seconds)
        return active_lineups.iloc[-1].to_dict()


def create_possession_record(possession: pd.Series, off_lineup: dict, def_lineup: dict) -> dict:
    """Create a clean record combining possession and lineup data."""
    
    return {
        # Possession info
        'possession_id': possession['possession_id'],
        'period': possession['period'],
        'start_time_seconds': possession['start_time_seconds'],
        'end_time_seconds': possession['end_time_seconds'],
        'duration_seconds': possession.get('duration_seconds', 0),
        'points_scored': possession.get('points_scored', 0),
        'end_type': possession.get('end_type', ''),
        
        # Team IDs (keep original possession team IDs)
        'off_team_id': possession['off_team'],
        'def_team_id': possession['def_team'],
        
        # Team abbreviations (from lineups)
        'off_team': off_lineup['team'],
        'def_team': def_lineup['team'], 
        
        # Offensive lineup
        'off_player_1': off_lineup['player_1'],
        'off_player_2': off_lineup['player_2'],
        'off_player_3': off_lineup['player_3'],
        'off_player_4': off_lineup['player_4'],
        'off_player_5': off_lineup['player_5'],
        'off_lineup_id': off_lineup['lineup_id'],
        
        # Defensive lineup
        'def_player_1': def_lineup['player_1'],
        'def_player_2': def_lineup['player_2'],
        'def_player_3': def_lineup['player_3'],
        'def_player_4': def_lineup['player_4'],
        'def_player_5': def_lineup['player_5'],
        'def_lineup_id': def_lineup['lineup_id'],
    }


def clean_output_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate the output DataFrame."""
    
    # Ensure numeric columns are proper types
    numeric_cols = [
        'possession_id', 'period', 'start_time_seconds', 'end_time_seconds',
        'duration_seconds', 'points_scored', 'off_team_id', 'def_team_id'
    ]
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Ensure player IDs are integers
    player_cols = []
    for prefix in ['off', 'def']:
        for i in range(1, 6):
            player_cols.append(f'{prefix}_player_{i}')
    
    for col in player_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    
    # Sort by possession order
    df = df.sort_values('possession_id').reset_index(drop=True)
    
    # Validate no critical missing data
    critical_cols = ['off_team_id', 'def_team_id', 'off_lineup_id', 'def_lineup_id']
    for col in critical_cols:
        if col in df.columns:
            missing_count = df[col].isna().sum()
            if missing_count > 0:
                print(f"Warning: {missing_count} rows missing {col}")
    
    return df


def _create_empty_output() -> pd.DataFrame:
    """Create empty DataFrame with correct column structure."""
    columns = [
        'possession_id', 'period', 'start_time_seconds', 'end_time_seconds',
        'duration_seconds', 'points_scored', 'end_type',
        'off_team_id', 'def_team_id', 'off_team', 'def_team',
        'off_player_1', 'off_player_2', 'off_player_3', 'off_player_4', 'off_player_5',
        'def_player_1', 'def_player_2', 'def_player_3', 'def_player_4', 'def_player_5',
        'off_lineup_id', 'def_lineup_id'
    ]
    return pd.DataFrame(columns=columns)


def validate_for_ratings_calculation(df: pd.DataFrame) -> bool:
    """Validate that the data is ready for step 4 (lineup ratings)."""
    
    if df.empty:
        print("Error: No data to validate")
        return False
    
    # Check required columns
    required_cols = [
        'off_team_id', 'def_team_id', 'points_scored',
        'off_player_1', 'off_player_2', 'off_player_3', 'off_player_4', 'off_player_5',
        'def_player_1', 'def_player_2', 'def_player_3', 'def_player_4', 'def_player_5'
    ]
    
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"Error: Missing required columns: {missing_cols}")
        return False
    
    # Check for missing data in critical columns
    for col in required_cols:
        missing_count = df[col].isna().sum()
        if missing_count > 0:
            print(f"Warning: {missing_count} missing values in {col}")
    
    # Check data quality
    print(f"Validation summary:")
    print(f"  Total possessions: {len(df)}")
    print(f"  Unique offensive lineups: {df['off_lineup_id'].nunique()}")
    print(f"  Unique defensive lineups: {df['def_lineup_id'].nunique()}")
    print(f"  Total points: {df['points_scored'].sum()}")
    print(f"  Periods covered: {sorted(df['period'].unique())}")
    print(f"  Teams covered: {sorted(df['off_team_id'].unique())}")
    
    return True


if __name__ == '__main__':
    # Test the transformation
    print("Testing lineup_possessions transformation...")
    
    # Import required modules (adjust paths as needed)
    import sys
    sys.path.append('../..')
    
    try:
        from lineup_states import extract_lineup_states
        from possessions import extract_possessions
        
        # Load test data
        box_score_df = pd.read_csv("../../data/box_HOU-DAL.csv")
        pbp_df = pd.read_csv("../../data/pbp_HOU-DAL.csv")
        
        print(f"Loaded {len(box_score_df)} box score records")
        print(f"Loaded {len(pbp_df)} PBP records")
        
        # Generate prerequisites
        print("\nStep 1: Generating lineup states...")
        lineup_states_df = extract_lineup_states(box_score_df, pbp_df)
        print(f"Generated {len(lineup_states_df)} lineup states")
        
        print("\nStep 2: Generating possessions...")
        possessions_df = extract_possessions(pbp_df)
        print(f"Generated {len(possessions_df)} possessions")
        
        # Run the main transformation
        print("\nStep 3: Matching lineups to possessions...")
        lineup_possessions_df = match_lineups_to_possessions(lineup_states_df, possessions_df)
        
        # Validate output
        print(f"\nValidating output for step 4...")
        is_valid = validate_for_ratings_calculation(lineup_possessions_df)
        
        if is_valid and not lineup_possessions_df.empty:
            print(f"\n✅ SUCCESS - Data ready for lineup ratings calculation")
            
            # Show sample for verification
            print(f"\nSample output:")
            sample_cols = ['possession_id', 'off_team', 'def_team', 'points_scored', 
                          'off_player_1', 'off_player_2', 'def_player_1', 'def_player_2']
            print(lineup_possessions_df[sample_cols].head())
            
        else:
            print(f"\n❌ Issues found - check warnings above")
            
    except ImportError as e:
        print(f"Import error: {e}")
        print("Run from correct directory or adjust import paths")
    except Exception as e:
        print(f"Error during testing: {e}")
        import traceback
        traceback.print_exc()