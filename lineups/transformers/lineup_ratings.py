# lineups/transformers/lineup_ratings.py

"""
Calculate 5-man lineup performance metrics from possession-level data.
Produces final table with offensive/defensive ratings per 100 possessions.
"""
import pandas as pd
import numpy as np
from typing import Dict, List


def calculate_lineup_ratings(lineup_possessions_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate offensive and defensive ratings for each unique 5-man lineup.
    
    Args:
        lineup_possessions_df: Output from lineup_possessions with possession-lineup matches
        
    Returns:
        DataFrame with columns:
        - team: Team abbreviation
        - player_1, player_2, player_3, player_4, player_5: Player IDs (sorted)
        - off_poss: Number of offensive possessions
        - def_poss: Number of defensive possessions  
        - off_rating: Points scored per 100 offensive possessions
        - def_rating: Points allowed per 100 defensive possessions
        - net_rating: off_rating - def_rating
    """
    
    if lineup_possessions_df.empty:
        return _create_empty_ratings()
    
    print(f"Calculating ratings for {len(lineup_possessions_df)} possession-lineup records")
    
    # Calculate offensive stats (when lineup is on offense)
    off_stats = calculate_offensive_stats(lineup_possessions_df)
    
    # Calculate defensive stats (when lineup is on defense)  
    def_stats = calculate_defensive_stats(lineup_possessions_df)
    
    # Combine offensive and defensive stats
    lineup_ratings = combine_offensive_defensive_stats(off_stats, def_stats)
    
    # Calculate final ratings
    lineup_ratings = calculate_final_ratings(lineup_ratings)
    
    # Clean and sort output
    lineup_ratings = clean_final_output(lineup_ratings)
    
    print(f"Generated ratings for {len(lineup_ratings)} unique lineups")
    return lineup_ratings


def calculate_offensive_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate stats when each lineup is on offense."""
    
    # Create standardized lineup representation (sorted players)
    df_off = df.copy()
    df_off['lineup_players'] = df_off.apply(
        lambda row: tuple(sorted([
            row['off_player_1'], row['off_player_2'], row['off_player_3'],
            row['off_player_4'], row['off_player_5']
        ])), axis=1
    )
    
    # Group by team and lineup
    off_grouped = df_off.groupby(['off_team', 'lineup_players']).agg({
        'possession_id': 'count',  # Number of offensive possessions
        'points_scored': 'sum'     # Points scored on offense
    }).reset_index()
    
    off_grouped.columns = ['team', 'lineup_players', 'off_poss', 'off_points']
    
    return off_grouped


def calculate_defensive_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate stats when each lineup is on defense."""
    
    # Create standardized lineup representation (sorted players)
    df_def = df.copy()
    df_def['lineup_players'] = df_def.apply(
        lambda row: tuple(sorted([
            row['def_player_1'], row['def_player_2'], row['def_player_3'],
            row['def_player_4'], row['def_player_5']  
        ])), axis=1
    )
    
    # Group by team and lineup  
    def_grouped = df_def.groupby(['def_team', 'lineup_players']).agg({
        'possession_id': 'count',  # Number of defensive possessions
        'points_scored': 'sum'     # Points allowed on defense
    }).reset_index()
    
    def_grouped.columns = ['team', 'lineup_players', 'def_poss', 'def_points_allowed']
    
    return def_grouped


def combine_offensive_defensive_stats(off_stats: pd.DataFrame, def_stats: pd.DataFrame) -> pd.DataFrame:
    """Combine offensive and defensive stats for each lineup."""
    
    # Full outer join to get all lineups (some may only have off or def data)
    combined = pd.merge(
        off_stats, def_stats, 
        on=['team', 'lineup_players'], 
        how='outer'
    ).fillna(0)
    
    # Convert to integers where appropriate
    int_cols = ['off_poss', 'off_points', 'def_poss', 'def_points_allowed']
    for col in int_cols:
        if col in combined.columns:
            combined[col] = combined[col].astype(int)
    
    return combined


def calculate_final_ratings(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate per-100-possession ratings."""
    
    ratings = df.copy()
    
    # Offensive rating = (points scored / offensive possessions) * 100
    ratings['off_rating'] = np.where(
        ratings['off_poss'] > 0,
        (ratings['off_points'] / ratings['off_poss']) * 100,
        0
    )
    
    # Defensive rating = (points allowed / defensive possessions) * 100  
    ratings['def_rating'] = np.where(
        ratings['def_poss'] > 0,
        (ratings['def_points_allowed'] / ratings['def_poss']) * 100,
        0
    )
    
    # Net rating = offensive rating - defensive rating
    ratings['net_rating'] = ratings['off_rating'] - ratings['def_rating']
    
    # Round ratings to 1 decimal place
    rating_cols = ['off_rating', 'def_rating', 'net_rating']
    for col in rating_cols:
        ratings[col] = ratings[col].round(1)
    
    return ratings


def clean_final_output(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and format the final output table."""
    
    if df.empty:
        return _create_empty_ratings()
    
    # Expand lineup_players tuple into separate columns
    player_data = pd.DataFrame(
        df['lineup_players'].tolist(), 
        columns=['player_1', 'player_2', 'player_3', 'player_4', 'player_5']
    )
    
    # Combine with other data
    result = pd.concat([df.drop('lineup_players', axis=1), player_data], axis=1)
    
    # Select and order final columns
    final_cols = [
        'team', 'player_1', 'player_2', 'player_3', 'player_4', 'player_5',
        'off_poss', 'def_poss', 'off_rating', 'def_rating', 'net_rating'
    ]
    
    result = result[final_cols]
    
    # Sort by team, then by most possessions, then by net rating
    result = result.sort_values([
        'team', 'off_poss', 'net_rating'
    ], ascending=[True, False, False]).reset_index(drop=True)
    
    # Ensure player IDs are integers
    player_cols = ['player_1', 'player_2', 'player_3', 'player_4', 'player_5']
    for col in player_cols:
        result[col] = pd.to_numeric(result[col], errors='coerce').astype('Int64')
    
    return result


def _create_empty_ratings() -> pd.DataFrame:
    """Create empty DataFrame with correct structure."""
    columns = [
        'team', 'player_1', 'player_2', 'player_3', 'player_4', 'player_5',
        'off_poss', 'def_poss', 'off_rating', 'def_rating', 'net_rating'
    ]
    return pd.DataFrame(columns=columns)


def get_lineup_summary(ratings_df: pd.DataFrame) -> Dict:
    """Generate summary statistics for lineup ratings."""
    
    if ratings_df.empty:
        return {}
    
    return {
        'total_lineups': len(ratings_df),
        'teams': sorted(ratings_df['team'].unique()),
        'avg_off_rating': ratings_df['off_rating'].mean(),
        'avg_def_rating': ratings_df['def_rating'].mean(),
        'best_net_rating': ratings_df['net_rating'].max(),
        'worst_net_rating': ratings_df['net_rating'].min(),
        'total_possessions': ratings_df['off_poss'].sum(),
        'lineups_with_both_stats': (
            (ratings_df['off_poss'] > 0) & (ratings_df['def_poss'] > 0)
        ).sum()
    }


def filter_lineups(ratings_df: pd.DataFrame, min_possessions: int = 10) -> pd.DataFrame:
    """Filter lineups by minimum possession threshold for statistical significance."""
    
    if ratings_df.empty:
        return ratings_df
    
    # Filter lineups with sufficient sample size
    filtered = ratings_df[
        (ratings_df['off_poss'] >= min_possessions) |
        (ratings_df['def_poss'] >= min_possessions)
    ].copy()
    
    print(f"Filtered to {len(filtered)} lineups with >= {min_possessions} possessions")
    return filtered


if __name__ == '__main__':
    # Test the complete pipeline
    print("Testing complete lineup analytics pipeline...")
    
    # Import required modules
    import sys
    sys.path.append('../..')
    
    try:
        from lineup_states import extract_lineup_states
        from possessions import extract_possessions  
        from lineup_possessions import match_lineups_to_possessions
        
        # Load test data
        box_score_df = pd.read_csv("../../data/box_HOU-DAL.csv")
        pbp_df = pd.read_csv("../../data/pbp_HOU-DAL.csv")
        
        print(f"Loaded {len(box_score_df)} box score and {len(pbp_df)} PBP records")
        
        # Run complete pipeline
        print("\nStep 1: Extracting lineup states...")
        lineup_states_df = extract_lineup_states(box_score_df, pbp_df)
        
        print("\nStep 2: Extracting possessions...")
        possessions_df = extract_possessions(pbp_df)
        
        print("\nStep 3: Matching lineups to possessions...")
        lineup_possessions_df = match_lineups_to_possessions(lineup_states_df, possessions_df)
        
        print("\nStep 4: Calculating lineup ratings...")
        lineup_ratings_df = calculate_lineup_ratings(lineup_possessions_df)
        
        # Show results
        if not lineup_ratings_df.empty:
            print(f"\n🏀 FINAL RESULTS 🏀")
            print(f"Generated ratings for {len(lineup_ratings_df)} unique 5-man lineups")
            
            # Summary stats
            summary = get_lineup_summary(lineup_ratings_df)
            print(f"\nSummary:")
            for key, value in summary.items():
                print(f"  {key}: {value}")
            
            # Show top lineups by net rating
            print(f"\nTop 5 lineups by net rating:")
            top_lineups = lineup_ratings_df.head()
            display_cols = ['team', 'player_1', 'player_2', 'off_poss', 'def_poss', 
                           'off_rating', 'def_rating', 'net_rating']
            print(top_lineups[display_cols])
            
            # Show filtered results (statistical significance)
            filtered_df = filter_lineups(lineup_ratings_df, min_possessions=5)
            if not filtered_df.empty:
                print(f"\nFiltered results (min 5 possessions):")
                print(filtered_df[display_cols].head())
            
            print(f"\n✅ Pipeline complete! Final table ready for analysis.")
            
        else:
            print(f"\n❌ No lineup ratings generated - check input data")
            
    except ImportError as e:
        print(f"Import error: {e}")
        print("Ensure all previous steps are implemented and accessible")
    except Exception as e:
        print(f"Pipeline error: {e}")
        import traceback
        traceback.print_exc()