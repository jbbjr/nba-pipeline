# impactr.py

"""
Calculate final player impact table combining possession counts and rim defense stats.
"""

import pandas as pd


def calculate_impact(rim_defense_df: pd.DataFrame, 
                    possession_counts_df: pd.DataFrame, 
                    box_score_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate final player impact table.
    
    Args:
        rim_defense_df: Rim defense stats from rim defense tracker
        possession_counts_df: Possession counts from possession analyzer  
        box_score_df: Box score data for player names and teams
        
    Returns:
        DataFrame with final impact table containing:
        [Player ID, Player Name, Team, Offensive possessions played, 
         Defensive possessions played, Opponent rim FG% when on court,
         Opponent rim FG% when off court, Opponent rim FG% on/off difference]
    """
    
    print(f"IMPACT DEBUG: Combining data for {len(rim_defense_df)} players with rim defense stats")
    
    # Start with rim defense stats as base
    impact_table = rim_defense_df.copy()
    
    # Merge with possession counts
    impact_table = impact_table.merge(
        possession_counts_df[['playerId', 'offensive_possessions', 'defensive_possessions']], 
        on='playerId', 
        how='left'
    )
    
    # Merge with player names and team info from box score
    impact_table = impact_table.merge(
        box_score_df[['nbaId', 'name', 'team']], 
        left_on='playerId', 
        right_on='nbaId',
        how='left'
    )
    
    # Create final table with target column structure
    final_table = pd.DataFrame({
        'Player ID': impact_table['playerId'],
        'Player Name': impact_table['name'],
        'Team': impact_table['team'],
        'Offensive possessions played': impact_table['offensive_possessions'],
        'Defensive possessions played': impact_table['defensive_possessions'],
        'Opponent rim FG% when player ON court': impact_table['rim_fg_pct_on'],
        'Opponent rim FG% when player OFF court': impact_table['rim_fg_pct_off'], 
        'Opponent rim FG% on/off difference (on-off)': impact_table['rim_fg_pct_diff']
    })
    
    # Round percentages for readability
    percentage_cols = [
        'Opponent rim FG% when player ON court',
        'Opponent rim FG% when player OFF court', 
        'Opponent rim FG% on/off difference (on-off)'
    ]
    
    for col in percentage_cols:
        final_table[col] = final_table[col].round(3)
    
    # Sort by defensive impact (most negative difference = best defenders)
    final_table = final_table.sort_values('Opponent rim FG% on/off difference (on-off)', ascending=True)
    
    print(f"IMPACT DEBUG: Generated final table with {len(final_table)} players")
    
    return final_table


def validate_final_table(final_table: pd.DataFrame):
    """Validate the final impact table."""
    
    print(f"\n=== FINAL TABLE VALIDATION ===")
    
    # Check for completeness
    total_players = len(final_table)
    complete_records = final_table.dropna(subset=[
        'Offensive possessions played', 
        'Defensive possessions played',
        'Opponent rim FG% when player ON court',
        'Opponent rim FG% when player OFF court'
    ])
    
    print(f"Total players in final table: {total_players}")
    print(f"Players with complete data: {len(complete_records)}")
    print(f"Data completeness: {len(complete_records)/total_players*100:.1f}%")
    
    # Show top defenders (most negative on/off difference)
    print(f"\n=== TOP 10 RIM DEFENDERS (Best On/Off Impact) ===")
    top_defenders = final_table.head(10)
    
    for i, (_, player) in enumerate(top_defenders.iterrows(), 1):
        on_pct = player['Opponent rim FG% when player ON court']
        off_pct = player['Opponent rim FG% when player OFF court'] 
        diff = player['Opponent rim FG% on/off difference (on-off)']
        poss = player['Defensive possessions played']
        
        print(f"{i:2d}. {player['Player Name']} ({player['Team']})")
        print(f"     On: {on_pct:.3f}, Off: {off_pct:.3f}, Diff: {diff:+.3f}")
        print(f"     Defensive possessions: {poss}")
        print()
    
    # Show summary statistics
    print(f"=== SUMMARY STATISTICS ===")
    avg_on = final_table['Opponent rim FG% when player ON court'].mean()
    avg_off = final_table['Opponent rim FG% when player OFF court'].mean()
    avg_diff = final_table['Opponent rim FG% on/off difference (on-off)'].mean()
    
    print(f"Average rim FG% allowed when on court: {avg_on:.3f}")
    print(f"Average rim FG% allowed when off court: {avg_off:.3f}")
    print(f"Average on/off difference: {avg_diff:+.3f}")
    
    # Show team breakdown
    print(f"\n=== TEAM BREAKDOWN ===")
    team_summary = final_table.groupby('Team').agg({
        'Opponent rim FG% when player ON court': 'mean',
        'Opponent rim FG% when player OFF court': 'mean',
        'Opponent rim FG% on/off difference (on-off)': 'mean',
        'Player Name': 'count'
    }).round(3)
    team_summary.columns = ['Avg ON FG%', 'Avg OFF FG%', 'Avg Diff', 'Players']
    
    print(team_summary.to_string())
    
    return complete_records


def export_final_table(final_table: pd.DataFrame, filename: str = "player_rim_defense_impact.csv"):
    """Export the final table to CSV."""
    
    final_table.to_csv(filename, index=False)
    print(f"\n=== TABLE EXPORTED ===")
    print(f"Final table saved to: {filename}")
    print(f"Columns: {list(final_table.columns)}")
    print(f"Shape: {final_table.shape}")


if __name__ == "__main__":
    # Load test data
    box_score_df = pd.read_csv("../../data/box_HOU-DAL.csv")
    pbp_df = pd.read_csv("../../data/pbp_HOU-DAL.csv")
    
    print("Calculating final player impact table...")
    
    # Get all required inputs from previous modules
    from shot_distance import calculate_shot_distances
    from court_time import track_lineup_states  
    from possessions import analyze_possessions
    from rim_defense import track_rim_defense
    
    print("Step 1: Calculating shot distances...")
    enhanced_pbp = calculate_shot_distances(pbp_df)
    
    print("Step 2: Tracking lineup states...")
    lineup_intervals = track_lineup_states(box_score_df, pbp_df)
    
    print("Step 3: Analyzing possessions...")
    possession_counts = analyze_possessions(box_score_df, pbp_df, lineup_intervals)
    
    print("Step 4: Tracking rim defense...")
    rim_defense_stats = track_rim_defense(enhanced_pbp, lineup_intervals)
    
    print("Step 5: Calculating final impact...")
    final_impact_table = calculate_impact(rim_defense_stats, possession_counts, box_score_df)
    
    # Validate results
    validation_results = validate_final_table(final_impact_table)
    
    # Show final table preview
    print(f"\n=== FINAL TABLE PREVIEW ===")
    print("First 10 rows of final impact table:")
    print(final_impact_table.head(10).to_string(index=False))
    
    print(f"\nPipeline complete! Final table contains {len(final_impact_table)} players.")