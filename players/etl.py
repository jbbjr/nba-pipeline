# players/etl.py

"""
Prefect pipeline for calculating player rim defense impact metrics
from game PBP and boxscore data with court time tracking
"""
from pathlib import Path
from typing import Dict, Union

import pandas as pd
from prefect import task, flow
from prefect.logging import get_run_logger

import sys
from pathlib import Path

# Add parent directory to Python path for absolute imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.schemas import validate_dataframe
from transformers.shot_distance import calculate_shot_distances
from transformers.court_time import track_lineup_states
from transformers.possessions import analyze_possessions
from transformers.rim_defense import track_rim_defense
from transformers.impact import calculate_impact


@task(name="ingest-players", retries=3, retry_delay_seconds=3)
def ingest_csv(file_path: Union[str, Path], schema_name: str, **read_csv_kwargs) -> pd.DataFrame:
    """
    Ingest a CSV file and validate against schema.
    
    Args:
        file_path: Path to CSV file
        schema_name: Name of schema to validate against
        **read_csv_kwargs: Additional arguments for pd.read_csv()
    
    Returns:
        Validated DataFrame
    """
    logger = get_run_logger()
    file_path = Path(file_path)
    
    logger.info(f"Reading {schema_name}: {file_path}")
    df = pd.read_csv(file_path, **read_csv_kwargs)
    logger.info(f"Loaded {len(df)} rows")
    
    validated_df = validate_dataframe(df, schema_name)
    logger.info(f"Validation successful")
    
    return validated_df


@task(name="calculate-shot-distances")
def calculate_shot_distances_task(pbp_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate shot distances and identify rim shots."""
    logger = get_run_logger()
    logger.info("Calculating shot distances...")
    
    enhanced_pbp = calculate_shot_distances(pbp_df)
    
    # Log shot statistics
    shot_attempts = enhanced_pbp[enhanced_pbp['shot_distance'] >= 0]
    rim_shots = enhanced_pbp[enhanced_pbp['is_rim_shot']]
    
    logger.info(f"Processed {len(shot_attempts)} shot attempts")
    logger.info(f"Identified {len(rim_shots)} rim shots ({len(rim_shots)/len(shot_attempts)*100:.1f}%)")
    
    return enhanced_pbp


@task(name="track-court-time")
def track_court_time_task(box_score_df: pd.DataFrame, pbp_df: pd.DataFrame) -> pd.DataFrame:
    """Track when each player was on court."""
    logger = get_run_logger()
    logger.info("Tracking player court time...")
    
    lineup_intervals = track_lineup_states(box_score_df, pbp_df)
    logger.info(f"Generated {len(lineup_intervals)} court time intervals for {lineup_intervals['playerId'].nunique()} players")
    
    return lineup_intervals


@task(name="analyze-possessions")
def analyze_possessions_task(box_score_df: pd.DataFrame, 
                           pbp_df: pd.DataFrame, 
                           lineup_intervals: pd.DataFrame) -> pd.DataFrame:
    """Analyze possessions per player."""
    logger = get_run_logger()
    logger.info("Analyzing player possessions...")
    
    possession_counts = analyze_possessions(box_score_df, pbp_df, lineup_intervals)
    
    total_off_poss = possession_counts['offensive_possessions'].sum()
    total_def_poss = possession_counts['defensive_possessions'].sum()
    
    logger.info(f"Calculated possessions for {len(possession_counts)} players")
    logger.info(f"Total offensive possessions: {total_off_poss}")
    logger.info(f"Total defensive possessions: {total_def_poss}")
    
    return possession_counts


@task(name="track-rim-defense")
def track_rim_defense_task(enhanced_pbp_df: pd.DataFrame, 
                          lineup_intervals: pd.DataFrame) -> pd.DataFrame:
    """Track rim defense statistics for each player."""
    logger = get_run_logger()
    logger.info("Tracking rim defense statistics...")
    
    rim_defense_stats = track_rim_defense(enhanced_pbp_df, lineup_intervals)
    
    # Log rim defense summary
    total_on_attempts = rim_defense_stats['rim_fga_on'].sum()
    total_off_attempts = rim_defense_stats['rim_fga_off'].sum()
    players_with_data = len(rim_defense_stats[rim_defense_stats['rim_fga_on'] > 0])
    
    logger.info(f"Calculated rim defense for {len(rim_defense_stats)} players")
    logger.info(f"Players with on-court rim defense data: {players_with_data}")
    logger.info(f"Total rim attempts tracked (on/off): {total_on_attempts}/{total_off_attempts}")
    
    return rim_defense_stats


@task(name="calculate-player-impact")
def calculate_player_impact_task(rim_defense_df: pd.DataFrame,
                                possession_counts_df: pd.DataFrame,
                                box_score_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate final player impact metrics."""
    logger = get_run_logger()
    logger.info("Calculating player impact metrics...")
    
    impact_table = calculate_impact(rim_defense_df, possession_counts_df, box_score_df)
    
    # Log impact summary
    complete_records = impact_table.dropna(subset=[
        'Offensive possessions played', 
        'Defensive possessions played',
        'Opponent rim FG% when player ON court',
        'Opponent rim FG% when player OFF court'
    ])
    
    logger.info(f"Generated impact table for {len(impact_table)} players")
    logger.info(f"Players with complete data: {len(complete_records)} ({len(complete_records)/len(impact_table)*100:.1f}%)")
    
    # Log top defenders
    if len(complete_records) > 0:
        best_impact = complete_records['Opponent rim FG% on/off difference (on-off)'].min()
        worst_impact = complete_records['Opponent rim FG% on/off difference (on-off)'].max()
        logger.info(f"Best defensive impact: {best_impact:+.3f}")
        logger.info(f"Worst defensive impact: {worst_impact:+.3f}")
    
    return impact_table


@task(name="export-parquet")
def export_parquet(df: pd.DataFrame, output_path: Union[str, Path]) -> Path:
    """Export DataFrame to parquet file."""
    logger = get_run_logger()
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_parquet(output_path, index=False)
    logger.info(f"Exported {len(df)} records to {output_path}")
    
    return output_path


@flow(name="player-impact-processor")
def process_player_impact(file_paths: Dict[str, str], 
                         output_path: str = "output/player_rim_defense_impact.parquet"):
    """
    Complete ETL pipeline for player rim defense impact analytics.
    
    Args:
        file_paths: Dictionary mapping schema names to file paths
        output_path: Where to save the final parquet file
    
    Returns:
        Path to the output parquet file
    """
    logger = get_run_logger()
    logger.info("Starting player impact analytics pipeline...")
    
    # Step 1: Ingest all required data
    logger.info("Step 1: Ingesting data files...")
    box_score_df = ingest_csv(file_paths["box_score"], "box_score")
    pbp_df = ingest_csv(file_paths["pbp"], "pbp")
    
    # Step 2: Calculate shot distances and identify rim shots
    logger.info("Step 2: Calculating shot distances...")
    enhanced_pbp_df = calculate_shot_distances_task(pbp_df)
    
    # Step 3: Track when players were on court
    logger.info("Step 3: Tracking player court time...")
    lineup_intervals_df = track_court_time_task(box_score_df, pbp_df)
    
    # Step 4: Analyze possessions per player
    logger.info("Step 4: Analyzing player possessions...")
    possession_counts_df = analyze_possessions_task(
        box_score_df, pbp_df, lineup_intervals_df
    )
    
    # Step 5: Track rim defense statistics
    logger.info("Step 5: Tracking rim defense...")
    rim_defense_df = track_rim_defense_task(enhanced_pbp_df, lineup_intervals_df)
    
    # Step 6: Calculate final impact metrics
    logger.info("Step 6: Calculating player impact...")
    impact_table_df = calculate_player_impact_task(
        rim_defense_df, possession_counts_df, box_score_df
    )
    
    # Step 7: Export to parquet
    logger.info("Step 7: Exporting results...")
    output_file = export_parquet(impact_table_df, output_path)
    
    logger.info(f"Pipeline complete! Results saved to {output_file}")
    return output_file


@flow(name="player-impact-validation")
def validate_player_impact_pipeline(file_paths: Dict[str, str]):
    """
    Validation flow that runs the complete pipeline and validates results.
    """
    logger = get_run_logger()
    logger.info("Running validation pipeline...")
    
    # Run the main pipeline
    output_file = process_player_impact(file_paths)
    
    # Load and validate results
    impact_table = pd.read_parquet(output_file)
    
    logger.info("=== PIPELINE VALIDATION RESULTS ===")
    logger.info(f"Total players: {len(impact_table)}")
    
    # Check data completeness
    complete_records = impact_table.dropna(subset=[
        'Offensive possessions played', 
        'Defensive possessions played',
        'Opponent rim FG% when player ON court',
        'Opponent rim FG% when player OFF court'
    ])
    
    logger.info(f"Players with complete data: {len(complete_records)}")
    logger.info(f"Data completeness: {len(complete_records)/len(impact_table)*100:.1f}%")
    
    # Show top defenders
    if len(complete_records) > 0:
        top_defenders = complete_records.nsmallest(5, 'Opponent rim FG% on/off difference (on-off)')
        logger.info("Top 5 rim defenders by impact:")
        for i, (_, player) in enumerate(top_defenders.iterrows(), 1):
            diff = player['Opponent rim FG% on/off difference (on-off)']
            logger.info(f"  {i}. {player['Player Name']}: {diff:+.3f}")
    
    # Validation checks
    validation_passed = True
    
    # Check 1: Reasonable data completeness
    if len(complete_records) / len(impact_table) < 0.7:
        logger.warning("Data completeness < 70% - check court time tracking")
        validation_passed = False
    
    # Check 2: Reasonable possession counts
    avg_off_poss = complete_records['Offensive possessions played'].mean()
    avg_def_poss = complete_records['Defensive possessions played'].mean()
    
    if avg_off_poss < 20 or avg_def_poss < 20:
        logger.warning(f"Low average possessions (off: {avg_off_poss:.1f}, def: {avg_def_poss:.1f})")
        validation_passed = False
    
    # Check 3: Reasonable rim FG% values
    avg_rim_fg_pct = complete_records['Opponent rim FG% when player ON court'].mean()
    if avg_rim_fg_pct < 0.4 or avg_rim_fg_pct > 0.8:
        logger.warning(f"Unusual rim FG% average: {avg_rim_fg_pct:.3f}")
        validation_passed = False
    
    if validation_passed:
        logger.info("All validation checks passed!")
    else:
        logger.warning("Some validation checks failed - review results")
    
    return validation_passed


if __name__ == '__main__':
    file_paths = {
        "box_score": "data/box_HOU-DAL.csv",
        "pbp": "data/pbp_HOU-DAL.csv"
    }
    
    # Run the complete pipeline with validation
    validation_passed = validate_player_impact_pipeline(file_paths)
    
    if validation_passed:
        print("\nPlayer impact pipeline completed successfully!")
        print("Results saved to: output/player_rim_defense_impact.parquet")
    else:
        print("\nPipeline completed with validation warnings - check logs")