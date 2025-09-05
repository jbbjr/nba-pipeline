# lineups/etl.py

"""
Prefect pipeline for building unique five-man lineups from 
game PBP and boxscore, with derived and aggregated metrics
"""
from pathlib import Path
from typing import Dict, Union

import pandas as pd
from prefect import task, flow
from prefect.logging import get_run_logger

import sys
from pathlib import Path

# Add parent directory to Python path for absolute imports
# In an actual architecture / data platform this would obviosuly be unnecessary 
# because we read directly from S3 and the repo structure would look different
sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.schemas import validate_dataframe
from transformers.lineup_states import extract_lineup_states
from transformers.possessions import extract_possessions
from transformers.lineup_possessions import match_lineups_to_possessions
from transformers.lineup_ratings import calculate_lineup_ratings


@task(name="ingest", retries=3, retry_delay_seconds=3)  # Fixed typo: retires -> retries
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


@task(name="extract-lineup-states")
def extract_lineup_states_task(box_score_df: pd.DataFrame, pbp_df: pd.DataFrame) -> pd.DataFrame:
    """Extract timeline of 5-man lineups."""
    logger = get_run_logger()
    logger.info("Extracting lineup states...")
    
    lineup_states = extract_lineup_states(box_score_df, pbp_df)
    logger.info(f"Generated {len(lineup_states)} lineup states")
    
    return lineup_states


@task(name="extract-possessions")
def extract_possessions_task(pbp_df: pd.DataFrame) -> pd.DataFrame:
    """Extract possession-level data."""
    logger = get_run_logger()
    logger.info("Extracting possessions...")
    
    possessions = extract_possessions(pbp_df)
    logger.info(f"Generated {len(possessions)} possessions")
    
    return possessions


@task(name="match-lineups-possessions")
def match_lineups_possessions_task(lineup_states_df: pd.DataFrame, 
                                  possessions_df: pd.DataFrame) -> pd.DataFrame:
    """Match lineups to possessions."""
    logger = get_run_logger()
    logger.info("Matching lineups to possessions...")
    
    lineup_possessions = match_lineups_to_possessions(lineup_states_df, possessions_df)
    logger.info(f"Matched {len(lineup_possessions)} possession-lineup pairs")
    
    return lineup_possessions


@task(name="calculate-lineup-ratings")
def calculate_lineup_ratings_task(lineup_possessions_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate final lineup ratings."""
    logger = get_run_logger()
    logger.info("Calculating lineup ratings...")
    
    lineup_ratings = calculate_lineup_ratings(lineup_possessions_df)
    logger.info(f"Generated ratings for {len(lineup_ratings)} unique lineups")
    
    return lineup_ratings


@task(name="export-parquet")
def export_parquet(df: pd.DataFrame, output_path: Union[str, Path]) -> Path:
    """Export DataFrame to parquet file."""
    logger = get_run_logger()
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_parquet(output_path, index=False)
    logger.info(f"Exported {len(df)} records to {output_path}")
    
    return output_path


@flow(name="lineups-processor")
def process_game_data(file_paths: Dict[str, str], output_path: str = "output/lineup_ratings.parquet"):
    """
    Complete ETL pipeline for lineup analytics.
    
    Args:
        file_paths: Dictionary mapping schema names to file paths
        output_path: Where to save the final parquet file
    
    Returns:
        Path to the output parquet file
    """
    logger = get_run_logger()
    logger.info("Starting lineup analytics pipeline...")
    
    # Step 1: Ingest all data
    logger.info("Step 1: Ingesting data files...")
    dataframes = {}
    for schema_name, file_path in file_paths.items():
        dataframes[schema_name] = ingest_csv(file_path, schema_name)
    
    # Step 2: Extract lineup states
    logger.info("Step 2: Extracting lineup states...")
    lineup_states_df = extract_lineup_states_task(
        dataframes["box_score"], 
        dataframes["pbp"]
    )
    
    # Step 3: Extract possessions
    logger.info("Step 3: Extracting possessions...")
    possessions_df = extract_possessions_task(dataframes["pbp"])
    
    # Step 4: Match lineups to possessions
    logger.info("Step 4: Matching lineups to possessions...")
    lineup_possessions_df = match_lineups_possessions_task(
        lineup_states_df, 
        possessions_df
    )
    
    # Step 5: Calculate lineup ratings
    logger.info("Step 5: Calculating lineup ratings...")
    lineup_ratings_df = calculate_lineup_ratings_task(lineup_possessions_df)
    
    # Step 6: Export to parquet
    logger.info("Step 6: Exporting results...")
    output_file = export_parquet(lineup_ratings_df, output_path)
    
    logger.info(f"Pipeline complete! Results saved to {output_file}")
    return output_file


if __name__ == '__main__':
    file_paths = {
        "box_score": "data/box_HOU-DAL.csv",
        "pbp": "data/pbp_HOU-DAL.csv", 
        "pbp_action_types": "data/pbp_action_types.csv",
        "pbp_event_msg_types": "data/pbp_event_msg_types.csv",
        "pbp_option_types": "data/pbp_option_types.csv"
    }
    
    # Run the complete pipeline
    output_file = process_game_data(
        file_paths=file_paths,
        output_path="output/lineup_ratings.parquet"
    )