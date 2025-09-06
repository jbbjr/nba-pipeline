# shot_distanc.py

"""
Calculate shot distances and identify rim shots from play-by-play data.
"""

import pandas as pd
import numpy as np
from typing import Tuple


def calculate_shot_distances(pbp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate shot distances and identify rim shots from PBP data.
    
    Args:
        pbp_df: Play-by-play dataframe containing locX, locY coordinates
        
    Returns:
        Enhanced PBP dataframe with shot_distance and is_rim_shot columns
    """
    # Create copy to avoid mutating input
    enhanced_pbp = pbp_df.copy()
    
    # Only process shot attempts (filter by relevant msgTypes)
    shot_mask = _is_shot_attempt(enhanced_pbp)
    
    # Calculate distances only for shots
    enhanced_pbp.loc[shot_mask, 'shot_distance'] = _calculate_distance_from_basket(
        enhanced_pbp.loc[shot_mask, 'locX'].values,
        enhanced_pbp.loc[shot_mask, 'locY'].values
    )
    
    # Mark rim shots (≤4 feet)
    enhanced_pbp['is_rim_shot'] = (
        shot_mask & 
        (enhanced_pbp['shot_distance'] <= 4.0)
    )
    
    # Fill non-shots with appropriate values
    enhanced_pbp['shot_distance'] = enhanced_pbp['shot_distance'].fillna(-1)
    enhanced_pbp['is_rim_shot'] = enhanced_pbp['is_rim_shot'].fillna(False)
    
    return enhanced_pbp


def _is_shot_attempt(pbp_df: pd.DataFrame) -> pd.Series:
    """
    Identify shot attempts from PBP data based on message types.
    
    Typical NBA shot msgTypes: 1 (Made Field Goal), 2 (Missed Field Goal)
    """
    shot_msg_types = [1, 2]  # Made and missed field goals
    return pbp_df['msgType'].isin(shot_msg_types)


def _calculate_distance_from_basket(loc_x: np.ndarray, loc_y: np.ndarray) -> np.ndarray:
    """
    Calculate Euclidean distance from basket location.
    
    Assumes NBA coordinate system where basket is at (0, 0).
    Distance returned in feet.
    """
    # NBA court coordinate system typically has basket at origin
    basket_x, basket_y = 0, 0
    
    # Convert coordinates to feet (assuming input is in tenths of feet)
    x_feet = loc_x / 10.0
    y_feet = loc_y / 10.0
    
    # Calculate Euclidean distance
    distances = np.sqrt((x_feet - basket_x) ** 2 + (y_feet - basket_y) ** 2)
    
    return distances

if __name__ == "__main__":
    # Load test data
    box_score_df = pd.read_csv("../../data/box_HOU-DAL.csv")
    pbp_df = pd.read_csv("../../data/pbp_HOU-DAL.csv")
    
    print("Original PBP shape:", pbp_df.shape)
    print("Columns before:", pbp_df.columns.tolist())
    
    # Run shot distance calculation
    enhanced_pbp = calculate_shot_distances(pbp_df)
    
    print("\nEnhanced PBP shape:", enhanced_pbp.shape)
    print("New columns:", ['shot_distance', 'is_rim_shot'])
    
    # Validation checks
    shot_attempts = enhanced_pbp[enhanced_pbp['shot_distance'] >= 0]
    rim_shots = enhanced_pbp[enhanced_pbp['is_rim_shot']]
    
    print(f"\nTotal plays: {len(enhanced_pbp)}")
    print(f"Shot attempts: {len(shot_attempts)}")
    print(f"Rim shots: {len(rim_shots)}")
    print(f"Rim shot percentage: {len(rim_shots)/len(shot_attempts)*100:.1f}%")
    
    # Sample rim shots
    if len(rim_shots) > 0:
        print("\nSample rim shots:")
        print(rim_shots[['description', 'locX', 'locY', 'shot_distance']].head())
    
    # Distance distribution for shots
    if len(shot_attempts) > 0:
        print(f"\nShot distance stats:")
        print(f"Min: {shot_attempts['shot_distance'].min():.1f} ft")
        print(f"Max: {shot_attempts['shot_distance'].max():.1f} ft") 
        print(f"Mean: {shot_attempts['shot_distance'].mean():.1f} ft")