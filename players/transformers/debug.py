# debug_inference_logic.py

"""
Debug why the inference logic isn't detecting Klay's re-entries.
"""

import pandas as pd
from court_time import _get_substitution_events, _get_player_activities, _get_team_mapping


def debug_inference_failure():
    """Debug why we're not inferring Klay's re-entries."""
    
    # Load data
    box_score_df = pd.read_csv("../../data/box_HOU-DAL.csv")
    pbp_df = pd.read_csv("../../data/pbp_HOU-DAL.csv")
    
    # Get the same data our hybrid tracker uses
    substitutions = _get_substitution_events(pbp_df)
    activities = _get_player_activities(pbp_df)
    team_mapping = _get_team_mapping(box_score_df)
    
    klay_id = 202691
    
    print("=== INFERENCE LOGIC DEBUG ===")
    print(f"Klay Thompson ID: {klay_id}")
    print(f"Klay's team: {team_mapping.get(klay_id)}")
    
    # Track player status like our inference function does
    player_status = {}
    
    print(f"\n=== TRACKING KLAY'S STATUS ===")
    
    # Process substitutions to track status
    for i, (_, sub) in enumerate(substitutions.iterrows()):
        player_out = int(sub['playerId1'])
        player_in = int(sub['playerId2'])
        
        if player_out == klay_id or player_in == klay_id:
            if player_out == klay_id:
                print(f"Sub {i+1}: Klay OUT at period {sub['period']}, wallClock {sub['wallClockInt']}")
                player_status[klay_id] = {
                    'status': 'OUT', 
                    'period': sub['period'], 
                    'wallClock': sub['wallClockInt']
                }
            else:
                print(f"Sub {i+1}: Klay IN at period {sub['period']}, wallClock {sub['wallClockInt']}")
                player_status[klay_id] = {
                    'status': 'IN', 
                    'period': sub['period'], 
                    'wallClock': sub['wallClockInt']
                }
    
    print(f"\nFinal status after substitutions: {player_status.get(klay_id, 'Not found')}")
    
    # Check Klay's activities
    klay_activities = activities[activities['playerId'] == klay_id].copy()
    print(f"\n=== KLAY'S ACTIVITIES ===")
    print(f"Total activities found: {len(klay_activities)}")
    
    if len(klay_activities) > 0:
        print("First 10 activities:")
        print(klay_activities[['period', 'wallClockInt', 'msgType', 'description']].head(10))
        
        # Test inference logic manually
        print(f"\n=== MANUAL INFERENCE TEST ===")
        
        if klay_id in player_status:
            last_status = player_status[klay_id]
            print(f"Last known status: {last_status}")
            
            for i, (_, activity) in enumerate(klay_activities.iterrows()):
                # Apply the same logic as our inference function
                if (last_status['status'] == 'OUT' and 
                    (activity['period'] > last_status['period'] or 
                     (activity['period'] == last_status['period'] and 
                      activity['wallClockInt'] > last_status['wallClock']))):
                    
                    print(f"Activity {i+1}: SHOULD INFER RE-ENTRY")
                    print(f"  Period {activity['period']}, wallClock {activity['wallClockInt']}")
                    print(f"  Last OUT was period {last_status['period']}, wallClock {last_status['wallClock']}")
                    print(f"  Description: {activity['description']}")
                    
                    # This should have triggered inference
                    if i == 0:  # First qualifying activity
                        print(f"  -> This should be the first inferred re-entry!")
                        break
                else:
                    print(f"Activity {i+1}: No inference triggered")
                    print(f"  Last status: {last_status['status']}")
                    print(f"  Activity period {activity['period']} vs last period {last_status['period']}")
                    print(f"  Activity wallClock {activity['wallClockInt']} vs last wallClock {last_status['wallClock']}")
    
    # Check if there are any issues with the activity extraction
    print(f"\n=== ACTIVITY EXTRACTION VALIDATION ===")
    
    # Manual check for Klay in period 2 (where we know he played)
    period_2_pbp = pbp_df[pbp_df['period'] == 2]
    klay_period_2_manual = period_2_pbp[
        (period_2_pbp['playerId1'] == klay_id) |
        (period_2_pbp['playerId2'] == klay_id) |
        (period_2_pbp['playerId3'] == klay_id)
    ]
    
    print(f"Manual check - Klay events in period 2: {len(klay_period_2_manual)}")
    print(f"Our activity extraction - Klay period 2 activities: {len(klay_activities[klay_activities['period'] == 2])}")
    
    if len(klay_period_2_manual) != len(klay_activities[klay_activities['period'] == 2]):
        print("❌ MISMATCH: Activity extraction is missing events!")
        print("Manual events:")
        print(klay_period_2_manual[['period', 'wallClockInt', 'msgType', 'description']].head())
    else:
        print("✅ Activity extraction looks correct")


if __name__ == "__main__":
    debug_inference_failure()