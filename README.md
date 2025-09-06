# NBA Analytics Pipelines

Serving layer ETL pipelines for calculating basketball performance metrics. Assumes preprocessed CSV inputs and outputs analysis-ready parquet tables.

## Pipelines

### Lineup Ratings (`lineups/`)
Calculates 5-man lineup offensive/defensive ratings per 100 possessions.

### Player Impact (`players/`)
Calculates individual player rim defense impact metrics (on/off court opponent FG%).

## Architecture Notes

**Current Implementation**: Proof-of-concept serving layer that reads CSV files and outputs parquet tables for downstream analytics.

**Production Architecture**: Would read from S3 data lake, deploy as containerized services on ECS with CI/CD pipelines, and leverage persistent stint/lineup tables rather than calculating them as intermediate steps.

**Ideal Design**: Player court time intervals and lineup states should be materialized tables that multiple ETLs consume, rather than recalculating these complex transformations in each pipeline.

## Known Limitations

**Court Time Tracking Issues**: The `court_time.py` script has difficulty accurately reconstructing player minutes from substitution events and activity inference. Player stint calculations often don't match box score total minutes, particularly for bench players and complex substitution patterns.

**Small Sample Sizes**: Lineup metrics are noisy due to single-game sample sizes. Many lineups have <10 possessions, making ratings unreliable.

**Impact on Output Quality**: Due to court time inaccuracies and sample size constraints, outputs should be used as directional indicators rather than precise measurements.

## Data Requirements

Place CSV files in `data/` directory:
- `data/box_HOU-DAL.csv` - Box score data
- `data/pbp_HOU-DAL.csv` - Play-by-play data  
- `data/pbp_action_types.csv` - Event type mappings
- `data/pbp_event_msg_types.csv` - Message type mappings
- `data/pbp_option_types.csv` - Option type mappings

## Quick Start

### Run Complete Pipelines
```bash
# Lineup ratings pipeline
python ./lineups/etl.py

# Player impact pipeline  
python ./players/etl.py

# Player impact with validation
python -c "
from players.etl import validate_player_impact_pipeline
file_paths = {'box_score': 'data/box_HOU-DAL.csv', 'pbp': 'data/pbp_HOU-DAL.csv'}
validate_player_impact_pipeline(file_paths)
"
```

### Test Individual Modules
```bash
# Test schema validation
python ./shared/schemas.py

# Test court time tracking
python ./players/transformers/court_time.py

# Test rim defense calculations
python ./players/transformers/rim_defense.py

# Test lineup state extraction
python ./lineups/lineup_states.py

# Test possession analysis
python ./lineups/possessions.py
```

### Programmatic Usage
```python
# Lineup ratings
from lineups.etl import process_game_data
lineup_output = process_game_data(file_paths, "lineup_ratings.parquet")

# Player impact  
from players.etl import process_player_impact
player_output = process_player_impact(file_paths, "player_impact.parquet")
```

## Data Requirements

- Box score CSV (starting lineups, player stats)
- Play-by-play CSV (events, substitutions, coordinates)
- Reference CSVs (event type mappings)

See individual pipeline READMEs for detailed documentation and validation approaches.