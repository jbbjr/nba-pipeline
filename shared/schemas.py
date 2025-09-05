# schemas.py

"""
Pandera schemas for PBP and Box Score data validation.
"""
import pandera.pandas as pa
from pandera.typing import DataFrame, Series


class BoxScoreSchema(pa.DataFrameModel):
    """Schema for box_score DataFrame."""
    
    # Game identifiers
    gameId: Series[str]
    nbaGameId: Series[int]
    date: Series[str]
    season: Series[int]
    seasonType: Series[str]
    
    # Team info
    nbaTeamId: Series[int]
    team: Series[str]
    opponentId: Series[int]
    opponent: Series[str]
    
    # Game results
    teamPts: Series[int]
    oppPts: Series[int]
    teamMargin: Series[int]
    outcome: Series[str]
    isHome: Series[int]
    
    # Player info
    nbaId: Series[int]
    name: Series[str]
    jerseyNum: Series[int]
    gp: Series[int]
    gs: Series[int]
    startPos: Series[str] = pa.Field(nullable=True)
    isOnCourt: Series[int]
    boxScoreOrder: Series[int]
    
    # Time played
    minDisplay: Series[int]
    secDisplay: Series[int]
    min: Series[float]
    secPlayed: Series[int]
    
    # Shooting stats
    fgm: Series[int]
    fga: Series[int]
    ftm: Series[int]
    fta: Series[int]
    tpm: Series[int]
    tpa: Series[int]
    
    # Rebounding and other stats
    oreb: Series[int]
    dreb: Series[int]
    reb: Series[int]
    ast: Series[int]
    stl: Series[int]
    blk: Series[int]
    tov: Series[int]
    pf: Series[int]
    pts: Series[int]
    plusMinus: Series[int]
    blkA: Series[int]
    
    # Status
    gameStatus: Series[int]
    status: Series[str]
    notPlayingReason: Series[str] = pa.Field(nullable=True)
    notPlayingDescription: Series[str] = pa.Field(nullable=True)


class PbpSchema(pa.DataFrameModel):
    """Schema for play-by-play (pbp) DataFrame."""
    
    # Game identifiers
    gameId: Series[str]
    nbaGameId: Series[int]
    date: Series[str]
    season: Series[int]
    seasonType: Series[str]
    
    # Team info (nullable for some plays)
    nbaTeamId: Series[float] = pa.Field(nullable=True)
    team: Series[str] = pa.Field(nullable=True)
    opponent: Series[str] = pa.Field(nullable=True)
    
    # Play info
    offTeamId: Series[int]
    defTeamId: Series[int]
    pbpId: Series[int]
    period: Series[int]
    gameClock: Series[str]
    wallClock: Series[str]
    wallClockInt: Series[int]
    description: Series[str] = pa.Field(nullable=True)
    
    # Event classification
    msgType: Series[int]
    actionType: Series[int]
    option1: Series[int]
    option2: Series[int]
    option3: Series[int]
    option4: Series[int]
    
    # Score and location
    homeScore: Series[int]
    awayScore: Series[int]
    locX: Series[int]
    locY: Series[int]
    pts: Series[int]
    pbpOrder: Series[int]
    
    # Player involvement (nullable)
    playerId1: Series[float] = pa.Field(nullable=True)
    playerId2: Series[float] = pa.Field(nullable=True)
    playerId3: Series[float] = pa.Field(nullable=True)
    lastName1: Series[str] = pa.Field(nullable=True)
    lastName2: Series[str] = pa.Field(nullable=True)
    lastName3: Series[str] = pa.Field(nullable=True)
    statCategory1: Series[str] = pa.Field(nullable=True)
    statCategory2: Series[str] = pa.Field(nullable=True)


class PbpActionTypesSchema(pa.DataFrameModel):
    """Schema for pbp_action_types lookup table."""
    
    EventType: Series[int]
    ActionType: Series[int]
    Event: Series[str]
    Description: Series[str]


class PbpEventMsgTypesSchema(pa.DataFrameModel):
    """Schema for pbp_event_msg_types lookup table."""
    
    EventType: Series[int]
    Description: Series[str]


class PbpOptionTypesSchema(pa.DataFrameModel):
    """Schema for pbp_option_types lookup table."""
    
    Event: Series[str]
    EventType: Series[int]
    Option1: Series[str]
    Option2: Series[str] = pa.Field(nullable=True)
    Option3: Series[str] = pa.Field(nullable=True)
    Option4: Series[float] = pa.Field(nullable=True)
    Description: Series[str]


# Schema registry for easy access
SCHEMAS = {
    'box_score': BoxScoreSchema,
    'pbp': PbpSchema,
    'pbp_action_types': PbpActionTypesSchema,
    'pbp_event_msg_types': PbpEventMsgTypesSchema,
    'pbp_option_types': PbpOptionTypesSchema,
}


def validate_dataframe(df: DataFrame, schema_name: str) -> DataFrame:
    """Validate a DataFrame against a schema."""
    if schema_name not in SCHEMAS:
        raise ValueError(f"Unknown schema: {schema_name}")
    
    return SCHEMAS[schema_name].validate(df)


def get_schema(schema_name: str) -> pa.DataFrameModel:
    """Get a schema by name."""
    if schema_name not in SCHEMAS:
        raise ValueError(f"Unknown schema: {schema_name}")
    
    return SCHEMAS[schema_name]