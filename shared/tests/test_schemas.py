# shared/tests/test_schemas.py

import pytest
import pandas as pd
import numpy as np
from pandera.errors import SchemaError

from ..schemas import (
    BoxScoreSchema, PbpSchema, PbpActionTypesSchema, 
    PbpEventMsgTypesSchema, PbpOptionTypesSchema,
    validate_dataframe, get_schema, SCHEMAS
)


@pytest.fixture
def valid_box_score_data():
    """Valid BoxScore data for testing."""
    return pd.DataFrame({
        'gameId': ['001'],
        'nbaGameId': [1],
        'date': ['2024-01-01'],
        'season': [2024],
        'seasonType': ['Regular'],
        'nbaTeamId': [1610612745],
        'team': ['HOU'],
        'opponentId': [1610612742],
        'opponent': ['DAL'],
        'teamPts': [110],
        'oppPts': [105],
        'teamMargin': [5],
        'outcome': ['W'],
        'isHome': [1],
        'nbaId': [201935],
        'name': ['James Harden'],
        'jerseyNum': [1],
        'gp': [1],
        'gs': [1],
        'startPos': ['G'],
        'isOnCourt': [1],
        'boxScoreOrder': [1],
        'minDisplay': [35],
        'secDisplay': [30],
        'min': [35.5],
        'secPlayed': [2130],
        'fgm': [8],
        'fga': [15],
        'ftm': [5],
        'fta': [6],
        'tpm': [3],
        'tpa': [8],
        'oreb': [1],
        'dreb': [5],
        'reb': [6],
        'ast': [8],
        'stl': [2],
        'blk': [0],
        'tov': [3],
        'pf': [2],
        'pts': [24],
        'plusMinus': [8],
        'blkA': [0],
        'gameStatus': [3],
        'status': ['Active'],
        'notPlayingReason': [None],
        'notPlayingDescription': [None]
    })


@pytest.fixture
def valid_pbp_data():
    """Valid PBP data for testing."""
    return pd.DataFrame({
        'gameId': ['001'],
        'nbaGameId': [1],
        'date': ['2024-01-01'],
        'season': [2024],
        'seasonType': ['Regular'],
        'nbaTeamId': [1610612745.0],
        'team': ['HOU'],
        'opponent': ['DAL'],
        'offTeamId': [1610612745],
        'defTeamId': [1610612742],
        'pbpId': [1],
        'period': [1],
        'gameClock': ['12:00'],
        'wallClock': ['7:00 PM'],
        'wallClockInt': [1900],
        'description': ['Jump ball'],
        'msgType': [10],
        'actionType': [0],
        'option1': [0],
        'option2': [0],
        'option3': [0],
        'option4': [0],
        'homeScore': [0],
        'awayScore': [0],
        'locX': [0],
        'locY': [0],
        'pts': [0],
        'pbpOrder': [1],
        'playerId1': [201935.0],
        'playerId2': [np.nan],
        'playerId3': [np.nan],
        'lastName1': ['Harden'],
        'lastName2': [None],
        'lastName3': [None],
        'statCategory1': ['FGM'],
        'statCategory2': [None]
    })


@pytest.fixture
def valid_action_types_data():
    """Valid action types data for testing."""
    return pd.DataFrame({
        'EventType': [1],
        'ActionType': [10],
        'Event': ['Made Shot'],
        'Description': ['Jump Shot']
    })


@pytest.fixture
def valid_event_msg_types_data():
    """Valid event msg types data for testing."""
    return pd.DataFrame({
        'EventType': [1],
        'Description': ['Made Shot']
    })


@pytest.fixture
def valid_option_types_data():
    """Valid option types data for testing."""
    return pd.DataFrame({
        'Event': ['Made Shot'],
        'EventType': [1],
        'Option1': ['Jump Shot'],
        'Option2': [None],
        'Option3': [None],
        'Option4': [np.nan],
        'Description': ['Player made a jump shot']
    })


class TestBoxScoreSchema:
    """Test BoxScore schema validation."""
    
    def test_valid_data_passes(self, valid_box_score_data):
        """Test that valid data passes validation."""
        result = BoxScoreSchema.validate(valid_box_score_data)
        assert len(result) == 1
    
    def test_missing_required_field_fails(self, valid_box_score_data):
        """Test that missing required fields cause validation to fail."""
        invalid_data = valid_box_score_data.drop(columns=['gameId'])
        with pytest.raises(SchemaError):
            BoxScoreSchema.validate(invalid_data)
    
    def test_wrong_dtype_fails(self, valid_box_score_data):
        """Test that wrong data types cause validation to fail."""
        invalid_data = valid_box_score_data.copy()
        invalid_data['nbaGameId'] = 'not_an_int'
        with pytest.raises(SchemaError):
            BoxScoreSchema.validate(invalid_data)


class TestPbpSchema:
    """Test PBP schema validation."""
    
    def test_valid_data_passes(self, valid_pbp_data):
        """Test that valid data passes validation."""
        result = PbpSchema.validate(valid_pbp_data)
        assert len(result) == 1
    
    def test_nullable_fields_accept_none(self, valid_pbp_data):
        """Test that nullable fields accept None/NaN values."""
        data_with_nulls = valid_pbp_data.copy()
        data_with_nulls['nbaTeamId'] = np.nan
        data_with_nulls['team'] = None
        result = PbpSchema.validate(data_with_nulls)
        assert len(result) == 1


class TestLookupSchemas:
    """Test lookup table schemas."""
    
    def test_action_types_schema(self, valid_action_types_data):
        """Test action types schema validation."""
        result = PbpActionTypesSchema.validate(valid_action_types_data)
        assert len(result) == 1
    
    def test_event_msg_types_schema(self, valid_event_msg_types_data):
        """Test event msg types schema validation."""
        result = PbpEventMsgTypesSchema.validate(valid_event_msg_types_data)
        assert len(result) == 1
    
    def test_option_types_schema(self, valid_option_types_data):
        """Test option types schema validation."""
        result = PbpOptionTypesSchema.validate(valid_option_types_data)
        assert len(result) == 1


class TestHelperFunctions:
    """Test helper functions."""
    
    def test_validate_dataframe_success(self, valid_box_score_data):
        """Test validate_dataframe with valid data."""
        result = validate_dataframe(valid_box_score_data, 'box_score')
        assert len(result) == 1
    
    def test_validate_dataframe_invalid_schema(self, valid_box_score_data):
        """Test validate_dataframe with invalid schema name."""
        with pytest.raises(ValueError, match="Unknown schema"):
            validate_dataframe(valid_box_score_data, 'invalid_schema')
    
    def test_get_schema_success(self):
        """Test get_schema with valid schema name."""
        schema = get_schema('box_score')
        assert schema == BoxScoreSchema
    
    def test_get_schema_invalid_name(self):
        """Test get_schema with invalid schema name."""
        with pytest.raises(ValueError, match="Unknown schema"):
            get_schema('invalid_schema')
    
    def test_schemas_registry_completeness(self):
        """Test that SCHEMAS registry contains all expected schemas."""
        expected_schemas = {
            'box_score', 'pbp', 'pbp_action_types', 
            'pbp_event_msg_types', 'pbp_option_types'
        }
        assert set(SCHEMAS.keys()) == expected_schemas