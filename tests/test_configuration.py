import pytest
import logging
from datetime import date
from unittest.mock import Mock
from configuration import (
    Configuration,
    Authentication,
    SyncOptions,
    GranularityEnum,
    DatasetEnum,
    EnvironmentEnum
)
from state_manager import StateManager


@pytest.fixture
def state_manager():
    """Creates a mock StateManager."""
    mock_state_manager = Mock(spec=StateManager)
    mock_state_manager.get_last_processed_date.return_value = "2025-01-01"
    return mock_state_manager


@pytest.fixture
def valid_auth():
    return Authentication(
        username="testuser",
        **{"#password": "securepassword"},
        environment=EnvironmentEnum.prod
    )

@pytest.fixture
def valid_sync_options():
    """Returns a valid SyncOptions instance."""
    return SyncOptions(
        dataset=DatasetEnum.xexport,
        nodes=[12345],
        date_from="2025-01-01",
        date_to="2025-01-31",
        granularity=GranularityEnum.day
    )


def test_authentication_properties(valid_auth):
    """Tests the computed properties of Authentication."""
    assert valid_auth.credentials == ("testuser", "securepassword")
    assert valid_auth.api_base_url == "https://bilance.c-energy.cz/cgi-bin/1.wsc/soap.r"


def test_authentication_validation():
    with pytest.raises(ValueError, match="Field 'username' cannot be empty"):
        Authentication(username="", **{"#password": "securepassword"})

    with pytest.raises(ValueError, match="Value error, Field 'password' cannot be empty"):
        Authentication(username="testuser", **{"#password": ""})


def test_sync_options_validation():
    """Tests that SyncOptions enforces correct values."""
    with pytest.raises(ValueError, match="Field 'nodes' cannot be empty"):
        SyncOptions(dataset=DatasetEnum.xexport, nodes=[], date_from="2025-01-01", date_to="2025-01-31", granularity=GranularityEnum.day)

    with pytest.raises(ValueError, match="Input should be 'year', 'quarterYear', 'month', 'day', 'hour', 'quarterHour' or 'minute'"):
        SyncOptions(dataset=DatasetEnum.xexport, nodes=[12345], date_from="2025-01-01", date_to="2025-01-31", granularity="invalid")


def test_sync_options_resolved_date_to(valid_sync_options):
    """Tests that resolved_date_to returns the correct value."""
    assert valid_sync_options.resolved_date_to == "2025-01-31"

    sync_options_without_date_to = SyncOptions(
        dataset=DatasetEnum.xexport,
        nodes=[12345],
        date_from="2025-01-01",
        date_to=None,
        granularity=GranularityEnum.day
    )
    assert sync_options_without_date_to.resolved_date_to == str(date.today())


def test_configuration_initialization(state_manager, valid_auth, valid_sync_options, caplog):
    """Tests Configuration initialization and state handling."""
    with caplog.at_level(logging.INFO):
        config = Configuration(state_manager=state_manager, authentication=valid_auth, sync_options=valid_sync_options)

    assert config.sync_options.date_from == "2025-01-01"
    assert config.sync_options.date_to == "2025-01-31"

    assert "Using date_from: 2025-01-01" in caplog.text
    assert f"Using date_to: {config.sync_options.date_to}" in caplog.text
    assert f"Using granularity: {config.sync_options.granularity.value}" in caplog.text


def test_configuration_validation_error(state_manager):
    """Tests that Configuration raises validation errors when required fields are missing."""
    with pytest.raises(ValueError, match="Field 'username' cannot be empty"):
        Configuration(
            state_manager=state_manager,
            authentication=Authentication(username="", password="password"),
            sync_options=SyncOptions(
                dataset=DatasetEnum.xexport,
                nodes=[12345],
                date_from="2025-01-01",
                date_to="2025-01-31",
                granularity=GranularityEnum.day
            )
        )
