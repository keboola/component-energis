import json
import os
import tempfile
import pytest
from datetime import datetime, timedelta
from src.state_manager import StateManager


@pytest.fixture
def temp_state_dir():
    """Creates a temporary directory to simulate state storage."""
    with tempfile.TemporaryDirectory() as temp_dir:
        os.makedirs(os.path.join(temp_dir, "in"), exist_ok=True)
        os.makedirs(os.path.join(temp_dir, "out"), exist_ok=True)
        yield temp_dir


def test_load_state_valid_file(temp_state_dir):
    """Test loading state when a valid state.json file exists."""
    state_file = os.path.join(temp_state_dir, "in", "state.json")
    state_data = {"last_processed_date": "2025-03-05"}

    with open(state_file, "w") as f:
        json.dump(state_data, f)

    state_manager = StateManager(temp_state_dir)
    loaded_state = state_manager.load_state()

    assert loaded_state == state_data


def test_load_state_no_file(temp_state_dir):
    """Test loading state when no state.json file exists."""
    state_manager = StateManager(temp_state_dir)
    loaded_state = state_manager.load_state()

    assert loaded_state == {}


def test_load_state_corrupt_file(temp_state_dir):
    """Test handling of a corrupted state.json file."""
    state_file = os.path.join(temp_state_dir, "in", "state.json")

    with open(state_file, "w") as f:
        f.write("{invalid json}")

    state_manager = StateManager(temp_state_dir)
    loaded_state = state_manager.load_state()

    assert loaded_state == {}


def test_get_last_processed_date_valid(temp_state_dir):
    """Test retrieving last processed date when a valid date is stored."""
    state_file = os.path.join(temp_state_dir, "in", "state.json")
    last_date = "2025-03-05"

    with open(state_file, "w") as f:
        json.dump({"last_processed_date": last_date}, f)

    state_manager = StateManager(temp_state_dir)
    expected_date = str(datetime.strptime(last_date, "%Y-%m-%d").date() - timedelta(days=1))

    assert state_manager.get_last_processed_date() == expected_date


def test_get_last_processed_date_missing(temp_state_dir):
    """Test retrieving last processed date when it's missing from state."""
    state_manager = StateManager(temp_state_dir)

    assert state_manager.get_last_processed_date() is None


def test_get_last_processed_date_invalid_format(temp_state_dir):
    """Test handling of invalid date format in state.json."""
    state_file = os.path.join(temp_state_dir, "in", "state.json")

    with open(state_file, "w") as f:
        json.dump({"last_processed_date": "invalid-date"}, f)

    state_manager = StateManager(temp_state_dir)

    assert state_manager.get_last_processed_date() is None


def test_save_state(temp_state_dir):
    """Test saving the last processed date."""
    state_manager = StateManager(temp_state_dir)
    last_date = "2025-03-06"

    StateManager.save_state(last_date, temp_state_dir)

    state_file = os.path.join(temp_state_dir, "out", "state.json")

    assert os.path.exists(state_file)

    with open(state_file, "r") as f:
        saved_state = json.load(f)

    assert saved_state == {"last_processed_date": last_date}
