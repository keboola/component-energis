import json
import os
from datetime import datetime, timedelta
import logging


class StateManager:
    """Handles state persistence for incremental fetching in Keboola Connection."""

    def __init__(self, state_dir: str):
        """Initialize the state manager with the path to state.json"""
        self.state_file = os.path.join(state_dir, "in", "state.json")

    def load_state(self) -> dict:
        """Loads the state file if it exists, otherwise returns an empty dictionary."""
        if os.path.exists(self.state_file):
            with open(self.state_file, "r") as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError:
                    logging.warning("State file is corrupted. Resetting state.")
                    return {}
        return {}

    def get_last_processed_date(self) -> str | None:
        """Returns the last processed date from the state file (adjusted by -1 day)."""
        state = self.load_state()
        last_processed_date = state.get("last_processed_date")

        if last_processed_date:
            try:
                last_date = datetime.strptime(last_processed_date, "%Y-%m-%d").date()
                adjusted_date = last_date - timedelta(days=1)
                return str(adjusted_date)
            except ValueError:
                logging.warning(f"Invalid date format in state file: {last_processed_date}")
        return None

    @staticmethod
    def save_state(last_date: str, state_dir: str):
        """Saves the last processed date to the state file."""
        state_out_file = os.path.join(state_dir, "out", "state.json")
        state = {"last_processed_date": last_date}

        with open(state_out_file, "w") as f:
            json.dump(state, f)

        logging.info(f"Saved last processed date: {last_date}")
