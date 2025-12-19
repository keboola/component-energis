import csv
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from configuration import (
    Configuration,
    GranularityEnum,
    DATASET_UNIQUE_FIELDS,
    DATASET_OUTPUT_FIELDS,
)
from api_client import EnergisClient


@dataclass
class FileMetadata:
    """Encapsulates output file information."""

    table_name: str
    file_name: str
    file_path: str


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.output_dir = None
        self.client = None
        self.config = None

    def run(self):
        """Main execution code."""
        last_processed_date = self._get_last_processed_date()
        self.config = Configuration(
            last_processed_date=last_processed_date, **self.configuration.parameters
        )
        self.client = EnergisClient(self.config)
        self.output_dir = os.path.join(self.configuration.data_dir, "out", "tables")
        os.makedirs(self.output_dir, exist_ok=True)
        file_metadata = self._build_file_metadata()
        file_created = self._fetch_and_save_to_csv(file_metadata)
        if file_created:
            self._create_manifest(file_metadata)
            self._save_state()
        logging.info("Data processing completed!")

    def _get_last_processed_date(self) -> str | None:
        """Returns the last processed date from state file (adjusted by -1 day)."""
        try:
            state = self.get_state_file()
        except Exception:
            logging.warning("Failed to read state file. Starting fresh.")
            return None
        last_processed_date = state.get("last_processed_date")
        if last_processed_date:
            try:
                last_date = datetime.strptime(last_processed_date, "%Y-%m-%d").date()
                adjusted_date = last_date - timedelta(days=1)
                return str(adjusted_date)
            except ValueError:
                logging.warning(
                    "Invalid date format in state file: %s", last_processed_date
                )
        return None

    def _build_file_metadata(self) -> FileMetadata:
        """Generates file metadata containing name and full path."""
        granularity = self._granularity_to_filename(
            self.config.sync_options.granularity
        )
        table_name = (
            f"energis_{self.config.sync_options.dataset.value}_{granularity}_data"
        )
        file_name = f"{table_name}.csv"
        file_path = os.path.join(self.output_dir, file_name)
        return FileMetadata(table_name, file_name, file_path)

    @staticmethod
    def _granularity_to_filename(granularity: GranularityEnum) -> str:
        """Returns a descriptive filename component based on the GranularityEnum value."""
        return re.sub(r"([A-Z])", r"_\1", granularity.value).lower()

    def _fetch_and_save_to_csv(self, file_metadata: FileMetadata) -> bool:
        """Fetches data and saves directly to CSV. Returns True if data was written."""
        fieldnames = DATASET_OUTPUT_FIELDS.get(self.config.sync_options.dataset, [])
        try:
            with open(
                file_metadata.file_path, mode="w", newline="", encoding="utf-8"
            ) as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writeheader()
                row_count = self.client.fetch_data(writer)
            if row_count == 0:
                logging.info("No data found")
                return False
            logging.info(
                "Data successfully saved to %s (%d rows)",
                file_metadata.file_path,
                row_count,
            )
            return True
        except Exception as e:
            logging.error("Failed to fetch/save data to CSV: %s", str(e))
            raise

    def _create_manifest(self, file_metadata: FileMetadata) -> None:
        """Creates a Keboola manifest file for the output table."""
        primary_keys = DATASET_UNIQUE_FIELDS.get(self.config.sync_options.dataset, [])
        output_table = self.create_out_table_definition(
            file_metadata.file_name,
            incremental=True,
            primary_key=primary_keys,
            destination=f"out.c-data.{file_metadata.table_name}",
        )
        self.write_manifest(output_table)
        logging.info("Manifest created for %s", file_metadata.file_name)

    def _save_state(self) -> None:
        """Saves the last processed date to state file."""
        try:
            self.write_state_file(
                {"last_processed_date": self.config.sync_options.date_to}
            )
            logging.info(
                "Saved last processed date: %s", self.config.sync_options.date_to
            )
        except Exception as e:
            logging.warning("Failed to save state file: %s", str(e))


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
