import os
import csv
import logging
from dataclasses import dataclass

from utils import granularity_to_filename


@dataclass
class FileMetadata:
    """Encapsulates output file information"""
    table_name: str
    file_name: str
    file_path: str


class FileManager:
    """Handles file path generation and saving data to CSV files."""

    def __init__(self, config, output_dir):
        self.config = config
        self.output_dir = output_dir

    def get_granularity(self) -> str:
        """Returns the granularity as a string."""
        return granularity_to_filename(self.config.sync_options.granularity)

    def get_file_metadata(self) -> FileMetadata:
        """Generates file metadata containing name and full path."""
        granularity = self.get_granularity()
        table_name = f"energis_{self.config.sync_options.dataset.value}_{granularity}_data"
        file_name = f"{table_name}.csv"
        file_path = os.path.join(self.output_dir, file_name)

        return FileMetadata(table_name, file_name, file_path)

    @staticmethod
    def save_to_csv(data: list[dict[str, str]], file_metadata: FileMetadata) -> None:
        """Saves the collected data to a CSV file."""
        if not data:
            logging.warning("No data to save to CSV.")
            return

        fieldnames = ["uzel", "hodnota", "cas"]

        try:
            with open(file_metadata.file_path, mode="w", newline="", encoding="utf-8") as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)

            logging.info("Data successfully saved to %s", file_metadata.file_path)

        except Exception as e:
            logging.error("Failed to save data to CSV: %s", str(e))
