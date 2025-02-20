import os
import csv
import logging
from dataclasses import dataclass


@dataclass
class FileMetadata:
    """Encapsulates output file information"""
    file_name: str
    file_path: str


class FileManager:
    """Handles file path generation and saving data to CSV files."""

    def __init__(self, config, output_dir):
        self.config = config
        self.output_dir = output_dir

    def get_granularity(self) -> str:
        """Returns the granularity as a string ('month' or 'day')."""
        granularity = self.config.sync_options.granularity
        return "month" if granularity == granularity.month else "day"

    def get_file_metadata(self) -> FileMetadata:
        """Generates file metadata containing name and full path."""
        granularity = self.get_granularity()
        file_name = f"energis_{granularity}_data.csv"
        file_path = os.path.join(self.output_dir, file_name)

        return FileMetadata(file_name, file_path)

    def save_to_csv(self, data: list[dict[str, str]], file_metadata: FileMetadata) -> None:
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
