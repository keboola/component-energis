import logging
from keboola.component.base import ComponentBase
from file_manager import FileManager
from configuration import Configuration, DATASET_UNIQUE_FIELDS


class ManifestManager:
    """Handles the generation of Keboola manifest files."""

    def __init__(self, component: ComponentBase, config: Configuration, file_manager: FileManager):
        self.config = config
        self.component = component
        self.file_manager = file_manager

    def get_primary_keys(self) -> list[str]:
        """Returns the primary keys for a given dataset."""
        return DATASET_UNIQUE_FIELDS.get(self.config.sync_options.dataset, [])

    def create_manifest(self):
        """
        Generates a Keboola manifest file for a dataset.
        Uses FileManager to ensure consistent file naming.
        """
        file_metadata = self.file_manager.get_file_metadata()

        output_table = self.component.create_out_table_definition(
            file_metadata.file_name,
            incremental=True,
            primary_key=ManifestManager.get_primary_keys(self),
            destination=f"out.c-data.{file_metadata.table_name}",
        )

        self.component.write_manifest(output_table)
        logging.info(f"Manifest created for {file_metadata.file_name}")
