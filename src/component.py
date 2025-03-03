import logging
import os

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from configuration import Configuration
from api_client import EnergisClient
from file_manager import FileManager
from manifest_manager import ManifestManager


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

    def run(self):
        """
        Main execution code
        """
        config = Configuration(**self.configuration.parameters)
        client = EnergisClient(config)

        result = client.fetch_data()

        output_dir = os.path.join(self.configuration.data_dir, "out", "tables")
        os.makedirs(output_dir, exist_ok=True)

        file_manager = FileManager(config, output_dir)
        manifest_manager = ManifestManager(self, config, file_manager)

        file_metadata = file_manager.get_file_metadata()
        file_manager.save_to_csv(result, file_metadata)
        manifest_manager.create_manifest()

        logging.info(f"Data processing completed successfully for {file_metadata.file_name}")


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
