import logging
import os

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from configuration import Configuration
from api_client import EnergisClient
from file_manager import FileManager
from manifest_manager import ManifestManager
from state_manager import StateManager


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

    def run(self):
        """
        Main execution code
        """
        data_dir = self.configuration.data_dir
        state_manager = StateManager(os.path.join(data_dir, "."))
        config = Configuration(state_manager, **self.configuration.parameters)

        client = EnergisClient(config)
        output_dir = os.path.join(data_dir, "out", "tables")
        os.makedirs(output_dir, exist_ok=True)

        file_manager = FileManager(config, output_dir)
        manifest_manager = ManifestManager(self, config, file_manager)
        file_metadata = file_manager.get_file_metadata()

        data_stream = client.fetch_data()

        file_created = file_manager.save_to_csv(data_stream, file_metadata)

        if file_created:
            manifest_manager.create_manifest()
            state_manager.save_state(config.sync_options.date_to, data_dir)

        logging.info("Data processing completed!")


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
