import pytest
import logging
from unittest.mock import Mock
from manifest_manager import ManifestManager
from configuration import Configuration, DATASET_UNIQUE_FIELDS, SyncOptions, DatasetEnum


@pytest.fixture
def config():
    """Create a mock Configuration object with a sample dataset."""
    mock_config = Mock(spec=Configuration)

    mock_config.sync_options = SyncOptions(
        dataset=DatasetEnum.xexport,
        nodes=[12345],
        date_from="2025-01-01",
        date_to="2025-01-31",
        granularity="day"
    )

    return mock_config


@pytest.fixture
def mock_file_manager():
    """Create a mock FileManager."""
    mock_fm = Mock()
    mock_fm.get_file_metadata.return_value = Mock(
        file_name="dataset_20250306.csv",
        table_name="dataset"
    )
    return mock_fm


@pytest.fixture
def mock_component():
    """Create a mock ComponentBase object."""
    mock_comp = Mock()
    mock_comp.create_out_table_definition.return_value = Mock()
    return mock_comp


def test_get_primary_keys(config, mock_component, mock_file_manager):
    """Test retrieving primary keys from the dataset."""
    manifest_manager = ManifestManager(mock_component, config, mock_file_manager)

    expected_keys = DATASET_UNIQUE_FIELDS.get(config.sync_options.dataset, [])
    assert manifest_manager.get_primary_keys() == expected_keys


def test_create_manifest(config, mock_component, mock_file_manager, caplog):
    """Test that create_manifest() correctly generates a manifest."""
    manifest_manager = ManifestManager(mock_component, config, mock_file_manager)

    with caplog.at_level(logging.INFO):
        manifest_manager.create_manifest()

    mock_component.create_out_table_definition.assert_called_once_with(
        "dataset_20250306.csv",
        incremental=True,
        primary_key=DATASET_UNIQUE_FIELDS.get(config.sync_options.dataset, []),
        destination="out.c-data.dataset"
    )

    mock_component.write_manifest.assert_called_once()

    assert "Manifest created for dataset_20250306.csv" in caplog.text
