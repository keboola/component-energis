import pytest
import os
import logging
from unittest.mock import Mock, patch, mock_open
from file_manager import FileManager, FileMetadata
from configuration import Configuration, SyncOptions, DatasetEnum, GranularityEnum


@pytest.fixture
def config():
    """Creates a mock Configuration object."""
    mock_config = Mock(spec=Configuration)

    mock_config.sync_options = SyncOptions(
        dataset=DatasetEnum.xexport,
        nodes=[12345],
        date_from="2025-01-01",
        date_to="2025-01-31",
        granularity=GranularityEnum.day
    )

    return mock_config


@pytest.fixture
def file_manager(config, tmp_path):
    """Creates a FileManager instance using a temporary directory."""
    return FileManager(config, str(tmp_path))


def test_get_file_metadata(file_manager):
    """Tests that get_file_metadata() generates correct file metadata."""
    file_metadata = file_manager.get_file_metadata()

    expected_table_name = "energis_xexport_day_data"
    expected_file_name = f"{expected_table_name}.csv"
    expected_file_path = os.path.join(file_manager.output_dir, expected_file_name)

    assert file_metadata.table_name == expected_table_name
    assert file_metadata.file_name == expected_file_name
    assert file_metadata.file_path == expected_file_path


@patch("builtins.open", new_callable=mock_open)
@patch("csv.DictWriter")
def test_save_to_csv(mock_csv_writer, mock_open_func, file_manager, caplog):
    """Tests that save_to_csv() correctly writes data to a CSV file."""
    file_metadata = file_manager.get_file_metadata()
    test_data = iter([{"uzel": "12345", "hodnota": "100", "cas": "2025-03-05 08:00"}])  # Generator

    with caplog.at_level(logging.INFO):
        result = file_manager.save_to_csv(test_data, file_metadata)

    assert result is True

    mock_open_func.assert_called_once_with(file_metadata.file_path, mode="w", newline="", encoding="utf-8")

    mock_csv_writer.return_value.writeheader.assert_called_once()
    mock_csv_writer.return_value.writerow.assert_called_once()

    assert f"Data successfully saved to {file_metadata.file_path}" in caplog.text


def test_save_to_csv_empty_data(file_manager, caplog):
    """Tests that save_to_csv() handles an empty dataset correctly."""
    file_metadata = file_manager.get_file_metadata()

    with caplog.at_level(logging.INFO):
        result = file_manager.save_to_csv([], file_metadata)

    assert result is False
    assert "No data found" in caplog.text
