"""
Test cases for asset-all-export.csv processing functionality.

This module contains tests for the process_asset_all_export_csv method
in the PolicyExportFormatter class.
"""

import csv
import logging
import tempfile
from pathlib import Path
from unittest.mock import Mock

import pytest

from src.adoc_migration_toolkit.execution.formatter import PolicyExportFormatter


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def mock_logger():
    """Create a mock logger."""
    return Mock(spec=logging.Logger)


class TestAssetAllExportCSVProcessing:
    """Test cases for asset-all-export.csv processing."""

    def test_process_asset_all_export_csv_success(self, temp_dir, mock_logger):
        """Test successful processing of asset-all-export.csv."""
        # Create asset-export directory
        asset_export_dir = temp_dir / "asset-export"
        asset_export_dir.mkdir(parents=True)

        # Create asset-all-export.csv with test data
        csv_file = asset_export_dir / "asset-all-export.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(["source_uid", "source_id", "target_uid", "tags"])
            writer.writerow(["uid1", "id1", "PROD_DB.table1", "tag1:tag2"])
            writer.writerow(["uid2", "id2", "PROD_DB.table2", "tag3"])
            writer.writerow(
                ["uid3", "id3", "DEV_DB.table3", "tag4"]
            )  # No replacement needed

        # Create formatter
        formatter = PolicyExportFormatter(
            input_dir=str(temp_dir),
            string_transforms={"PROD_DB": "DEV_DB"},
            output_dir=str(temp_dir),
            logger=mock_logger,
        )

        # Process the CSV
        result = formatter.process_asset_all_export_csv()

        # Verify result
        assert result is True

        # Check that asset-import directory was created
        asset_import_dir = temp_dir / "asset-import"
        assert asset_import_dir.exists()

        # Check that output file was created
        output_file = asset_import_dir / "asset-all-import-ready.csv"
        assert output_file.exists()

        # Verify the content was processed correctly
        with open(output_file, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)

        # Check header
        assert rows[0] == ["source_uid", "source_id", "target_uid", "tags"]

        # Check that target_uid values were updated correctly
        assert rows[1][2] == "DEV_DB.table1"  # Changed from PROD_DB.table1
        assert rows[2][2] == "DEV_DB.table2"  # Changed from PROD_DB.table2
        assert (
            rows[3][2] == "DEV_DB.table3"
        )  # No change needed, but should remain the same

        # Verify other columns were not changed
        assert rows[1][0] == "uid1"
        assert rows[1][1] == "id1"
        assert rows[1][3] == "tag1:tag2"

    def test_process_asset_all_export_csv_not_found(self, temp_dir, mock_logger):
        """Test processing when asset-all-export.csv doesn't exist."""
        # Create formatter without creating the CSV file
        formatter = PolicyExportFormatter(
            input_dir=str(temp_dir),
            string_transforms={"PROD_DB": "DEV_DB"},
            output_dir=str(temp_dir),
            logger=mock_logger,
        )

        # Process the CSV
        result = formatter.process_asset_all_export_csv()

        # Should return True (not an error)
        assert result is True

        # Verify log message (use resolve() to handle path differences on macOS)
        expected_path = (temp_dir / "asset-export" / "asset-all-export.csv").resolve()
        mock_logger.info.assert_called_with(
            f"asset-all-export.csv not found at {expected_path}"
        )

    def test_process_asset_all_export_csv_empty(self, temp_dir, mock_logger):
        """Test processing of empty asset-all-export.csv."""
        # Create asset-export directory
        asset_export_dir = temp_dir / "asset-export"
        asset_export_dir.mkdir(parents=True)

        # Create truly empty CSV file (no content at all)
        csv_file = asset_export_dir / "asset-all-export.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            pass  # Create empty file

        # Create formatter
        formatter = PolicyExportFormatter(
            input_dir=str(temp_dir),
            string_transforms={"PROD_DB": "DEV_DB"},
            output_dir=str(temp_dir),
            logger=mock_logger,
        )

        # Process the CSV
        result = formatter.process_asset_all_export_csv()

        # Should return True
        assert result is True

        # Verify warning was logged
        mock_logger.warning.assert_called_with("asset-all-export.csv is empty")

    def test_process_asset_all_export_csv_no_replacement_needed(
        self, temp_dir, mock_logger
    ):
        """Test processing when no replacements are needed."""
        # Create asset-export directory
        asset_export_dir = temp_dir / "asset-export"
        asset_export_dir.mkdir(parents=True)

        # Create CSV file with no strings that need replacement
        csv_file = asset_export_dir / "asset-all-export.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(["source_uid", "source_id", "target_uid", "tags"])
            writer.writerow(["uid1", "id1", "DEV_DB.table1", "tag1"])
            writer.writerow(["uid2", "id2", "TEST_DB.table2", "tag2"])

        # Create formatter
        formatter = PolicyExportFormatter(
            input_dir=str(temp_dir),
            string_transforms={"PROD_DB": "DEV_DB"},
            output_dir=str(temp_dir),
            logger=mock_logger,
        )

        # Process the CSV
        result = formatter.process_asset_all_export_csv()

        # Should return True
        assert result is True

        # Check that output file was created
        asset_import_dir = temp_dir / "asset-import"
        output_file = asset_import_dir / "asset-all-import-ready.csv"
        assert output_file.exists()

        # Verify content was preserved (no changes made)
        with open(output_file, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)

        assert rows[1][2] == "DEV_DB.table1"  # No change
        assert rows[2][2] == "TEST_DB.table2"  # No change

    def test_process_asset_all_export_csv_invalid_rows(self, temp_dir, mock_logger):
        """Test processing with rows that have fewer than 3 columns."""
        # Create asset-export directory
        asset_export_dir = temp_dir / "asset-export"
        asset_export_dir.mkdir(parents=True)

        # Create CSV file with invalid rows
        csv_file = asset_export_dir / "asset-all-export.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(["source_uid", "source_id", "target_uid", "tags"])
            writer.writerow(["uid1", "id1", "PROD_DB.table1", "tag1"])  # Valid row
            writer.writerow(["uid2", "id2"])  # Invalid row - missing columns
            writer.writerow(["uid3"])  # Invalid row - missing columns

        # Create formatter
        formatter = PolicyExportFormatter(
            input_dir=str(temp_dir),
            string_transforms={"PROD_DB": "DEV_DB"},
            output_dir=str(temp_dir),
            logger=mock_logger,
        )

        # Process the CSV
        result = formatter.process_asset_all_export_csv()

        # Should return True
        assert result is True

        # Check that output file was created
        asset_import_dir = temp_dir / "asset-import"
        output_file = asset_import_dir / "asset-all-import-ready.csv"
        assert output_file.exists()

        # Verify only valid rows were processed
        with open(output_file, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)

        # Should have processed the valid row
        assert rows[1][2] == "DEV_DB.table1"  # Changed from PROD_DB.table1
        # Invalid rows should be preserved as-is
        assert len(rows[2]) == 2  # ['uid2', 'id2']
        assert len(rows[3]) == 1  # ['uid3']
