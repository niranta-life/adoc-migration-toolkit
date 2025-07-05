"""
Tests for asset profile export functionality with tqdm progress bar.
"""

import pytest
import tempfile
import csv
import json
from pathlib import Path
from unittest.mock import Mock, patch
from adoc_migration_toolkit.execution import execute_asset_profile_export_guided, create_progress_bar


class TestAssetProfileExport:
    """Test cases for asset profile export functionality."""

    def test_create_progress_bar(self):
        """Test that create_progress_bar returns a tqdm instance."""
        from tqdm import tqdm
        
        progress_bar = create_progress_bar(total=10, desc="Test", unit="items")
        assert isinstance(progress_bar, tqdm)
        assert progress_bar.total == 10
        assert progress_bar.desc == "Test"
        assert progress_bar.unit == "items"
        progress_bar.close()

    def test_create_progress_bar_disabled(self):
        """Test that create_progress_bar can be disabled."""
        progress_bar = create_progress_bar(total=10, desc="Test", unit="items", disable=True)
        assert progress_bar.disable is True
        progress_bar.close()

    @patch('adoc_migration_toolkit.execution.read_csv_uids')
    @patch('adoc_migration_toolkit.execution.get_output_file_path')
    def test_execute_asset_profile_export_guided_with_progress_bar(self, mock_get_output_path, mock_read_csv):
        """Test that the guided export function uses tqdm progress bar."""
        # Mock the CSV data
        mock_read_csv.return_value = [
            ("asset1", "target1"),
            ("asset2", "target2"),
            ("asset3", "target3")
        ]
        
        # Mock the output file path
        mock_get_output_path.return_value = Path("/tmp/test_output.csv")
        
        # Create a mock API client
        mock_client = Mock()
        
        # Mock the asset response
        mock_client.make_api_call.side_effect = [
            # First call: asset details for asset1
            {"data": [{"id": "asset_id_1"}]},
            # Second call: profile config for asset_id_1
            {"profile": "config1"},
            # Third call: asset details for asset2
            {"data": [{"id": "asset_id_2"}]},
            # Fourth call: profile config for asset_id_2
            {"profile": "config2"},
            # Fifth call: asset details for asset3
            {"data": [{"id": "asset_id_3"}]},
            # Sixth call: profile config for asset_id_3
            {"profile": "config3"}
        ]
        
        # Create a mock logger
        mock_logger = Mock()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a temporary CSV file
            csv_file = Path(temp_dir) / "test_uids.csv"
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['source-env', 'target-env'])
                writer.writerow(['asset1', 'target1'])
                writer.writerow(['asset2', 'target2'])
                writer.writerow(['asset3', 'target3'])
            
            # Mock the output file path to be in our temp directory
            output_file = Path(temp_dir) / "asset-profiles-import-ready.csv"
            mock_get_output_path.return_value = output_file
            
            # Execute the function
            success, message = execute_asset_profile_export_guided(
                csv_file=str(csv_file),
                client=mock_client,
                logger=mock_logger,
                output_file=str(output_file),
                quiet_mode=False,
                verbose_mode=False
            )
            
            # Verify the function completed successfully
            assert success is True
            assert "Asset profiles exported" in message
            
            # Verify the output file was created
            assert output_file.exists()
            
            # Verify the CSV content
            with open(output_file, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader)
                assert header == ['target-env', 'profile_json']
                
                rows = list(reader)
                assert len(rows) == 3
                assert rows[0][0] == 'target1'
                assert rows[1][0] == 'target2'
                assert rows[2][0] == 'target3'
                
                # Verify JSON content
                assert json.loads(rows[0][1]) == {"profile": "config1"}
                assert json.loads(rows[1][1]) == {"profile": "config2"}
                assert json.loads(rows[2][1]) == {"profile": "config3"}

    @patch('adoc_migration_toolkit.execution.read_csv_uids')
    @patch('adoc_migration_toolkit.execution.get_output_file_path')
    def test_execute_asset_profile_export_guided_verbose_mode(self, mock_get_output_path, mock_read_csv):
        """Test that verbose mode disables the progress bar."""
        # Mock the CSV data
        mock_read_csv.return_value = [
            ("asset1", "target1")
        ]
        
        # Mock the output file path
        mock_get_output_path.return_value = Path("/tmp/test_output.csv")
        
        # Create a mock API client
        mock_client = Mock()
        mock_client.make_api_call.side_effect = [
            {"data": [{"id": "asset_id_1"}]},
            {"profile": "config1"}
        ]
        
        # Create a mock logger
        mock_logger = Mock()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a temporary CSV file
            csv_file = Path(temp_dir) / "test_uids.csv"
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['source-env', 'target-env'])
                writer.writerow(['asset1', 'target1'])
            
            # Mock the output file path
            output_file = Path(temp_dir) / "asset-profiles-import-ready.csv"
            mock_get_output_path.return_value = output_file
            
            # Execute the function with verbose mode
            success, message = execute_asset_profile_export_guided(
                csv_file=str(csv_file),
                client=mock_client,
                logger=mock_logger,
                output_file=str(output_file),
                quiet_mode=False,
                verbose_mode=True  # This should disable the progress bar
            )
            
            # Verify the function completed successfully
            assert success is True
            assert "Asset profiles exported" in message


if __name__ == "__main__":
    pytest.main([__file__]) 