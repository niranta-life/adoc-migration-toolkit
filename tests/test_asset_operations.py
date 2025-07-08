"""
Test cases for the asset_operations module.

This module contains comprehensive tests for asset-related operations
including profile export/import, config export/import, and list export.
"""

import pytest
import json
import csv
import tempfile
import logging
from pathlib import Path
from unittest.mock import Mock, patch, mock_open, MagicMock
from io import StringIO

from src.adoc_migration_toolkit.execution.asset_operations import (
    execute_asset_profile_export_guided,
    execute_asset_profile_export,
    execute_asset_profile_import,
    execute_asset_config_export,
    execute_asset_config_import,
    execute_asset_list_export,
    execute_asset_list_export_parallel
)
from src.adoc_migration_toolkit.shared import globals


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def mock_client():
    """Create a mock API client."""
    client = Mock()
    client.host = "https://test.example.com"
    client.tenant = "test-tenant"
    client.target_tenant = "target-tenant"
    return client


@pytest.fixture
def mock_logger():
    """Create a mock logger."""
    return Mock(spec=logging.Logger)


@pytest.fixture
def sample_csv_data():
    """Sample CSV data for testing."""
    return """source-env,target-env
asset-1-PROD_DB,asset-1-DEV_DB
asset-2-PROD_DB,asset-2-DEV_DB
asset-3-PROD_DB,asset-3-DEV_DB"""


@pytest.fixture
def sample_asset_response():
    """Sample asset API response."""
    return {
        "data": [
            {
                "id": 12345,
                "uid": "asset-1-PROD_DB",
                "name": "Test Asset 1",
                "type": "database"
            }
        ]
    }


@pytest.fixture
def sample_profile_response():
    """Sample profile API response."""
    return {
        "profile": {
            "name": "Test Profile",
            "config": {
                "setting1": "value1",
                "setting2": "value2"
            }
        }
    }


@pytest.fixture
def sample_config_response():
    """Sample config API response."""
    return {
        "config": {
            "database": "test_db",
            "connection": {
                "host": "localhost",
                "port": 5432
            }
        }
    }


class TestExecuteAssetProfileExportGuided:
    """Test cases for execute_asset_profile_export_guided function."""
    
    def test_execute_asset_profile_export_guided_success(self, temp_dir, mock_client, mock_logger, sample_csv_data, sample_asset_response, sample_profile_response):
        """Test successful asset profile export guided execution."""
        # Create test CSV file
        csv_file = temp_dir / "test_assets.csv"
        with open(csv_file, 'w') as f:
            f.write(sample_csv_data)
        
        # Mock API responses - repeat for each asset
        mock_client.make_api_call.side_effect = [
            sample_asset_response,  # First call for asset details
            sample_profile_response,  # Second call for profile config
            sample_asset_response,  # Third call for asset details
            sample_profile_response,  # Fourth call for profile config
            sample_asset_response,  # Fifth call for asset details
            sample_profile_response   # Sixth call for profile config
        ]
        
        result, message = execute_asset_profile_export_guided(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger,
            verbose_mode=True
        )
        
        assert result == True
        assert "Asset profiles exported to" in message
        assert mock_client.make_api_call.call_count == 6  # 3 assets * 2 calls each
        
        # Verify output file was created in global output directory
        # The function uses get_output_file_path which creates files in the global output directory
        # We need to check the actual output location
        import glob
        output_files = glob.glob("adoc-migration-toolkit-*/asset-import/asset-profiles-import-ready.csv")
        assert len(output_files) > 0
        
        # Verify CSV content
        with open(output_files[0], 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
            
        assert rows[0] == ['target-env', 'profile_json']
        assert len(rows) == 4  # Header + 3 data rows
        
        # Verify JSON content in CSV
        for i in range(1, 4):
            target_env = rows[i][0]
            profile_json = rows[i][1]
            assert target_env.startswith("asset-") and target_env.endswith("-DEV_DB")
            assert json.loads(profile_json) == sample_profile_response
    
    def test_execute_asset_profile_export_guided_no_mappings(self, temp_dir, mock_client, mock_logger):
        """Test asset profile export guided with empty CSV file."""
        # Create empty CSV file
        csv_file = temp_dir / "empty.csv"
        with open(csv_file, 'w') as f:
            f.write("source-env,target-env\n")
        
        result, message = execute_asset_profile_export_guided(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger
        )
        
        assert result == False
        assert "No environment mappings found" in message
        mock_client.make_api_call.assert_not_called()
    
    def test_execute_asset_profile_export_guided_api_error(self, temp_dir, mock_client, mock_logger, sample_csv_data):
        """Test asset profile export guided with API error."""
        # Create test CSV file
        csv_file = temp_dir / "test_assets.csv"
        with open(csv_file, 'w') as f:
            f.write(sample_csv_data)
        
        # Mock API error
        mock_client.make_api_call.side_effect = Exception("API Error")
        
        result, message = execute_asset_profile_export_guided(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger
        )
        
        assert result == True  # Function returns True even with some failures
        assert "Asset profiles exported to" in message
        mock_logger.error.assert_called()
    
    def test_execute_asset_profile_export_guided_invalid_response(self, temp_dir, mock_client, mock_logger, sample_csv_data):
        """Test asset profile export guided with invalid API response."""
        # Create test CSV file
        csv_file = temp_dir / "test_assets.csv"
        with open(csv_file, 'w') as f:
            f.write(sample_csv_data)
        
        # Mock invalid response (no 'data' field)
        mock_client.make_api_call.return_value = {"error": "Not found"}
        
        result, message = execute_asset_profile_export_guided(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger
        )
        
        assert result == True  # Function returns True even with some failures
        assert "Asset profiles exported to" in message
        mock_logger.error.assert_called()


class TestExecuteAssetProfileExport:
    """Test cases for execute_asset_profile_export function."""
    
    def test_execute_asset_profile_export_success(self, temp_dir, mock_client, mock_logger, sample_csv_data, sample_asset_response, sample_profile_response):
        """Test successful asset profile export execution."""
        # Create test CSV file
        csv_file = temp_dir / "test_assets.csv"
        with open(csv_file, 'w') as f:
            f.write(sample_csv_data)
        
        # Mock API responses - repeat for each asset
        mock_client.make_api_call.side_effect = [
            sample_asset_response,  # First call for asset details
            sample_profile_response,  # Second call for profile config
            sample_asset_response,  # Third call for asset details
            sample_profile_response,  # Fourth call for profile config
            sample_asset_response,  # Fifth call for asset details
            sample_profile_response   # Sixth call for profile config
        ]
        
        execute_asset_profile_export(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger,
            verbose_mode=True
        )
        
        assert mock_client.make_api_call.call_count == 6  # 3 assets * 2 calls each
        
        # Verify output file was created in global output directory
        # The function uses get_output_file_path which creates files in the global output directory
        import glob
        output_files = glob.glob("adoc-migration-toolkit-*/asset-import/asset-profiles-import-ready.csv")
        assert len(output_files) > 0
    
    def test_execute_asset_profile_export_csv_not_found(self, mock_client, mock_logger):
        """Test asset profile export with nonexistent CSV file."""
        execute_asset_profile_export(
            csv_file="/nonexistent/file.csv",
            client=mock_client,
            logger=mock_logger
        )
        
        mock_client.make_api_call.assert_not_called()
        mock_logger.error.assert_called()
    
    def test_execute_asset_profile_export_empty_csv(self, temp_dir, mock_client, mock_logger):
        """Test asset profile export with empty CSV file."""
        # Create empty CSV file
        csv_file = temp_dir / "empty.csv"
        with open(csv_file, 'w') as f:
            f.write("source-env,target-env\n")
        
        execute_asset_profile_export(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger
        )
        
        mock_client.make_api_call.assert_not_called()
        mock_logger.warning.assert_called()
    
    def test_execute_asset_profile_export_with_global_output_dir(self, temp_dir, mock_client, mock_logger, sample_csv_data, sample_asset_response, sample_profile_response):
        """Test asset profile export with global output directory."""
        # Set up global output directory
        global_output_dir = temp_dir / "global_output"
        global_output_dir.mkdir()
        
        # Create test CSV file
        csv_file = temp_dir / "test_assets.csv"
        with open(csv_file, 'w') as f:
            f.write(sample_csv_data)
        
        # Mock API responses
        mock_client.make_api_call.side_effect = [
            sample_asset_response,
            sample_profile_response
        ]
        
        with patch('src.adoc_migration_toolkit.execution.asset_operations.globals.GLOBAL_OUTPUT_DIR', global_output_dir):
            execute_asset_profile_export(
                csv_file=str(csv_file),
                client=mock_client,
                logger=mock_logger
            )
        
        # Verify output was created in global directory
        output_files = list(global_output_dir.glob("**/asset-profiles-import-ready.csv"))
        assert len(output_files) > 0


class TestExecuteAssetProfileImport:
    """Test cases for execute_asset_profile_import function."""
    
    def test_execute_asset_profile_import_success(self, temp_dir, mock_client, mock_logger, sample_asset_response):
        """Test successful asset profile import execution."""
        # Create test CSV file with profile data
        csv_file = temp_dir / "test_profiles.csv"
        profile_data = {
            "profile": {
                "name": "Test Profile",
                "config": {"setting": "value"}
            }
        }
        
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['target-env', 'profile_json'])
            writer.writerow(['asset-1-DEV_DB', json.dumps(profile_data)])
        
        # Mock API responses
        mock_client.make_api_call.side_effect = [
            sample_asset_response,  # Asset details
            {"status": "success"}   # Profile update
        ]
        
        execute_asset_profile_import(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger,
            dry_run=False,
            verbose_mode=True
        )
        
        assert mock_client.make_api_call.call_count == 2  # Asset details + profile update
    
    def test_execute_asset_profile_import_dry_run(self, temp_dir, mock_client, mock_logger):
        """Test asset profile import in dry run mode."""
        # Create test CSV file
        csv_file = temp_dir / "test_profiles.csv"
        profile_data = {"profile": {"name": "Test"}}
        
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['target-env', 'profile_json'])
            writer.writerow(['asset-1-DEV_DB', json.dumps(profile_data)])
        
        execute_asset_profile_import(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger,
            dry_run=True
        )
        
        # In dry run mode, no actual API calls should be made
        mock_client.make_api_call.assert_not_called()
    
    def test_execute_asset_profile_import_csv_not_found(self, mock_client, mock_logger):
        """Test asset profile import with nonexistent CSV file."""
        execute_asset_profile_import(
            csv_file="/nonexistent/file.csv",
            client=mock_client,
            logger=mock_logger
        )
        
        mock_client.make_api_call.assert_not_called()
        mock_logger.error.assert_called()
    
    def test_execute_asset_profile_import_invalid_csv_format(self, temp_dir, mock_client, mock_logger):
        """Test asset profile import with invalid CSV format."""
        # Create CSV with wrong header
        csv_file = temp_dir / "invalid.csv"
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['wrong', 'header'])
        
        execute_asset_profile_import(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger
        )
        
        mock_client.make_api_call.assert_not_called()
        mock_logger.error.assert_called()
    
    def test_execute_asset_profile_import_invalid_json(self, temp_dir, mock_client, mock_logger, sample_asset_response):
        """Test asset profile import with invalid JSON in CSV."""
        # Create CSV with invalid JSON
        csv_file = temp_dir / "invalid_json.csv"
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['target-env', 'profile_json'])
            writer.writerow(['asset-1-DEV_DB', 'invalid json'])
        
        # Mock asset response
        mock_client.make_api_call.return_value = sample_asset_response
        
        execute_asset_profile_import(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger
        )
        
        mock_logger.error.assert_called()


class TestExecuteAssetConfigExport:
    """Test cases for execute_asset_config_export function."""
    
    def test_execute_asset_config_export_success(self, temp_dir, mock_client, mock_logger, sample_asset_response, sample_config_response):
        """Test successful asset config export execution."""
        # Create test CSV file with single column UIDs
        csv_file = temp_dir / "test_uids.csv"
        with open(csv_file, 'w') as f:
            f.write("uid\nasset-1-PROD_DB\nasset-2-PROD_DB")
        
        # Mock API responses
        mock_client.make_api_call.side_effect = [
            sample_asset_response,  # Asset details
            sample_config_response, # Config
            sample_asset_response,  # Asset details
            sample_config_response  # Config
        ]
        
        execute_asset_config_export(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger,
            verbose_mode=True
        )
        
        assert mock_client.make_api_call.call_count == 4  # 2 assets * 2 calls each
        
        # Verify output file was created in global output directory
        import glob
        output_files = glob.glob("adoc-migration-toolkit-*/asset-export/asset-config-export.csv")
        assert len(output_files) > 0
    
    def test_execute_asset_config_export_no_uids(self, temp_dir, mock_client, mock_logger):
        """Test asset config export with empty CSV file."""
        # Create empty CSV file
        csv_file = temp_dir / "empty.csv"
        with open(csv_file, 'w') as f:
            f.write("uid\n")
        
        execute_asset_config_export(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger
        )
        
        mock_client.make_api_call.assert_not_called()
        mock_logger.warning.assert_called()
    
    def test_execute_asset_config_export_api_error(self, temp_dir, mock_client, mock_logger):
        """Test asset config export with API error."""
        # Create test CSV file
        csv_file = temp_dir / "test_uids.csv"
        with open(csv_file, 'w') as f:
            f.write("uid\nasset-1-PROD_DB")
        
        # Mock API error
        mock_client.make_api_call.side_effect = Exception("API Error")
        
        execute_asset_config_export(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger
        )
        
        mock_logger.error.assert_called()


class TestExecuteAssetConfigImport:
    """Test cases for execute_asset_config_import function."""
    
    def test_execute_asset_config_import_success(self, temp_dir, mock_client, mock_logger):
        """Test successful asset config import execution."""
        # Create test CSV file with config data
        csv_file = temp_dir / "test_configs.csv"
        config_data = {
            "config": {
                "database": "test_db",
                "connection": {"host": "localhost"}
            }
        }
        
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['target-env', 'config_json'])
            writer.writerow(['asset-1-DEV_DB', json.dumps(config_data)])
        
        execute_asset_config_import(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger,
            dry_run=False,
            verbose_mode=True
        )
        
        mock_logger.info.assert_called()
    
    def test_execute_asset_config_import_dry_run(self, temp_dir, mock_client, mock_logger):
        """Test asset config import in dry run mode."""
        # Create test CSV file
        csv_file = temp_dir / "test_configs.csv"
        config_data = {"config": {"setting": "value"}}
        
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['target-env', 'config_json'])
            writer.writerow(['asset-1-DEV_DB', json.dumps(config_data)])
        
        execute_asset_config_import(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger,
            dry_run=True
        )
        
        # In dry run mode, should show what would be executed
        mock_logger.info.assert_called()
    
    def test_execute_asset_config_import_csv_not_found(self, mock_client, mock_logger):
        """Test asset config import with nonexistent CSV file."""
        execute_asset_config_import(
            csv_file="/nonexistent/file.csv",
            client=mock_client,
            logger=mock_logger
        )
        
        mock_logger.error.assert_called()
    
    def test_execute_asset_config_import_invalid_json(self, temp_dir, mock_client, mock_logger):
        """Test asset config import with invalid JSON in CSV."""
        # Create CSV with invalid JSON
        csv_file = temp_dir / "invalid_json.csv"
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['target-env', 'config_json'])
            writer.writerow(['asset-1-DEV_DB', 'invalid json'])
        
        execute_asset_config_import(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger
        )
        
        mock_logger.error.assert_called()


class TestExecuteAssetListExport:
    """Test cases for execute_asset_list_export function."""
    
    def test_execute_asset_list_export_success(self, temp_dir, mock_client, mock_logger):
        """Test successful asset list export execution."""
        # Mock count response
        count_response = {
            "meta": {
                "count": 5
            }
        }
        
        # Mock page responses with tags
        page_response = {
            "data": {
                "assets": [
                    {
                        "asset": {"id": 1, "uid": "asset-1"},
                        "tags": [{"name": "tag1"}, {"name": "tag2"}]
                    },
                    {
                        "asset": {"id": 2, "uid": "asset-2"},
                        "tags": [{"name": "tag3"}]
                    },
                    {
                        "asset": {"id": 3, "uid": "asset-3"},
                        "tags": []
                    }
                ]
            }
        }
        
        mock_client.make_api_call.side_effect = [
            count_response,  # Count call
            page_response,   # First page
            page_response    # Second page
        ]
        
        with patch('src.adoc_migration_toolkit.execution.asset_operations.globals.GLOBAL_OUTPUT_DIR', temp_dir):
            execute_asset_list_export(
                client=mock_client,
                logger=mock_logger,
                verbose_mode=True
            )
        
        # Verify output file was created
        output_file = temp_dir / "asset-export" / "asset-all-export.csv"
        assert output_file.exists()
        
        # Verify CSV content
        with open(output_file, 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
            
        assert rows[0] == ['source_uid', 'source_id', 'target_uid', 'tags']
        assert len(rows) > 1  # Header + data rows
        
        # Verify first row has correct format
        assert rows[1] == ['asset-1', '1', 'asset-1', 'tag1:tag2']
        assert rows[2] == ['asset-2', '2', 'asset-2', 'tag3']
        assert rows[3] == ['asset-3', '3', 'asset-3', '']
    
    def test_execute_asset_list_export_no_assets(self, temp_dir, mock_client, mock_logger):
        """Test asset list export with no assets."""
        # Mock count response with 0 assets
        count_response = {
            "meta": {
                "count": 0
            }
        }
        
        mock_client.make_api_call.return_value = count_response
        
        with patch('src.adoc_migration_toolkit.execution.asset_operations.globals.GLOBAL_OUTPUT_DIR', temp_dir):
            execute_asset_list_export(
                client=mock_client,
                logger=mock_logger
            )
        
        # Verify output file was created (even with no assets)
        output_file = temp_dir / "asset-export" / "asset-all-export.csv"
        assert output_file.exists()
    
    def test_execute_asset_list_export_api_error(self, temp_dir, mock_client, mock_logger):
        """Test asset list export with API error."""
        # Mock API error
        mock_client.make_api_call.side_effect = Exception("API Error")
        
        with patch('src.adoc_migration_toolkit.execution.asset_operations.globals.GLOBAL_OUTPUT_DIR', temp_dir):
            execute_asset_list_export(
                client=mock_client,
                logger=mock_logger
            )
        
        mock_logger.error.assert_called()
    
    def test_execute_asset_list_export_invalid_count_response(self, temp_dir, mock_client, mock_logger):
        """Test asset list export with invalid count response."""
        # Mock invalid count response
        mock_client.make_api_call.return_value = {"error": "Invalid response"}
        
        with patch('src.adoc_migration_toolkit.execution.asset_operations.globals.GLOBAL_OUTPUT_DIR', temp_dir):
            execute_asset_list_export(
                client=mock_client,
                logger=mock_logger
            )
        
        mock_logger.error.assert_called()
    
    def test_execute_asset_list_export_without_global_output_dir(self, temp_dir, mock_client, mock_logger):
        """Test asset list export without global output directory."""
        # Mock count response
        count_response = {
            "meta": {
                "count": 1
            }
        }
        
        # Mock page response with tags
        page_response = {
            "data": {
                "assets": [
                    {
                        "asset": {"id": 1, "uid": "asset-1"},
                        "tags": [{"name": "test-tag"}]
                    }
                ]
            }
        }
        
        mock_client.make_api_call.side_effect = [
            count_response,
            page_response
        ]
        
        # Mock the global output directory to be the temp directory
        with patch('src.adoc_migration_toolkit.execution.asset_operations.globals.GLOBAL_OUTPUT_DIR', temp_dir):
            execute_asset_list_export(
                client=mock_client,
                logger=mock_logger
            )
            
            # Verify output file was created in the temp directory
            output_file = temp_dir / "asset-export" / "asset-all-export.csv"
            assert output_file.exists()
    
    def test_execute_asset_list_export_alternative_response_structure(self, temp_dir, mock_client, mock_logger):
        """Test asset list export with alternative response structure."""
        # Mock count response
        count_response = {
            "meta": {
                "count": 2
            }
        }
        
        # Mock page response with different structure
        page_response = {
            "data": {
                "items": [
                    {"id": 1, "uid": "asset-1"},
                    {"id": 2, "uid": "asset-2"}
                ]
            }
        }
        
        mock_client.make_api_call.side_effect = [
            count_response,
            page_response
        ]
        
        with patch('src.adoc_migration_toolkit.execution.asset_operations.globals.GLOBAL_OUTPUT_DIR', temp_dir):
            execute_asset_list_export(
                client=mock_client,
                logger=mock_logger
            )
        
        # Verify output file was created
        output_file = temp_dir / "asset-export" / "asset-all-export.csv"
        assert output_file.exists()
        
        # Verify CSV content
        with open(output_file, 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
            
        assert rows[0] == ['source_uid', 'source_id', 'target_uid', 'tags']
        assert len(rows) == 3  # Header + 2 data rows
    
    def test_execute_asset_list_export_parallel_success(self, temp_dir, mock_client, mock_logger):
        """Test successful parallel asset list export execution."""
        # Mock count response
        count_response = {
            "meta": {
                "count": 10
            }
        }
        
        # Mock page responses with tags
        page_response = {
            "data": {
                "assets": [
                    {
                        "asset": {"id": 1, "uid": "asset-1"},
                        "tags": [{"name": "tag1"}, {"name": "tag2"}]
                    },
                    {
                        "asset": {"id": 2, "uid": "asset-2"},
                        "tags": [{"name": "tag3"}]
                    }
                ]
            }
        }
        
        # Mock multiple page calls for parallel processing
        mock_client.make_api_call.side_effect = [
            count_response,  # Count call
            page_response,   # Page 0
            page_response,   # Page 1
            page_response,   # Page 2
            page_response,   # Page 3
            page_response    # Page 4
        ]
        
        # Mock the client constructor to return the same mock client
        with patch('src.adoc_migration_toolkit.execution.asset_operations.globals.GLOBAL_OUTPUT_DIR', temp_dir), \
             patch.object(type(mock_client), '__init__', return_value=None), \
             patch.object(type(mock_client), '__new__', return_value=mock_client):
            
            execute_asset_list_export_parallel(
                client=mock_client,
                logger=mock_logger,
                verbose_mode=False
            )
        
        # Verify output file was created
        output_file = temp_dir / "asset-export" / "asset-all-export.csv"
        assert output_file.exists()
        
        # Verify CSV content
        with open(output_file, 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
            
        assert rows[0] == ['source_uid', 'source_id', 'target_uid', 'tags']
        assert len(rows) > 1  # Header + data rows
        
        # Verify first row has correct format
        assert rows[1] == ['asset-1', '1', 'asset-1', 'tag1:tag2']
        assert rows[2] == ['asset-2', '2', 'asset-2', 'tag3']


class TestAssetOperationsIntegration:
    """Integration tests for asset operations."""
    
    def test_asset_profile_export_import_workflow(self, temp_dir, mock_client, mock_logger, sample_asset_response, sample_profile_response):
        """Test complete asset profile export and import workflow."""
        # Step 1: Export profiles
        csv_file = temp_dir / "test_assets.csv"
        with open(csv_file, 'w') as f:
            f.write("source-env,target-env\nasset-1-PROD_DB,asset-1-DEV_DB")
        
        # Mock export API responses
        mock_client.make_api_call.side_effect = [
            sample_asset_response,
            sample_profile_response
        ]
        
        execute_asset_profile_export(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger
        )
        
        # Verify export file was created in global output directory
        import glob
        export_files = glob.glob("adoc-migration-toolkit-*/asset-import/asset-profiles-import-ready.csv")
        assert len(export_files) > 0
        
        # Step 2: Import profiles
        import_csv = export_files[0]
        
        # Mock import API responses
        mock_client.make_api_call.side_effect = [
            sample_asset_response,  # Asset details
            {"status": "success"}   # Profile update
        ]
        
        execute_asset_profile_import(
            csv_file=str(import_csv),
            client=mock_client,
            logger=mock_logger,
            dry_run=True  # Use dry run for testing
        )
        
        # Verify both operations completed
        assert mock_client.make_api_call.call_count >= 2
    
    def test_asset_config_export_import_workflow(self, temp_dir, mock_client, mock_logger, sample_asset_response, sample_config_response):
        """Test complete asset config export and import workflow."""
        # Step 1: Export configs
        csv_file = temp_dir / "test_uids.csv"
        with open(csv_file, 'w') as f:
            f.write("uid\nasset-1-PROD_DB")
        
        # Mock export API responses
        mock_client.make_api_call.side_effect = [
            sample_asset_response,
            sample_config_response
        ]
        
        execute_asset_config_export(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger
        )
        
        # Verify export file was created in global output directory
        import glob
        export_files = glob.glob("adoc-migration-toolkit-*/asset-export/asset-config-export.csv")
        assert len(export_files) > 0
        
        # Step 2: Import configs
        import_csv = export_files[0]
        
        execute_asset_config_import(
            csv_file=str(import_csv),
            client=mock_client,
            logger=mock_logger,
            dry_run=True  # Use dry run for testing
        )
        
        # Verify both operations completed
        assert mock_client.make_api_call.call_count >= 2
    
    def test_error_handling_across_operations(self, temp_dir, mock_client, mock_logger):
        """Test error handling across different asset operations."""
        # Test with various error conditions
        test_cases = [
            # CSV file not found
            ("/nonexistent/file.csv", "asset-profile-export"),
            ("/nonexistent/file.csv", "asset-profile-import"),
            ("/nonexistent/file.csv", "asset-config-export"),
            ("/nonexistent/file.csv", "asset-config-import"),
        ]
        
        for csv_file, operation in test_cases:
            if operation == "asset-profile-export":
                execute_asset_profile_export(
                    csv_file=csv_file,
                    client=mock_client,
                    logger=mock_logger
                )
            elif operation == "asset-profile-import":
                execute_asset_profile_import(
                    csv_file=csv_file,
                    client=mock_client,
                    logger=mock_logger
                )
            elif operation == "asset-config-export":
                execute_asset_config_export(
                    csv_file=csv_file,
                    client=mock_client,
                    logger=mock_logger
                )
            elif operation == "asset-config-import":
                execute_asset_config_import(
                    csv_file=csv_file,
                    client=mock_client,
                    logger=mock_logger
                )
            
            # Verify error was logged
            mock_logger.error.assert_called()
            mock_logger.reset_mock() 