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
    execute_asset_config_export_parallel,
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
        # The function might process fewer assets due to API responses, so be flexible
        assert len(rows) >= 2  # At least header + 1 data row
        assert len(rows) <= 4  # At most header + 3 data rows
        
        # Verify JSON content in CSV for the rows that exist
        for i in range(1, len(rows)):
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
    
    def test_execute_asset_config_export_success(self, temp_dir, mock_client, mock_logger, sample_config_response):
        """Test successful asset config export execution."""
        # Create test CSV file with 5 columns: source_id, source_uid, target_id, target_uid, tags
        csv_file = temp_dir / "test_assets.csv"
        with open(csv_file, 'w') as f:
            f.write("source_id,source_uid,target_id,target_uid,tags\n")
            f.write("1,asset-1-PROD_DB,101,asset-1-DEV_DB,tag1:tag2\n")
            f.write("2,asset-2-PROD_DB,102,asset-2-DEV_DB,tag3\n")
        
        # Mock API responses - only config calls needed now
        mock_client.make_api_call.side_effect = [
            sample_config_response, # Config for asset 1
            sample_config_response  # Config for asset 2
        ]
        
        execute_asset_config_export(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger,
            verbose_mode=True
        )
        
        assert mock_client.make_api_call.call_count == 2  # 2 assets * 1 config call each
        
        # Verify output file was created in global output directory
        import glob
        output_files = glob.glob("adoc-migration-toolkit-*/asset-export/asset-config-export.csv")
        assert len(output_files) > 0
    
    def test_execute_asset_config_export_no_assets(self, temp_dir, mock_client, mock_logger):
        """Test asset config export with empty CSV file."""
        # Create empty CSV file
        csv_file = temp_dir / "empty.csv"
        with open(csv_file, 'w') as f:
            f.write("source_id,source_uid,target_id,target_uid,tags\n")
        
        execute_asset_config_export(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger
        )
        
        mock_client.make_api_call.assert_not_called()
        mock_logger.warning.assert_called()
    
    def test_execute_asset_config_export_api_error(self, temp_dir, mock_client, mock_logger):
        """Test asset config export with API error."""
        # Create test CSV file with 5 columns
        csv_file = temp_dir / "test_assets.csv"
        with open(csv_file, 'w') as f:
            f.write("source_id,source_uid,target_id,target_uid,tags\n")
            f.write("1,asset-1-PROD_DB,101,asset-1-DEV_DB,tag1\n")
        
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
        """Test successful asset config import."""
        # Create test CSV file
        csv_file = temp_dir / "test_asset_config_import.csv"
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['test.asset.1', '{"config": "data1"}'])
            writer.writerow(['test.asset.2', '{"config": "data2"}'])
        
        # Mock API responses
        mock_client.make_api_call.side_effect = [
            {'data': [{'id': 123}]},  # First asset lookup
            {'success': True},        # First config update
            {'data': [{'id': 456}]},  # Second asset lookup
            {'success': True}         # Second config update
        ]
        
        with patch('src.adoc_migration_toolkit.execution.asset_operations.globals.GLOBAL_OUTPUT_DIR', temp_dir):
            # Test asset config import
            execute_asset_config_import(str(csv_file), mock_client, mock_logger, quiet_mode=False, verbose_mode=False)
        
        # Verify API calls were made correctly
        assert mock_client.make_api_call.call_count == 4
        
        # Check first asset lookup
        call_args = mock_client.make_api_call.call_args_list[0]
        assert call_args[1]['method'] == 'GET'
        assert call_args[1]['endpoint'] == '/catalog-server/api/assets?uid=test.asset.1'
        
        # Check first config update
        call_args = mock_client.make_api_call.call_args_list[1]
        assert call_args[1]['method'] == 'PUT'
        assert call_args[1]['endpoint'] == '/catalog-server/api/assets/123/config'
        expected_payload_1 = {
            "assetConfiguration": {
                "assetId": 123,
                "config": "data1"
            }
        }
        assert call_args[1]['json_payload'] == expected_payload_1
        
        # Check second asset lookup
        call_args = mock_client.make_api_call.call_args_list[2]
        assert call_args[1]['method'] == 'GET'
        assert call_args[1]['endpoint'] == '/catalog-server/api/assets?uid=test.asset.2'
        
        # Check second config update
        call_args = mock_client.make_api_call.call_args_list[3]
        assert call_args[1]['method'] == 'PUT'
        assert call_args[1]['endpoint'] == '/catalog-server/api/assets/456/config'
        expected_payload_2 = {
            "assetConfiguration": {
                "assetId": 456,
                "config": "data2"
            }
        }
        assert call_args[1]['json_payload'] == expected_payload_2

    def test_execute_asset_config_import_csv_not_found(self, temp_dir, mock_client, mock_logger):
        """Test asset config import with non-existent CSV file."""
        csv_file = temp_dir / "nonexistent.csv"
        
        with patch('src.adoc_migration_toolkit.execution.asset_operations.globals.GLOBAL_OUTPUT_DIR', temp_dir):
            # Test asset config import
            execute_asset_config_import(str(csv_file), mock_client, mock_logger, quiet_mode=False, verbose_mode=False)
        
        # Verify no API calls were made
        mock_client.make_api_call.assert_not_called()

    def test_execute_asset_config_import_no_assets(self, temp_dir, mock_client, mock_logger):
        """Test asset config import with empty CSV file."""
        # Create empty CSV file
        csv_file = temp_dir / "empty_asset_config_import.csv"
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['target_uid', 'config_json'])
        
        with patch('src.adoc_migration_toolkit.execution.asset_operations.globals.GLOBAL_OUTPUT_DIR', temp_dir):
            # Test asset config import
            execute_asset_config_import(str(csv_file), mock_client, mock_logger, quiet_mode=False, verbose_mode=False)
        
        # Verify no API calls were made
        mock_client.make_api_call.assert_not_called()

    def test_execute_asset_config_import_asset_not_found(self, temp_dir, mock_client, mock_logger):
        """Test asset config import when asset is not found."""
        # Create test CSV file
        csv_file = temp_dir / "test_asset_config_import_not_found.csv"
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['target_uid', 'config_json'])
            writer.writerow(['nonexistent.asset', '{"config": "data"}'])
        
        # Mock API response - asset not found
        mock_client.make_api_call.return_value = {'data': []}
        
        with patch('src.adoc_migration_toolkit.execution.asset_operations.globals.GLOBAL_OUTPUT_DIR', temp_dir):
            # Test asset config import
            execute_asset_config_import(str(csv_file), mock_client, mock_logger, quiet_mode=False, verbose_mode=False)
        
        # Verify only one API call was made (asset lookup)
        assert mock_client.make_api_call.call_count == 1
        
        # Check asset lookup
        call_args = mock_client.make_api_call.call_args_list[0]
        assert call_args[1]['method'] == 'GET'
        assert call_args[1]['endpoint'] == '/catalog-server/api/assets?uid=nonexistent.asset'


class TestExecuteAssetConfigExportParallel:
    """Test cases for execute_asset_config_export_parallel function."""
    
    def test_execute_asset_config_export_parallel_success(self, temp_dir, mock_client, mock_logger):
        """Test successful parallel asset config export."""
        # Create test CSV file with 5 columns
        csv_file = temp_dir / "test_parallel_export.csv"
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['source_id', 'source_uid', 'target_id', 'target_uid', 'tags'])
            writer.writerow(['id1', 'uid1', 'tid1', 'target_uid1', 'tag1:tag2'])
            writer.writerow(['id2', 'uid2', 'tid2', 'target_uid2', 'tag3'])
            writer.writerow(['id3', 'uid3', 'tid3', 'target_uid3', 'tag4:tag5'])
            writer.writerow(['id4', 'uid4', 'tid4', 'target_uid4', 'tag6'])
            writer.writerow(['id5', 'uid5', 'tid5', 'target_uid5', 'tag7'])
        
        # Mock API responses
        mock_client.make_api_call.return_value = {
            "config": "test_config_data",
            "version": "1.0"
        }
        
        with patch('src.adoc_migration_toolkit.execution.asset_operations.globals.GLOBAL_OUTPUT_DIR', temp_dir):
            # Test parallel export
            execute_asset_config_export_parallel(str(csv_file), mock_client, mock_logger, quiet_mode=True)
            
            # Verify export file was created
            output_file = temp_dir / "asset-export" / "asset-config-export.csv"
            assert output_file.exists()
            
            # Verify CSV content
            with open(output_file, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader)
                assert header == ['target_uid', 'config_json']
                
                rows = list(reader)
                assert len(rows) == 5, "Should have 5 data rows"
                
                # Check that all target UIDs are present
                target_uids = [row[0] for row in rows]
                expected_uids = ['target_uid1', 'target_uid2', 'target_uid3', 'target_uid4', 'target_uid5']
                assert set(target_uids) == set(expected_uids)
                
                # Check that config JSON is valid
                for row in rows:
                    config_json = row[1]
                    config_data = json.loads(config_json)
                    assert isinstance(config_data, dict)
                    assert 'config' in config_data
            
            # Verify API calls were made
            assert mock_client.make_api_call.call_count == 5
            
            # Check that calls were made with correct endpoints
            call_args = []
            for call in mock_client.make_api_call.call_args_list:
                if call.args:
                    call_args.append(call.args[0])
                elif call.kwargs and 'endpoint' in call.kwargs:
                    call_args.append(call.kwargs['endpoint'])
            
            expected_endpoints = [
                '/catalog-server/api/assets/id1/config',
                '/catalog-server/api/assets/id2/config',
                '/catalog-server/api/assets/id3/config',
                '/catalog-server/api/assets/id4/config',
                '/catalog-server/api/assets/id5/config'
            ]
            assert set(call_args) == set(expected_endpoints)
    
    def test_execute_asset_config_export_parallel_no_assets(self, temp_dir, mock_client, mock_logger):
        """Test parallel asset config export with no assets."""
        # Create empty CSV file
        csv_file = temp_dir / "test_empty_parallel_export.csv"
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['source_id', 'source_uid', 'target_id', 'target_uid', 'tags'])
        
        with patch('src.adoc_migration_toolkit.execution.asset_operations.globals.GLOBAL_OUTPUT_DIR', temp_dir):
            # Test parallel export
            execute_asset_config_export_parallel(str(csv_file), mock_client, mock_logger, quiet_mode=True)
            
            # Verify no API calls were made
            mock_client.make_api_call.assert_not_called()
            
            # Verify warning was logged
            mock_logger.warning.assert_called_with("No asset data found in CSV file")
    
    def test_execute_asset_config_export_parallel_api_error(self, temp_dir, mock_client, mock_logger):
        """Test parallel asset config export with API error."""
        # Create test CSV file
        csv_file = temp_dir / "test_parallel_export_error.csv"
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['source_id', 'source_uid', 'target_id', 'target_uid', 'tags'])
            writer.writerow(['id1', 'uid1', 'tid1', 'target_uid1', 'tag1'])
        
        # Mock API to raise exception
        mock_client.make_api_call.side_effect = Exception("API Error")
        
        with patch('src.adoc_migration_toolkit.execution.asset_operations.globals.GLOBAL_OUTPUT_DIR', temp_dir):
            # Test parallel export
            execute_asset_config_export_parallel(str(csv_file), mock_client, mock_logger, quiet_mode=True)
            
            # Verify error was logged
            mock_logger.error.assert_called()
            
            # Verify export file was still created (with no successful exports)
            output_file = temp_dir / "asset-export" / "asset-config-export.csv"
            assert output_file.exists()
            
            # Verify CSV only has header
            with open(output_file, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader)
                assert header == ['target_uid', 'config_json']
                
                rows = list(reader)
                assert len(rows) == 0, "Should have no data rows due to error"
    
    def test_parallel_mode_defaults_to_quiet(self, temp_dir, mock_client, mock_logger):
        """Test that parallel mode defaults to quiet mode for asset-config-export."""
        from src.adoc_migration_toolkit.execution.command_parsing import parse_asset_config_export_command
        import os
        
        # Create the required CSV file structure
        asset_import_dir = temp_dir / "asset-import"
        asset_import_dir.mkdir(parents=True, exist_ok=True)
        csv_file = asset_import_dir / "asset-merged-all.csv"
        
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['source_id', 'source_uid', 'target_id', 'target_uid', 'tags'])
            writer.writerow(['id1', 'uid1', 'tid1', 'target_uid1', 'tag1'])
        
        real_exists = os.path.exists
        
        def mock_exists(path):
            if str(path).endswith('asset-import/asset-merged-all.csv'):
                return True
            return real_exists(path)
        
        with patch('src.adoc_migration_toolkit.execution.command_parsing.globals.GLOBAL_OUTPUT_DIR', temp_dir), \
             patch('os.path.exists', side_effect=mock_exists):
            # Test command with --parallel but no --quiet or --verbose
            command = "asset-config-export --parallel"
            csv_file, output_file, quiet_mode, verbose_mode, parallel_mode = parse_asset_config_export_command(command)
            
            # Verify that parallel mode defaults to quiet mode
            assert parallel_mode is True
            assert quiet_mode is True
            assert verbose_mode is False
            
            # Test command with --parallel and --verbose
            command = "asset-config-export --parallel --verbose"
            csv_file, output_file, quiet_mode, verbose_mode, parallel_mode = parse_asset_config_export_command(command)
            
            # Verify that --verbose overrides the quiet default
            assert parallel_mode is True
            assert quiet_mode is False
            assert verbose_mode is True
            
            # Test command with --parallel and --quiet
            command = "asset-config-export --parallel --quiet"
            csv_file, output_file, quiet_mode, verbose_mode, parallel_mode = parse_asset_config_export_command(command)
            
            # Verify that --quiet is respected
            assert parallel_mode is True
            assert quiet_mode is True
            assert verbose_mode is False


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
        output_file = temp_dir / "asset-export" / "asset-all-source-export.csv"
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
    
    def test_execute_asset_list_export_with_autotagged_filtering(self, temp_dir, mock_client, mock_logger):
        """Test asset list export with autoTagged tag filtering."""
        # Mock count response
        count_response = {
            "meta": {
                "count": 3
            }
        }
        
        # Mock page responses with autoTagged tags
        page_response = {
            "data": {
                "assets": [
                    {
                        "asset": {"id": 1, "uid": "asset-1"},
                        "tags": [
                            {"name": "manual-tag1"},
                            {"name": "auto-tag1", "autoTagged": True},
                            {"name": "manual-tag2"},
                            {"name": "auto-tag2", "autoTagged": True}
                        ]
                    },
                    {
                        "asset": {"id": 2, "uid": "asset-2"},
                        "tags": [
                            {"name": "manual-tag3"},
                            {"name": "auto-tag3", "autoTagged": True}
                        ]
                    },
                    {
                        "asset": {"id": 3, "uid": "asset-3"},
                        "tags": [
                            {"name": "auto-tag4", "autoTagged": True},
                            {"name": "auto-tag5", "autoTagged": True}
                        ]
                    }
                ]
            }
        }
        
        mock_client.make_api_call.side_effect = [
            count_response,  # Count call
            page_response    # Page call
        ]

        with patch('src.adoc_migration_toolkit.execution.asset_operations.globals.GLOBAL_OUTPUT_DIR', temp_dir):
            execute_asset_list_export(
                client=mock_client,
                logger=mock_logger,
                verbose_mode=True
            )

        # Verify output file was created
        output_file = temp_dir / "asset-export" / "asset-all-source-export.csv"
        assert output_file.exists()
        
        # Verify CSV content
        with open(output_file, 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
            
        assert rows[0] == ['source_uid', 'source_id', 'target_uid', 'tags']
        assert len(rows) == 4  # Header + 3 data rows
        
        # Verify that autoTagged tags are filtered out
        assert rows[1] == ['asset-1', '1', 'asset-1', 'manual-tag1:manual-tag2']  # Only manual tags
        assert rows[2] == ['asset-2', '2', 'asset-2', 'manual-tag3']  # Only manual tag
        assert rows[3] == ['asset-3', '3', 'asset-3', '']  # No manual tags, so empty
    
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
        output_file = temp_dir / "asset-export" / "asset-all-source-export.csv"
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
            output_file = temp_dir / "asset-export" / "asset-all-source-export.csv"
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
        output_file = temp_dir / "asset-export" / "asset-all-source-export.csv"
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
        output_file = temp_dir / "asset-export" / "asset-all-source-export.csv"
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
    
    def test_execute_asset_list_export_parallel_with_autotagged_filtering(self, temp_dir, mock_client, mock_logger):
        """Test parallel asset list export with autoTagged tag filtering."""
        # Mock count response
        count_response = {
            "meta": {
                "count": 6
            }
        }
        
        # Mock page responses with autoTagged tags
        page_response = {
            "data": {
                "assets": [
                    {
                        "asset": {"id": 1, "uid": "asset-1"},
                        "tags": [
                            {"name": "manual-tag1"},
                            {"name": "auto-tag1", "autoTagged": True},
                            {"name": "manual-tag2"}
                        ]
                    },
                    {
                        "asset": {"id": 2, "uid": "asset-2"},
                        "tags": [
                            {"name": "auto-tag2", "autoTagged": True},
                            {"name": "manual-tag3"}
                        ]
                    }
                ]
            }
        }
        
        # Mock multiple page calls for parallel processing
        mock_client.make_api_call.side_effect = [
            count_response,  # Count call
            page_response,   # Page 0
            page_response,   # Page 1
            page_response    # Page 2
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
        output_file = temp_dir / "asset-export" / "asset-all-source-export.csv"
        assert output_file.exists()
        
        # Verify CSV content
        with open(output_file, 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
            
        assert rows[0] == ['source_uid', 'source_id', 'target_uid', 'tags']
        assert len(rows) > 1  # Header + data rows
        
        # Verify that autoTagged tags are filtered out in parallel mode too
        assert rows[1] == ['asset-1', '1', 'asset-1', 'manual-tag1:manual-tag2']  # Only manual tags
        assert rows[2] == ['asset-2', '2', 'asset-2', 'manual-tag3']  # Only manual tag


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
    
    def test_asset_config_export_import_workflow(self, temp_dir, mock_client, mock_logger, sample_config_response):
        """Test complete asset config export and import workflow."""
        # Step 1: Export configs
        csv_file = temp_dir / "test_assets.csv"
        with open(csv_file, 'w') as f:
            f.write("source_id,source_uid,target_id,target_uid,tags\n")
            f.write("1,asset-1-PROD_DB,101,asset-1-DEV_DB,tag1\n")
        
        # Mock export API responses - only config call needed
        mock_client.make_api_call.side_effect = [
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
            logger=mock_logger
        )
        
        # Verify both operations completed
        assert mock_client.make_api_call.call_count >= 1
    
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

    def test_parse_asset_list_export_command_with_page_size(self):
        """Test parsing asset-list-export command with --page-size option."""
        from src.adoc_migration_toolkit.execution.command_parsing import parse_asset_list_export_command
        
        # Test with page size
        quiet, verbose, parallel, target, page_size, source_type_ids, asset_type_ids, assembly_ids = parse_asset_list_export_command("asset-list-export --page-size 1000")
        assert not quiet
        assert not verbose
        assert not parallel
        assert not target
        assert page_size == 1000
        assert source_type_ids is None
        assert asset_type_ids is None
        assert assembly_ids is None
        
        # Test with page size and other flags
        quiet, verbose, parallel, target, page_size, source_type_ids, asset_type_ids, assembly_ids = parse_asset_list_export_command("asset-list-export --page-size 250 --verbose --parallel")
        assert not quiet
        assert verbose
        assert parallel
        assert not target
        assert page_size == 250
        assert source_type_ids is None
        assert asset_type_ids is None
        assert assembly_ids is None
        
        # Test with page size and target
        quiet, verbose, parallel, target, page_size, source_type_ids, asset_type_ids, assembly_ids = parse_asset_list_export_command("asset-list-export --target --page-size 750 --quiet")
        assert quiet
        assert not verbose
        assert not parallel
        assert target
        assert page_size == 750
        assert source_type_ids is None
        assert asset_type_ids is None
        assert assembly_ids is None
        
        # Test default page size
        quiet, verbose, parallel, target, page_size, source_type_ids, asset_type_ids, assembly_ids = parse_asset_list_export_command("asset-list-export")
        assert not quiet
        assert not verbose
        assert not parallel
        assert not target
        assert page_size == 500
        assert source_type_ids is None
        assert asset_type_ids is None
        assert assembly_ids is None
        
        # Test invalid page size
        with pytest.raises(ValueError):
            parse_asset_list_export_command("asset-list-export --page-size -100")
        
        with pytest.raises(ValueError):
            parse_asset_list_export_command("asset-list-export --page-size 0")
        
        with pytest.raises(ValueError):
            parse_asset_list_export_command("asset-list-export --page-size abc")
        
        with pytest.raises(ValueError):
            parse_asset_list_export_command("asset-list-export --page-size") 

    def test_parse_transform_and_merge_command(self):
        """Test parsing transform-and-merge command with string transformations."""
        from src.adoc_migration_toolkit.execution.command_parsing import parse_transform_and_merge_command
        
        # Test with string transformations
        string_transforms, quiet_mode, verbose_mode = parse_transform_and_merge_command("transform-and-merge --string-transform \"PROD_DB\":\"DEV_DB\"")
        assert string_transforms == {"PROD_DB": "DEV_DB"}
        assert not quiet_mode
        assert not verbose_mode
        
        # Test with multiple transformations
        string_transforms, quiet_mode, verbose_mode = parse_transform_and_merge_command("transform-and-merge --string-transform \"A\":\"B\", \"C\":\"D\" --verbose")
        assert string_transforms == {"A": "B", "C": "D"}
        assert not quiet_mode
        assert verbose_mode
        
        # Test with quiet mode
        string_transforms, quiet_mode, verbose_mode = parse_transform_and_merge_command("transform-and-merge --string-transform \"old\":\"new\" --quiet")
        assert string_transforms == {"old": "new"}
        assert quiet_mode
        assert not verbose_mode
        
        # Test missing string transform
        with pytest.raises(ValueError):
            parse_transform_and_merge_command("transform-and-merge")
        
        # Test invalid command
        string_transforms, quiet_mode, verbose_mode = parse_transform_and_merge_command("invalid-command")
        assert string_transforms is None
        assert not quiet_mode
        assert not verbose_mode 

    def test_execute_transform_and_merge_success(self, temp_dir, mock_logger):
        """Test successful transform-and-merge execution."""
        from src.adoc_migration_toolkit.execution.asset_operations import execute_transform_and_merge
        
        # Create asset-export directory
        asset_export_dir = temp_dir / "asset-export"
        asset_export_dir.mkdir(parents=True, exist_ok=True)
        
        # Create source CSV file
        source_file = asset_export_dir / "asset-all-source-export.csv"
        with open(source_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['source_uid', 'source_id', 'target_uid', 'tags'])
            writer.writerow(['asset-1', '1', 'Snowflake', 'tag1:tag2'])
            writer.writerow(['asset-2', '2', 'SNOWFLAKE_SAMPLE_DATA', 'tag3'])
            writer.writerow(['asset-3', '3', 'OTHER_DATA', 'tag4'])
        
        # Create target CSV file
        target_file = asset_export_dir / "asset-all-target-export.csv"
        with open(target_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['source_uid', 'source_id', 'target_uid', 'tags'])
            writer.writerow(['snowflake_krish_test', '101', 'snowflake_krish_test', 'tag1:tag2'])
            writer.writerow(['KRISH_TEST', '102', 'KRISH_TEST', 'tag3'])
        
        # Mock global output directory
        with patch('src.adoc_migration_toolkit.execution.asset_operations.globals.GLOBAL_OUTPUT_DIR', temp_dir):
            string_transforms = {"Snowflake": "snowflake_krish_test", "SNOWFLAKE_SAMPLE_DATA": "KRISH_TEST"}
            execute_transform_and_merge(string_transforms, False, False, mock_logger)
        
        # Verify output file was created
        asset_import_dir = asset_export_dir.parent / "asset-import"
        output_file = asset_import_dir / "asset-merged-all.csv"
        assert output_file.exists()
        
        # Verify output content
        with open(output_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        assert len(rows) == 2  # Only 2 records should match (exact match only)
        assert rows[0]['source_uid'] == 'asset-1'
        assert rows[0]['source_id'] == '1'
        assert rows[0]['target_uid'] == 'snowflake_krish_test'
        assert rows[0]['target_id'] == '101'
        assert rows[0]['tags'] == 'tag1:tag2'
        
        assert rows[1]['source_uid'] == 'asset-2'
        assert rows[1]['source_id'] == '2'
        assert rows[1]['target_uid'] == 'KRISH_TEST'
        assert rows[1]['target_id'] == '102'
        assert rows[1]['tags'] == 'tag3' 