"""
Test cases for the formatter module.

This module contains comprehensive tests for the PolicyExportFormatter class
and related functions in the formatter module.
"""

import pytest
import json
import tempfile
import zipfile
import csv
import logging
from pathlib import Path
from unittest.mock import Mock, patch, mock_open, MagicMock
from datetime import datetime

from src.adoc_migration_toolkit.execution.formatter import (
    PolicyExportFormatter,
    validate_arguments,
    parse_formatter_command,
    execute_formatter
)
from src.adoc_migration_toolkit.shared import globals


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def sample_json_data():
    """Sample JSON data for testing."""
    return {
        "name": "test_policy",
        "uid": "test-uid-123",
        "description": "A test policy with PROD_DB references",
        "config": {
            "database": "PROD_DB.users",
            "query": "SELECT * FROM PROD_DB.users WHERE active = true"
        },
        "backingAssets": [
            {"uid": "asset-1-PROD_DB"},
            {"uid": "asset-2-PROD_DB"}
        ]
    }


@pytest.fixture
def sample_policy_data():
    """Sample policy data for testing."""
    return [
        {
            "name": "segmented_spark_policy",
            "isSegmented": True,
            "engineType": "SPARK",
            "backingAssets": [
                {"uid": "asset-spark-1-PROD_DB"},
                {"uid": "asset-spark-2-PROD_DB"}
            ]
        },
        {
            "name": "segmented_jdbc_policy",
            "isSegmented": True,
            "engineType": "JDBC_SQL",
            "backingAssets": [
                {"uid": "asset-jdbc-1-PROD_DB"}
            ]
        },
        {
            "name": "non_segmented_policy",
            "isSegmented": False,
            "engineType": "SPARK",
            "backingAssets": [
                {"uid": "asset-non-seg-1-PROD_DB"}
            ]
        }
    ]


class TestPolicyExportFormatter:
    """Test cases for PolicyExportFormatter class."""
    
    def test_init_with_valid_parameters(self, temp_dir):
        """Test PolicyExportFormatter initialization with valid parameters."""
        formatter = PolicyExportFormatter(
            input_dir=str(temp_dir),
            search_string="PROD_DB",
            replace_string="DEV_DB"
        )
        
        assert formatter.input_dir == temp_dir.resolve()
        assert formatter.search_string == "PROD_DB"
        assert formatter.replace_string == "DEV_DB"
        assert formatter.output_dir.exists()
        assert formatter.asset_export_dir.exists()
        assert formatter.policy_export_dir.exists()
    
    def test_init_with_empty_input_dir(self):
        """Test PolicyExportFormatter initialization with empty input directory."""
        with pytest.raises(ValueError, match="Input directory cannot be empty"):
            PolicyExportFormatter(
                input_dir="",
                search_string="PROD_DB",
                replace_string="DEV_DB"
            )
    
    def test_init_with_empty_search_string(self, temp_dir):
        """Test PolicyExportFormatter initialization with empty search string."""
        with pytest.raises(ValueError, match="Search string cannot be empty"):
            PolicyExportFormatter(
                input_dir=str(temp_dir),
                search_string="",
                replace_string="DEV_DB"
            )
    
    def test_init_with_none_replace_string(self, temp_dir):
        """Test PolicyExportFormatter initialization with None replace string."""
        with pytest.raises(ValueError, match="Replace string cannot be None"):
            PolicyExportFormatter(
                input_dir=str(temp_dir),
                search_string="PROD_DB",
                replace_string=None
            )
    
    def test_init_with_nonexistent_input_dir(self):
        """Test PolicyExportFormatter initialization with nonexistent input directory."""
        with pytest.raises(FileNotFoundError):
            PolicyExportFormatter(
                input_dir="/nonexistent/directory",
                search_string="PROD_DB",
                replace_string="DEV_DB"
            )
    
    def test_init_with_file_as_input_dir(self, temp_dir):
        """Test PolicyExportFormatter initialization with file as input directory."""
        test_file = temp_dir / "test.txt"
        test_file.write_text("test")
        
        with pytest.raises(ValueError, match="Input path is not a directory"):
            PolicyExportFormatter(
                input_dir=str(test_file),
                search_string="PROD_DB",
                replace_string="DEV_DB"
            )
    
    def test_init_with_custom_output_dir(self, temp_dir):
        """Test PolicyExportFormatter initialization with custom output directory."""
        custom_output = temp_dir / "custom_output"
        
        formatter = PolicyExportFormatter(
            input_dir=str(temp_dir),
            search_string="PROD_DB",
            replace_string="DEV_DB",
            output_dir=str(custom_output)
        )
        
        assert formatter.base_output_dir == custom_output.resolve()
        assert formatter.output_dir == custom_output.resolve() / "policy-import"
        assert formatter.asset_export_dir == custom_output.resolve() / "asset-export"
        assert formatter.policy_export_dir == custom_output.resolve() / "policy-export"
    
    def test_replace_in_value_string(self, temp_dir):
        """Test replace_in_value with string values."""
        formatter = PolicyExportFormatter(
            input_dir=str(temp_dir),
            search_string="PROD_DB",
            replace_string="DEV_DB"
        )
        
        # Test string replacement
        result = formatter.replace_in_value("PROD_DB.users")
        assert result == "DEV_DB.users"
        assert formatter.stats["changes_made"] == 1
        
        # Test string without replacement
        result = formatter.replace_in_value("DEV_DB.users")
        assert result == "DEV_DB.users"
        # changes_made should not increment
    
    def test_replace_in_value_dict(self, temp_dir):
        """Test replace_in_value with dictionary values."""
        formatter = PolicyExportFormatter(
            input_dir=str(temp_dir),
            search_string="PROD_DB",
            replace_string="DEV_DB"
        )
        
        input_dict = {
            "name": "test",
            "database": "PROD_DB.users",
            "nested": {
                "query": "SELECT * FROM PROD_DB.users"
            }
        }
        
        result = formatter.replace_in_value(input_dict)
        
        expected = {
            "name": "test",
            "database": "DEV_DB.users",
            "nested": {
                "query": "SELECT * FROM DEV_DB.users"
            }
        }
        
        assert result == expected
        assert formatter.stats["changes_made"] == 2
    
    def test_replace_in_value_list(self, temp_dir):
        """Test replace_in_value with list values."""
        formatter = PolicyExportFormatter(
            input_dir=str(temp_dir),
            search_string="PROD_DB",
            replace_string="DEV_DB"
        )
        
        input_list = [
            "PROD_DB.users",
            {"query": "SELECT * FROM PROD_DB.users"},
            ["PROD_DB.schema", "other_value"]
        ]
        
        result = formatter.replace_in_value(input_list)
        
        expected = [
            "DEV_DB.users",
            {"query": "SELECT * FROM DEV_DB.users"},
            ["DEV_DB.schema", "other_value"]
        ]
        
        assert result == expected
        assert formatter.stats["changes_made"] == 3
    
    def test_replace_in_value_other_types(self, temp_dir):
        """Test replace_in_value with non-string types."""
        formatter = PolicyExportFormatter(
            input_dir=str(temp_dir),
            search_string="PROD_DB",
            replace_string="DEV_DB"
        )
        
        # Test with numbers, booleans, None
        assert formatter.replace_in_value(123) == 123
        assert formatter.replace_in_value(True) == True
        assert formatter.replace_in_value(None) == None
        assert formatter.stats["changes_made"] == 0
    
    def test_extract_data_quality_assets_list(self, temp_dir, sample_policy_data):
        """Test extract_data_quality_assets with list of policies."""
        formatter = PolicyExportFormatter(
            input_dir=str(temp_dir),
            search_string="PROD_DB",
            replace_string="DEV_DB"
        )
        
        formatter.extract_data_quality_assets(sample_policy_data)
        
        # Should extract assets from segmented SPARK policies only
        assert "asset-spark-1-PROD_DB" in formatter.extracted_assets
        assert "asset-spark-2-PROD_DB" in formatter.extracted_assets
        assert "asset-jdbc-1-PROD_DB" not in formatter.extracted_assets
        assert "asset-non-seg-1-PROD_DB" not in formatter.extracted_assets
        
        # All assets should be in all_asset_uids
        assert "asset-spark-1-PROD_DB" in formatter.all_asset_uids
        assert "asset-spark-2-PROD_DB" in formatter.all_asset_uids
        assert "asset-jdbc-1-PROD_DB" in formatter.all_asset_uids
        assert "asset-non-seg-1-PROD_DB" in formatter.all_asset_uids
        
        # Check statistics
        assert formatter.stats["total_policies_processed"] == 3
        assert formatter.stats["segmented_spark_policies"] == 1
        assert formatter.stats["segmented_jdbc_policies"] == 1
        assert formatter.stats["non_segmented_policies"] == 1
    
    def test_extract_data_quality_assets_single_policy(self, temp_dir):
        """Test extract_data_quality_assets with single policy."""
        formatter = PolicyExportFormatter(
            input_dir=str(temp_dir),
            search_string="PROD_DB",
            replace_string="DEV_DB"
        )
        
        single_policy = {
            "name": "single_policy",
            "isSegmented": True,
            "engineType": "SPARK",
            "backingAssets": [
                {"uid": "single-asset-PROD_DB"}
            ]
        }
        
        formatter.extract_data_quality_assets(single_policy)
        
        assert "single-asset-PROD_DB" in formatter.extracted_assets
        assert "single-asset-PROD_DB" in formatter.all_asset_uids
        assert formatter.stats["total_policies_processed"] == 1
        assert formatter.stats["segmented_spark_policies"] == 1
    
    def test_write_extracted_assets_csv(self, temp_dir):
        """Test write_extracted_assets_csv method."""
        formatter = PolicyExportFormatter(
            input_dir=str(temp_dir),
            search_string="PROD_DB",
            replace_string="DEV_DB"
        )
        
        # Add some extracted assets
        formatter.extracted_assets.add("asset-1-PROD_DB")
        formatter.extracted_assets.add("asset-2-PROD_DB")
        
        formatter.write_extracted_assets_csv()
        
        csv_file = formatter.policy_export_dir / "segmented_spark_uids.csv"
        assert csv_file.exists()
        
        # Read and verify CSV content
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        assert rows[0] == ['source-env', 'target-env']
        assert ['asset-1-PROD_DB', 'asset-1-DEV_DB'] in rows
        assert ['asset-2-PROD_DB', 'asset-2-DEV_DB'] in rows
    
    def test_write_all_assets_csv(self, temp_dir):
        """Test write_all_assets_csv method."""
        formatter = PolicyExportFormatter(
            input_dir=str(temp_dir),
            search_string="PROD_DB",
            replace_string="DEV_DB"
        )
        
        # Add some assets
        formatter.all_asset_uids.add("asset-1-PROD_DB")
        formatter.all_asset_uids.add("asset-2-PROD_DB")
        
        formatter.write_all_assets_csv()
        
        csv_file = formatter.asset_export_dir / "asset_uids.csv"
        assert csv_file.exists()
        
        # Read and verify CSV content
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        assert rows[0] == ['source-env', 'target-env']
        assert ['asset-1-PROD_DB', 'asset-1-DEV_DB'] in rows
        assert ['asset-2-PROD_DB', 'asset-2-DEV_DB'] in rows
    
    def test_process_json_file_success(self, temp_dir, sample_json_data):
        """Test successful JSON file processing."""
        # Resolve the temp_dir to handle symlink issues on macOS
        resolved_temp_dir = temp_dir.resolve()
        
        formatter = PolicyExportFormatter(
            input_dir=str(resolved_temp_dir),
            search_string="PROD_DB",
            replace_string="DEV_DB"
        )
        
        # Create test JSON file within the input directory
        json_file = resolved_temp_dir / "test.json"
        with open(json_file, 'w') as f:
            json.dump(sample_json_data, f)
        
        result = formatter.process_json_file(json_file)
        
        assert result == True
        assert formatter.stats["files_investigated"] == 1
        assert formatter.stats["json_files_processed"] == 1
        
        # Check output file
        output_file = formatter.output_dir / "test.json"
        assert output_file.exists()
        
        # Verify content was replaced
        with open(output_file, 'r') as f:
            output_data = json.load(f)
        
        assert output_data["config"]["database"] == "DEV_DB.users"
        assert output_data["config"]["query"] == "SELECT * FROM DEV_DB.users WHERE active = true"
    
    def test_process_json_file_with_data_quality_policies(self, temp_dir, sample_policy_data):
        """Test JSON file processing with data quality policy definitions."""
        # Resolve the temp_dir to handle symlink issues on macOS
        resolved_temp_dir = temp_dir.resolve()
        
        formatter = PolicyExportFormatter(
            input_dir=str(resolved_temp_dir),
            search_string="PROD_DB",
            replace_string="DEV_DB"
        )
        
        # Create test JSON file with data quality policy definitions
        json_file = resolved_temp_dir / "data_quality_policy_definitions.json"
        with open(json_file, 'w') as f:
            json.dump(sample_policy_data, f)
        
        result = formatter.process_json_file(json_file)
        
        assert result == True
        assert formatter.stats["files_investigated"] == 1
        assert formatter.stats["json_files_processed"] == 1
        
        # Check that assets were extracted
        assert "asset-spark-1-PROD_DB" in formatter.extracted_assets
        assert "asset-spark-2-PROD_DB" in formatter.extracted_assets
        assert formatter.stats["total_policies_processed"] == 3
    
    def test_process_json_file_nonexistent(self, temp_dir):
        """Test JSON file processing with nonexistent file."""
        formatter = PolicyExportFormatter(
            input_dir=str(temp_dir),
            search_string="PROD_DB",
            replace_string="DEV_DB"
        )
        
        nonexistent_file = temp_dir / "nonexistent.json"
        result = formatter.process_json_file(nonexistent_file)
        
        assert result == False
        assert len(formatter.stats["errors"]) > 0
    
    def test_process_json_file_invalid_json(self, temp_dir):
        """Test JSON file processing with invalid JSON."""
        formatter = PolicyExportFormatter(
            input_dir=str(temp_dir),
            search_string="PROD_DB",
            replace_string="DEV_DB"
        )
        
        # Create invalid JSON file
        json_file = temp_dir / "invalid.json"
        json_file.write_text("{ invalid json }")
        
        result = formatter.process_json_file(json_file)
        
        assert result == False
        assert len(formatter.stats["errors"]) > 0
    
    def test_process_zip_file_success(self, temp_dir, sample_json_data):
        """Test successful ZIP file processing."""
        formatter = PolicyExportFormatter(
            input_dir=str(temp_dir),
            search_string="PROD_DB",
            replace_string="DEV_DB"
        )
        
        # Create test ZIP file
        zip_file = temp_dir / "test.zip"
        with zipfile.ZipFile(zip_file, 'w') as zip_ref:
            # Add JSON file to ZIP
            json_content = json.dumps(sample_json_data)
            zip_ref.writestr("test.json", json_content)
            # Add non-JSON file
            zip_ref.writestr("readme.txt", "This is a readme file")
        
        result = formatter.process_zip_file(zip_file)
        
        assert result == True
        # Note: process_zip_file calls process_json_file_in_zip which increments files_investigated
        assert formatter.stats["zip_files_processed"] == 1
        
        # Check output ZIP file
        output_zip = formatter.output_dir / "test-import-ready.zip"
        assert output_zip.exists()
        
        # Verify ZIP contents
        with zipfile.ZipFile(output_zip, 'r') as zip_ref:
            files = zip_ref.namelist()
            assert "test.json" in files
            assert "readme.txt" in files
            
            # Check that JSON content was processed
            json_content = zip_ref.read("test.json").decode('utf-8')
            json_data = json.loads(json_content)
            assert "DEV_DB" in json_content
            assert "PROD_DB" not in json_content
    
    def test_process_zip_file_nonexistent(self, temp_dir):
        """Test ZIP file processing with nonexistent file."""
        formatter = PolicyExportFormatter(
            input_dir=str(temp_dir),
            search_string="PROD_DB",
            replace_string="DEV_DB"
        )
        
        nonexistent_file = temp_dir / "nonexistent.zip"
        result = formatter.process_zip_file(nonexistent_file)
        
        assert result == False
        assert len(formatter.stats["errors"]) > 0
    
    def test_process_zip_file_invalid_zip(self, temp_dir):
        """Test ZIP file processing with invalid ZIP file."""
        formatter = PolicyExportFormatter(
            input_dir=str(temp_dir),
            search_string="PROD_DB",
            replace_string="DEV_DB"
        )
        
        # Create invalid ZIP file
        zip_file = temp_dir / "invalid.zip"
        zip_file.write_text("This is not a ZIP file")
        
        result = formatter.process_zip_file(zip_file)
        
        assert result == False
        assert len(formatter.stats["errors"]) > 0
    
    def test_process_directory_empty(self, temp_dir):
        """Test process_directory with empty directory."""
        formatter = PolicyExportFormatter(
            input_dir=str(temp_dir),
            search_string="PROD_DB",
            replace_string="DEV_DB"
        )
        
        stats = formatter.process_directory()
        
        assert stats["total_files"] == 0
        assert stats["json_files"] == 0
        assert stats["zip_files"] == 0
        assert stats["successful"] == 0
        assert stats["failed"] == 0
    
    def test_process_directory_with_files(self, temp_dir, sample_json_data):
        """Test process_directory with JSON and ZIP files."""
        formatter = PolicyExportFormatter(
            input_dir=str(temp_dir),
            search_string="PROD_DB",
            replace_string="DEV_DB"
        )
        
        # Create test files
        json_file = temp_dir / "test.json"
        with open(json_file, 'w') as f:
            json.dump(sample_json_data, f)
        
        zip_file = temp_dir / "test.zip"
        with zipfile.ZipFile(zip_file, 'w') as zip_ref:
            zip_ref.writestr("nested.json", json.dumps(sample_json_data))
        
        stats = formatter.process_directory()
        
        assert stats["total_files"] == 2
        assert stats["json_files"] == 1
        assert stats["zip_files"] == 1
        assert stats["successful"] == 2
        assert stats["failed"] == 0
        
        # Check that CSV files were created
        assert (formatter.policy_export_dir / "segmented_spark_uids.csv").exists()
        assert (formatter.asset_export_dir / "asset_uids.csv").exists()


class TestValidateArguments:
    """Test cases for validate_arguments function."""
    
    def test_validate_arguments_valid(self, temp_dir):
        """Test validate_arguments with valid arguments."""
        args = Mock()
        args.input_dir = str(temp_dir)
        args.search_string = "PROD_DB"
        args.replace_string = "DEV_DB"
        
        # Should not raise any exception
        validate_arguments(args)
    
    def test_validate_arguments_empty_input_dir(self):
        """Test validate_arguments with empty input directory."""
        args = Mock()
        args.input_dir = ""
        args.search_string = "PROD_DB"
        args.replace_string = "DEV_DB"
        
        with pytest.raises(ValueError, match="Input directory cannot be empty"):
            validate_arguments(args)
    
    def test_validate_arguments_empty_search_string(self, temp_dir):
        """Test validate_arguments with empty search string."""
        args = Mock()
        args.input_dir = str(temp_dir)
        args.search_string = ""
        args.replace_string = "DEV_DB"
        
        with pytest.raises(ValueError, match="Search string cannot be empty"):
            validate_arguments(args)
    
    def test_validate_arguments_none_replace_string(self, temp_dir):
        """Test validate_arguments with None replace string."""
        args = Mock()
        args.input_dir = str(temp_dir)
        args.search_string = "PROD_DB"
        args.replace_string = None
        
        with pytest.raises(ValueError, match="Replace string cannot be None"):
            validate_arguments(args)
    
    def test_validate_arguments_nonexistent_input_dir(self):
        """Test validate_arguments with nonexistent input directory."""
        args = Mock()
        args.input_dir = "/nonexistent/directory"
        args.search_string = "PROD_DB"
        args.replace_string = "DEV_DB"
        
        with pytest.raises(FileNotFoundError):
            validate_arguments(args)


class TestParseFormatterCommand:
    """Test cases for parse_formatter_command function."""
    
    def test_parse_formatter_command_valid(self):
        """Test parse_formatter_command with valid command."""
        command = 'policy-xfr --source-env-string PROD_DB --target-env-string DEV_DB'
        
        input_dir, source_string, target_string, output_dir, quiet_mode, verbose_mode = parse_formatter_command(command)
        
        assert input_dir is None
        assert source_string == "PROD_DB"
        assert target_string == "DEV_DB"
        assert output_dir is None
        assert quiet_mode is False
        assert verbose_mode is False
    
    def test_parse_formatter_command_with_input_dir(self):
        """Test parse_formatter_command with input directory."""
        command = 'policy-xfr --input /path/to/input --source-env-string PROD_DB --target-env-string DEV_DB'
        
        input_dir, source_string, target_string, output_dir, quiet_mode, verbose_mode = parse_formatter_command(command)
        
        assert input_dir == "/path/to/input"
        assert source_string == "PROD_DB"
        assert target_string == "DEV_DB"
    
    def test_parse_formatter_command_with_output_dir(self):
        """Test parse_formatter_command with output directory."""
        command = 'policy-xfr --output-dir /path/to/output --source-env-string PROD_DB --target-env-string DEV_DB'
        
        input_dir, source_string, target_string, output_dir, quiet_mode, verbose_mode = parse_formatter_command(command)
        
        assert output_dir == "/path/to/output"
        assert source_string == "PROD_DB"
        assert target_string == "DEV_DB"
    
    def test_parse_formatter_command_with_flags(self):
        """Test parse_formatter_command with quiet and verbose flags."""
        command = 'policy-xfr --source-env-string PROD_DB --target-env-string DEV_DB --quiet --verbose'
        
        input_dir, source_string, target_string, output_dir, quiet_mode, verbose_mode = parse_formatter_command(command)
        
        assert quiet_mode is True
        assert verbose_mode is True
    
    def test_parse_formatter_command_missing_source_string(self):
        """Test parse_formatter_command with missing source string."""
        command = 'policy-xfr --target-env-string DEV_DB'
        
        input_dir, source_string, target_string, output_dir, quiet_mode, verbose_mode = parse_formatter_command(command)
        
        assert input_dir is None
        assert source_string is None
        assert target_string is None
    
    def test_parse_formatter_command_missing_target_string(self):
        """Test parse_formatter_command with missing target string."""
        command = 'policy-xfr --source-env-string PROD_DB'
        
        input_dir, source_string, target_string, output_dir, quiet_mode, verbose_mode = parse_formatter_command(command)
        
        assert input_dir is None
        assert source_string is None
        assert target_string is None
    
    def test_parse_formatter_command_help(self):
        """Test parse_formatter_command with help flag."""
        command = 'policy-xfr --help'
        
        input_dir, source_string, target_string, output_dir, quiet_mode, verbose_mode = parse_formatter_command(command)
        
        assert input_dir is None
        assert source_string is None
        assert target_string is None
    
    def test_parse_formatter_command_unknown_argument(self):
        """Test parse_formatter_command with unknown argument."""
        command = 'policy-xfr --unknown-arg value --source-env-string PROD_DB --target-env-string DEV_DB'
        
        input_dir, source_string, target_string, output_dir, quiet_mode, verbose_mode = parse_formatter_command(command)
        
        assert input_dir is None
        assert source_string is None
        assert target_string is None


class TestExecuteFormatter:
    """Test cases for execute_formatter function."""
    
    @pytest.fixture
    def mock_logger(self):
        """Create a mock logger."""
        return Mock(spec=logging.Logger)
    
    def test_execute_formatter_with_input_dir(self, temp_dir, mock_logger):
        """Test execute_formatter with specified input directory."""
        # Create test JSON file
        json_file = temp_dir / "test.json"
        with open(json_file, 'w') as f:
            json.dump({"name": "test", "database": "PROD_DB.users"}, f)
        
        execute_formatter(
            input_dir=str(temp_dir),
            source_string="PROD_DB",
            target_string="DEV_DB",
            output_dir=None,
            quiet_mode=False,
            verbose_mode=False,
            logger=mock_logger
        )
        
        # Verify that processing occurred
        mock_logger.info.assert_called()
    
    def test_execute_formatter_with_global_output_dir(self, temp_dir, mock_logger):
        """Test execute_formatter with global output directory."""
        # Set up global output directory
        global_output_dir = temp_dir / "global_output"
        global_output_dir.mkdir()
        policy_export_dir = global_output_dir / "policy-export"
        policy_export_dir.mkdir()
        
        # Create test file in policy-export
        json_file = policy_export_dir / "test.json"
        with open(json_file, 'w') as f:
            json.dump({"name": "test", "database": "PROD_DB.users"}, f)
        
        # Mock globals.GLOBAL_OUTPUT_DIR
        with patch('src.adoc_migration_toolkit.execution.formatter.globals.GLOBAL_OUTPUT_DIR', global_output_dir):
            execute_formatter(
                input_dir=None,
                source_string="PROD_DB",
                target_string="DEV_DB",
                output_dir=None,
                quiet_mode=False,
                verbose_mode=False,
                logger=mock_logger
            )
        
        # Verify that processing occurred
        mock_logger.info.assert_called()
    
    def test_execute_formatter_auto_detect_toolkit_dir(self, temp_dir, mock_logger):
        """Test execute_formatter with auto-detection of toolkit directory."""
        # Create toolkit directory
        toolkit_dir = temp_dir / "adoc-migration-toolkit-202401011200"
        toolkit_dir.mkdir()
        policy_export_dir = toolkit_dir / "policy-export"
        policy_export_dir.mkdir()
        
        # Create test file
        json_file = policy_export_dir / "test.json"
        with open(json_file, 'w') as f:
            json.dump({"name": "test", "database": "PROD_DB.users"}, f)
        
        # Mock current working directory
        with patch('pathlib.Path.cwd', return_value=temp_dir):
            execute_formatter(
                input_dir=None,
                source_string="PROD_DB",
                target_string="DEV_DB",
                output_dir=None,
                quiet_mode=False,
                verbose_mode=False,
                logger=mock_logger
            )
        
        # Verify that processing occurred
        mock_logger.info.assert_called()
    
    def test_execute_formatter_no_toolkit_dir_found(self, mock_logger):
        """Test execute_formatter when no toolkit directory is found."""
        # Mock current working directory with no toolkit dirs
        with patch('pathlib.Path.cwd', return_value=Path("/tmp")):
            execute_formatter(
                input_dir=None,
                source_string="PROD_DB",
                target_string="DEV_DB",
                output_dir=None,
                quiet_mode=False,
                verbose_mode=False,
                logger=mock_logger
            )
        
        # Should not call logger.info for processing
        mock_logger.info.assert_not_called()
    
    def test_execute_formatter_quiet_mode(self, temp_dir, mock_logger):
        """Test execute_formatter in quiet mode."""
        # Create test JSON file
        json_file = temp_dir / "test.json"
        with open(json_file, 'w') as f:
            json.dump({"name": "test", "database": "PROD_DB.users"}, f)
        
        execute_formatter(
            input_dir=str(temp_dir),
            source_string="PROD_DB",
            target_string="DEV_DB",
            output_dir=None,
            quiet_mode=True,
            verbose_mode=False,
            logger=mock_logger
        )
        
        # Verify that processing occurred but with minimal output
        mock_logger.info.assert_called()
    
    def test_execute_formatter_with_error(self, temp_dir, mock_logger):
        """Test execute_formatter with processing error."""
        # Create invalid JSON file
        json_file = temp_dir / "invalid.json"
        json_file.write_text("{ invalid json }")
        
        execute_formatter(
            input_dir=str(temp_dir),
            source_string="PROD_DB",
            target_string="DEV_DB",
            output_dir=None,
            quiet_mode=False,
            verbose_mode=False,
            logger=mock_logger
        )
        
        # Verify that error was logged
        mock_logger.error.assert_called() 