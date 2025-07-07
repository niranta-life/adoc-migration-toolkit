"""
Test cases for the segment_operations module.

This module contains tests for segment export and import functions.
"""

import pytest
import json
import csv
import tempfile
import logging
from pathlib import Path
from unittest.mock import patch, Mock

from src.adoc_migration_toolkit.execution.segment_operations import (
    execute_segments_export,
    execute_segments_import
)

@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)

@pytest.fixture
def mock_client():
    client = Mock()
    client.host = "https://test.example.com"
    client.tenant = "test-tenant"
    client.target_tenant = "target-tenant"
    return client

@pytest.fixture
def mock_logger():
    return Mock()

@pytest.fixture
def sample_csv_data():
    return "source-env,target-env\nasset-1-PROD_DB,asset-1-DEV_DB\nasset-2-PROD_DB,asset-2-DEV_DB"

@pytest.fixture
def sample_asset_response():
    return {
        "data": [
            {"id": 12345, "uid": "asset-1-PROD_DB", "name": "Test Asset 1"}
        ]
    }

@pytest.fixture
def sample_segments_response():
    return {
        "assetSegments": {
            "segments": [
                {"name": "seg1", "conditions": [{"columnId": 1, "condition": "EQUALS", "value": "foo"}]},
                {"name": "seg2", "conditions": []}
            ]
        }
    }

class TestExecuteSegmentsExport:
    def test_success(self, temp_dir, mock_client, mock_logger, sample_csv_data, sample_asset_response, sample_segments_response):
        csv_file = temp_dir / "test.csv"
        with open(csv_file, 'w') as f:
            f.write(sample_csv_data)
        
        # Debug: print the mock responses
        print(f"Sample asset response: {sample_asset_response}")
        print(f"Sample segments response: {sample_segments_response}")
        
        mock_client.make_api_call.side_effect = [
            sample_asset_response, sample_segments_response,
            sample_asset_response, sample_segments_response
        ]
        
        # Debug: check if segments are valid
        segments = sample_segments_response.get("assetSegments", {}).get("segments", [])
        print(f"Extracted segments: {segments}")
        print(f"Segments is list: {isinstance(segments, list)}")
        print(f"Segments length: {len(segments) if isinstance(segments, list) else 'N/A'}")
        
        # Patch globals in both modules to ensure output file is created in temp directory
        with patch('src.adoc_migration_toolkit.execution.segment_operations.globals.GLOBAL_OUTPUT_DIR', temp_dir), \
             patch('src.adoc_migration_toolkit.shared.file_utils.globals.GLOBAL_OUTPUT_DIR', temp_dir):
            execute_segments_export(
                csv_file=str(csv_file),
                client=mock_client,
                logger=mock_logger,
                quiet_mode=False  # Enable verbose output for debugging
            )
        
        # Debug: print temp_dir contents
        print(f"temp_dir contents: {list(temp_dir.rglob('*'))}")
        
        # Look for output file in the policy-import subdirectory
        output_files = list(temp_dir.glob("policy-import/segments_output.csv"))
        print(f"Found output files: {output_files}")
        
        if len(output_files) == 0:
            # Try to find any CSV files
            all_csv_files = list(temp_dir.rglob("*.csv"))
            print(f"All CSV files in temp_dir: {all_csv_files}")
        
        assert len(output_files) > 0
        with open(output_files[0], 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
        assert rows[0] == ['target-env', 'segments_json']
        assert len(rows) == 3
    def test_csv_not_found(self, mock_client, mock_logger):
        execute_segments_export(
            csv_file="/nonexistent/file.csv",
            client=mock_client,
            logger=mock_logger
        )
        mock_logger.error.assert_called()
    def test_empty_csv(self, temp_dir, mock_client, mock_logger):
        csv_file = temp_dir / "empty.csv"
        with open(csv_file, 'w') as f:
            f.write("source-env,target-env\n")
        execute_segments_export(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger
        )
        mock_logger.warning.assert_called()
    def test_api_error(self, temp_dir, mock_client, mock_logger, sample_csv_data):
        csv_file = temp_dir / "test.csv"
        with open(csv_file, 'w') as f:
            f.write(sample_csv_data)
        mock_client.make_api_call.side_effect = Exception("API Error")
        execute_segments_export(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger
        )
        mock_logger.error.assert_called()

class TestExecuteSegmentsImport:
    def test_success(self, temp_dir, mock_client, mock_logger, sample_segments_response):
        csv_file = temp_dir / "segments.csv"
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['target-env', 'segments_json'])
            writer.writerow(['asset-1-DEV_DB', json.dumps(sample_segments_response)])
        mock_client.make_api_call.return_value = {"data": [{"id": 12345}]}
        execute_segments_import(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger,
            dry_run=True,
            quiet_mode=True
        )
        mock_client.make_api_call.assert_not_called()  # dry_run disables API calls
    def test_csv_not_found(self, mock_client, mock_logger):
        execute_segments_import(
            csv_file="/nonexistent/file.csv",
            client=mock_client,
            logger=mock_logger
        )
        mock_logger.error.assert_called()
    def test_invalid_csv_format(self, temp_dir, mock_client, mock_logger):
        csv_file = temp_dir / "bad.csv"
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['bad', 'header'])
        execute_segments_import(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger
        )
        mock_logger.error.assert_called()
    def test_invalid_json(self, temp_dir, mock_client, mock_logger):
        csv_file = temp_dir / "badjson.csv"
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['target-env', 'segments_json'])
            writer.writerow(['asset-1-DEV_DB', 'not json'])
        mock_client.make_api_call.return_value = {"data": [{"id": 12345}]}
        execute_segments_import(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger
        )
        mock_logger.error.assert_called()
    def test_no_segments(self, temp_dir, mock_client, mock_logger):
        csv_file = temp_dir / "nosegments.csv"
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['target-env', 'segments_json'])
            writer.writerow(['asset-1-DEV_DB', json.dumps({})])
        mock_client.make_api_call.return_value = {"data": [{"id": 12345}]}
        execute_segments_import(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger
        )
        mock_logger.error.assert_called()
    def test_api_error(self, temp_dir, mock_client, mock_logger, sample_segments_response):
        csv_file = temp_dir / "segments.csv"
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['target-env', 'segments_json'])
            writer.writerow(['asset-1-DEV_DB', json.dumps(sample_segments_response)])
        mock_client.make_api_call.side_effect = Exception("API Error")
        execute_segments_import(
            csv_file=str(csv_file),
            client=mock_client,
            logger=mock_logger
        )
        mock_logger.error.assert_called() 