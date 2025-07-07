"""
Test cases for the lineage_operations module.

This module contains comprehensive tests for lineage creation operations
including CSV parsing, asset validation, and API integration.
"""

import pytest
import csv
import tempfile
import logging
from pathlib import Path
from unittest.mock import Mock, patch, mock_open, MagicMock
from io import StringIO

from src.adoc_migration_toolkit.execution.lineage_operations import (
    LineageRow,
    LineageProcessor,
    execute_create_lineage
)


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
def sample_lineage_csv_data():
    """Sample lineage CSV data for testing."""
    return (
        'Group ID,Step Order,Source Asset ID,Source Column,Target Asset ID,Target Column,Relationship Type,Transformation,Notes\n'
        'G1,1,table_456,col1,table_123,key1,upstream,join,"Join on col1, col2"\n'
        'G1,1,table_456,col2,table_123,key2,upstream,join,"Join on col1, col2"\n'
        'G2,1,table_457,id,table_123,join_id,upstream,join,\n'
        ',2,table_123,key1,table_789,final_key,downstream,filter(key1 > 100),Filtered output\n'
        ',,table_789,,table_999,,downstream,,,Full table'
    )


@pytest.fixture
def sample_asset_response():
    """Sample asset API response."""
    return {
        "uid": "table_456",
        "id": 12345,
        "name": "Test Table 456",
        "type": "table"
    }


@pytest.fixture
def sample_lineage_api_response():
    """Sample lineage API response."""
    return {
        "status": "success",
        "message": "Lineage created successfully",
        "id": 67890
    }


class TestLineageRow:
    """Test cases for LineageRow dataclass."""
    
    def test_lineage_row_creation(self):
        """Test creating a LineageRow with all fields."""
        row = LineageRow(
            group_id="G1",
            step_order=1,
            source_asset_id="table_456",
            source_column="col1",
            target_asset_id="table_123",
            target_column="key1",
            relationship_type="upstream",
            transformation="join",
            notes="Test join operation"
        )
        
        assert row.group_id == "G1"
        assert row.step_order == 1
        assert row.source_asset_id == "table_456"
        assert row.source_column == "col1"
        assert row.target_asset_id == "table_123"
        assert row.target_column == "key1"
        assert row.relationship_type == "upstream"
        assert row.transformation == "join"
        assert row.notes == "Test join operation"
    
    def test_lineage_row_optional_fields(self):
        """Test creating a LineageRow with optional fields as None."""
        row = LineageRow(
            group_id=None,
            step_order=None,
            source_asset_id="table_456",
            source_column=None,
            target_asset_id="table_123",
            target_column=None,
            relationship_type="downstream",
            transformation=None,
            notes=None
        )
        
        assert row.group_id is None
        assert row.step_order is None
        assert row.source_column is None
        assert row.target_column is None
        assert row.transformation is None
        assert row.notes is None


class TestLineageProcessor:
    """Test cases for LineageProcessor class."""
    
    def test_lineage_processor_initialization(self, mock_client, mock_logger):
        """Test LineageProcessor initialization."""
        processor = LineageProcessor(mock_client, mock_logger)
        
        assert processor.client == mock_client
        assert processor.logger == mock_logger
    
    def test_lineage_processor_default_logger(self, mock_client):
        """Test LineageProcessor initialization with default logger."""
        processor = LineageProcessor(mock_client)
        
        assert processor.client == mock_client
        assert processor.logger is not None
    
    def test_parse_lineage_csv_success(self, temp_dir, mock_client, sample_lineage_csv_data):
        """Test successful CSV parsing."""
        # Create test CSV file
        csv_file = temp_dir / "test_lineage.csv"
        with open(csv_file, 'w') as f:
            f.write(sample_lineage_csv_data)
        
        processor = LineageProcessor(mock_client)
        rows = processor.parse_lineage_csv(str(csv_file))
        
        assert len(rows) == 5
        
        # Check first row
        first_row = rows[0]
        assert first_row.group_id == "G1"
        assert first_row.step_order == 1
        assert first_row.source_asset_id == "table_456"
        assert first_row.source_column == "col1"
        assert first_row.target_asset_id == "table_123"
        assert first_row.target_column == "key1"
        assert first_row.relationship_type == "upstream"
        assert first_row.transformation == "join"
        assert first_row.notes == "Join on col1, col2"
        
        # Check row with empty fields
        empty_row = rows[2]
        assert empty_row.group_id == "G2"
        assert empty_row.step_order == 1
        assert empty_row.source_asset_id == "table_457"
        assert empty_row.source_column == "id"
        assert empty_row.target_asset_id == "table_123"
        assert empty_row.target_column == "join_id"
        assert empty_row.relationship_type == "upstream"
        assert empty_row.transformation == "join"
        assert empty_row.notes is None
    
    def test_parse_lineage_csv_missing_required_columns(self, temp_dir, mock_client):
        """Test CSV parsing with missing required columns."""
        # Create CSV with missing required columns
        csv_data = "Group ID,Step Order,Source Asset ID,Source Column,Target Asset ID,Target Column,Notes\nG1,1,table_456,col1,table_123,key1,Test"
        
        csv_file = temp_dir / "invalid_lineage.csv"
        with open(csv_file, 'w') as f:
            f.write(csv_data)
        
        processor = LineageProcessor(mock_client)
        
        with pytest.raises(ValueError, match="Missing required columns in CSV"):
            processor.parse_lineage_csv(str(csv_file))
    
    def test_parse_lineage_csv_invalid_relationship_type(self, temp_dir, mock_client):
        """Test CSV parsing with invalid relationship type."""
        # Create CSV with invalid relationship type
        csv_data = "Group ID,Step Order,Source Asset ID,Source Column,Target Asset ID,Target Column,Relationship Type,Transformation,Notes\nG1,1,table_456,col1,table_123,key1,invalid,join,Test"
        
        csv_file = temp_dir / "invalid_relationship.csv"
        with open(csv_file, 'w') as f:
            f.write(csv_data)
        
        processor = LineageProcessor(mock_client)
        
        with pytest.raises(ValueError, match="Invalid relationship type"):
            processor.parse_lineage_csv(str(csv_file))
    
    def test_parse_lineage_csv_invalid_step_order(self, temp_dir, mock_client):
        """Test CSV parsing with invalid step order."""
        # Create CSV with invalid step order
        csv_data = "Group ID,Step Order,Source Asset ID,Source Column,Target Asset ID,Target Column,Relationship Type,Transformation,Notes\nG1,invalid,table_456,col1,table_123,key1,upstream,join,Test"
        
        csv_file = temp_dir / "invalid_step_order.csv"
        with open(csv_file, 'w') as f:
            f.write(csv_data)
        
        processor = LineageProcessor(mock_client)
        rows = processor.parse_lineage_csv(str(csv_file))
        
        # Should parse successfully but with step_order as None
        assert len(rows) == 1
        assert rows[0].step_order is None
    
    def test_parse_lineage_csv_file_not_found(self, mock_client):
        """Test CSV parsing with non-existent file."""
        processor = LineageProcessor(mock_client)
        
        with pytest.raises(FileNotFoundError):
            processor.parse_lineage_csv("non_existent_file.csv")
    
    def test_validate_assets_success(self, mock_client, sample_asset_response):
        """Test successful asset validation."""
        # Create sample lineage rows
        rows = [
            LineageRow(None, None, "table_456", None, "table_123", None, "upstream", None, None),
            LineageRow(None, None, "table_789", None, "table_999", None, "downstream", None, None)
        ]
        
        # Use a mapping for asset responses
        asset_map = {
            "table_456": {"uid": "table_456", "id": 12345},
            "table_123": {"uid": "table_123", "id": 12346},
            "table_789": {"uid": "table_789", "id": 12347},
            "table_999": {"uid": "table_999", "id": 12348},
        }
        def get_asset_by_uid_side_effect(asset_id):
            return asset_map[asset_id]
        mock_client.get_asset_by_uid.side_effect = get_asset_by_uid_side_effect
        
        processor = LineageProcessor(mock_client)
        asset_uid_map = processor.validate_assets(rows)
        
        expected_map = {
            "table_456": 12345,
            "table_123": 12346,
            "table_789": 12347,
            "table_999": 12348
        }
        assert asset_uid_map == expected_map
        assert mock_client.get_asset_by_uid.call_count == 4
    
    def test_validate_assets_missing_asset(self, mock_client):
        """Test asset validation with missing asset."""
        # Create sample lineage rows
        rows = [
            LineageRow(None, None, "table_456", None, "table_123", None, "upstream", None, None),
            LineageRow(None, None, "missing_table", None, "table_999", None, "downstream", None, None)
        ]
        
        # Mock asset responses - one missing
        mock_client.get_asset_by_uid.side_effect = [
            {"uid": "table_456", "id": 12345},  # Found
            Exception("Asset not found"),  # Missing
            {"uid": "table_123", "id": 12346},  # Found
            Exception("Asset not found"),  # Missing
            {"uid": "table_999", "id": 12348}   # Found
        ]
        
        processor = LineageProcessor(mock_client)
        
        with pytest.raises(ValueError, match="Assets not found"):
            processor.validate_assets(rows)
    
    def test_group_lineage_by_target(self, mock_client):
        """Test grouping lineage rows by target asset."""
        # Create sample lineage rows
        rows = [
            LineageRow("G1", 1, "table_456", "col1", "table_123", "key1", "upstream", "join", None),
            LineageRow("G1", 1, "table_456", "col2", "table_123", "key2", "upstream", "join", None),
            LineageRow("G2", 1, "table_789", "id", "table_999", "key3", "downstream", "filter", None)
        ]
        
        processor = LineageProcessor(mock_client)
        grouped = processor.group_lineage_by_target(rows)
        
        assert len(grouped) == 2
        assert "table_123" in grouped
        assert "table_999" in grouped
        assert len(grouped["table_123"]) == 2
        assert len(grouped["table_999"]) == 1
    
    def test_create_lineage_batch_upstream_only(self, mock_client, sample_lineage_api_response):
        """Test creating lineage batch with upstream relationships only."""
        # Create sample lineage rows (all upstream)
        rows = [
            LineageRow("G1", 1, "table_456", "col1", "table_123", "key1", "upstream", "join", "Test join"),
            LineageRow("G1", 1, "table_456", "col2", "table_123", "key2", "upstream", "join", "Test join")
        ]
        
        asset_uid_map = {
            "table_456": 12345,
            "table_123": 12346
        }
        
        # Mock API response
        mock_client.make_api_call.return_value = sample_lineage_api_response
        
        processor = LineageProcessor(mock_client)
        result = processor.create_lineage_batch("table_123", rows, asset_uid_map)
        
        assert "upstream" in result
        assert result["upstream"] == sample_lineage_api_response
        
        # Verify API call
        mock_client.make_api_call.assert_called_once()
        call_args = mock_client.make_api_call.call_args
        assert call_args[1]["endpoint"] == "/torch-pipeline/api/assets/12346/lineage"
        assert call_args[1]["method"] == "POST"
        
        payload = call_args[1]["json_payload"]
        assert payload["direction"] == "UPSTREAM"
        assert payload["assetIds"] == [12345, 12345]  # Same asset twice for different columns
        assert "process" in payload
    
    def test_create_lineage_batch_downstream_only(self, mock_client, sample_lineage_api_response):
        """Test creating lineage batch with downstream relationships only."""
        # Create sample lineage rows (all downstream)
        rows = [
            LineageRow(None, None, "table_123", "key1", "table_789", "final_key", "downstream", "filter", "Test filter"),
            LineageRow(None, None, "table_123", "key2", "table_999", "other_key", "downstream", "transform", "Test transform")
        ]
        
        asset_uid_map = {
            "table_123": 12346,
            "table_789": 12347,
            "table_999": 12348
        }
        
        # Mock API response
        mock_client.make_api_call.return_value = sample_lineage_api_response
        
        processor = LineageProcessor(mock_client)
        result = processor.create_lineage_batch("table_123", rows, asset_uid_map)
        
        assert "downstream" in result
        assert result["downstream"] == sample_lineage_api_response
        
        # Verify API call
        mock_client.make_api_call.assert_called_once()
        call_args = mock_client.make_api_call.call_args
        assert call_args[1]["endpoint"] == "/torch-pipeline/api/assets/12346/lineage"
        assert call_args[1]["method"] == "POST"
        
        payload = call_args[1]["json_payload"]
        assert payload["direction"] == "DOWNSTREAM"
        assert payload["assetIds"] == [12347, 12348]
        assert "process" in payload
    
    def test_create_lineage_batch_mixed_relationships(self, mock_client, sample_lineage_api_response):
        """Test creating lineage batch with mixed upstream and downstream relationships."""
        # Create sample lineage rows (mixed relationships)
        rows = [
            LineageRow("G1", 1, "table_456", "col1", "table_123", "key1", "upstream", "join", "Test join"),
            LineageRow(None, None, "table_123", "key1", "table_789", "final_key", "downstream", "filter", "Test filter")
        ]
        
        asset_uid_map = {
            "table_456": 12345,
            "table_123": 12346,
            "table_789": 12347
        }
        
        # Mock API responses
        mock_client.make_api_call.side_effect = [
            sample_lineage_api_response,  # Upstream call
            sample_lineage_api_response   # Downstream call
        ]
        
        processor = LineageProcessor(mock_client)
        result = processor.create_lineage_batch("table_123", rows, asset_uid_map)
        
        assert "upstream" in result
        assert "downstream" in result
        assert result["upstream"] == sample_lineage_api_response
        assert result["downstream"] == sample_lineage_api_response
        
        # Verify two API calls
        assert mock_client.make_api_call.call_count == 2
    
    def test_generate_process_name_with_group_id(self, mock_client):
        """Test process name generation with group ID."""
        rows = [
            LineageRow("G1", 1, "table_456", "col1", "table_123", "key1", "upstream", "join", None),
            LineageRow("G1", 1, "table_456", "col2", "table_123", "key2", "upstream", "join", None)
        ]
        
        processor = LineageProcessor(mock_client)
        name = processor._generate_process_name(rows, "upstream")
        
        assert "Group G1" in name
        assert "upstream" in name.lower()
    
    def test_generate_process_name_with_transformation(self, mock_client):
        """Test process name generation with transformation."""
        rows = [
            LineageRow(None, None, "table_456", "col1", "table_123", "key1", "upstream", "filter(col1 > 100)", None)
        ]
        
        processor = LineageProcessor(mock_client)
        name = processor._generate_process_name(rows, "upstream")
        
        assert "filter" in name.lower()
        assert "upstream" in name.lower()
    
    def test_generate_process_description(self, mock_client):
        """Test process description generation."""
        rows = [
            LineageRow("G1", 1, "table_456", "col1", "table_123", "key1", "upstream", "join", "Test join operation"),
            LineageRow("G1", 1, "table_456", "col2", "table_123", "key2", "upstream", "join", "Test join operation")
        ]
        
        processor = LineageProcessor(mock_client)
        description = processor._generate_process_description(rows, "upstream")
        
        assert "table_456 -> table_123" in description
        assert "(join)" in description
        assert "Test join operation" in description
    
    def test_create_lineage_from_csv_success(self, temp_dir, mock_client, sample_lineage_csv_data, sample_asset_response):
        """Test successful lineage creation from CSV."""
        # Create test CSV file
        csv_file = temp_dir / "test_lineage.csv"
        with open(csv_file, 'w') as f:
            f.write(sample_lineage_csv_data)
        
        # Mock asset validation responses
        mock_client.get_asset_by_uid.side_effect = [
            {"uid": "table_456", "id": 12345},
            {"uid": "table_123", "id": 12346},
            {"uid": "table_457", "id": 12347},
            {"uid": "table_789", "id": 12348},
            {"uid": "table_999", "id": 12349}
        ]
        
        # Mock lineage creation responses
        mock_client.make_api_call.return_value = {"status": "success"}
        
        processor = LineageProcessor(mock_client)
        result = processor.create_lineage_from_csv(str(csv_file), dry_run=False)
        
        assert result["status"] == "completed"
        assert result["rows_processed"] == 5
        assert result["assets_processed"] == 3  # table_123, table_789, table_999
        assert result["success_count"] == 3
        assert result["error_count"] == 0
    
    def test_create_lineage_from_csv_dry_run(self, temp_dir, mock_client, sample_lineage_csv_data, sample_asset_response):
        """Test lineage creation dry run."""
        # Create test CSV file
        csv_file = temp_dir / "test_lineage.csv"
        with open(csv_file, 'w') as f:
            f.write(sample_lineage_csv_data)
        
        # Mock asset validation responses
        mock_client.get_asset_by_uid.side_effect = [
            {"uid": "table_456", "id": 12345},
            {"uid": "table_123", "id": 12346},
            {"uid": "table_457", "id": 12347},
            {"uid": "table_789", "id": 12348},
            {"uid": "table_999", "id": 12349}
        ]
        
        processor = LineageProcessor(mock_client)
        result = processor.create_lineage_from_csv(str(csv_file), dry_run=True)
        
        assert result["status"] == "dry_run"
        assert result["rows_processed"] == 5
        assert result["assets_validated"] == 5
        
        # No API calls should be made in dry run
        mock_client.make_api_call.assert_not_called()
    
    def test_create_lineage_from_csv_empty_file(self, temp_dir, mock_client):
        """Test lineage creation with empty CSV file."""
        # Create empty CSV file
        csv_file = temp_dir / "empty_lineage.csv"
        with open(csv_file, 'w') as f:
            f.write("Group ID,Step Order,Source Asset ID,Source Column,Target Asset ID,Target Column,Relationship Type,Transformation,Notes\n")
        
        processor = LineageProcessor(mock_client)
        
        with pytest.raises(ValueError, match="No valid lineage rows found"):
            processor.create_lineage_from_csv(str(csv_file), dry_run=False)
    
    def test_create_lineage_from_csv_validation_error(self, temp_dir, mock_client, sample_lineage_csv_data):
        """Test lineage creation with validation error."""
        # Create test CSV file
        csv_file = temp_dir / "test_lineage.csv"
        with open(csv_file, 'w') as f:
            f.write(sample_lineage_csv_data)
        
        # Mock asset validation failure
        mock_client.get_asset_by_uid.side_effect = Exception("Asset not found")
        
        processor = LineageProcessor(mock_client)
        
        with pytest.raises(ValueError, match="Assets not found"):
            processor.create_lineage_from_csv(str(csv_file), dry_run=False)


class TestExecuteCreateLineage:
    """Test cases for execute_create_lineage function."""
    
    def test_execute_create_lineage_success(self, temp_dir, mock_client, mock_logger, sample_lineage_csv_data, sample_asset_response):
        """Test successful lineage creation execution."""
        # Create test CSV file
        csv_file = temp_dir / "test_lineage.csv"
        with open(csv_file, 'w') as f:
            f.write(sample_lineage_csv_data)
        
        # Mock asset validation responses
        mock_client.get_asset_by_uid.side_effect = [
            {"uid": "table_456", "id": 12345},
            {"uid": "table_123", "id": 12346},
            {"uid": "table_457", "id": 12347},
            {"uid": "table_789", "id": 12348},
            {"uid": "table_999", "id": 12349}
        ]
        
        # Mock lineage creation responses
        mock_client.make_api_call.return_value = {"status": "success"}
        
        # Capture print output
        with patch('builtins.print') as mock_print:
            execute_create_lineage(str(csv_file), mock_client, mock_logger, dry_run=False, quiet_mode=False)
        
        # Verify print calls
        assert mock_print.call_count > 0
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        assert any("Creating lineage from CSV file" in call for call in print_calls)
        assert any("Lineage creation completed" in call for call in print_calls)
    
    def test_execute_create_lineage_dry_run(self, temp_dir, mock_client, mock_logger, sample_lineage_csv_data, sample_asset_response):
        """Test lineage creation execution in dry run mode."""
        # Create test CSV file
        csv_file = temp_dir / "test_lineage.csv"
        with open(csv_file, 'w') as f:
            f.write(sample_lineage_csv_data)
        
        # Mock asset validation responses
        mock_client.get_asset_by_uid.side_effect = [
            {"uid": "table_456", "id": 12345},
            {"uid": "table_123", "id": 12346},
            {"uid": "table_457", "id": 12347},
            {"uid": "table_789", "id": 12348},
            {"uid": "table_999", "id": 12349}
        ]
        
        # Capture print output
        with patch('builtins.print') as mock_print:
            execute_create_lineage(str(csv_file), mock_client, mock_logger, dry_run=True, quiet_mode=False)
        
        # Verify print calls
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        assert any("DRY RUN MODE" in call for call in print_calls)
        assert any("Validation successful" in call for call in print_calls)
        
        # No API calls should be made in dry run
        mock_client.make_api_call.assert_not_called()
    
    def test_execute_create_lineage_quiet_mode(self, temp_dir, mock_client, mock_logger, sample_lineage_csv_data, sample_asset_response):
        """Test lineage creation execution in quiet mode."""
        # Create test CSV file
        csv_file = temp_dir / "test_lineage.csv"
        with open(csv_file, 'w') as f:
            f.write(sample_lineage_csv_data)
        
        # Mock asset validation responses
        mock_client.get_asset_by_uid.side_effect = [
            {"uid": "table_456", "id": 12345},
            {"uid": "table_123", "id": 12346},
            {"uid": "table_457", "id": 12347},
            {"uid": "table_789", "id": 12348},
            {"uid": "table_999", "id": 12349}
        ]
        
        # Mock lineage creation responses
        mock_client.make_api_call.return_value = {"status": "success"}
        
        # Capture print output
        with patch('builtins.print') as mock_print:
            execute_create_lineage(str(csv_file), mock_client, mock_logger, dry_run=False, quiet_mode=True)
        
        # In quiet mode, should not expect summary print
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        # The implementation does not print summary in quiet mode, so we check that no summary is printed
        assert not any("Lineage creation completed" in call for call in print_calls)
    
    def test_execute_create_lineage_error(self, temp_dir, mock_client, mock_logger):
        """Test lineage creation execution with error."""
        # Create test CSV file with invalid data
        csv_file = temp_dir / "invalid_lineage.csv"
        with open(csv_file, 'w') as f:
            f.write("Invalid CSV data")
        
        # Capture print output
        with patch('builtins.print') as mock_print:
            with pytest.raises(Exception):
                execute_create_lineage(str(csv_file), mock_client, mock_logger, dry_run=False, quiet_mode=False)
        
        # Verify error message was printed
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        assert any("Failed to create lineage" in call for call in print_calls)


class TestLineageOperationsIntegration:
    """Integration test cases for lineage operations."""
    
    def test_complete_lineage_workflow(self, temp_dir, mock_client, mock_logger):
        """Test complete lineage creation workflow."""
        # Create comprehensive test CSV
        csv_data = (
            'Group ID,Step Order,Source Asset ID,Source Column,Target Asset ID,Target Column,Relationship Type,Transformation,Notes\n'
            'G1,1,raw_table,customer_id,staged_table,user_id,upstream,extract,"Extract customer data"\n'
            'G1,2,staged_table,user_id,final_table,customer_key,downstream,transform,"Transform user ID to customer key"\n'
            'G2,1,raw_table,email,staged_table,email_address,upstream,validate,"Validate email format"\n'
            'G2,2,staged_table,email_address,final_table,email_key,downstream,hash,"Hash email for privacy"'
        )
        
        csv_file = temp_dir / "workflow_lineage.csv"
        with open(csv_file, 'w') as f:
            f.write(csv_data)
        
        # Mock asset validation responses
        mock_client.get_asset_by_uid.side_effect = [
            {"uid": "raw_table", "id": 1001},
            {"uid": "staged_table", "id": 1002},
            {"uid": "final_table", "id": 1003}
        ]
        
        # Mock lineage creation responses
        mock_client.make_api_call.return_value = {"status": "success"}
        
        processor = LineageProcessor(mock_client, mock_logger)
        result = processor.create_lineage_from_csv(str(csv_file), dry_run=False)
        
        assert result["status"] == "completed"
        assert result["rows_processed"] == 4
        assert result["assets_processed"] == 2  # staged_table and final_table
        assert result["success_count"] == 2
        assert result["error_count"] == 0
        
        # Verify API calls were made for both assets
        assert mock_client.make_api_call.call_count == 2
    
    def test_lineage_with_complex_grouping(self, temp_dir, mock_client, mock_logger):
        """Test lineage creation with complex grouping scenarios."""
        # Create CSV with complex grouping
        csv_data = (
            'Group ID,Step Order,Source Asset ID,Source Column,Target Asset ID,Target Column,Relationship Type,Transformation,Notes\n'
            'G1,1,table_a,col1,table_b,key1,upstream,join,"Multi-column join"\n'
            'G1,1,table_a,col2,table_b,key2,upstream,join,"Multi-column join"\n'
            'G1,1,table_c,col3,table_b,key3,upstream,join,"Multi-column join"\n'
            'G2,1,table_b,key1,table_d,id,downstream,filter,"Filter active records"\n'
            'G2,2,table_d,id,table_e,final_id,downstream,aggregate,"Final aggregation"'
        )
        
        csv_file = temp_dir / "complex_lineage.csv"
        with open(csv_file, 'w') as f:
            f.write(csv_data)
        
        # Mock asset validation responses
        mock_client.get_asset_by_uid.side_effect = [
            {"uid": "table_a", "id": 2001},
            {"uid": "table_b", "id": 2002},
            {"uid": "table_c", "id": 2003},
            {"uid": "table_d", "id": 2004},
            {"uid": "table_e", "id": 2005}
        ]
        
        # Mock lineage creation responses
        mock_client.make_api_call.return_value = {"status": "success"}
        
        processor = LineageProcessor(mock_client, mock_logger)
        result = processor.create_lineage_from_csv(str(csv_file), dry_run=False)
        
        assert result["status"] == "completed"
        assert result["rows_processed"] == 5
        assert result["assets_processed"] == 3  # table_b, table_d, table_e
        assert result["success_count"] == 3
        assert result["error_count"] == 0
        
        # Verify API calls were made for all target assets
        assert mock_client.make_api_call.call_count == 3 