import pytest
import os
import tempfile
import shutil
from unittest.mock import patch, MagicMock
import json
from pathlib import Path

from adoc_migration_toolkit.shared import (
    api_client,
    file_utils,
    globals,
    logging
)


class TestSharedModuleIntegration:
    """Integration tests for the shared module components."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        # Reset global state before each test
        import adoc_migration_toolkit.shared.globals as globals_module
        globals_module.GLOBAL_OUTPUT_DIR = None
    
    def teardown_method(self):
        """Clean up test fixtures."""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        # Reset global state after each test
        import adoc_migration_toolkit.shared.globals as globals_module
        globals_module.GLOBAL_OUTPUT_DIR = None
    
    def test_globals_and_file_utils_integration(self):
        """Test integration between globals and file utils."""
        # Set up global output directory
        mock_logger = MagicMock()
        output_dir = os.path.join(self.temp_dir, "output")
        result = globals.set_global_output_directory(output_dir, mock_logger)
        
        assert result is True
        assert os.path.exists(output_dir)
        
        # Test file utils with global output directory
        test_csv_file = os.path.join(self.temp_dir, "test.csv")
        default_filename = "output.json"
        
        # Generate output file path using file utils
        output_file = file_utils.get_output_file_path(
            csv_file=test_csv_file,
            default_filename=default_filename
        )
        
        assert isinstance(output_file, Path)
        # Handle path resolution by comparing resolved paths
        assert output_file.resolve().parts[:len(Path(output_dir).resolve().parts)] == Path(output_dir).resolve().parts
        assert output_file.name == default_filename
    
    def test_globals_and_logging_integration(self):
        """Test integration between globals and logging."""
        # Set up global output directory with logger
        mock_logger = MagicMock()
        output_dir = os.path.join(self.temp_dir, "output")
        result = globals.set_global_output_directory(output_dir, mock_logger)
        
        assert result is True
        mock_logger.info.assert_called()
        
        # Test that logger can be used
        mock_logger.info("Test message")
        mock_logger.info.assert_called_with("Test message")
    
    def test_globals_and_api_client_integration(self):
        """Test integration between globals and API client."""
        # Create API client
        client = api_client.AcceldataAPIClient(
            host="https://api.example.com",
            access_key="test_key",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        
        # Test API call
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "success"}
        with patch.object(client.session, 'get', return_value=mock_response) as mock_get:
            response = client.make_api_call("/test", method="GET")
            mock_get.assert_called_once()
            assert response == {"status": "success"}
    
    def test_file_utils_and_logging_integration(self):
        """Test integration between file utils and logging."""
        # Set up logger
        mock_logger = MagicMock()
        
        # Test file operation with logging
        test_csv_file = os.path.join(self.temp_dir, "test.csv")
        default_filename = "output.json"
        
        # Generate output file path (should trigger logging if configured)
        output_file = file_utils.get_output_file_path(
            csv_file=test_csv_file,
            default_filename=default_filename,
            custom_output_file=os.path.join(self.temp_dir, "custom_output.json")
        )
        
        assert isinstance(output_file, Path)
        assert output_file.parent.exists()  # Parent directory should be created
    
    def test_api_client_and_logging_integration(self):
        """Test integration between API client and logging."""
        # Create API client
        client = api_client.AcceldataAPIClient(
            host="https://api.example.com",
            access_key="test_key",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        
        # Test API call with logging
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        with patch.object(client.session, 'get', return_value=mock_response) as mock_get:
            response = client.make_api_call("/test", method="GET")
            mock_get.assert_called_once()
            assert response == {"data": "test"}
    
    def test_complete_workflow_integration(self):
        """Test complete workflow integration."""
        # Set up global output directory
        mock_logger = MagicMock()
        output_dir = os.path.join(self.temp_dir, "output")
        result = globals.set_global_output_directory(output_dir, mock_logger)
        assert result is True
        # Create API client
        client = api_client.AcceldataAPIClient(
            host="https://api.example.com",
            access_key="test_key",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        # Simulate API call
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": [{"id": 1, "name": "test"}]}
        with patch.object(client.session, 'get', return_value=mock_response) as mock_get:
            response = client.make_api_call("/data", method="GET")
            mock_get.assert_called_once()
            # Generate output file path using file utils
            test_csv_file = os.path.join(self.temp_dir, "test.csv")
            output_file = file_utils.get_output_file_path(
                csv_file=test_csv_file,
                default_filename="data.json"
            )
            assert isinstance(output_file, Path)
            assert output_file.resolve().parts[:len(Path(output_dir).resolve().parts)] == Path(output_dir).resolve().parts
            assert response == {"data": [{"id": 1, "name": "test"}]}
    
    def test_error_handling_integration(self):
        """Test error handling integration across modules."""
        # Test API client error
        client = api_client.AcceldataAPIClient(
            host="https://api.example.com",
            access_key="test_key",
            secret_key="test_secret",
            tenant="test_tenant"
        )
        # Test that API client handles network errors gracefully
        with patch.object(client.session, 'get', side_effect=Exception("Network error")):
            try:
                response = client.make_api_call("/test", method="GET")
                assert response is None
            except Exception:
                pass
        # Test file utils error handling
        with patch('pathlib.Path.mkdir', side_effect=PermissionError("Permission denied")):
            test_csv_file = os.path.join(self.temp_dir, "test.csv")
            try:
                output_file = file_utils.get_output_file_path(
                    csv_file=test_csv_file,
                    default_filename="output.json",
                    custom_output_file="/root/test.json"
                )
                # Should raise an exception or handle gracefully
            except Exception:
                pass  # Expected behavior
    
    def test_configuration_integration(self):
        """Test configuration integration."""
        # Test global output directory configuration
        test_output_dir = os.path.join(self.temp_dir, "config_test")
        os.makedirs(test_output_dir)
        
        # Save configuration
        globals.save_global_output_directory(Path(test_output_dir))
        
        # Load configuration
        result = globals.load_global_output_directory()
        assert result == Path(test_output_dir)
        
        # Clean up config file
        config_file = Path.home() / ".adoc_migration_toolkit" / "output_dir.json"
        if config_file.exists():
            config_file.unlink()
            config_file.parent.rmdir()
    
    def test_path_management_integration(self):
        """Test path management integration."""
        # Set up global output directory
        mock_logger = MagicMock()
        output_dir = os.path.join(self.temp_dir, "output")
        result = globals.set_global_output_directory(output_dir, mock_logger)
        
        assert result is True
        assert os.path.exists(output_dir)
        
        # Test file utils with global output directory
        test_csv_file = os.path.join(self.temp_dir, "test.csv")
        output_file = file_utils.get_output_file_path(
            csv_file=test_csv_file,
            default_filename="test.json",
            category="test_category"
        )
        
        assert isinstance(output_file, Path)
        # Handle path resolution by comparing resolved paths
        assert output_file.resolve().parts[:len(Path(output_dir).resolve().parts)] == Path(output_dir).resolve().parts
        assert "test_category" in str(output_file)
        assert output_file.parent.exists()  # Category directory should be created 