"""
Tests for the shared logging functionality.

This module contains test cases for the logging utilities in the shared module.
"""

import pytest
import tempfile
import logging
import os
from pathlib import Path
from unittest.mock import patch, Mock, mock_open
from datetime import datetime

from adoc_migration_toolkit.shared.logging import setup_logging


class TestSetupLogging:
    """Test cases for setup_logging function."""

    def test_setup_logging_default(self):
        """Test logging setup with default parameters."""
        with patch('logging.basicConfig') as mock_basic_config:
            with patch('logging.getLogger') as mock_get_logger:
                mock_logger = Mock()
                mock_get_logger.return_value = mock_logger
                
                logger = setup_logging()
                
                # Verify logger was created
                mock_get_logger.assert_called_with('adoc_migration_toolkit.shared.logging')
                
                # Verify basic config was called
                mock_basic_config.assert_called_once()
                
                # Verify log level is INFO by default
                call_args = mock_basic_config.call_args
                assert call_args[1]['level'] == logging.INFO
                
                # Verify handlers were set up
                assert 'handlers' in call_args[1]
                handlers = call_args[1]['handlers']
                assert len(handlers) == 1  # Only file handler
                assert isinstance(handlers[0], logging.FileHandler)

    def test_setup_logging_verbose(self):
        """Test logging setup with verbose mode."""
        with patch('logging.basicConfig') as mock_basic_config:
            with patch('logging.getLogger') as mock_get_logger:
                mock_logger = Mock()
                mock_get_logger.return_value = mock_logger
                
                logger = setup_logging(verbose=True)
                
                # Verify log level is DEBUG when verbose is True
                call_args = mock_basic_config.call_args
                assert call_args[1]['level'] == logging.DEBUG

    def test_setup_logging_custom_level(self):
        """Test logging setup with custom log level."""
        with patch('logging.basicConfig') as mock_basic_config:
            with patch('logging.getLogger') as mock_get_logger:
                mock_logger = Mock()
                mock_get_logger.return_value = mock_logger
                
                logger = setup_logging(log_level="INFO")
                
                # Verify log level is INFO
                call_args = mock_basic_config.call_args
                assert call_args[1]['level'] == logging.INFO

    def test_setup_logging_verbose_overrides_level(self):
        """Test that verbose mode overrides log_level parameter."""
        with patch('logging.basicConfig') as mock_basic_config:
            with patch('logging.getLogger') as mock_get_logger:
                mock_logger = Mock()
                mock_get_logger.return_value = mock_logger
                
                logger = setup_logging(verbose=True, log_level="WARNING")
                
                # Verify log level is DEBUG (verbose overrides)
                call_args = mock_basic_config.call_args
                assert call_args[1]['level'] == logging.DEBUG

    def test_setup_logging_invalid_level(self):
        """Test logging setup with invalid log level."""
        with patch('logging.basicConfig') as mock_basic_config:
            with patch('logging.getLogger') as mock_get_logger:
                mock_logger = Mock()
                mock_get_logger.return_value = mock_logger
                
                logger = setup_logging(log_level="INVALID")
                
                # Verify log level defaults to INFO for invalid level
                call_args = mock_basic_config.call_args
                assert call_args[1]['level'] == logging.INFO

    def test_setup_logging_case_insensitive_level(self):
        """Test logging setup with case insensitive log level."""
        with patch('logging.basicConfig') as mock_basic_config:
            with patch('logging.getLogger') as mock_get_logger:
                mock_logger = Mock()
                mock_get_logger.return_value = mock_logger
                
                logger = setup_logging(log_level="info")
                
                # Verify log level is INFO (case insensitive)
                call_args = mock_basic_config.call_args
                assert call_args[1]['level'] == logging.INFO

    def test_setup_logging_file_handler_creation(self):
        """Test that file handler is created with correct configuration."""
        with patch('logging.basicConfig') as mock_basic_config:
            with patch('logging.getLogger') as mock_get_logger:
                with patch('logging.FileHandler') as mock_file_handler:
                    mock_handler = Mock()
                    mock_file_handler.return_value = mock_handler
                    mock_logger = Mock()
                    mock_get_logger.return_value = mock_logger
                    
                    logger = setup_logging()
                    
                    # Verify FileHandler was created
                    mock_file_handler.assert_called_once()
                    
                    # Verify file name pattern
                    call_args = mock_file_handler.call_args
                    filename = call_args[0][0]
                    assert filename.startswith("adoc-migration-toolkit-")
                    assert filename.endswith(".log")
                    
                    # Verify encoding and mode
                    assert call_args[1]['encoding'] == 'utf-8'
                    assert call_args[1]['mode'] == 'a'

    def test_setup_logging_formatter_configuration(self):
        """Test that formatter is configured correctly."""
        with patch('logging.basicConfig') as mock_basic_config:
            with patch('logging.getLogger') as mock_get_logger:
                with patch('adoc_migration_toolkit.shared.logging.CustomFormatter') as mock_formatter:
                    with patch('logging.FileHandler') as mock_file_handler:
                        mock_handler = Mock()
                        mock_file_handler.return_value = mock_handler
                        mock_formatter_instance = Mock()
                        mock_formatter.return_value = mock_formatter_instance
                        mock_logger = Mock()
                        mock_get_logger.return_value = mock_logger
                        
                        logger = setup_logging()
                        
                        # Verify CustomFormatter was created without arguments
                        mock_formatter.assert_called_once_with()

    def test_setup_logging_handler_setup(self):
        """Test that handler is properly configured."""
        with patch('logging.basicConfig') as mock_basic_config:
            with patch('logging.getLogger') as mock_get_logger:
                with patch('logging.FileHandler') as mock_file_handler:
                    with patch('adoc_migration_toolkit.shared.logging.CustomFormatter') as mock_formatter:
                        mock_handler = Mock()
                        mock_file_handler.return_value = mock_handler
                        mock_formatter_instance = Mock()
                        mock_formatter.return_value = mock_formatter_instance
                        mock_logger = Mock()
                        mock_get_logger.return_value = mock_logger
                        
                        logger = setup_logging()
                        
                        # Verify handler was configured
                        mock_handler.setFormatter.assert_called_once_with(mock_formatter_instance)

    def test_setup_logging_force_config(self):
        """Test that force=True is passed to basicConfig."""
        with patch('logging.basicConfig') as mock_basic_config:
            with patch('logging.getLogger') as mock_get_logger:
                mock_logger = Mock()
                mock_get_logger.return_value = mock_logger
                
                logger = setup_logging()
                
                # Verify force=True was passed
                call_args = mock_basic_config.call_args
                assert call_args[1]['force'] is True

    def test_setup_logging_logger_info_calls(self):
        """Test that logger info messages are called."""
        with patch('logging.basicConfig'):
            with patch('logging.getLogger') as mock_get_logger:
                mock_logger = Mock()
                mock_get_logger.return_value = mock_logger
                
                logger = setup_logging()
                
                # Verify info messages were logged
                assert mock_logger.info.call_count == 2
                
                # Check first info call (log file)
                first_call = mock_logger.info.call_args_list[0]
                assert "Logging initialized" in first_call[0][0]
                
                # Check second info call (log level)
                second_call = mock_logger.info.call_args_list[1]
                assert "Log level set to" in second_call[0][0]

    def test_setup_logging_returns_logger(self):
        """Test that setup_logging returns the correct logger."""
        with patch('logging.basicConfig'):
            with patch('logging.getLogger') as mock_get_logger:
                mock_logger = Mock()
                mock_get_logger.return_value = mock_logger
                
                logger = setup_logging()
                
                # Verify the returned logger is the same as the created one
                assert logger == mock_logger

    def test_setup_logging_no_console_handler(self):
        """Test that no console handler is added (as per comment in code)."""
        with patch('logging.basicConfig') as mock_basic_config:
            with patch('logging.getLogger') as mock_get_logger:
                mock_logger = Mock()
                mock_get_logger.return_value = mock_logger
                
                logger = setup_logging()
                
                # Verify only one handler (file handler, no console handler)
                call_args = mock_basic_config.call_args
                handlers = call_args[1]['handlers']
                assert len(handlers) == 1
                
                # Verify it's a FileHandler, not StreamHandler
                assert isinstance(handlers[0], logging.FileHandler)

    def test_setup_logging_all_levels(self):
        """Test logging setup with all supported log levels."""
        levels = ["ERROR", "WARNING", "INFO", "DEBUG"]
        
        for level in levels:
            with patch('logging.basicConfig') as mock_basic_config:
                with patch('logging.getLogger') as mock_get_logger:
                    mock_logger = Mock()
                    mock_get_logger.return_value = mock_logger
                    
                    logger = setup_logging(log_level=level)
                    
                    # Verify correct level was set
                    call_args = mock_basic_config.call_args
                    expected_level = getattr(logging, level)
                    assert call_args[1]['level'] == expected_level

    def test_setup_logging_file_name_date_format(self):
        """Test that log file name uses correct date format."""
        with patch('logging.basicConfig'):
            with patch('logging.getLogger') as mock_get_logger:
                with patch('logging.FileHandler') as mock_file_handler:
                    with patch('adoc_migration_toolkit.shared.logging.datetime') as mock_datetime:
                        mock_date = Mock()
                        mock_date.strftime.return_value = "20231201"
                        mock_datetime.now.return_value = mock_date
                        mock_logger = Mock()
                        mock_get_logger.return_value = mock_logger
                        
                        logger = setup_logging()
                        
                        # Verify date format was used
                        mock_date.strftime.assert_called_with('%Y%m%d')
                        
                        # Verify file name was constructed correctly
                        mock_file_handler.assert_called_once()
                        filename = mock_file_handler.call_args[0][0]
                        assert filename == "adoc-migration-toolkit-20231201.log"


if __name__ == "__main__":
    pytest.main([__file__]) 