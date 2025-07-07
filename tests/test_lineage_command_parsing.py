"""
Test cases for lineage command parsing functionality.

This module contains tests for parsing create-lineage commands
in the interactive mode.
"""

import pytest
from unittest.mock import Mock

from src.adoc_migration_toolkit.execution.command_parsing import parse_create_lineage_command


class TestParseCreateLineageCommand:
    """Test cases for parse_create_lineage_command function."""
    
    def test_parse_create_lineage_basic(self):
        """Test basic create-lineage command parsing."""
        command = "create-lineage test.csv"
        csv_file, dry_run, quiet_mode, verbose_mode = parse_create_lineage_command(command)
        
        assert csv_file == "test.csv"
        assert dry_run is False
        assert quiet_mode is False
        assert verbose_mode is False
    
    def test_parse_create_lineage_with_dry_run(self):
        """Test create-lineage command with --dry-run flag."""
        command = "create-lineage test.csv --dry-run"
        csv_file, dry_run, quiet_mode, verbose_mode = parse_create_lineage_command(command)
        
        assert csv_file == "test.csv"
        assert dry_run is True
        assert quiet_mode is False
        assert verbose_mode is False
    
    def test_parse_create_lineage_with_quiet(self):
        """Test create-lineage command with --quiet flag."""
        command = "create-lineage test.csv --quiet"
        csv_file, dry_run, quiet_mode, verbose_mode = parse_create_lineage_command(command)
        
        assert csv_file == "test.csv"
        assert dry_run is False
        assert quiet_mode is True
        assert verbose_mode is False
    
    def test_parse_create_lineage_with_verbose(self):
        """Test create-lineage command with --verbose flag."""
        command = "create-lineage test.csv --verbose"
        csv_file, dry_run, quiet_mode, verbose_mode = parse_create_lineage_command(command)
        
        assert csv_file == "test.csv"
        assert dry_run is False
        assert quiet_mode is False
        assert verbose_mode is True
    
    def test_parse_create_lineage_multiple_flags(self):
        """Test create-lineage command with multiple flags."""
        command = "create-lineage test.csv --dry-run --quiet"
        csv_file, dry_run, quiet_mode, verbose_mode = parse_create_lineage_command(command)
        
        assert csv_file == "test.csv"
        assert dry_run is True
        assert quiet_mode is True
        assert verbose_mode is False
    
    def test_parse_create_lineage_verbose_overrides_quiet(self):
        """Test that --verbose overrides --quiet flag."""
        command = "create-lineage test.csv --quiet --verbose"
        csv_file, dry_run, quiet_mode, verbose_mode = parse_create_lineage_command(command)
        
        assert csv_file == "test.csv"
        assert dry_run is False
        assert quiet_mode is False  # Should be overridden by verbose
        assert verbose_mode is True
    
    def test_parse_create_lineage_quiet_overrides_verbose(self):
        """Test that --quiet overrides --verbose flag."""
        command = "create-lineage test.csv --verbose --quiet"
        csv_file, dry_run, quiet_mode, verbose_mode = parse_create_lineage_command(command)
        
        assert csv_file == "test.csv"
        assert dry_run is False
        assert quiet_mode is True  # Should override verbose
        assert verbose_mode is False
    
    def test_parse_create_lineage_with_path(self):
        """Test create-lineage command with file path."""
        command = "create-lineage /path/to/data/lineage.csv"
        csv_file, dry_run, quiet_mode, verbose_mode = parse_create_lineage_command(command)
        
        assert csv_file == "/path/to/data/lineage.csv"
        assert dry_run is False
        assert quiet_mode is False
        assert verbose_mode is False
    
    def test_parse_create_lineage_with_spaces_in_path(self):
        """Test create-lineage command with spaces in file path."""
        command = "create-lineage 'My Data/lineage file.csv' --dry-run"
        csv_file, dry_run, quiet_mode, verbose_mode = parse_create_lineage_command(command)
        
        assert csv_file == "My Data/lineage file.csv"
        assert dry_run is True
        assert quiet_mode is False
        assert verbose_mode is False
    
    def test_parse_create_lineage_invalid_command(self):
        """Test parsing invalid command."""
        command = "invalid-command test.csv"
        csv_file, dry_run, quiet_mode, verbose_mode = parse_create_lineage_command(command)
        
        assert csv_file is None
        assert dry_run is False
        assert quiet_mode is False
        assert verbose_mode is False
    
    def test_parse_create_lineage_missing_csv_file(self):
        """Test create-lineage command without CSV file."""
        command = "create-lineage"
        
        with pytest.raises(ValueError, match="CSV file path is required"):
            parse_create_lineage_command(command)
    
    def test_parse_create_lineage_empty_command(self):
        """Test parsing empty command."""
        command = ""
        csv_file, dry_run, quiet_mode, verbose_mode = parse_create_lineage_command(command)
        
        assert csv_file is None
        assert dry_run is False
        assert quiet_mode is False
        assert verbose_mode is False
    
    def test_parse_create_lineage_whitespace_only(self):
        """Test parsing whitespace-only command."""
        command = "   "
        csv_file, dry_run, quiet_mode, verbose_mode = parse_create_lineage_command(command)
        
        assert csv_file is None
        assert dry_run is False
        assert quiet_mode is False
        assert verbose_mode is False
    
    def test_parse_create_lineage_case_insensitive(self):
        """Test that command parsing is case insensitive."""
        command = "CREATE-LINEAGE test.csv --DRY-RUN"
        csv_file, dry_run, quiet_mode, verbose_mode = parse_create_lineage_command(command)
        
        assert csv_file == "test.csv"
        assert dry_run is True
        assert quiet_mode is False
        assert verbose_mode is False
    
    def test_parse_create_lineage_with_extra_spaces(self):
        """Test create-lineage command with extra spaces."""
        command = "  create-lineage   test.csv   --dry-run   "
        csv_file, dry_run, quiet_mode, verbose_mode = parse_create_lineage_command(command)
        
        assert csv_file == "test.csv"
        assert dry_run is True
        assert quiet_mode is False
        assert verbose_mode is False
    
    def test_parse_create_lineage_all_flags(self):
        """Test create-lineage command with all flags."""
        command = "create-lineage test.csv --dry-run --quiet --verbose"
        csv_file, dry_run, quiet_mode, verbose_mode = parse_create_lineage_command(command)
        
        assert csv_file == "test.csv"
        assert dry_run is True
        # Last flag should take precedence
        assert quiet_mode is False
        assert verbose_mode is True
    
    def test_parse_create_lineage_flags_only(self):
        """Test create-lineage command with flags but no CSV file."""
        command = "create-lineage --dry-run --quiet"
        
        with pytest.raises(ValueError, match="CSV file path is required"):
            parse_create_lineage_command(command)
    
    def test_parse_create_lineage_unknown_flag(self):
        """Test create-lineage command with unknown flag."""
        command = "create-lineage test.csv --unknown-flag"
        csv_file, dry_run, quiet_mode, verbose_mode = parse_create_lineage_command(command)
        
        # Should still parse successfully, ignoring unknown flag
        assert csv_file == "test.csv"
        assert dry_run is False
        assert quiet_mode is False
        assert verbose_mode is False
    
    def test_parse_create_lineage_flag_without_value(self):
        """Test create-lineage command with flag that expects a value."""
        command = "create-lineage test.csv --output-file"
        csv_file, dry_run, quiet_mode, verbose_mode = parse_create_lineage_command(command)
        
        # Should parse successfully, ignoring incomplete flag
        assert csv_file == "test.csv"
        assert dry_run is False
        assert quiet_mode is False
        assert verbose_mode is False


class TestCreateLineageCommandIntegration:
    """Integration tests for create-lineage command parsing."""
    
    def test_parse_create_lineage_real_world_example(self):
        """Test parsing a real-world create-lineage command."""
        command = "create-lineage /data/lineage/etl_pipeline.csv --dry-run --verbose"
        csv_file, dry_run, quiet_mode, verbose_mode = parse_create_lineage_command(command)
        
        assert csv_file == "/data/lineage/etl_pipeline.csv"
        assert dry_run is True
        assert quiet_mode is False
        assert verbose_mode is True
    
    def test_parse_create_lineage_batch_processing(self):
        """Test parsing create-lineage command for batch processing."""
        command = "create-lineage batch_lineage.csv --quiet"
        csv_file, dry_run, quiet_mode, verbose_mode = parse_create_lineage_command(command)
        
        assert csv_file == "batch_lineage.csv"
        assert dry_run is False
        assert quiet_mode is True
        assert verbose_mode is False
    
    def test_parse_create_lineage_validation_mode(self):
        """Test parsing create-lineage command for validation mode."""
        command = "create-lineage validation.csv --dry-run --verbose"
        csv_file, dry_run, quiet_mode, verbose_mode = parse_create_lineage_command(command)
        
        assert csv_file == "validation.csv"
        assert dry_run is True
        assert quiet_mode is False
        assert verbose_mode is True 