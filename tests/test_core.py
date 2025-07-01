"""
Tests for the core functionality of adoc_export_import.
"""

import pytest
import tempfile
import json
from pathlib import Path
from adoc_export_import import JSONStringReplacer


class TestJSONStringReplacer:
    """Test cases for JSONStringReplacer class."""

    def test_replace_in_value_string(self):
        """Test string replacement in simple string values."""
        replacer = JSONStringReplacer(
            input_dir="/tmp/test",
            search_string="old",
            replace_string="new"
        )
        
        # Test string replacement
        result = replacer.replace_in_value("old_value")
        assert result == "new_value"
        
        # Test no replacement
        result = replacer.replace_in_value("unchanged")
        assert result == "unchanged"

    def test_replace_in_value_dict(self):
        """Test string replacement in dictionary values."""
        replacer = JSONStringReplacer(
            input_dir="/tmp/test",
            search_string="old",
            replace_string="new"
        )
        
        data = {
            "key1": "old_value",
            "key2": "unchanged",
            "nested": {
                "key3": "old_nested_value"
            }
        }
        
        result = replacer.replace_in_value(data)
        expected = {
            "key1": "new_value",
            "key2": "unchanged",
            "nested": {
                "key3": "new_nested_value"
            }
        }
        
        assert result == expected

    def test_replace_in_value_list(self):
        """Test string replacement in list values."""
        replacer = JSONStringReplacer(
            input_dir="/tmp/test",
            search_string="old",
            replace_string="new"
        )
        
        data = ["old_item", "unchanged", "old_another"]
        result = replacer.replace_in_value(data)
        expected = ["new_item", "unchanged", "new_another"]
        
        assert result == expected

    def test_invalid_input_directory(self):
        """Test initialization with invalid input directory."""
        with pytest.raises(FileNotFoundError):
            JSONStringReplacer(
                input_dir="/nonexistent/directory",
                search_string="old",
                replace_string="new"
            )

    def test_empty_search_string(self):
        """Test initialization with empty search string."""
        with tempfile.TemporaryDirectory() as temp_dir:
            with pytest.raises(ValueError, match="Search string cannot be empty"):
                JSONStringReplacer(
                    input_dir=temp_dir,
                    search_string="",
                    replace_string="new"
                )

    def test_none_replace_string(self):
        """Test initialization with None replace string."""
        with tempfile.TemporaryDirectory() as temp_dir:
            with pytest.raises(ValueError, match="Replace string cannot be None"):
                JSONStringReplacer(
                    input_dir=temp_dir,
                    search_string="old",
                    replace_string=None
                )


if __name__ == "__main__":
    pytest.main([__file__]) 