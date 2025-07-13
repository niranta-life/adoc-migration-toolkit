"""
Tests for the core functionality of adoc_migration_toolkit.
"""

import json
import tempfile
from pathlib import Path

import pytest

from adoc_migration_toolkit import PolicyExportFormatter


class TestPolicyExportFormatter:
    """Test cases for PolicyExportFormatter class."""

    def test_replace_in_value_string(self):
        """Test string replacement in simple string values."""
        with tempfile.TemporaryDirectory() as temp_dir:
            replacer = PolicyExportFormatter(
                input_dir=temp_dir, string_transforms={"old": "new"}
            )

            # Test string replacement
            result = replacer.replace_in_value("old_value")
            assert result == "new_value"

            # Test no replacement
            result = replacer.replace_in_value("unchanged")
            assert result == "unchanged"

    def test_replace_in_value_dict(self):
        """Test string replacement in dictionary values."""
        with tempfile.TemporaryDirectory() as temp_dir:
            replacer = PolicyExportFormatter(
                input_dir=temp_dir, string_transforms={"old": "new"}
            )

            data = {
                "key1": "old_value",
                "key2": "unchanged",
                "nested": {"key3": "old_nested_value"},
            }

            result = replacer.replace_in_value(data)
            expected = {
                "key1": "new_value",
                "key2": "unchanged",
                "nested": {"key3": "new_nested_value"},
            }

            assert result == expected

    def test_replace_in_value_list(self):
        """Test string replacement in list values."""
        with tempfile.TemporaryDirectory() as temp_dir:
            replacer = PolicyExportFormatter(
                input_dir=temp_dir, string_transforms={"old": "new"}
            )

            data = ["old_item", "unchanged", "old_another"]
            result = replacer.replace_in_value(data)
            expected = ["new_item", "unchanged", "new_another"]

            assert result == expected

    def test_invalid_input_directory(self):
        """Test initialization with invalid input directory."""
        with pytest.raises(FileNotFoundError):
            PolicyExportFormatter(
                input_dir="/nonexistent/directory", string_transforms={"old": "new"}
            )

    def test_empty_search_string(self):
        """Test initialization with empty search string."""
        with tempfile.TemporaryDirectory() as temp_dir:
            with pytest.raises(
                ValueError, match="String transforms must be a non-empty dictionary"
            ):
                PolicyExportFormatter(input_dir=temp_dir, string_transforms={})

    def test_none_replace_string(self):
        """Test initialization with None replace string."""
        with tempfile.TemporaryDirectory() as temp_dir:
            with pytest.raises(
                ValueError, match="String transforms must be a non-empty dictionary"
            ):
                PolicyExportFormatter(input_dir=temp_dir, string_transforms=None)


if __name__ == "__main__":
    pytest.main([__file__])
