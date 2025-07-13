import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from adoc_migration_toolkit.shared.file_utils import get_output_file_path


class TestFileUtils:
    """Test cases for file utility functions."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_csv_file = os.path.join(self.temp_dir, "test.csv")
        self.default_filename = "output.json"

    def teardown_method(self):
        """Clean up test fixtures."""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_get_output_file_path_with_custom_output_file(self):
        """Test generating output file path with custom output file."""
        custom_output_file = os.path.join(self.temp_dir, "custom", "output.json")

        result = get_output_file_path(
            csv_file=self.test_csv_file,
            default_filename=self.default_filename,
            custom_output_file=custom_output_file,
        )

        assert isinstance(result, Path)
        assert str(result) == custom_output_file
        assert result.parent.exists()  # Parent directory should be created

    def test_get_output_file_path_with_category(self):
        """Test generating output file path with category."""
        with patch("adoc_migration_toolkit.shared.globals.GLOBAL_OUTPUT_DIR", None):
            result = get_output_file_path(
                csv_file=self.test_csv_file,
                default_filename=self.default_filename,
                category="test_category",
            )

            assert isinstance(result, Path)
            assert "test_category" in str(result)
            assert result.name == self.default_filename
            assert result.parent.exists()  # Category directory should be created

    def test_get_output_file_path_with_global_output_dir(self):
        """Test generating output file path with global output directory."""
        global_output_dir = os.path.join(self.temp_dir, "global_output")
        os.makedirs(global_output_dir)

        with patch(
            "adoc_migration_toolkit.shared.globals.GLOBAL_OUTPUT_DIR",
            Path(global_output_dir),
        ):
            result = get_output_file_path(
                csv_file=self.test_csv_file, default_filename=self.default_filename
            )

            assert isinstance(result, Path)
            assert str(result).startswith(global_output_dir)
            assert result.name == self.default_filename

    def test_get_output_file_path_without_global_output_dir(self):
        """Test generating output file path without global output directory."""
        with patch("adoc_migration_toolkit.shared.globals.GLOBAL_OUTPUT_DIR", None):
            with patch(
                "adoc_migration_toolkit.shared.file_utils.datetime"
            ) as mock_datetime:
                mock_datetime.now.return_value.strftime.return_value = "202401011200"

                result = get_output_file_path(
                    csv_file=self.test_csv_file, default_filename=self.default_filename
                )

                assert isinstance(result, Path)
                assert "adoc-migration-toolkit-202401011200" in str(result)
                assert result.name == self.default_filename
                assert result.parent.exists()  # Timestamped directory should be created

    def test_get_output_file_path_with_category_and_global_output_dir(self):
        """Test generating output file path with category and global output directory."""
        global_output_dir = os.path.join(self.temp_dir, "global_output")
        os.makedirs(global_output_dir)

        with patch(
            "adoc_migration_toolkit.shared.globals.GLOBAL_OUTPUT_DIR",
            Path(global_output_dir),
        ):
            result = get_output_file_path(
                csv_file=self.test_csv_file,
                default_filename=self.default_filename,
                category="test_category",
            )

            assert isinstance(result, Path)
            assert str(result).startswith(global_output_dir)
            assert "test_category" in str(result)
            assert result.name == self.default_filename
            assert result.parent.exists()  # Category directory should be created

    def test_get_output_file_path_creates_nested_directories(self):
        """Test that nested directories are created for custom output file."""
        custom_output_file = os.path.join(
            self.temp_dir, "nested", "deep", "path", "output.json"
        )

        result = get_output_file_path(
            csv_file=self.test_csv_file,
            default_filename=self.default_filename,
            custom_output_file=custom_output_file,
        )

        assert isinstance(result, Path)
        assert str(result) == custom_output_file
        assert result.parent.exists()  # All parent directories should be created
        assert result.parent.parent.exists()
        assert result.parent.parent.parent.exists()

    def test_get_output_file_path_handles_existing_directories(self):
        """Test handling of existing directories."""
        existing_dir = os.path.join(self.temp_dir, "existing")
        os.makedirs(existing_dir)

        custom_output_file = os.path.join(existing_dir, "output.json")

        result = get_output_file_path(
            csv_file=self.test_csv_file,
            default_filename=self.default_filename,
            custom_output_file=custom_output_file,
        )

        assert isinstance(result, Path)
        assert str(result) == custom_output_file
        assert result.parent.exists()

    def test_get_output_file_path_uses_absolute_paths(self):
        """Test that absolute paths are used."""
        custom_output_file = os.path.join(self.temp_dir, "output.json")

        result = get_output_file_path(
            csv_file=self.test_csv_file,
            default_filename=self.default_filename,
            custom_output_file=custom_output_file,
        )

        assert isinstance(result, Path)
        assert result.is_absolute()  # Should be absolute path
