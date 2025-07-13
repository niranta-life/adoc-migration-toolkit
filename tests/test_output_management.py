"""
Test cases for the output_management module.

This module contains tests for output directory management functions.
"""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, mock_open, patch

import pytest

from src.adoc_migration_toolkit.execution import output_management


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def mock_logger():
    return Mock()


class TestParseSetOutputDirCommand:
    def test_valid_command(self):
        cmd = "set-output-dir /tmp/testdir"
        result = output_management.parse_set_output_dir_command(cmd)
        assert result.endswith("/tmp/testdir")

    def test_invalid_command(self):
        assert output_management.parse_set_output_dir_command("not-a-command") is None

    def test_missing_path(self):
        assert output_management.parse_set_output_dir_command("set-output-dir") is None

    def test_empty_path(self):
        assert (
            output_management.parse_set_output_dir_command("set-output-dir   ") is None
        )

    def test_expand_user(self):
        cmd = "set-output-dir ~/testdir"
        result = output_management.parse_set_output_dir_command(cmd)
        assert str(Path(result)).startswith(str(Path.home()))

    def test_invalid_path(self):
        with patch("pathlib.Path.resolve", side_effect=Exception("bad path")):
            assert (
                output_management.parse_set_output_dir_command(
                    "set-output-dir /bad/path"
                )
                is None
            )


class TestLoadGlobalOutputDirectory:
    def test_loads_from_config(self, temp_dir):
        config_dir = Path.home() / ".adoc_migration_toolkit"
        config_file = config_dir / "config.json"
        config_dir.mkdir(exist_ok=True)
        with open(config_file, "w") as f:
            json.dump({"global_output_directory": str(temp_dir)}, f)
        # Clear any cached value
        output_management.globals.GLOBAL_OUTPUT_DIR = None
        result = output_management.load_global_output_directory()
        assert str(result) == str(temp_dir)
        config_file.unlink()

    def test_returns_none_if_not_set(self):
        output_management.globals.GLOBAL_OUTPUT_DIR = None
        with patch("pathlib.Path.exists", return_value=False):
            assert output_management.load_global_output_directory() is None

    def test_handles_corrupt_config(self):
        config_dir = Path.home() / ".adoc_migration_toolkit"
        config_file = config_dir / "config.json"
        config_dir.mkdir(exist_ok=True)
        with open(config_file, "w") as f:
            f.write("{bad json")
        output_management.globals.GLOBAL_OUTPUT_DIR = None
        result = output_management.load_global_output_directory()
        assert result is None
        config_file.unlink()


class TestSaveGlobalOutputDirectory:
    def test_saves_and_loads(self, temp_dir):
        output_management.save_global_output_directory(temp_dir)
        config_file = Path.home() / ".adoc_migration_toolkit" / "config.json"
        assert config_file.exists()
        with open(config_file) as f:
            data = json.load(f)
        assert data["global_output_directory"] == str(temp_dir)
        config_file.unlink()

    def test_handles_write_error(self, temp_dir):
        with patch("builtins.open", side_effect=Exception("fail")):
            output_management.save_global_output_directory(temp_dir)


class TestSetGlobalOutputDirectory:
    def test_sets_and_saves(self, temp_dir, mock_logger):
        result = output_management.set_global_output_directory(
            str(temp_dir), mock_logger
        )
        assert result is True
        assert (
            output_management.globals.GLOBAL_OUTPUT_DIR.resolve() == temp_dir.resolve()
        )

    def test_creates_directory(self, temp_dir, mock_logger):
        new_dir = temp_dir / "new"
        result = output_management.set_global_output_directory(
            str(new_dir), mock_logger
        )
        assert result is True
        assert new_dir.exists()

    def test_invalid_directory(self, temp_dir, mock_logger):
        file_path = temp_dir / "afile.txt"
        file_path.write_text("hi")
        result = output_management.set_global_output_directory(
            str(file_path), mock_logger
        )
        assert result is False

    def test_cannot_create(self, temp_dir, mock_logger):
        with patch("pathlib.Path.mkdir", side_effect=Exception("fail")):
            result = output_management.set_global_output_directory(
                str(temp_dir / "bad"), mock_logger
            )
            assert result is False

    def test_exception(self, temp_dir, mock_logger):
        with patch("pathlib.Path.expanduser", side_effect=Exception("fail")):
            result = output_management.set_global_output_directory("bad", mock_logger)
            assert result is False
