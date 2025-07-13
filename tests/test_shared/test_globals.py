import json
import os
import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from adoc_migration_toolkit.shared.globals import (
    GLOBAL_OUTPUT_DIR,
    load_global_output_directory,
    save_global_output_directory,
    set_global_output_directory,
)


class TestGlobals:
    """Test cases for global state management functions."""

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

    def test_load_global_output_directory_success(self):
        """Test successful loading of global output directory."""
        config_dir = Path.home() / ".adoc_migration_toolkit"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_file = config_dir / "output_dir.json"

        # Create test config file
        test_output_dir = os.path.join(self.temp_dir, "test_output")
        os.makedirs(test_output_dir)

        with open(config_file, "w") as f:
            json.dump({"output_dir": test_output_dir}, f)

        result = load_global_output_directory()

        assert result == Path(test_output_dir)
        # Check the global variable directly
        import adoc_migration_toolkit.shared.globals as globals_module

        assert globals_module.GLOBAL_OUTPUT_DIR == Path(test_output_dir)

        # Clean up
        config_file.unlink()
        if config_dir.exists() and not any(config_dir.iterdir()):
            config_dir.rmdir()

    def test_load_global_output_directory_no_config_file(self):
        """Test loading when config file doesn't exist."""
        result = load_global_output_directory()
        assert result is None
        assert GLOBAL_OUTPUT_DIR is None

    def test_load_global_output_directory_invalid_config(self):
        """Test loading with invalid config file."""
        config_dir = Path.home() / ".adoc_migration_toolkit"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_file = config_dir / "output_dir.json"

        # Create invalid config file
        with open(config_file, "w") as f:
            f.write("invalid json")

        result = load_global_output_directory()

        assert result is None
        assert GLOBAL_OUTPUT_DIR is None

        # Clean up
        config_file.unlink()
        if config_dir.exists() and not any(config_dir.iterdir()):
            config_dir.rmdir()

    def test_load_global_output_directory_nonexistent_directory(self):
        """Test loading when directory in config doesn't exist."""
        config_dir = Path.home() / ".adoc_migration_toolkit"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_file = config_dir / "output_dir.json"

        # Create config with non-existent directory
        with open(config_file, "w") as f:
            json.dump({"output_dir": "/nonexistent/directory"}, f)

        result = load_global_output_directory()

        assert result is None
        assert GLOBAL_OUTPUT_DIR is None

        # Clean up
        config_file.unlink()
        if config_dir.exists() and not any(config_dir.iterdir()):
            config_dir.rmdir()

    def test_save_global_output_directory_success(self):
        """Test successful saving of global output directory."""
        test_output_dir = os.path.join(self.temp_dir, "test_output")
        os.makedirs(test_output_dir)

        save_global_output_directory(Path(test_output_dir))

        # Check the global variable directly
        import adoc_migration_toolkit.shared.globals as globals_module

        assert globals_module.GLOBAL_OUTPUT_DIR == Path(test_output_dir)

        # Verify config file was created
        config_file = Path.home() / ".adoc_migration_toolkit" / "output_dir.json"
        assert config_file.exists()

        with open(config_file, "r") as f:
            data = json.load(f)
            assert data["output_dir"] == test_output_dir

        # Clean up: remove config file before removing directory
        config_file.unlink(missing_ok=True)
        try:
            config_dir = Path.home() / ".adoc_migration_toolkit"
            config_dir.rmdir()
        except OSError:
            pass

    def test_save_global_output_directory_creates_config_dir(self):
        """Test that config directory is created if it doesn't exist."""
        config_dir = Path.home() / ".adoc_migration_toolkit"
        if config_dir.exists():
            shutil.rmtree(config_dir)

        test_output_dir = os.path.join(self.temp_dir, "test_output")
        os.makedirs(test_output_dir)

        save_global_output_directory(Path(test_output_dir))

        assert config_dir.exists()
        assert (config_dir / "output_dir.json").exists()

        # Clean up
        (config_dir / "output_dir.json").unlink()
        config_dir.rmdir()

    def test_set_global_output_directory_success(self):
        """Test successful setting of global output directory."""
        mock_logger = MagicMock()
        test_dir = os.path.join(self.temp_dir, "new_output")

        result = set_global_output_directory(test_dir, mock_logger)

        assert result is True
        # Check the global variable directly
        import adoc_migration_toolkit.shared.globals as globals_module

        assert globals_module.GLOBAL_OUTPUT_DIR == Path(test_dir).resolve()
        assert os.path.exists(test_dir)
        assert os.path.isdir(test_dir)

        mock_logger.info.assert_called()

    def test_set_global_output_directory_existing_directory(self):
        """Test setting global output directory when directory already exists."""
        mock_logger = MagicMock()
        test_dir = os.path.join(self.temp_dir, "existing_output")
        os.makedirs(test_dir)

        result = set_global_output_directory(test_dir, mock_logger)

        assert result is True
        # Check the global variable directly
        import adoc_migration_toolkit.shared.globals as globals_module

        assert globals_module.GLOBAL_OUTPUT_DIR == Path(test_dir).resolve()
        mock_logger.info.assert_called()

    def test_set_global_output_directory_not_directory(self):
        """Test setting global output directory when path is not a directory."""
        mock_logger = MagicMock()
        test_file = os.path.join(self.temp_dir, "test_file")

        # Create a file instead of directory
        with open(test_file, "w") as f:
            f.write("test")

        result = set_global_output_directory(test_file, mock_logger)

        assert result is False
        mock_logger.error.assert_called()

    def test_set_global_output_directory_permission_error(self):
        """Test setting global output directory with permission error."""
        mock_logger = MagicMock()

        with patch(
            "pathlib.Path.mkdir", side_effect=PermissionError("Permission denied")
        ):
            result = set_global_output_directory("/root/test", mock_logger)

            assert result is False
            mock_logger.error.assert_called()

    def test_global_output_directory_persistence(self):
        """Test that global output directory persists across function calls."""
        test_output_dir = os.path.join(self.temp_dir, "persistent_output")
        os.makedirs(test_output_dir)

        # Set global output directory
        save_global_output_directory(Path(test_output_dir))
        # Check the global variable directly
        import adoc_migration_toolkit.shared.globals as globals_module

        assert globals_module.GLOBAL_OUTPUT_DIR == Path(test_output_dir)

        # Reset global state
        globals_module.GLOBAL_OUTPUT_DIR = None

        # Load global output directory
        result = load_global_output_directory()
        assert result == Path(test_output_dir)
        assert globals_module.GLOBAL_OUTPUT_DIR == Path(test_output_dir)

    def test_global_output_directory_resolution(self):
        """Test that relative paths are resolved to absolute paths."""
        mock_logger = MagicMock()
        relative_dir = "relative_output"

        result = set_global_output_directory(relative_dir, mock_logger)

        assert result is True
        # Check the global variable directly
        import adoc_migration_toolkit.shared.globals as globals_module

        assert globals_module.GLOBAL_OUTPUT_DIR.is_absolute()
        assert globals_module.GLOBAL_OUTPUT_DIR.name == "relative_output"
