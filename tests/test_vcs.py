"""
Tests for VCS module.
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from adoc_migration_toolkit.vcs.config import VCSConfig, VCSConfigManager
from adoc_migration_toolkit.vcs.operations import (
    determine_auth_method,
    execute_vcs_config,
    is_valid_remote_url,
    parse_vcs_config_command,
)


class TestVCSConfig:
    """Test VCSConfig class."""

    def test_vcs_config_creation(self):
        """Test creating a VCSConfig object."""
        config = VCSConfig(
            vcs_type="git",
            remote_url="https://github.com/user/repo.git",
            username="user",
            token="secret",
        )

        assert config.vcs_type == "git"
        assert config.remote_url == "https://github.com/user/repo.git"
        assert config.username == "user"
        assert config.token == "secret"

    def test_vcs_config_to_dict_excludes_sensitive_fields(self):
        """Test that to_dict excludes sensitive fields."""
        config = VCSConfig(
            vcs_type="git",
            remote_url="https://github.com/user/repo.git",
            username="user",
            token="secret",
            ssh_passphrase="passphrase",
            proxy_password="proxy_pass",
        )

        config_dict = config.to_dict()

        assert "token" not in config_dict
        assert "ssh_passphrase" not in config_dict
        assert "proxy_password" not in config_dict
        assert config_dict["vcs_type"] == "git"
        assert config_dict["remote_url"] == "https://github.com/user/repo.git"
        assert config_dict["username"] == "user"


class TestVCSConfigManager:
    """Test VCSConfigManager class."""

    def test_config_manager_initialization(self):
        """Test VCSConfigManager initialization."""
        with tempfile.NamedTemporaryFile() as tmp:
            manager = VCSConfigManager(tmp.name)
            assert manager.config_file == Path(tmp.name)
            assert manager.service_name == "adoc-migration-toolkit-vcs"

    def test_config_manager_default_path(self):
        """Test VCSConfigManager uses default path."""
        manager = VCSConfigManager()
        assert manager.config_file == Path.home() / ".adoc_vcs_config.json"

    @patch("adoc_migration_toolkit.vcs.config.keyring")
    def test_save_config(self, mock_keyring):
        """Test saving VCS configuration."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "test_config.json"
            manager = VCSConfigManager(str(config_file))

            config = VCSConfig(
                vcs_type="git",
                remote_url="https://github.com/user/repo.git",
                username="user",
                token="secret",
            )

            result = manager.save_config(config)

            assert result is True
            assert config_file.exists()

            # Check that keyring was called
            mock_keyring.set_password.assert_called_once_with(
                "adoc-migration-toolkit-vcs",
                "git_https://github.com/user/repo.git_token",
                "secret",
            )

    @patch("adoc_migration_toolkit.vcs.config.keyring")
    def test_load_config(self, mock_keyring):
        """Test loading VCS configuration."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "test_config.json"
            manager = VCSConfigManager(str(config_file))

            # Create a config file
            config_data = {
                "vcs_type": "git",
                "remote_url": "https://github.com/user/repo.git",
                "username": "user",
            }

            with open(config_file, "w") as f:
                import json

                json.dump(config_data, f)

            # Mock keyring to return a token
            mock_keyring.get_password.return_value = "secret"

            config = manager.load_config()

            assert config is not None
            assert config.vcs_type == "git"
            assert config.remote_url == "https://github.com/user/repo.git"
            assert config.username == "user"
            assert config.token == "secret"

    def test_load_config_file_not_exists(self):
        """Test loading config when file doesn't exist."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "nonexistent.json"
            manager = VCSConfigManager(str(config_file))

            config = manager.load_config()
            assert config is None

    def test_config_exists(self):
        """Test config_exists method."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "test_config.json"
            manager = VCSConfigManager(str(config_file))

            # File doesn't exist initially
            assert manager.config_exists() is False

            # Create the file
            config_file.touch()
            assert manager.config_exists() is True


class TestVCSOperations:
    """Test VCS operations."""

    def test_parse_vcs_config_command_no_args(self):
        """Test parsing vcs-config command with no arguments."""
        result = parse_vcs_config_command("vcs-config")
        assert result == (None, None, None, None, None, None, None, None, None)

    def test_parse_vcs_config_command_with_args(self):
        """Test parsing vcs-config command with arguments."""
        result = parse_vcs_config_command(
            "vcs-config --vcs-type git --remote-url https://github.com/user/repo.git --username user --token secret"
        )

        assert result[0] == "git"  # vcs_type
        assert result[1] == "https://github.com/user/repo.git"  # remote_url
        assert result[2] == "user"  # username
        assert result[3] == "secret"  # token

    def test_parse_vcs_config_command_ssh(self):
        """Test parsing vcs-config command with SSH options."""
        result = parse_vcs_config_command(
            "vcs-config --vcs-type git --remote-url git@github.com:user/repo.git --ssh-key-path ~/.ssh/id_rsa --ssh-passphrase secret"
        )

        assert result[0] == "git"  # vcs_type
        assert result[1] == "git@github.com:user/repo.git"  # remote_url
        assert result[4] == "~/.ssh/id_rsa"  # ssh_key_path
        assert result[5] == "secret"  # ssh_passphrase

    def test_parse_vcs_config_command_proxy(self):
        """Test parsing vcs-config command with proxy options."""
        result = parse_vcs_config_command(
            "vcs-config --vcs-type git --remote-url https://github.com/user/repo.git --proxy-url http://proxy:8080 --proxy-username proxy_user --proxy-password proxy_pass"
        )

        assert result[0] == "git"  # vcs_type
        assert result[1] == "https://github.com/user/repo.git"  # remote_url
        assert result[6] == "http://proxy:8080"  # proxy_url
        assert result[7] == "proxy_user"  # proxy_username
        assert result[8] == "proxy_pass"  # proxy_password

    def test_is_valid_remote_url_https(self):
        """Test URL validation for HTTPS URLs."""
        assert is_valid_remote_url("https://github.com/user/repo.git") is True
        assert is_valid_remote_url("http://github.com/user/repo.git") is True
        assert (
            is_valid_remote_url("https://enterprise.gitlab.com/user/repo.git") is True
        )

    def test_is_valid_remote_url_ssh(self):
        """Test URL validation for SSH URLs."""
        assert is_valid_remote_url("ssh://user@github.com:22/repo.git") is True
        assert is_valid_remote_url("git@github.com:user/repo.git") is True
        assert is_valid_remote_url("user@github.com:user/repo.git") is True

    def test_is_valid_remote_url_invalid(self):
        """Test URL validation for invalid URLs."""
        assert is_valid_remote_url("not-a-url") is False
        assert is_valid_remote_url("ftp://github.com/user/repo.git") is False
        assert is_valid_remote_url("") is False

    def test_determine_auth_method_https(self):
        """Test auth method determination for HTTPS URLs."""
        assert determine_auth_method("https://github.com/user/repo.git") == "https"
        assert determine_auth_method("http://github.com/user/repo.git") == "https"

    def test_determine_auth_method_ssh(self):
        """Test auth method determination for SSH URLs."""
        assert determine_auth_method("ssh://user@github.com/repo.git") == "ssh"
        assert determine_auth_method("git@github.com:user/repo.git") == "ssh"

    def test_determine_auth_method_unknown(self):
        """Test auth method determination for unknown URLs."""
        assert (
            determine_auth_method("ftp://github.com/user/repo.git") == "https"
        )  # defaults to https

    @patch("adoc_migration_toolkit.vcs.operations.execute_vcs_config_interactive")
    def test_execute_vcs_config_interactive_mode(self, mock_interactive):
        """Test execute_vcs_config in interactive mode."""
        mock_interactive.return_value = True

        result = execute_vcs_config("vcs-config")

        assert result is True
        mock_interactive.assert_called_once()

    @patch("adoc_migration_toolkit.vcs.operations.save_vcs_config")
    def test_execute_vcs_config_with_args(self, mock_save):
        """Test execute_vcs_config with arguments."""
        mock_save.return_value = True

        result = execute_vcs_config(
            "vcs-config --vcs-type git --remote-url https://github.com/user/repo.git"
        )

        assert result is True
        mock_save.assert_called_once()
