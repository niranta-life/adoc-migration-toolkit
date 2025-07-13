"""
VCS Configuration management.

This module handles VCS configuration storage and retrieval,
including secure credential management using system keyring.
"""

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import keyring


@dataclass
class VCSConfig:
    """VCS configuration data class."""

    vcs_type: str  # git, hg, svn
    remote_url: str
    username: Optional[str] = None
    token: Optional[str] = None
    ssh_key_path: Optional[str] = None
    ssh_passphrase: Optional[str] = None
    proxy_url: Optional[str] = None
    proxy_username: Optional[str] = None
    proxy_password: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, excluding sensitive fields."""
        config_dict = asdict(self)
        # Remove sensitive fields from dict representation
        sensitive_fields = ["token", "ssh_passphrase", "proxy_password"]
        for field in sensitive_fields:
            if field in config_dict:
                del config_dict[field]
        return config_dict


class VCSConfigManager:
    """Manages VCS configuration storage and retrieval."""

    def __init__(self, config_file: Optional[str] = None):
        """Initialize VCS config manager.

        Args:
            config_file: Path to configuration file. Defaults to ~/.adoc_vcs_config.json
        """
        if config_file is None:
            self.config_file = Path.home() / ".adoc_vcs_config.json"
        else:
            self.config_file = Path(config_file)

        self.service_name = "adoc-migration-toolkit-vcs"

    def save_config(self, config: VCSConfig) -> bool:
        """Save VCS configuration to file and keyring.

        Args:
            config: VCS configuration object

        Returns:
            True if successful, False otherwise
        """
        try:
            # Save non-sensitive config to file
            config_dict = config.to_dict()

            # Ensure config directory exists
            self.config_file.parent.mkdir(parents=True, exist_ok=True)

            with open(self.config_file, "w") as f:
                json.dump(config_dict, f, indent=2)

            # Save sensitive credentials to keyring
            if config.token:
                keyring.set_password(
                    self.service_name,
                    f"{config.vcs_type}_{config.remote_url}_token",
                    config.token,
                )

            if config.ssh_passphrase:
                keyring.set_password(
                    self.service_name,
                    f"{config.vcs_type}_{config.remote_url}_ssh_passphrase",
                    config.ssh_passphrase,
                )

            if config.proxy_password:
                keyring.set_password(
                    self.service_name,
                    f"{config.vcs_type}_{config.remote_url}_proxy_password",
                    config.proxy_password,
                )

            return True

        except Exception as e:
            print(f"❌ Error saving VCS configuration: {e}")
            return False

    def load_config(self) -> Optional[VCSConfig]:
        """Load VCS configuration from file and keyring.

        Returns:
            VCSConfig object if found, None otherwise
        """
        try:
            if not self.config_file.exists():
                return None

            with open(self.config_file, "r") as f:
                config_dict = json.load(f)

            # Create config object
            config = VCSConfig(**config_dict)

            # Load sensitive credentials from keyring
            if config.vcs_type and config.remote_url:
                # Load token
                token = keyring.get_password(
                    self.service_name, f"{config.vcs_type}_{config.remote_url}_token"
                )
                if token:
                    config.token = token

                # Load SSH passphrase
                ssh_passphrase = keyring.get_password(
                    self.service_name,
                    f"{config.vcs_type}_{config.remote_url}_ssh_passphrase",
                )
                if ssh_passphrase:
                    config.ssh_passphrase = ssh_passphrase

                # Load proxy password
                proxy_password = keyring.get_password(
                    self.service_name,
                    f"{config.vcs_type}_{config.remote_url}_proxy_password",
                )
                if proxy_password:
                    config.proxy_password = proxy_password

            return config

        except Exception as e:
            print(f"❌ Error loading VCS configuration: {e}")
            return None

    def delete_config(self) -> bool:
        """Delete VCS configuration file and keyring entries.

        Returns:
            True if successful, False otherwise
        """
        try:
            # Load config first to get credentials to delete from keyring
            config = self.load_config()

            # Delete from keyring
            if config and config.vcs_type and config.remote_url:
                keyring.delete_password(
                    self.service_name, f"{config.vcs_type}_{config.remote_url}_token"
                )
                keyring.delete_password(
                    self.service_name,
                    f"{config.vcs_type}_{config.remote_url}_ssh_passphrase",
                )
                keyring.delete_password(
                    self.service_name,
                    f"{config.vcs_type}_{config.remote_url}_proxy_password",
                )

            # Delete config file
            if self.config_file.exists():
                self.config_file.unlink()

            return True

        except Exception as e:
            print(f"❌ Error deleting VCS configuration: {e}")
            return False

    def config_exists(self) -> bool:
        """Check if VCS configuration exists.

        Returns:
            True if configuration file exists, False otherwise
        """
        return self.config_file.exists()
