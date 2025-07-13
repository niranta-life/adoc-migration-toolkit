"""
VCS (Version Control System) module for ADOC Migration Toolkit.

This module provides VCS-related interactive commands for configuring
and managing version control systems in enterprise environments.
"""

from .config import VCSConfig
from .operations import execute_vcs_config

__all__ = [
    "VCSConfig",
    "execute_vcs_config",
]
