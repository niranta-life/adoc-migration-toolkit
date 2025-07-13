"""
Shared utilities and global state management.

This module contains shared utilities, global variables, and state management
functions used across the adoc migration toolkit.
"""

from .api_client import AcceldataAPIClient, create_api_client
from .file_utils import get_output_file_path
from .globals import (
    load_global_output_directory,
    save_global_output_directory,
    set_global_output_directory,
)

__all__ = [
    "load_global_output_directory",
    "save_global_output_directory",
    "set_global_output_directory",
    "get_output_file_path",
    "AcceldataAPIClient",
    "create_api_client",
]
