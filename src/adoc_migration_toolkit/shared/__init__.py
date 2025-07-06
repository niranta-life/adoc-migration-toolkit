"""
Shared utilities and global state management.

This module contains shared utilities, global variables, and state management
functions used across the adoc migration toolkit.
"""

from .globals import (
    GLOBAL_OUTPUT_DIR,
    load_global_output_directory,
    save_global_output_directory,
    set_global_output_directory
)

from .file_utils import get_output_file_path
from .api_client import AcceldataAPIClient, create_api_client

__all__ = [
    'GLOBAL_OUTPUT_DIR',
    'load_global_output_directory', 
    'save_global_output_directory',
    'set_global_output_directory',
    'get_output_file_path',
    'AcceldataAPIClient',
    'create_api_client'
] 