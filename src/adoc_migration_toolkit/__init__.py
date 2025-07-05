"""
ADOC Migration Toolkit - A professional tool for replacing substrings in JSON files and ZIP archives.

This package provides functionality to process policy export files by replacing
substrings in JSON files and ZIP archives with comprehensive error handling.
"""

__version__ = "1.0.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

from .core import PolicyExportFormatter, main
from .api_client import AcceldataAPIClient, create_api_client

__all__ = ["PolicyExportFormatter", "main", "AcceldataAPIClient", "create_api_client"] 