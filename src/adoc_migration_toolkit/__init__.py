"""
ADOC Migration Toolkit

A tool for migrating Acceldata configs between environments.
"""

__version__ = "1.0.0"
__author__ = "ADOC Migration Toolkit Team"
__email__ = "support@acceldata.io"

# Import main components from new modular structure
from .execution.formatter import PolicyExportFormatter, validate_arguments
from .shared.logging import setup_logging
from .cli import main, run_interactive
from .shared import (
    GLOBAL_OUTPUT_DIR,
    load_global_output_directory,
    save_global_output_directory,
    set_global_output_directory,
    get_output_file_path,
    AcceldataAPIClient,
    create_api_client
)

# Import execution functions from new modular structure
from .execution.utils import create_progress_bar, read_csv_uids, read_csv_uids_single_column
from .execution.asset_operations import execute_asset_profile_export_guided

__all__ = [
    # Core functionality
    'PolicyExportFormatter',
    'setup_logging',
    'validate_arguments',
    
    # CLI functionality
    'main',
    'run_asset_export',
    'run_interactive',
    
    # Shared utilities
    'GLOBAL_OUTPUT_DIR',
    'load_global_output_directory',
    'save_global_output_directory',
    'set_global_output_directory',
    'get_output_file_path',
    'AcceldataAPIClient',
    'create_api_client',
    
    # Execution utilities
    'create_progress_bar',
    'read_csv_uids',
    'read_csv_uids_single_column',
    'execute_asset_profile_export_guided'
]