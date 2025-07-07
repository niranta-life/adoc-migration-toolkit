"""
File utilities for path and output management.

This module contains utilities for handling file paths, output directories,
and file operations used across the adoc migration toolkit.
"""

import csv
from pathlib import Path
from datetime import datetime
from typing import Optional

# Import the globals module to access the global variable dynamically
from . import globals


def get_output_file_path(csv_file: str, default_filename: str, custom_output_file: str = None, category: str = None) -> Path:
    """Generate output file path based on input CSV file and configuration.
    
    Args:
        csv_file: Input CSV file path
        default_filename: Default filename to use if no custom output file specified
        custom_output_file: Custom output file path (optional)
        category: Category subdirectory (optional)
        
    Returns:
        Path object for the output file
    """
    if custom_output_file:
        # Use custom output file path
        output_path = Path(custom_output_file)
        
        # Create parent directory if it doesn't exist
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        return output_path
    
    # Use global output directory or create timestamped directory
    # Get the global variable dynamically to ensure we get the current value
    if globals.GLOBAL_OUTPUT_DIR:
        base_output_dir = globals.GLOBAL_OUTPUT_DIR
    else:
        # Create timestamped directory in current working directory
        timestamp = datetime.now().strftime('%Y%m%d%H%M')
        base_output_dir = Path.cwd() / f"adoc-migration-toolkit-{timestamp}"
    
    # Create category subdirectory if specified
    if category:
        output_dir = base_output_dir / category
    else:
        output_dir = base_output_dir
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate output file path
    output_file = output_dir / default_filename
    
    return output_file 