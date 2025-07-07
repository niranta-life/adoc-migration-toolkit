"""
Core utility functions.

This module contains core utility functions used across the adoc migration toolkit.
"""

import argparse
from pathlib import Path


def validate_arguments(args: argparse.Namespace) -> None:
    """Validate command line arguments.
    
    Args:
        args: Parsed command line arguments
        
    Raises:
        ValueError: If arguments are invalid
    """
    if not args.input_dir or not args.input_dir.strip():
        raise ValueError("Input directory cannot be empty")
    
    if not args.search_string or not args.search_string.strip():
        raise ValueError("Search string cannot be empty")
    
    if args.replace_string is None:
        raise ValueError("Replace string cannot be None")
    
    # Check if input directory exists
    input_path = Path(args.input_dir)
    if not input_path.exists():
        raise FileNotFoundError(f"Input directory does not exist: {args.input_dir}")
    
    if not input_path.is_dir():
        raise ValueError(f"Input path is not a directory: {args.input_dir}") 