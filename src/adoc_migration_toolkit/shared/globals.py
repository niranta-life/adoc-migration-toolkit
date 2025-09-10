"""
Global variables and state management.

This module contains global variables and functions for managing
application state across the adoc migration toolkit.
"""

import json
import logging
from pathlib import Path
from typing import Optional

# Global variable to store the output directory
GLOBAL_OUTPUT_DIR: Optional[Path] = None

# Global HTTP config for timeout, retry, and proxy
HTTP_CONFIG = {
    'timeout': 300,   # seconds
    'retry': 3,     # number of retries
    'proxy': None   # proxy URL or None
}


def load_global_output_directory() -> Path:
    """Load the global output directory from configuration file."""
    global GLOBAL_OUTPUT_DIR
    
    config_file = Path.home() / ".adoc_migration_toolkit" / "output_dir.json"
    
    if config_file.exists():
        try:
            with open(config_file, 'r') as f:
                data = json.load(f)
                output_dir = Path(data.get('output_dir', ''))
                if output_dir.exists() and output_dir.is_dir():
                    GLOBAL_OUTPUT_DIR = output_dir
                    return GLOBAL_OUTPUT_DIR
        except Exception as e:
            print(f"Warning: Could not load output directory from config: {e}")
    
    return None


def save_global_output_directory(output_dir: Path):
    """Save the global output directory to configuration file."""
    global GLOBAL_OUTPUT_DIR
    
    GLOBAL_OUTPUT_DIR = output_dir
    
    config_dir = Path.home() / ".adoc_migration_toolkit"
    config_dir.mkdir(parents=True, exist_ok=True)
    
    config_file = config_dir / "output_dir.json"
    
    try:
        with open(config_file, 'w') as f:
            json.dump({'output_dir': str(output_dir)}, f, indent=2)
    except Exception as e:
        print(f"Warning: Could not save output directory to config: {e}")


def set_global_output_directory(directory: str, logger: logging.Logger) -> bool:
    """Set the global output directory.
    
    Args:
        directory: Directory path to set as global output directory
        logger: Logger instance
        
    Returns:
        True if successful, False otherwise
    """
    try:
        output_dir = Path(directory).resolve()
        
        # Create directory if it doesn't exist
        if not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created output directory: {output_dir}")
        
        # Validate it's a directory
        if not output_dir.is_dir():
            logger.error(f"Path is not a directory: {output_dir}")
            return False
        
        # Save to global state
        save_global_output_directory(output_dir)
        
        logger.info(f"Global output directory set to: {output_dir}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to set global output directory: {e}")
        return False 