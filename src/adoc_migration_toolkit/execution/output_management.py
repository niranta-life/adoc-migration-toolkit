"""
Output management functions for the adoc-migration-toolkit.

This module handles global output directory management, file path generation,
and output directory configuration.
"""

import json
import os
from pathlib import Path
from typing import Optional
from ..shared.file_utils import get_output_file_path
from adoc_migration_toolkit.shared.globals import GLOBAL_OUTPUT_DIR

# Global output directory - shared across modules
GLOBAL_OUTPUT_DIR: Optional[Path] = None

def parse_set_output_dir_command(command: str) -> str:
    """Parse the set-output-dir command and extract the directory path.
    
    Args:
        command: The full command string
        
    Returns:
        str: The directory path, or None if invalid
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'set-output-dir':
        return None
    
    if len(parts) < 2:
        print("❌ Error: Directory path is required")
        print("Usage: set-output-dir <directory_path>")
        return None
    
    directory = parts[1].strip()
    
    # Validate the directory
    if not directory:
        print("❌ Error: Directory path cannot be empty")
        return None
    
    # Expand user path and resolve
    try:
        expanded_path = Path(directory).expanduser().resolve()
        return str(expanded_path)
    except Exception as e:
        print(f"❌ Error: Invalid directory path '{directory}': {e}")
        return None

def load_global_output_directory() -> Path:
    """Load the global output directory from the config file.
    
    Returns:
        Path: The global output directory, or None if not set
    """
    global GLOBAL_OUTPUT_DIR
    
    config_file = Path.home() / ".adoc_migration_toolkit" / "config.json"
    
    if config_file.exists():
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
                output_dir = config.get('global_output_directory')
                if output_dir:
                    GLOBAL_OUTPUT_DIR = Path(output_dir)
                    return GLOBAL_OUTPUT_DIR
        except Exception as e:
            print(f"Warning: Could not load config file: {e}")
    
    return None

def save_global_output_directory(output_dir: Path):
    """Save the global output directory to the config file.
    
    Args:
        output_dir: The output directory to save
    """
    config_dir = Path.home() / ".adoc_migration_toolkit"
    config_file = config_dir / "config.json"
    
    try:
        # Create config directory if it doesn't exist
        config_dir.mkdir(parents=True, exist_ok=True)
        
        # Load existing config or create new one
        config = {}
        if config_file.exists():
            try:
                with open(config_file, 'r') as f:
                    config = json.load(f)
            except Exception:
                config = {}
        
        # Update the global output directory
        config['global_output_directory'] = str(output_dir)
        
        # Save the config
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)
            
    except Exception as e:
        print(f"Warning: Could not save config file: {e}")

def set_global_output_directory(directory: str, logger) -> bool:
    """Set the global output directory and save it to config.
    
    Args:
        directory: The directory path to set
        logger: Logger instance for logging
        
    Returns:
        bool: True if successful, False otherwise
    """
    global GLOBAL_OUTPUT_DIR
    
    try:
        # Validate and resolve the directory path
        output_dir = Path(directory).expanduser().resolve()
        
        # Check if directory exists or can be created
        if not output_dir.exists():
            try:
                output_dir.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                error_msg = f"Cannot create directory '{directory}': {e}"
                print(f"❌ {error_msg}")
                logger.error(error_msg)
                return False
        
        # Check if it's a directory
        if not output_dir.is_dir():
            error_msg = f"'{directory}' is not a directory"
            print(f"❌ {error_msg}")
            logger.error(error_msg)
            return False
        
        # Set the global output directory
        GLOBAL_OUTPUT_DIR = output_dir
        
        # Save to config file
        save_global_output_directory(GLOBAL_OUTPUT_DIR)
        
        success_msg = f"Global output directory set to: {GLOBAL_OUTPUT_DIR}"
        print(f"✅ {success_msg}")
        logger.info(success_msg)
        
        return True
        
    except Exception as e:
        error_msg = f"Error setting global output directory '{directory}': {e}"
        print(f"❌ {error_msg}")
        logger.error(error_msg)
        return False 