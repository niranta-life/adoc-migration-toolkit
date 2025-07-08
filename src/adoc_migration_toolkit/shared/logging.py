"""
Shared logging utilities.

This module contains shared logging functionality used across the toolkit.
"""

import logging
import os
from datetime import datetime
from pathlib import Path

def setup_logging(verbose: bool = False, log_level: str = "INFO", log_file_path: str = None) -> logging.Logger:
    """Setup professional logging configuration.
    
    Args:
        verbose (bool): Enable verbose logging (overrides log_level)
        log_level (str): Logging level (ERROR, WARNING, INFO, DEBUG)
        log_file_path (str): Custom log file path (optional, overrides environment variable)
    
    Returns:
        logging.Logger: Configured logger instance
    """
    # Map string log levels to logging constants
    level_map = {
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG
    }
    
    # Determine the actual log level
    if verbose:
        # Verbose overrides log_level
        actual_level = logging.DEBUG
    else:
        # Use the specified log level, default to INFO
        actual_level = level_map.get(log_level.upper(), logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Setup handlers
    handlers = []
    
    # Determine log file path
    if log_file_path:
        # Use provided log file path
        log_file = log_file_path
    else:
        # Check environment variable
        env_log_file = os.getenv('AD_LOG_FILE_PATH')
        if env_log_file:
            log_file = env_log_file
        else:
            # Use default date-based log file in current directory
            log_file = f"adoc-migration-toolkit-{datetime.now().strftime('%Y%m%d')}.log"
    
    # Create log directory if it doesn't exist
    log_path = Path(log_file)
    if log_path.parent != Path('.'):
        log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # File handler with date-based rotation
    file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='a')  # Append mode
    file_handler.setFormatter(formatter)
    handlers.append(file_handler)
    
    # Console handler - REMOVED to prevent breaking progress bars
    # console_handler = logging.StreamHandler(sys.stdout)
    # console_handler.setFormatter(formatter)
    # handlers.append(console_handler)
    
    # Configure root logger
    logging.basicConfig(
        level=actual_level,
        handlers=handlers,
        force=True
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_file}")
    logger.info(f"Log level set to: {log_level.upper()}")
    return logger 