"""
Shared logging utilities.

This module contains shared logging functionality used across the toolkit.
"""

import logging
import sys
from datetime import datetime


def setup_logging(verbose: bool = False, log_level: str = "ERROR") -> logging.Logger:
    """Setup professional logging configuration.
    
    Args:
        verbose (bool): Enable verbose logging (overrides log_level)
        log_level (str): Logging level (ERROR, WARNING, INFO, DEBUG)
    
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
        # Use the specified log level, default to ERROR
        actual_level = level_map.get(log_level.upper(), logging.ERROR)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Setup handlers
    handlers = []
    
    # File handler with date-based rotation
    log_file = f"adoc-migration-toolkit-{datetime.now().strftime('%Y%m%d')}.log"
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