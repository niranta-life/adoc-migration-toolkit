"""
Logging configuration and setup.

This module contains logging setup and configuration functions.
"""

import json
import logging
import sys
import argparse
import zipfile
import tempfile
import shutil
import os
import csv
import getpass
import socket
from pathlib import Path
from typing import Any, Dict, List, Union, Optional, Set, Tuple
from datetime import datetime


class CustomFormatter(logging.Formatter):
    """Custom formatter that includes username and hostname in log messages."""
    
    def __init__(self):
        super().__init__(
            '%(asctime)s - %(levelname)s - [%(username)s@%(hostname)s] - [%(filename)s:%(lineno)d] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.username = getpass.getuser()
        self.hostname = socket.gethostname()
    
    def format(self, record):
        record.username = self.username
        record.hostname = self.hostname
        return super().format(record)


def setup_logging(verbose: bool = False, log_level: str = "DEBUG") -> logging.Logger:
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
        actual_level = level_map.get(log_level.upper(), logging.DEBUG)
    
    # Create custom formatter with username and hostname
    formatter = CustomFormatter()
    
    # Setup handlers
    handlers = []
    
    # File handler with date-based rotation
    log_file = f"adoc-migration-toolkit-{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='a')  # Append mode
    file_handler.setFormatter(formatter)
    # handlers.append(file_handler)


    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Create a console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
    console_handler.setFormatter(formatter)
    handlers.append(console_handler)
    
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