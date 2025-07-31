"""
Shared logging utilities.

This module contains shared logging functionality used across the toolkit.
"""

import logging
import os
import getpass
import socket
from datetime import datetime
from pathlib import Path
from logging.handlers import RotatingFileHandler
import stat
import platform

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

class ReadOnlyRotatingFileHandler(RotatingFileHandler):
    """RotatingFileHandler that sets rotated files to read-only."""
    def doRollover(self):
        super().doRollover()
        # Find the most recent rotated file
        if self.backupCount > 0:
            rotated_file = f"{self.baseFilename}.1"
            if os.path.exists(rotated_file):
                try:
                    if os.name == 'nt' or platform.system().lower().startswith('win'):
                        os.chmod(rotated_file, stat.S_IREAD)
                    else:
                        os.chmod(rotated_file, 0o444)
                except Exception as e:
                    # Log error if unable to set permissions
                    logging.getLogger(__name__).warning(f"Could not set rotated log file to read-only: {e}")


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
    
    # Create custom formatter with username and hostname
    formatter = CustomFormatter()
    
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
    
    # Use ReadOnlyRotatingFileHandler for log rotation and permissions
    file_handler = ReadOnlyRotatingFileHandler(
        log_file, maxBytes=1_048_576, backupCount=5, encoding='utf-8', mode='a'
    )
    file_handler.setFormatter(formatter)
    handlers = [file_handler]
    
    # Console handler - REMOVED to prevent breaking progress bars
    # console_handler = logging.StreamHandler(sys.stdout)
    # console_handler.setFormatter(formatter)
    # handlers.append(console_handler)
    
    # Configure root logger
    logging.basicConfig(
        level=actual_level,
        # handlers=handlers,
        force=True
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_file}")
    logger.info(f"Log level set to: {log_level.upper()}")
    return logger


def change_log_level(new_level: str) -> bool:
    """Dynamically change the log level for all loggers in the application.
    
    Args:
        new_level (str): New log level (ERROR, WARNING, INFO, DEBUG)
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Map string log levels to logging constants
        level_map = {
            "ERROR": logging.ERROR,
            "WARNING": logging.WARNING,
            "INFO": logging.INFO,
            "DEBUG": logging.DEBUG
        }
        
        if new_level.upper() not in level_map:
            return False
        
        new_log_level = level_map[new_level.upper()]
        
        # Change the root logger level
        logging.getLogger().setLevel(new_log_level)
        
        # Change the level for all existing loggers
        for logger_name in logging.root.manager.loggerDict:
            logger = logging.getLogger(logger_name)
            logger.setLevel(new_log_level)
        
        # Log the change
        logger = logging.getLogger(__name__)
        logger.info(f"Log level changed to: {new_level.upper()}")
        
        return True
        
    except Exception as e:
        # If logging fails, print to console
        print(f"❌ Failed to change log level: {e}")
        return False 