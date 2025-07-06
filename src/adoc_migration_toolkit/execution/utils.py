"""
Execution utility functions.

This module contains utility functions used by execution operations
including progress bars, CSV reading, and file path utilities.
"""

import csv
import logging
from pathlib import Path
from typing import List, Tuple
from tqdm import tqdm

from ..shared.file_utils import get_output_file_path


def create_progress_bar(total: int, desc: str = "Processing", unit: str = "items", disable: bool = False):
    """Create a tqdm progress bar with consistent styling.
    
    Args:
        total: Total number of items to process
        desc: Description for the progress bar
        unit: Unit of measurement (items, rules, files, etc.)
        disable: Whether to disable the progress bar (for verbose mode)
        
    Returns:
        tqdm progress bar instance
    """
    return tqdm(
        total=total,
        desc=desc,
        unit=unit,
        disable=disable,
        bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]',
        colour='green',
        ncols=120
    )


def read_csv_uids(csv_file: str, logger: logging.Logger) -> List[Tuple[str, str]]:
    """Read UIDs and target environments from CSV file.
    
    Args:
        csv_file: Path to the CSV file
        logger: Logger instance
        
    Returns:
        List of tuples (source_env, target_env) from the CSV file
    """
    uids = []
    
    try:
        with open(csv_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            
            # Skip header row
            header = next(reader, None)
            if header:
                logger.info(f"CSV header: {header}")
            
            # Read source-env and target-env from first and second columns
            for row_num, row in enumerate(reader, start=2):  # Start at 2 since we skipped header
                if row and len(row) >= 2:
                    source_env = row[0].strip()
                    target_env = row[1].strip()
                    if source_env and target_env:  # Skip empty values
                        uids.append((source_env, target_env))
                        logger.debug(f"Row {row_num}: Found source-env: {source_env}, target-env: {target_env}")
                    else:
                        logger.warning(f"Row {row_num}: Empty source-env or target-env value")
                else:
                    logger.warning(f"Row {row_num}: Insufficient columns (need at least 2)")
        
        logger.info(f"Read {len(uids)} environment mappings from CSV file: {csv_file}")
        return uids
        
    except Exception as e:
        logger.error(f"Error reading CSV file {csv_file}: {e}")
        raise


def read_csv_uids_single_column(csv_file: str, logger: logging.Logger) -> List[str]:
    """Read UIDs from the first column of CSV file.
    
    Args:
        csv_file: Path to the CSV file
        logger: Logger instance
        
    Returns:
        List of UIDs from the first column of the CSV file
    """
    uids = []
    
    try:
        with open(csv_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            
            # Skip header row
            header = next(reader, None)
            if header:
                logger.info(f"CSV header: {header}")
            
            # Read UIDs from first column
            for row_num, row in enumerate(reader, start=2):  # Start at 2 since we skipped header
                if row and len(row) >= 1:
                    uid = row[0].strip()
                    if uid:  # Skip empty values
                        uids.append(uid)
                        logger.debug(f"Row {row_num}: Found UID: {uid}")
                    else:
                        logger.warning(f"Row {row_num}: Empty UID value")
                else:
                    logger.warning(f"Row {row_num}: No columns found")
        
        logger.info(f"Read {len(uids)} UIDs from CSV file: {csv_file}")
        return uids
        
    except Exception as e:
        logger.error(f"Error reading CSV file {csv_file}: {e}")
        raise 