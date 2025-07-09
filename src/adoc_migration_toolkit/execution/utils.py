"""
Execution utility functions.

This module contains utility functions used by execution operations
including progress bars, CSV reading, and file path utilities.
"""

import csv
import logging
from pathlib import Path
from typing import List, Tuple, Dict
from tqdm import tqdm

from ..shared.file_utils import get_output_file_path


def create_progress_bar(total: int, desc: str = "Processing", unit: str = "items", disable: bool = False, position: int = None, leave: bool = True):
    """Create a tqdm progress bar with consistent styling.
    
    Args:
        total: Total number of items to process
        desc: Description for the progress bar
        unit: Unit of measurement (items, rules, files, etc.)
        disable: Whether to disable the progress bar (for verbose mode)
        position: Position for the progress bar (for multiple bars)
        leave: Whether to leave the progress bar after completion
        
    Returns:
        tqdm progress bar instance
    """
    return tqdm(
        total=total,
        desc=desc,
        unit=unit,
        disable=disable,
        position=position,
        leave=leave,
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


def read_csv_asset_data(csv_file: str, logger: logging.Logger) -> List[Dict[str, str]]:
    """Read asset data from CSV file with 5 columns: source_id, source_uid, target_id, target_uid, tags.
    
    Args:
        csv_file: Path to the CSV file
        logger: Logger instance
        
    Returns:
        List of dictionaries with asset data from the CSV file
    """
    asset_data = []
    
    try:
        with open(csv_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            
            # Skip header row
            header = next(reader, None)
            if header:
                logger.info(f"CSV header: {header}")
            
            # Read asset data from all 5 columns (asset-merged-all.csv format)
            for row_num, row in enumerate(reader, start=2):  # Start at 2 since we skipped header
                if row and len(row) >= 5:
                    source_id = row[0].strip()
                    source_uid = row[1].strip()
                    target_id = row[2].strip()
                    target_uid = row[3].strip()
                    tags = row[4].strip()
                    
                    if source_id and target_uid:  # Skip rows with empty required fields
                        asset_data.append({
                            'source_uid': source_uid,
                            'source_id': source_id,
                            'target_uid': target_uid,
                            'tags': tags
                        })
                        logger.debug(f"Row {row_num}: Found asset - source_id: {source_id}, source_uid: {source_uid}, target_uid: {target_uid}, tags: {tags}")
                    else:
                        logger.warning(f"Row {row_num}: Empty required fields (source_id or target_uid)")
                else:
                    logger.warning(f"Row {row_num}: Insufficient columns (need at least 5, got {len(row) if row else 0})")
        
        logger.info(f"Read {len(asset_data)} asset records from CSV file: {csv_file}")
        return asset_data
        
    except Exception as e:
        logger.error(f"Error reading CSV file {csv_file}: {e}")
        raise 