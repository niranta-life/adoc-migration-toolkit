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
from ..shared import globals



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


def read_csv_asset_data(csv_file: str, logger: logging.Logger, allowed_types: list[str] = ['table', 'sql_view', 'view']) -> List[Dict[str, str]]:
    """Read asset data from CSV file with 5 columns: source_id, source_uid, target_id, target_uid, tags.
    
    Args:
        csv_file: Path to the CSV file
        logger: Logger instance
        
    Returns:
        List of dictionaries with asset data from the CSV file
    """
    asset_data = []
    
    try:
        print(f"Reading asset data from CSV file allowed types :{allowed_types}")
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
                    asset_type = row[5].strip().lower() if row[5] else None
                    
                    if source_id and target_uid and (tags or (asset_type in allowed_types)):
                        logger.debug(f"Row {row_num} is processed")# Skip rows with empty required fields
                        asset_data.append({
                            'source_uid': source_uid,
                            'source_id': source_id,
                            'target_uid': target_uid,
                            'target_id' : target_id,
                            'tags': tags
                        })
                        logger.debug(f"Row {row_num}: Found asset - source_id: {source_id}, source_uid: {source_uid}, target_uid: {target_uid}, target_id: {target_id}, tags: {tags}")
                    else:
                        logger.warning(f"Row {row_num}: Empty required fields (source_id or target_uid)")
                        logger.warning(f"Row {row_num}: Empty required fields {source_id} {target_id} {tags}")
                else:
                    logger.warning(f"Row {row_num}: Insufficient columns (need at least 5, got {len(row) if row else 0})")
        
        logger.info(f"Read {len(asset_data)} asset records from CSV file: {csv_file}")
        return asset_data
        
    except Exception as e:
        logger.error(f"Error reading CSV file {csv_file}: {e}")
        raise 


def get_source_to_target_asset_id_map(csv_file: str, logger, quiet_mode: bool = False):
    """Load a mapping from source_id to a dict with target_id and target_uid from a CSV file.

    Args:
        csv_file: Path to the CSV file containing asset data
        logger: Logger instance
        quiet_mode: Whether to suppress console output
    Returns:
        dict: Mapping from source_id (str) to dict with keys 'target_id' and 'target_uid'
    """
    import csv
    from pathlib import Path
    try:
        # Check if CSV file exists
        csv_path = Path(csv_file)
        if not csv_path.exists():
            error_msg = f"CSV file does not exist: {csv_file}"
            print(f"❌ {error_msg}")
            print(f"💡 Please run 'transform-and-merge' first to generate the asset-merged-all.csv file")
            if hasattr(globals, 'GLOBAL_OUTPUT_DIR') and globals.GLOBAL_OUTPUT_DIR:
                print(f"   Expected location: {globals.GLOBAL_OUTPUT_DIR}/asset-import/asset-merged-all.csv")
            else:
                print(f"   Expected location: adoc-migration-toolkit-YYYYMMDDHHMM/asset-import/asset-merged-all.csv")
            logger.error(error_msg)
            return None

        # Read CSV data
        asset_data = []
        with open(csv_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # Skip header
            for row in reader:
                if len(row) >= 5:
                    source_id = str(row[0])
                    target_id = str(row[2])
                    target_uid = str(row[3])
                    asset_data.append((source_id, target_id, target_uid))

        if not asset_data:
            print("❌ No valid asset data found in CSV file")
            logger.warning("No valid asset data found in CSV file")
            return None

        # Return as a dict mapping source_id to dict with target_id and target_uid
        return {source_id: {"target_id": target_id, "target_uid": target_uid} for source_id, target_id, target_uid in asset_data}

    except Exception as e:
        error_msg = f"Error in asset-source to target map reading: {e}"
        if not quiet_mode:
            print(f"❌ {error_msg}")
        logger.error(error_msg)
        return None 


def get_thread_names():
    """Return a list of thread names for progress bars."""
    thread_names = [
        "Aryabhata Thread    ",
        "Bhaskara Thread     ",
        "Chandrayaan Thread  ",
        "Mangalyaan Thread   ",
        "INSAT Thread        ",
        "GSAT Thread         ",
        "RISAT Thread        ",
        "Astrosat Thread     ",
        "Oceansat Thread     ",
        "Aditya Thread       ",
        "PSLV Thread         ",
        "GSLV Thread         ",
        "SSLV Thread         ",
        "Antrix Thread       ",
        "HysIS Thread        ",
        "TES Thread          ",
        "Microsat Thread     ",
        "Rocket Thread       ",
        "Lightning Thread    ",
        "Unicorn Thread      ",
        "Dragon Thread       ",
        "Shark Thread        ",
        "Mercury Thread      ",
        "Venus Thread        ",
        "Earth Thread        ",
        "Mars Thread         ",
        "Jupiter Thread      ",
        "Saturn Thread       ",
        "Uranus Thread       ",
        "Neptune Thread      ",
        "Pluto Thread        ",
        "Agni Thread         ",
        "Prithvi Thread      ",
        "BrahMos Thread      ",
        "Nirbhay Thread      ",
        "Akash Thread        ",
        "Trishul Thread      ",
        "Dhanush Thread      ",
        "Nag Thread          ",
        "Helina Thread       ",
        "Shaurya Thread      ",
        "Falcon Thread       ",
        "Thunderbolt Thread  ",
        "Phoenix Thread      ",
        "Tornado Thread      ",
        "Viper Thread        ",
        "Titan Thread        ",
        "Comet Thread        ",
        "Cyclone Thread      ",
        "Predator Thread     ",
        "Blizzard Thread     "
    ]
    return thread_names