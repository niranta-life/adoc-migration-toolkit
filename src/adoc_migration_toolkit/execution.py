"""
Execution functions for guided migration steps.

This module contains the actual execution logic for migration steps,
separated from the CLI to avoid circular imports.
"""

import csv
import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Tuple


def execute_asset_profile_export_guided(
    csv_file: str, 
    client, 
    logger: logging.Logger, 
    output_file: str = None, 
    quiet_mode: bool = True, 
    verbose_mode: bool = False
) -> Tuple[bool, str]:
    """Execute asset profile export for guided migration."""
    try:
        # Read source-env and target-env mappings from CSV file
        env_mappings = read_csv_uids(csv_file, logger)
        
        if not env_mappings:
            logger.warning("No environment mappings found in CSV file")
            return False, "No environment mappings found in CSV file"
        
        # Generate default output file if not provided
        if not output_file:
            output_file = get_output_file_path(csv_file, "asset-profiles-import-ready.csv", category="asset-import")
        
        if verbose_mode:
            print(f"\nProcessing {len(env_mappings)} asset profile exports from CSV file: {csv_file}")
            print(f"Output will be written to: {output_file}")
            print("🔊 VERBOSE MODE - Detailed output including headers and responses")
            print("="*80)
        
        # Open output file for writing
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        successful = 0
        failed = 0
        total_assets_processed = 0
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            
            # Write header
            writer.writerow(['target-env', 'profile_json'])
            
            for i, (source_env, target_env) in enumerate(env_mappings, 1):
                if verbose_mode:
                    print(f"\n[{i}/{len(env_mappings)}] Processing source-env: {source_env}")
                    print(f"Target-env: {target_env}")
                    print("-" * 60)
                else:
                    print(f"Processing [{i}/{len(env_mappings)}] UID: {source_env}")
                
                try:
                    # Step 1: Get asset details by source-env (UID)
                    if verbose_mode:
                        print(f"Getting asset details for UID: {source_env}")
                    
                    asset_response = client.make_api_call(
                        endpoint=f"/catalog-server/api/assets?uid={source_env}",
                        method='GET'
                    )
                    
                    # Step 2: Extract the asset ID
                    if not asset_response or 'data' not in asset_response:
                        error_msg = f"No 'data' field found in asset response for UID: {source_env}"
                        if verbose_mode:
                            print(f"❌ {error_msg}")
                        logger.error(error_msg)
                        failed += 1
                        continue
                    
                    data_array = asset_response['data']
                    if not data_array or len(data_array) == 0:
                        error_msg = f"Empty 'data' array in asset response for UID: {source_env}"
                        if verbose_mode:
                            print(f"❌ {error_msg}")
                        logger.error(error_msg)
                        failed += 1
                        continue
                    
                    first_asset = data_array[0]
                    if 'id' not in first_asset:
                        error_msg = f"No 'id' field found in first asset for UID: {source_env}"
                        if verbose_mode:
                            print(f"❌ {error_msg}")
                        logger.error(error_msg)
                        failed += 1
                        continue
                    
                    asset_id = first_asset['id']
                    if verbose_mode:
                        print(f"Extracted asset ID: {asset_id}")
                    
                    # Step 3: Get profile configuration for the asset
                    if verbose_mode:
                        print(f"Getting profile configuration for asset ID: {asset_id}")
                    
                    profile_response = client.make_api_call(
                        endpoint=f"/catalog-server/api/profile/{asset_id}/config",
                        method='GET'
                    )
                    
                    # Step 4: Write to CSV
                    profile_json = json.dumps(profile_response, ensure_ascii=False)
                    writer.writerow([target_env, profile_json])
                    
                    if verbose_mode:
                        print(f"✅ Written to file: {target_env}")
                    else:
                        print(f"✅ [{i}/{len(env_mappings)}] {source_env}: Profile exported successfully")
                    
                    successful += 1
                    total_assets_processed += 1
                    
                except Exception as e:
                    error_msg = f"Failed to process source-env {source_env}: {e}"
                    if verbose_mode:
                        print(f"❌ {error_msg}")
                    else:
                        print(f"❌ [{i}/{len(env_mappings)}] {source_env}: {error_msg}")
                    logger.error(error_msg)
                    failed += 1
        
        # Print summary
        if verbose_mode:
            print("\n" + "="*80)
            print("ASSET PROFILE EXPORT COMPLETED")
            print("="*80)
            print(f"Output file: {output_file}")
            print(f"Total environment mappings processed: {len(env_mappings)}")
            print(f"Successful: {successful}")
            print(f"Failed: {failed}")
            print(f"Total assets processed: {total_assets_processed}")
            print("="*80)
        else:
            print(f"✅ Asset profile export completed: {successful} successful, {failed} failed")
            print(f"Output written to: {output_file}")
        
        return True, f"Asset profiles exported to {output_file}"
        
    except Exception as e:
        error_msg = f"Error in asset-profile-export: {e}"
        if verbose_mode:
            print(f"❌ {error_msg}")
        logger.error(error_msg)
        return False, error_msg


def read_csv_uids(csv_file: str, logger: logging.Logger) -> list:
    """Read UIDs and target environments from CSV file."""
    uids = []
    
    try:
        with open(csv_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            
            # Skip header row
            header = next(reader, None)
            if header:
                logger.info(f"CSV header: {header}")
            
            # Read source-env and target-env from first and second columns
            for row_num, row in enumerate(reader, start=2):
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


def get_output_file_path(csv_file: str, default_filename: str, custom_output_file: str = None, category: str = None) -> Path:
    """Get the output file path based on global output directory and custom settings.
    
    Args:
        csv_file: Path to the input CSV file
        default_filename: Default filename to use
        custom_output_file: Custom output file path (overrides global directory)
        category: Subdirectory under output dir (e.g., 'policy-export', 'asset-export')
        
    Returns:
        Path: Output file path
    """
    if custom_output_file:
        # Use custom output file if specified
        return Path(custom_output_file)
    
    # Import here to avoid circular imports
    from .cli import GLOBAL_OUTPUT_DIR
    
    if GLOBAL_OUTPUT_DIR:
        base_dir = GLOBAL_OUTPUT_DIR
    else:
        from datetime import datetime
        base_dir = Path.cwd() / f"adoc-migration-toolkit-{datetime.now().strftime('%Y%m%d%H%M')}"
        base_dir.mkdir(parents=True, exist_ok=True)
    
    if category:
        category_dir = base_dir / category
        category_dir.mkdir(parents=True, exist_ok=True)
        return category_dir / default_filename
    else:
        return base_dir / default_filename 