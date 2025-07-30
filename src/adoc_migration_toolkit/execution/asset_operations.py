"""
Asset operations execution functions.

This module contains execution functions for asset-related operations
including profile export/import, config export/import, and list export.
"""

import csv
import json
import logging
import threading
import tempfile
import os
from pathlib import Path
from typing import Dict, Any, List, Tuple
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

from adoc_migration_toolkit.execution.utils import create_progress_bar, read_csv_uids, read_csv_uids_single_column, read_csv_asset_data
from ..shared.file_utils import get_output_file_path
from ..shared import globals
from .utils import get_source_to_target_asset_id_map


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
        
        # Create progress bar using tqdm utility
        progress_bar = create_progress_bar(
            total=len(env_mappings),
            desc="Exporting asset profiles",
            unit="assets",
            disable=verbose_mode
        )
        
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
                    # Update progress bar with current asset UID using set_postfix
                    progress_bar.set_postfix(asset=source_env)
                
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
                    
                    successful += 1
                    total_assets_processed += 1
                    
                except Exception as e:
                    error_msg = f"Failed to process source-env {source_env}: {e}"
                    if verbose_mode:
                        print(f"❌ {error_msg}")
                    logger.error(error_msg)
                    failed += 1
                
                # Update progress bar
                progress_bar.update(1)
        
        # Close progress bar
        progress_bar.close()
        
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


def execute_asset_profile_export(csv_file: str, client, logger: logging.Logger, output_file: str = None, quiet_mode: bool = False, verbose_mode: bool = False):
    """Execute the asset-profile-export command.
    
    Args:
        csv_file: Path to the CSV file containing source-env and target-env mappings
        client: API client instance
        logger: Logger instance
        output_file: Path to output file for writing results
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
    """
    try:
        # Check if CSV file exists
        csv_path = Path(csv_file)
        if not csv_path.exists():
            error_msg = f"CSV file does not exist: {csv_file}"
            print(f"❌ {error_msg}")
            print(f"💡 Please run 'policy-xfr' first to generate the asset_uids.csv file")
            if globals.GLOBAL_OUTPUT_DIR:
                print(f"   Expected location: {globals.GLOBAL_OUTPUT_DIR}/asset-export/asset_uids.csv")
            else:
                print(f"   Expected location: adoc-migration-toolkit-YYYYMMDDHHMM/asset-export/asset_uids.csv")
            logger.error(error_msg)
            return
        
        # Read source-env and target-env mappings from CSV file
        env_mappings = read_csv_uids(csv_file, logger)
        
        if not env_mappings:
            logger.warning("No environment mappings found in CSV file")
            return
        
        # Generate default output file if not provided - use asset-import category
        if not output_file:
            output_file = get_output_file_path(csv_file, "asset-profiles-import-ready.csv", category="asset-import")
        
        if not quiet_mode:
            print(f"\nProcessing {len(env_mappings)} asset profile exports from CSV file: {csv_file}")
            print(f"Output will be written to: {output_file}")
            if globals.GLOBAL_OUTPUT_DIR:
                print(f"Using global output directory: {globals.GLOBAL_OUTPUT_DIR}")
            if verbose_mode:
                print("🔊 VERBOSE MODE - Detailed output including headers and responses")
            print("="*80)
        
        # Open output file for writing
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        successful = 0
        failed = 0
        total_assets_processed = 0
        failed_indices = set()  # Track failed indices for progress bar
        
        # Create progress bar using tqdm utility
        progress_bar = create_progress_bar(
            total=len(env_mappings),
            desc="Exporting asset profiles",
            unit="assets",
            disable=verbose_mode
        )
        
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
                    # Update progress bar with current asset UID using set_postfix
                    progress_bar.set_postfix(asset=source_env)
                
                try:
                    # Step 1: Get asset details by source-env (UID)
                    if verbose_mode:
                        print(f"Getting asset details for UID: {source_env}")
                    
                    # Show headers in verbose mode
                    if verbose_mode:
                        print("\nGET Request Headers:")
                        print(f"  Endpoint: /catalog-server/api/assets?uid={source_env}")
                        print(f"  Method: GET")
                        print(f"  Content-Type: application/json")
                        print(f"  Authorization: Bearer [REDACTED]")
                        if hasattr(client, 'tenant') and client.tenant:
                            print(f"  X-Tenant: {client.tenant}")
                    
                    asset_response = client.make_api_call(
                        endpoint=f"/catalog-server/api/assets?uid={source_env}",
                        method='GET'
                    )
                    
                    # Show response in verbose mode
                    if verbose_mode:
                        print("\nAsset Response:")
                        print(json.dumps(asset_response, indent=2, ensure_ascii=False))
                    
                    # Step 2: Extract the asset ID
                    if not asset_response or 'data' not in asset_response:
                        error_msg = f"No 'data' field found in asset response for UID: {source_env}"
                        if verbose_mode:
                            print(f"❌ {error_msg}")
                        logger.error(error_msg)
                        failed += 1
                        failed_indices.add(i - 1)  # Add to failed indices (0-based)
                        continue
                    
                    data_array = asset_response['data']
                    if not data_array or len(data_array) == 0:
                        error_msg = f"Empty 'data' array in asset response for UID: {source_env}"
                        if verbose_mode:
                            print(f"❌ {error_msg}")
                        logger.error(error_msg)
                        failed += 1
                        failed_indices.add(i - 1)  # Add to failed indices (0-based)
                        continue
                    
                    first_asset = data_array[0]
                    if 'id' not in first_asset:
                        error_msg = f"No 'id' field found in first asset for UID: {source_env}"
                        if verbose_mode:
                            print(f"❌ {error_msg}")
                        logger.error(error_msg)
                        failed += 1
                        failed_indices.add(i - 1)  # Add to failed indices (0-based)
                        continue
                    
                    asset_id = first_asset['id']
                    if verbose_mode:
                        print(f"Extracted asset ID: {asset_id}")
                    
                    # Step 3: Get profile configuration for the asset
                    if verbose_mode:
                        print(f"Getting profile configuration for asset ID: {asset_id}")
                    
                    # Show headers in verbose mode
                    if verbose_mode:
                        print("\nGET Request Headers:")
                        print(f"  Endpoint: /catalog-server/api/profile/{asset_id}/config")
                        print(f"  Method: GET")
                        print(f"  Content-Type: application/json")
                        print(f"  Authorization: Bearer [REDACTED]")
                        if hasattr(client, 'tenant') and client.tenant:
                            print(f"  X-Tenant: {client.tenant}")
                    
                    profile_response = client.make_api_call(
                        endpoint=f"/catalog-server/api/profile/{asset_id}/config",
                        method='GET'
                    )
                    
                    # Show response in verbose mode
                    if verbose_mode:
                        print("\nProfile Response:")
                        print(json.dumps(profile_response, indent=2, ensure_ascii=False))
                    
                    # Step 4: Write to CSV
                    profile_json = json.dumps(profile_response, ensure_ascii=False)
                    writer.writerow([target_env, profile_json])
                    
                    if verbose_mode:
                        print(f"✅ Written to file: {target_env}")
                    
                    successful += 1
                    total_assets_processed += 1
                    
                except Exception as e:
                    error_msg = f"Failed to process source-env {source_env}: {e}"
                    if verbose_mode:
                        print(f"❌ {error_msg}")
                    logger.error(error_msg)
                    failed += 1
                
                # Update progress bar
                progress_bar.update(1)
            
            # Close progress bar
            progress_bar.close()
            
            # Print summary
            if not quiet_mode:
                print("\n" + "="*80)
                print("ASSET PROFILE EXPORT COMPLETED")
                print("="*80)
                if verbose_mode:
                    print("🔊 VERBOSE MODE - Detailed output including headers and responses")
                print(f"Output file: {output_file}")
                print(f"Total mappings processed: {len(env_mappings)}")
                print(f"Successful: {successful}")
                print(f"Failed: {failed}")
                print("="*80)
            else:
                print(f"✅ Asset profile export completed: {successful} successful, {failed} failed")
                print(f"Output written to: {output_file}")
        
    except Exception as e:
        error_msg = f"Error in asset-profile-export: {e}"
        if not quiet_mode:
            print(f"❌ {error_msg}")
        logger.error(error_msg)


def execute_asset_profile_import(csv_file: str, client, logger: logging.Logger, dry_run: bool = False, quiet_mode: bool = True, verbose_mode: bool = False):
    """Execute the asset-profile-import command.
    
    Args:
        csv_file: Path to the CSV file containing target-env and profile_json
        client: API client instance
        logger: Logger instance
        dry_run: Whether to perform a dry run (no actual API calls)
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
    """
    try:
        # Check if CSV file exists
        csv_path = Path(csv_file)
        if not csv_path.exists():
            error_msg = f"CSV file does not exist: {csv_file}"
            if not quiet_mode:
                print(f"❌ {error_msg}")
                print(f"💡 Please run 'asset-profile-export' first to generate the asset-profiles-import-ready.csv file")
                if globals.GLOBAL_OUTPUT_DIR:
                    print(f"   Expected location: {globals.GLOBAL_OUTPUT_DIR}/asset-import/asset-profiles-import-ready.csv")
                else:
                    print(f"   Expected location: adoc-migration-toolkit-YYYYMMDDHHMM/asset-import/asset-profiles-import-ready.csv")
            logger.error(error_msg)
            return
        
        if not quiet_mode:
            print(f"\nProcessing asset profile import from CSV file: {csv_file}")
            if dry_run:
                print("🔍 DRY RUN MODE - No actual API calls will be made")
                print("📋 Will show detailed information about what would be executed")
            if quiet_mode:
                print("🔇 QUIET MODE - Minimal output")
            if verbose_mode:
                print("🔊 VERBOSE MODE - Detailed output including headers and responses")
            print("="*80)
            
            # Show environment information in dry-run mode
            if dry_run:
                print("\n🌍 TARGET ENVIRONMENT INFORMATION:")
                print(f"  Host: {client.host}")
                if hasattr(client, 'target_tenant') and client.target_tenant:
                    print(f"  Target Tenant: {client.target_tenant}")
                else:
                    print(f"  Source Tenant: {client.tenant} (will be used as target)")
                print(f"  Authentication: Target access key and secret key")
                print("="*80)
        
        # Read CSV file
        import_mappings = []
        with open(csv_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            
            if len(header) != 2 or header[0] != 'target-env' or header[1] != 'profile_json':
                error_msg = f"Invalid CSV format. Expected header: ['target-env', 'profile_json'], got: {header}"
                if not quiet_mode:
                    print(f"❌ {error_msg}")
                logger.error(error_msg)
                return
            
            for row_num, row in enumerate(reader, start=2):
                if len(row) != 2:
                    logger.warning(f"Row {row_num}: Expected 2 columns, got {len(row)}")
                    continue
                
                target_env = row[0].strip()
                profile_json = row[1].strip()
                
                if target_env and profile_json:
                    import_mappings.append((target_env, profile_json))
                    logger.debug(f"Row {row_num}: Found target-env: {target_env}")
                else:
                    logger.warning(f"Row {row_num}: Empty target-env or profile_json value")
        
        if not import_mappings:
            logger.warning("No valid import mappings found in CSV file")
            return
        
        logger.info(f"Read {len(import_mappings)} import mappings from CSV file: {csv_file}")
        
        successful = 0
        failed = 0
        
        # Create progress bar using tqdm utility
        progress_bar = create_progress_bar(
            total=len(import_mappings),
            desc="Importing asset profiles",
            unit="assets",
            disable=verbose_mode
        )
        
        for i, (target_env, profile_json) in enumerate(import_mappings, 1):
            # Update progress bar with current asset UID using set_postfix
            progress_bar.set_postfix(asset=target_env)
            if not quiet_mode:
                print(f"\n[{i}/{len(import_mappings)}] Processing target-env: {target_env}")
                print("-" * 60)
            
            try:
                # Step 1: Get asset details by target-env (UID)
                if not quiet_mode:
                    print(f"Getting asset details for UID: {target_env}")
                
                if not dry_run:
                    asset_response = client.make_api_call(
                        endpoint=f"/catalog-server/api/assets?uid={target_env}",
                        method='GET',
                        use_target_auth=True,
                        use_target_tenant=True
                    )
                else:
                    # Mock response for dry run
                    asset_response = {
                        "data": [
                            {
                                "id": 12345,
                                "name": "MOCK_ASSET",
                                "uid": target_env
                            }
                        ]
                    }
                    
                    # Show detailed dry-run information for first API call
                    if not quiet_mode:
                        print(f"\n🔍 DRY RUN - API CALL #1: Get Asset Details")
                        print(f"  Method: GET")
                        print(f"  Endpoint: /catalog-server/api/assets?uid={target_env}")
                        print(f"  Headers:")
                        print(f"    Content-Type: application/json")
                        print(f"    Authorization: Bearer [REDACTED]")
                        if hasattr(client, 'target_tenant') and client.target_tenant:
                            print(f"    X-Tenant: {client.target_tenant}")
                        else:
                            print(f"    X-Tenant: {client.tenant}")
                        print(f"  Expected Response: Asset details with ID field")
                        print(f"  Mock Response: {json.dumps(asset_response, indent=2, ensure_ascii=False)}")
                
                # Show response in verbose mode (only for non-dry-run)
                if verbose_mode and not dry_run:
                    print("\nAsset Response:")
                    print(json.dumps(asset_response, indent=2, ensure_ascii=False))
                
                # Step 2: Extract the asset ID
                if not asset_response or 'data' not in asset_response:
                    error_msg = f"No 'data' field found in asset response for UID: {target_env}"
                    if not quiet_mode:
                        print(f"❌ [{i}/{len(import_mappings)}] {target_env}: {error_msg}")
                    logger.error(error_msg)
                    failed += 1
                    progress_bar.update(1)
                    continue
                
                data_array = asset_response['data']
                if not data_array or len(data_array) == 0:
                    error_msg = f"Empty 'data' array in asset response for UID: {target_env}"
                    if not quiet_mode:
                        print(f"❌ [{i}/{len(import_mappings)}] {target_env}: {error_msg}")
                    logger.error(error_msg)
                    failed += 1
                    progress_bar.update(1)
                    continue
                
                first_asset = data_array[0]
                if 'id' not in first_asset:
                    error_msg = f"No 'id' field found in first asset for UID: {target_env}"
                    if not quiet_mode:
                        print(f"❌ [{i}/{len(import_mappings)}] {target_env}: {error_msg}")
                    logger.error(error_msg)
                    failed += 1
                    progress_bar.update(1)
                    continue
                
                asset_id = first_asset['id']
                if not quiet_mode:
                    print(f"Extracted asset ID: {asset_id}")
                
                # Step 3: Parse profile JSON
                try:
                    profile_data = json.loads(profile_json)
                except json.JSONDecodeError as e:
                    error_msg = f"Invalid JSON in profile_json for UID {target_env}: {e}"
                    if not quiet_mode:
                        print(f"❌ [{i}/{len(import_mappings)}] {target_env}: {error_msg}")
                    logger.error(error_msg)
                    failed += 1
                    progress_bar.update(1)
                    continue
                
                # Step 4: Make PUT request to update profile configuration
                if not quiet_mode:
                    print(f"Updating profile configuration for asset ID: {asset_id}")
                
                if not dry_run:
                    # Show headers in verbose mode
                    if verbose_mode:
                        print("\nPUT Request Headers:")
                        print(f"  Endpoint: /catalog-server/api/profile/{asset_id}/config")
                        print(f"  Method: PUT")
                        print(f"  Content-Type: application/json")
                        print(f"  Authorization: Bearer [REDACTED]")
                        if hasattr(client, 'target_tenant') and client.target_tenant:
                            print(f"  X-Tenant: {client.target_tenant}")
                        print(f"  Payload: {json.dumps(profile_data, ensure_ascii=False)}")
                    
                    import_response = client.make_api_call(
                        endpoint=f"/catalog-server/api/profile/{asset_id}/config",
                        method='PUT',
                        json_payload=profile_data,
                        use_target_auth=True,
                        use_target_tenant=True
                    )
                    
                    # Show response in verbose mode
                    if verbose_mode:
                        print("\nImport Response:")
                        print(json.dumps(import_response, indent=2, ensure_ascii=False))
                    
                    if not quiet_mode:
                        print("✅ Import successful")
                else:
                    # Show detailed dry-run information for second API call
                    if not quiet_mode:
                        print(f"\n🔍 DRY RUN - API CALL #2: Update Profile Configuration")
                        print(f"  Method: PUT")
                        print(f"  Endpoint: /catalog-server/api/profile/{asset_id}/config")
                        print(f"  Headers:")
                        print(f"    Content-Type: application/json")
                        print(f"    Authorization: Bearer [REDACTED]")
                        if hasattr(client, 'target_tenant') and client.target_tenant:
                            print(f"    X-Tenant: {client.target_tenant}")
                        else:
                            print(f"    X-Tenant: {client.tenant}")
                        print(f"  Payload:")
                        print(json.dumps(profile_data, indent=4, ensure_ascii=False))
                        print(f"  Expected Action: Update profile configuration for asset {asset_id}")
                        print(f"  Status: Would be executed in live mode")
                    else:
                        print(f"🔍 [{i}/{len(import_mappings)}] {target_env}: Would update profile (dry-run)")
                
                successful += 1
                progress_bar.update(1)
                logger.info(f"Successfully processed target-env {target_env} (asset ID: {asset_id})")
                
            except Exception as e:
                error_msg = f"Failed to process UID {target_env}: {e}"
                if not quiet_mode:
                    print(f"❌ [{i}/{len(import_mappings)}] {target_env}: {error_msg}")
                logger.error(error_msg)
                failed += 1
                progress_bar.update(1)
        
        # Close progress bar
        progress_bar.close()
        
        # Print summary
        if not quiet_mode:
            print("\n" + "="*80)
            print("ASSET PROFILE IMPORT COMPLETED")
            print("="*80)
            if dry_run:
                print("🔍 DRY RUN MODE - No actual changes were made")
            print(f"Total mappings processed: {len(import_mappings)}")
            print(f"Successful: {successful}")
            print(f"Failed: {failed}")
            print("="*80)
        else:
            print(f"✅ Asset profile import completed: {successful} successful, {failed} failed")
            if dry_run:
                print("🔍 DRY RUN MODE - No actual changes were made")
        
    except Exception as e:
        error_msg = f"Error in asset-profile-import: {e}"
        if not quiet_mode:
            print(f"❌ {error_msg}")
        logger.error(error_msg)


def execute_asset_config_export(csv_file: str, client, logger: logging.Logger, output_file: str = None, quiet_mode: bool = False, verbose_mode: bool = False):
    """Execute the asset-config-export command.
    
    Args:
        csv_file: Path to the CSV file containing asset data with 5 columns: source_id, source_uid, target_id, target_uid, tags
        client: API client instance
        logger: Logger instance
        output_file: Path to output file for writing results
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
    """
    try:
        # Read asset data from CSV file with 4 columns
        asset_data = read_csv_asset_data(csv_file, logger)
        
        if not asset_data:
            logger.warning("No asset data found in CSV file")
            return
        
        # Generate default output file if not provided
        if not output_file:
            output_file = get_output_file_path(csv_file, "asset-config-export.csv", category="asset-export")
        
        if not quiet_mode:
            print(f"\nProcessing {len(asset_data)} asset config exports from CSV file: {csv_file}")
            print(f"Output will be written to: {output_file}")
            if globals.GLOBAL_OUTPUT_DIR:
                print(f"Using global output directory: {globals.GLOBAL_OUTPUT_DIR}")
            if verbose_mode:
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
            writer.writerow(['target_uid', 'config_json'])
            
            for i, asset in enumerate(asset_data, 1):
                source_uid = asset['source_uid']
                source_id = asset['source_id']
                target_uid = asset['target_uid']
                
                if not quiet_mode:
                    print(f"\n[{i}/{len(asset_data)}] Processing asset - source_uid: {source_uid}, source_id: {source_id}")
                    print("-" * 60)
                else:
                    print(f"Processing [{i}/{len(asset_data)}] source_id: {source_id}")
                
                try:
                    # Step 1: Get asset configuration using source_id directly
                    if not quiet_mode:
                        print(f"Getting asset configuration for ID: {source_id}")
                    
                    # Show headers in verbose mode
                    if verbose_mode:
                        print("\nGET Request Headers:")
                        print(f"  Endpoint: /catalog-server/api/assets/{source_id}/config")
                        print(f"  Method: GET")
                        print(f"  Content-Type: application/json")
                        print(f"  Authorization: Bearer [REDACTED]")
                        if hasattr(client, 'tenant') and client.tenant:
                            print(f"  X-Tenant: {client.tenant}")
                    
                    config_response = client.make_api_call(
                        endpoint=f"/catalog-server/api/assets/{source_id}/config",
                        method='GET'
                    )
                    
                    # Show response in verbose mode
                    if verbose_mode:
                        print("\nConfig Response:")
                        print(json.dumps(config_response, indent=2, ensure_ascii=False))
                    
                    # Step 2: Write compressed JSON response to CSV
                    if not quiet_mode:
                        print(f"Writing asset configuration to CSV")
                    
                    # Write the compressed JSON response to CSV with target_uid
                    config_json = json.dumps(config_response, ensure_ascii=False, separators=(',', ':'))
                    writer.writerow([target_uid, config_json])
                    
                    if not quiet_mode:
                        print(f"✅ Written to file: {target_uid}")
                        if not verbose_mode:  # Only show response if not in verbose mode (to avoid duplication)
                            print("Config Response:")
                            print(json.dumps(config_response, indent=2, ensure_ascii=False))
                    else:
                        print(f"✅ [{i}/{len(asset_data)}] {target_uid}: Config exported successfully")
                    
                    successful += 1
                    total_assets_processed += 1
                    
                except Exception as e:
                    error_msg = f"Error processing source_id {source_id}: {e}"
                    if not quiet_mode:
                        print(f"❌ {error_msg}")
                    logger.error(error_msg)
                    failed += 1
                    continue
        
        # Verify the CSV file can be read correctly
        if not quiet_mode:
            print("\nVerifying CSV file can be read correctly...")
        
        try:
            with open(output_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader)
                row_count = 0
                validation_errors = []
                
                # Validate header
                if len(header) != 2:
                    validation_errors.append(f"Invalid header: expected 2 columns, got {len(header)}")
                elif header[0] != 'target_uid' or header[1] != 'config_json':
                    validation_errors.append(f"Invalid header: expected ['target_uid', 'config_json'], got {header}")
                
                # Validate each row
                for row_num, row in enumerate(reader, start=2):
                    row_count += 1
                    
                    # Check column count
                    if len(row) != 2:
                        validation_errors.append(f"Row {row_num}: Expected 2 columns, got {len(row)}")
                        continue
                    
                    target_uid, config_json_str = row
                    
                    # Check for empty values
                    if not target_uid.strip():
                        validation_errors.append(f"Row {row_num}: Empty target_uid value")
                    
                    if not config_json_str.strip():
                        validation_errors.append(f"Row {row_num}: Empty config_json value")
                        continue
                    
                    # Verify JSON is parsable
                    try:
                        config_data = json.loads(config_json_str)
                        
                        # Additional validation: check if it's a valid config response
                        if not isinstance(config_data, dict):
                            validation_errors.append(f"Row {row_num}: config_json is not a valid JSON object")
                        elif not config_data:  # Empty object
                            validation_errors.append(f"Row {row_num}: config_json is empty")
                        
                    except json.JSONDecodeError as e:
                        validation_errors.append(f"Row {row_num}: Invalid JSON in config_json - {e}")
                    except Exception as e:
                        validation_errors.append(f"Row {row_num}: Error parsing config_json - {e}")
                
                # Report validation results
                if not quiet_mode:
                    if validation_errors:
                        print(f"❌ CSV validation failed with {len(validation_errors)} errors:")
                        for error in validation_errors[:10]:  # Show first 10 errors
                            print(f"   - {error}")
                        if len(validation_errors) > 10:
                            print(f"   ... and {len(validation_errors) - 10} more errors")
                        logger.error(f"CSV validation failed: {len(validation_errors)} errors found")
                    else:
                        print(f"✅ CSV validation successful: {row_count} data rows read")
                        print(f"   Header: {header}")
                        print(f"   Expected columns: target_uid, config_json")
                        print(f"   All JSON entries are valid and parseable")
                        logger.info(f"CSV validation successful: {row_count} rows validated")
                
        except FileNotFoundError:
            error_msg = f"Output file not found: {output_path}"
            if not quiet_mode:
                print(f"❌ {error_msg}")
            logger.error(error_msg)
        except PermissionError:
            error_msg = f"Permission denied reading output file: {output_path}"
            if not quiet_mode:
                print(f"❌ {error_msg}")
            logger.error(error_msg)
        except Exception as e:
            error_msg = f"CSV verification failed: {e}"
            if not quiet_mode:
                print(f"❌ {error_msg}")
            logger.error(error_msg)
        
        # Print summary
        if not quiet_mode:
            print("\n" + "="*80)
            print("ASSET CONFIG EXPORT COMPLETED")
            print("="*80)
            print(f"Output file: {output_file}")
            print(f"Total assets processed: {len(asset_data)}")
            print(f"Successful: {successful}")
            print(f"Failed: {failed}")
            print(f"Total assets processed: {total_assets_processed}")
            print("="*80)
        else:
            print(f"✅ Asset config export completed: {successful} successful, {failed} failed")
            print(f"Output written to: {output_file}")
        
    except Exception as e:
        error_msg = f"Error in asset-config-export: {e}"
        if not quiet_mode:
            print(f"❌ {error_msg}")
        logger.error(error_msg)


def execute_asset_config_import(csv_file: str, client, logger: logging.Logger, quiet_mode: bool = False, verbose_mode: bool = False, parallel_mode: bool = False, dry_run: bool = False, max_threads: int = 1):
    """Execute the asset-config-import command.
    
    Args:
        csv_file: Path to the CSV file containing target_uid and config_json
        client: API client instance
        logger: Logger instance
        quiet_mode: Whether to show progress bars
        verbose_mode: Whether to enable verbose logging
        parallel_mode: Whether to use parallel processing
        dry_run: If True, print the request and payload instead of making the API call
    """
    if parallel_mode:
        execute_asset_config_import_parallel(csv_file, client, logger, quiet_mode, verbose_mode, dry_run, max_threads)
        return
    
    try:
        # Check if CSV file exists
        csv_path = Path(csv_file)
        if not csv_path.exists():
            error_msg = f"CSV file does not exist: {csv_file}"
            print(f"❌ {error_msg}")
            print(f"💡 Please run 'policy-xfr' first to generate the asset-config-import-ready.csv file")
            if globals.GLOBAL_OUTPUT_DIR:
                print(f"   Expected location: {globals.GLOBAL_OUTPUT_DIR}/asset-import/asset-config-import-ready.csv")
            else:
                print(f"   Expected location: adoc-migration-toolkit-YYYYMMDDHHMM/asset-import/asset-config-import-ready.csv")
            logger.error(error_msg)
            return
        
        # Read CSV data
        asset_data = []
        with open(csv_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            # Skip header row if it exists
            header = next(reader, None)
            if header and len(header) >= 2:
                if 'target_uid' in header[0].lower() or 'config_json' in header[1].lower():
                    pass  # This is a header row, skip
                else:
                    # This might be data, add it back
                    if header[0].strip() and header[1].strip():
                        asset_data.append({
                            'target_uid': header[0].strip(),
                            'config_json': header[1].strip()
                        })
            for row in reader:
                if len(row) >= 2:  # Ensure we have at least target_uid and config_json
                    target_uid = row[0].strip()
                    config_json = row[1].strip()
                    if target_uid and config_json:  # Skip empty rows
                        asset_data.append({
                            'target_uid': target_uid,
                            'config_json': config_json
                        })
        
        if not asset_data:
            print("❌ No valid asset data found in CSV file")
            logger.warning("No valid asset data found in CSV file")
            return
        
        if not quiet_mode:
            print(f"📊 Found {len(asset_data)} assets to process")
        
        # Create progress bar if in quiet mode
        if quiet_mode and not verbose_mode:
            pbar = tqdm(total=len(asset_data), desc="Processing assets", colour='green')
        
        successful = 0
        failed = 0
        failed_assets = []
        
        for i, asset in enumerate(asset_data):
            target_uid = asset['target_uid']
            config_json = asset['config_json']
            
            try:
                # Step 1: Get asset ID from target_uid
                if verbose_mode:
                    print(f"\n🔍 Processing asset {i+1}/{len(asset_data)}: {target_uid}")
                    print(f"   GET /catalog-server/api/assets?uid={target_uid}")
                
                # Make GET request to get asset ID
                response = client.make_api_call(
                    endpoint=f'/catalog-server/api/assets?uid={target_uid}',
                    method='GET',
                    use_target_auth=True,
                    use_target_tenant=True
                )
                
                if verbose_mode:
                    print(f"   Response: {response}")
                
                if not response or 'data' not in response or not response['data']:
                    error_msg = f"No asset found for UID: {target_uid}"
                    if verbose_mode:
                        print(f"   ❌ {error_msg}")
                    failed += 1
                    failed_assets.append({'target_uid': target_uid, 'error': error_msg})
                    continue
                
                # Extract asset ID
                asset_id = response['data'][0]['id']
                
                if verbose_mode:
                    print(f"   Asset ID: {asset_id}")
                    print(f"   PUT /catalog-server/api/assets/{asset_id}/config")
                    print(f"   Data: {config_json}")
                
                # Step 2: Transform and update asset configuration
                config_data = json.loads(config_json)
                transformed_config = transform_config_json_to_asset_configuration(config_data, asset_id)
                
                if dry_run:
                    print(f"\n[DRY RUN] Would send PUT to /catalog-server/api/assets/{asset_id}/config")
                    print(f"[DRY RUN] Payload:")
                    print(json.dumps(transformed_config, indent=2))
                    successful += 1
                    continue
                
                config_response = client.make_api_call(
                    endpoint=f'/catalog-server/api/assets/{asset_id}/config',
                    method='PUT',
                    json_payload=transformed_config,
                    use_target_auth=True,
                    use_target_tenant=True
                )
                
                if verbose_mode:
                    print(f"   Config Response: {config_response}")
                
                if config_response:
                    successful += 1
                    if verbose_mode:
                        print(f"   ✅ Successfully updated config for {target_uid}")
                else:
                    error_msg = f"Failed to update config for asset ID: {asset_id}"
                    if verbose_mode:
                        print(f"   ❌ {error_msg}")
                    failed += 1
                    failed_assets.append({'target_uid': target_uid, 'error': error_msg})
            
            except Exception as e:
                error_msg = f"Error processing {target_uid}: {str(e)}"
                if verbose_mode:
                    print(f"   ❌ {error_msg}")
                failed += 1
                failed_assets.append({'target_uid': target_uid, 'error': error_msg})
            
            # Update progress bar
            if quiet_mode and not verbose_mode:
                pbar.update(1)
        
        # Close progress bar
        if quiet_mode and not verbose_mode:
            pbar.close()
        
        # Print summary
        print("\n" + "="*60)
        print("ASSET CONFIG IMPORT SUMMARY")
        print("="*60)
        print(f"Total assets processed: {len(asset_data)}")
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")
        
        if failed > 0:
            print(f"\nFailed assets:")
            for failed_asset in failed_assets:
                print(f"  - {failed_asset['target_uid']}: {failed_asset['error']}")
        
        print("="*60)
        
        if failed == 0:
            print("✅ Asset config import completed successfully!")
        else:
            print(f"⚠️  Asset config import completed with {failed} failures. Check the details above.")
        
    except Exception as e:
        error_msg = f"Error executing asset config import: {e}"
        if not quiet_mode:
            print(f"❌ {error_msg}")
        logger.error(error_msg)


def execute_asset_list_export(client, logger: logging.Logger, source_type_ids: str = None, asset_type_ids: str = None, assembly_ids: str = None, quiet_mode: bool = False, verbose_mode: bool = False, use_target: bool = False, page_size: int = 500):
    """Execute the asset-list-export command.
    
    Args:
        client: API client instance
        logger: Logger instance
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
        use_target: Whether to use target environment instead of source
        page_size: Number of assets per page (default: 500)
    """
    try:
        # Determine output file path based on environment
        if use_target:
            if globals.GLOBAL_OUTPUT_DIR:
                output_file = globals.GLOBAL_OUTPUT_DIR / "asset-export" / "asset-all-target-export.csv"
            else:
                output_file = Path("asset-all-target-export.csv")
            env_type = "TARGET"
        else:
            if globals.GLOBAL_OUTPUT_DIR:
                output_file = globals.GLOBAL_OUTPUT_DIR / "asset-export" / "asset-all-source-export.csv"
            else:
                output_file = Path("asset-all-source-export.csv")
            env_type = "SOURCE"
        
        if not quiet_mode:
            print(f"\nExporting all assets from ADOC {env_type} environment")
            print(f"Environment: {env_type}")
            if use_target:
                tenant = getattr(client, 'target_tenant', getattr(client, 'tenant', 'N/A'))
            else:
                tenant = getattr(client, 'tenant', 'N/A')
            print(f"Host: {client._build_host_url(use_target_tenant=use_target)}")
            print(f"Tenant: {tenant}")
            print(f"Output will be written to: {output_file}")
            if globals.GLOBAL_OUTPUT_DIR:
                print(f"Using global output directory: {globals.GLOBAL_OUTPUT_DIR}")
            if verbose_mode:
                print("🔊 VERBOSE MODE - Detailed output including headers and responses")
            print("="*80)

        query_params = [
            f"size=0",
            f"page=0",
            f"parents=true"
        ]

        if asset_type_ids not in [None, 'None', 'null', '']:
            query_params.append(f"asset_type_ids={asset_type_ids}")

        if source_type_ids not in [None, 'None', 'null', '']:
            query_params.append(f"source_type_ids={source_type_ids}")

        if assembly_ids not in [None, 'None', 'null', '']:
            query_params.append(f"assembly_ids={assembly_ids}")

        query_string = "&".join(query_params)
        end_point = f"/catalog-server/api/assets/discover?{query_string}"

        # Step 1: Get total count of assets
        if not quiet_mode:
            print("Getting total asset count...")
        
        if verbose_mode:
            print("\nGET Request Headers:")
            print(f"  Endpoint: {end_point}")
            print(f"  Method: GET")
            print(f"  Content-Type: application/json")
            print(f"  Authorization: Bearer [REDACTED]")
            if hasattr(client, 'tenant') and client.tenant:
                print(f"  X-Tenant: {client.tenant}")
        
        count_response = client.make_api_call(
            endpoint=f"{end_point}",
            method='GET',
            use_target_auth=use_target,
            use_target_tenant=use_target
        )
        
        if verbose_mode:
            print("\nCount Response:")
            print(json.dumps(count_response, indent=2, ensure_ascii=False))
        
        # Extract total count
        if not count_response or 'meta' not in count_response or 'count' not in count_response['meta']:
            error_msg = "Failed to get total asset count from response"
            print(f"❌ {error_msg}")
            logger.error(error_msg)
            return
        
        total_count = count_response['meta']['count']
        total_pages = (total_count + page_size - 1) // page_size  # Ceiling division
        
        if not quiet_mode:
            print(f"Total assets found: {total_count}")
            print(f"Page size: {page_size}")
            print(f"Total pages to retrieve: {total_pages}")
            print("="*80)
        
        # Step 2: Retrieve all pages and collect assets
        all_assets = []
        successful_pages = 0
        failed_pages = 0
        
        for page in range(total_pages):
            if not quiet_mode:
                print(f"\n[Page {page + 1}/{total_pages}] Retrieving assets...")
            
            try:
                query_params = [
                    f"size={page_size}",
                    f"page={page}",
                    f"parents=true"
                ]

                if asset_type_ids not in [None, 'None', 'null', '']:
                    query_params.append(f"asset_type_ids={asset_type_ids}")

                if source_type_ids not in [None, 'None', 'null', '']:
                    query_params.append(f"source_type_ids={source_type_ids}")

                if assembly_ids not in [None, 'None', 'null', '']:
                    query_params.append(f"assembly_ids={assembly_ids}")

                query_string = "&".join(query_params)
                end_point_per_page = f"/catalog-server/api/assets/discover?{query_string}"

                if verbose_mode:
                    print(f"\nGET Request Headers:")
                    print(f"  Endpoint: {end_point_per_page}")
                    print(f"  Method: GET")
                    print(f"  Content-Type: application/json")
                    print(f"  Authorization: Bearer [REDACTED]")
                    if hasattr(client, 'tenant') and client.tenant:
                        print(f"  X-Tenant: {client.tenant}")
                
                page_response = client.make_api_call(
                    endpoint=f"{end_point_per_page}",
                    method='GET',
                    use_target_auth=use_target,
                    use_target_tenant=use_target
                )
                
                if verbose_mode:
                    print(f"\nPage {page + 1} Response:")
                    print(json.dumps(page_response, indent=2, ensure_ascii=False))
                
                # Extract assets from response
                if page_response and 'data' in page_response and 'assets' in page_response['data']:
                    page_assets = page_response['data']['assets']
                    # Store the full asset wrapper to preserve tags
                    all_assets.extend(page_assets)
                    
                    if not quiet_mode:
                        print(f"✅ Page {page + 1}: Retrieved {len(page_assets)} assets")
                    else:
                        print(f"✅ Page {page + 1}/{total_pages}: {len(page_assets)} assets")
                    
                    successful_pages += 1
                else:
                    # Debug: Let's see what the actual response structure looks like
                    if not quiet_mode:
                        print(f"❌ Page {page + 1}: Unexpected response structure")
                        print("Response keys:", list(page_response.keys()) if page_response else "No response")
                        if page_response and 'data' in page_response:
                            print("Data keys:", list(page_response['data'].keys()))
                        if page_response:
                            print("Sample response structure:")
                            print(json.dumps(page_response, indent=2, ensure_ascii=False)[:1000] + "...")
                    
                    # Try alternative response structures
                    assets_found = False
                    if page_response and 'data' in page_response:
                        # Try different possible locations for assets
                        possible_asset_locations = ['assets', 'asset', 'items', 'results']
                        for location in possible_asset_locations:
                            if location in page_response['data']:
                                page_assets = page_response['data'][location]
                                if isinstance(page_assets, list):
                                    # Store the full asset wrapper to preserve tags
                                    all_assets.extend(page_assets)
                                    if not quiet_mode:
                                        print(f"✅ Page {page + 1}: Found {len(page_assets)} assets in 'data.{location}'")
                                    else:
                                        print(f"✅ Page {page + 1}/{total_pages}: {len(page_assets)} assets")
                                    successful_pages += 1
                                    assets_found = True
                                    break
                    
                    if not assets_found:
                        error_msg = f"Invalid response format for page {page + 1} - no assets found"
                        if not quiet_mode:
                            print(f"❌ {error_msg}")
                        logger.error(error_msg)
                        failed_pages += 1
                    
            except Exception as e:
                error_msg = f"Failed to retrieve page {page + 1}: {e}"
                if not quiet_mode:
                    print(f"❌ {error_msg}")
                logger.error(error_msg)
                failed_pages += 1
        
        # Step 3: Write assets to CSV file
        if not quiet_mode:
            print(f"\nWriting {len(all_assets)} assets to CSV file...")
        
        # Create output directory if needed
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            
            # Write header: source_uid, source_id, target_uid, tags
            writer.writerow(['source_uid', 'source_id', 'target_uid', 'tags'])
            
            # Write asset data
            for asset_wrapper in all_assets:
                # Extract asset information from the wrapper structure
                if 'asset' in asset_wrapper:
                    asset = asset_wrapper['asset']
                else:
                    # Fallback: if no 'asset' wrapper, use the object directly
                    asset = asset_wrapper
                
                # Extract required fields
                asset_id = asset.get('id', '')
                asset_uid = asset.get('uid', '')
                
                # Extract tags and concatenate with colon separator
                tags = []
                if 'tags' in asset_wrapper and asset_wrapper['tags']:
                    for tag in asset_wrapper['tags']:
                        if 'name' in tag:
                            # Check if autoTagged is true and skip if so
                            if tag.get('autoTagged', False):
                                continue
                            tags.append(tag['name'])
                
                tags_str = ':'.join(tags) if tags else ''
                
                # Write row: source_uid (asset.uid), source_id (asset.id), target_uid (asset.uid), tags
                writer.writerow([asset_uid, asset_id, asset_uid, tags_str])
        
        # Step 4: Sort the CSV file by source_uid, then source_id
        if not quiet_mode:
            print("Sorting CSV file by source_uid, then source_id...")
        
        # Read all rows
        rows = []
        with open(output_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # Skip header
            rows = list(reader)
        
        # Sort rows: first by source_uid, then by source_id
        def sort_key(row):
            source_uid = row[0] if len(row) > 0 else ''
            source_id = row[1] if len(row) > 1 else ''
            # Convert source_id to int for proper numeric sorting, fallback to string
            try:
                source_id_int = int(source_id) if source_id else 0
            except (ValueError, TypeError):
                source_id_int = 0
            return (source_uid, source_id_int)
        
        rows.sort(key=sort_key)
        
        # Write sorted data back to file
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(header)
            writer.writerows(rows)
        
        # Step 5: Print statistics
        if not quiet_mode:
            print("\n" + "="*80)
            print("ASSET LIST EXPORT COMPLETED")
            print("="*80)
            print(f"Environment: {env_type}")
            print(f"Output file: {output_file}")
            print(f"Total assets exported: {len(all_assets)}")
            print(f"Successful pages: {successful_pages}")
            print(f"Failed pages: {failed_pages}")
            print(f"Total pages processed: {total_pages}")
            
            # Additional statistics
            print(f"\nAsset Statistics:")
            print(f"  Total assets exported: {len(all_assets)}")
            print("="*80)
        else:
            print(f"✅ Asset list export completed: {len(all_assets)} assets exported to {output_file}")
        
    except Exception as e:
        error_msg = f"Error in asset-list-export: {e}"
        if not quiet_mode:
            print(f"❌ {error_msg}")
        logger.error(error_msg)


def execute_asset_list_export_parallel(client, logger: logging.Logger, source_type_ids: str = None, asset_type_ids: str = None, assembly_ids: str = None, quiet_mode: bool = False, verbose_mode: bool = False, use_target: bool = False, page_size: int = 500, max_threads: int = 5):
    """Execute the asset-list-export command with parallel processing.
    Args:
        client: API client instance
        logger: Logger instance
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
        use_target: Whether to use target environment instead of source
        page_size: Number of assets per page (default: 500)
        max_threads: Maximum number of threads to use (default: 5)
    """
    try:
        # Determine output file path based on environment
        if use_target:
            if globals.GLOBAL_OUTPUT_DIR:
                output_file = globals.GLOBAL_OUTPUT_DIR / "asset-export" / "asset-all-target-export.csv"
            else:
                output_file = Path("asset-all-target-export.csv")
            env_type = "TARGET"
        else:
            if globals.GLOBAL_OUTPUT_DIR:
                output_file = globals.GLOBAL_OUTPUT_DIR / "asset-export" / "asset-all-source-export.csv"
            else:
                output_file = Path("asset-all-source-export.csv")
            env_type = "SOURCE"
        
        if not quiet_mode:
            print(f"\nExporting all assets from ADOC {env_type} environment (Parallel Mode)")
            print(f"Environment: {env_type}")
            if use_target:
                tenant = getattr(client, 'target_tenant', getattr(client, 'tenant', 'N/A'))
            else:
                tenant = getattr(client, 'tenant', 'N/A')
            print(f"Host: {client._build_host_url(use_target_tenant=use_target)}")
            print(f"Tenant: {tenant}")
            print(f"Output will be written to: {output_file}")
            if globals.GLOBAL_OUTPUT_DIR:
                print(f"Using global output directory: {globals.GLOBAL_OUTPUT_DIR}")
            if verbose_mode:
                print("🔊 VERBOSE MODE - Detailed output including headers and responses")
            print("="*80)


        query_params = [
            f"size=0",
            f"page=0",
            f"parents=true"
        ]

        if asset_type_ids not in [None, 'None', 'null', '']:
            query_params.append(f"asset_type_ids={asset_type_ids}")

        if source_type_ids not in [None, 'None', 'null', '']:
            query_params.append(f"source_type_ids={source_type_ids}")

        if assembly_ids not in [None, 'None', 'null', '']:
            query_params.append(f"assembly_ids={assembly_ids}")

        query_string = "&".join(query_params)
        end_point = f"/catalog-server/api/assets/discover?{query_string}"

        # Step 1: Get total count of assets
        if not quiet_mode:
            print("Getting total asset count...")
        
        if verbose_mode:
            print("\nGET Request Headers:")
            print(f"  Endpoint: {end_point}")
            print(f"  Method: GET")
            print(f"  Content-Type: application/json")
            print(f"  Authorization: Bearer [REDACTED]")
            if hasattr(client, 'tenant') and client.tenant:
                print(f"  X-Tenant: {client.tenant}")
        
        count_response = client.make_api_call(
            endpoint=f"{end_point}",
            method='GET',
            use_target_auth=use_target,
            use_target_tenant=use_target
        )
        
        if verbose_mode:
            print("\nCount Response:")
            print(json.dumps(count_response, indent=2, ensure_ascii=False))
        
        # Extract total count
        if not count_response or 'meta' not in count_response or 'count' not in count_response['meta']:
            error_msg = "Failed to get total asset count from response"
            print(f"❌ {error_msg}")
            logger.error(error_msg)
            return
        
        total_count = count_response['meta']['count']
        total_pages = (total_count + page_size - 1) // page_size  # Ceiling division
        
        if not quiet_mode:
            print(f"{end_point}")
            print(f"Total assets found: {total_count}")
            print(f"Page size: {page_size}")
            print(f"Total pages to retrieve: {total_pages}")
        
        # Calculate thread configuration
        min_pages_per_thread = 2
        if total_pages < min_pages_per_thread:
            num_threads = 1
            pages_per_thread = total_pages
        else:
            num_threads = min(max_threads, (total_pages + min_pages_per_thread - 1) // min_pages_per_thread)
            pages_per_thread = (total_pages + num_threads - 1) // num_threads
        
        if not quiet_mode:
            print(f"Using {num_threads} threads to process {total_pages} pages")
            print(f"Pages per thread: {pages_per_thread}")
            print("="*80)
        
        # Process pages in parallel
        thread_results = []
        temp_files = []
        
        # Funny thread names for progress indicators (all same length)
        thread_names = [
            "Rocket Thread     ",
            "Lightning Thread  ", 
            "Unicorn Thread    ",
            "Dragon Thread     ",
            "Shark Thread      "
        ]
        
        def process_page_chunk(thread_id, start_page, end_page):
            """Process a chunk of pages for a specific thread."""
            # Create a thread-local client instance
            thread_client = type(client)(
                host=client.host,
                access_key=client.access_key,
                secret_key=client.secret_key,
                tenant=getattr(client, 'tenant', None)
            )
            # Copy target credentials to thread client
            thread_client.target_access_key = getattr(client, 'target_access_key', None)
            thread_client.target_secret_key = getattr(client, 'target_secret_key', None)
            thread_client.target_tenant = getattr(client, 'target_tenant', None)
            # Copy host template for tenant substitution
            thread_client.host_template = getattr(client, 'host_template', None)
            
            # Create temporary file for this thread
            temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8')
            print(f"Writing to temp file - {temp_file.name}")
            temp_files.append(temp_file.name)
            
            # Create progress bar for this thread with green color
            progress_bar = create_progress_bar(
                total=end_page - start_page,
                desc=thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}",
                unit="",
                disable=quiet_mode,
                position=thread_id,
                leave=False
            )
            
            successful_pages = 0
            failed_pages = 0
            total_assets = 0
            
            # Process each page in this thread's range
            for page in range(start_page, end_page):
                try:
                    query_params = [
                        f"size={page_size}",
                        f"page={page}",
                        f"parents=true"
                    ]

                    if asset_type_ids not in [None, 'None', 'null', '']:
                        query_params.append(f"asset_type_ids={asset_type_ids}")

                    if source_type_ids not in [None, 'None', 'null', '']:
                        query_params.append(f"source_type_ids={source_type_ids}")

                    if assembly_ids not in [None, 'None', 'null', '']:
                        query_params.append(f"assembly_ids={assembly_ids}")

                    query_string = "&".join(query_params)
                    print(f"Query::: {query_string}")
                    end_point_per_page = f"/catalog-server/api/assets/discover?{query_string}"
                    if verbose_mode:
                        thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                        print(f"\n{thread_name} - Processing page {page + 1}")
                        print(f"GET Request Headers:")
                        print(f"  Endpoint: {end_point_per_page}")
                        print(f"  Method: GET")
                        print(f"  Content-Type: application/json")
                        print(f"  Authorization: Bearer [REDACTED]")
                        if hasattr(thread_client, 'tenant') and thread_client.tenant:
                            print(f"  X-Tenant: {thread_client.tenant}")
                    page_response = thread_client.make_api_call(
                        endpoint=f"{end_point_per_page}",
                        method='GET',
                        use_target_auth=use_target,
                        use_target_tenant=use_target
                    )
                    if verbose_mode:
                        thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                        print(f"\n{thread_name} - Page {page + 1} Response:")
                        print(json.dumps(page_response, indent=2, ensure_ascii=False))
                    
                    # Extract assets from response
                    if page_response and 'data' in page_response and 'assets' in page_response['data']:
                        page_assets = page_response['data']['assets']
                        
                        # Write assets to temporary CSV file
                        with open(temp_file.name, 'a', newline='', encoding='utf-8') as f:
                            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                            
                            # Write asset data
                            for asset_wrapper in page_assets:
                                # Extract asset information from the wrapper structure
                                if 'asset' in asset_wrapper:
                                    asset = asset_wrapper['asset']
                                else:
                                    # Fallback: if no 'asset' wrapper, use the object directly
                                    asset = asset_wrapper
                                
                                # Extract required fields
                                asset_id = asset.get('id', '')
                                asset_uid = asset.get('uid', '')
                                assembly_id = asset.get('assemblyId', '')

                                asset_type_definition = asset.get('assetType')
                                if asset_type_definition:
                                    asset_type = asset_type_definition.get('name')
                                # Extract tags and concatenate with colon separator
                                tags = []
                                if 'tags' in asset_wrapper and asset_wrapper['tags']:
                                    for tag in asset_wrapper['tags']:
                                        if 'name' in tag:
                                            # Check if autoTagged is true and skip if so
                                            if tag.get('autoTagged', False):
                                                continue
                                            tags.append(tag['name'])
                                
                                tags_str = ':'.join(tags) if tags else ''
                                
                                # Write row: source_uid (asset.uid), source_id (asset.id), target_uid (asset.uid), tags
                                writer.writerow([asset_uid, asset_id, asset_uid, tags_str, assembly_id, asset_type])
                        
                        total_assets += len(page_assets)
                        successful_pages += 1
                        
                        # Progress is shown via tqdm progress bar, no need for additional prints
                        pass
                    
                    else:
                        # Try alternative response structures
                        assets_found = False
                        if page_response and 'data' in page_response:
                            # Try different possible locations for assets
                            possible_asset_locations = ['assets', 'asset', 'items', 'results']
                            for location in possible_asset_locations:
                                if location in page_response['data']:
                                    page_assets = page_response['data'][location]
                                    if isinstance(page_assets, list):
                                        # Write assets to temporary CSV file
                                        with open(temp_file.name, 'a', newline='', encoding='utf-8') as f:
                                            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                                            
                                            # Write asset data
                                            for asset_wrapper in page_assets:
                                                # Extract asset information from the wrapper structure
                                                if 'asset' in asset_wrapper:
                                                    asset = asset_wrapper['asset']
                                                else:
                                                    # Fallback: if no 'asset' wrapper, use the object directly
                                                    asset = asset_wrapper
                                                
                                                # Extract required fields
                                                asset_id = asset.get('id', '')
                                                asset_uid = asset.get('uid', '')
                                                assembly_id = asset.get('assemblyId', '')

                                                asset_type_definition = asset.get('assetType')
                                                if asset_type_definition:
                                                    asset_type = asset_type_definition.get('name')
                                                
                                                # Extract tags and concatenate with colon separator
                                                tags = []
                                                if 'tags' in asset_wrapper and asset_wrapper['tags']:
                                                    for tag in asset_wrapper['tags']:
                                                        if 'name' in tag:
                                                            # Check if autoTagged is true and skip if so
                                                            if tag.get('autoTagged', False):
                                                                continue
                                                            tags.append(tag['name'])
                                                
                                                tags_str = ':'.join(tags) if tags else ''
                                                
                                                # Write row: source_uid (asset.uid), source_id (asset.id), target_uid (asset.uid), tags
                                                writer.writerow([asset_uid, asset_id, asset_uid, tags_str, assembly_id, asset_type])
                                        
                                        total_assets += len(page_assets)
                                        successful_pages += 1
                                        assets_found = True
                                        
                                        # Progress is shown via tqdm progress bar, no need for additional prints
                                        pass
                                        break
                        
                        if not assets_found:
                            error_msg = f"Invalid response format for page {page + 1} - no assets found"
                            if not quiet_mode:
                                thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                                print(f"❌ {thread_name} - {error_msg}")
                            logger.error(error_msg)
                            failed_pages += 1
                    
                except Exception as e:
                    error_msg = f"Failed to retrieve page {page + 1}: {e}"
                    if not quiet_mode:
                        thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                        print(f"❌ {thread_name} - {error_msg}")
                    logger.error(error_msg)
                    failed_pages += 1
                
                # Update progress bar
                progress_bar.update(1)
            
            # Close progress bar
            progress_bar.close()
            
            return {
                'thread_id': thread_id,
                'successful_pages': successful_pages,
                'failed_pages': failed_pages,
                'total_assets': total_assets,
                'temp_file': temp_file.name
            }
        
        # Execute parallel processing
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Submit tasks for each thread
            futures = []
            for thread_id in range(num_threads):
                start_page = thread_id * pages_per_thread
                end_page = min(start_page + pages_per_thread, total_pages)
                
                if start_page < total_pages:  # Only submit if there are pages to process
                    future = executor.submit(process_page_chunk, thread_id, start_page, end_page)
                    futures.append(future)
            
            # Collect results
            for future in as_completed(futures):
                try:
                    result = future.result()
                    thread_results.append(result)
                except Exception as e:
                    logger.error(f"Thread failed with exception: {e}")
        
        # Step 3: Combine all temporary files into final output
        if not quiet_mode:
            print(f"\nCombining {len(temp_files)} temporary files into final output...")
        
        # Create output directory if needed
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Combine all temporary files
        with open(output_file, 'w', newline='', encoding='utf-8') as output_f:
            writer = csv.writer(output_f, quoting=csv.QUOTE_ALL)
            
            # Write header
            writer.writerow(['source_uid', 'source_id', 'target_uid', 'tags', 'assembly_id', 'asset_type'])
            
            # Combine all temporary files
            for temp_file in temp_files:
                if os.path.exists(temp_file):
                    with open(temp_file, 'r', newline='', encoding='utf-8') as temp_f:
                        reader = csv.reader(temp_f)
                        for row in reader:
                            writer.writerow(row)
        
        # Clean up temporary files
        for temp_file in temp_files:
            try:
                os.unlink(temp_file)
            except Exception as e:
                logger.warning(f"Failed to delete temporary file {temp_file}: {e}")
        
        # Step 4: Sort the CSV file by source_uid, then source_id
        if not quiet_mode:
            print("Sorting CSV file by source_uid, then source_id...")
        
        # Read all rows
        rows = []
        with open(output_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # Skip header
            rows = list(reader)
        
        # Sort rows: first by source_uid, then by source_id
        def sort_key(row):
            source_uid = row[0] if len(row) > 0 else ''
            source_id = row[1] if len(row) > 1 else ''
            # Convert source_id to int for proper numeric sorting, fallback to string
            try:
                source_id_int = int(source_id) if source_id else 0
            except (ValueError, TypeError):
                source_id_int = 0
            return (source_uid, source_id_int)
        
        rows.sort(key=sort_key)
        
        # Write sorted data back to file
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(header)
            writer.writerows(rows)
        
        # Step 5: Calculate total statistics
        total_successful_pages = sum(result['successful_pages'] for result in thread_results)
        total_failed_pages = sum(result['failed_pages'] for result in thread_results)
        total_assets_exported = sum(result['total_assets'] for result in thread_results)
        
        # Step 6: Print statistics
        if not quiet_mode:
            print("\n" + "="*80)
            print("ASSET LIST EXPORT COMPLETED (PARALLEL MODE)")
            print("="*80)
            print(f"Environment: {env_type}")
            print(f"Output file: {output_file}")
            print(f"Total assets exported: {total_assets_exported}")
            print(f"Successful pages: {total_successful_pages}")
            print(f"Failed pages: {total_failed_pages}")
            print(f"Total pages processed: {total_pages}")
            print(f"Threads used: {num_threads}")
            
            # Thread-specific statistics
            print(f"\nThread Statistics:")
            for result in thread_results:
                thread_name = thread_names[result['thread_id']] if result['thread_id'] < len(thread_names) else f"Thread {result['thread_id']}"
                print(f"  {thread_name}: {result['successful_pages']} successful, {result['failed_pages']} failed, {result['total_assets']} assets")
            
            print("="*80)
        else:
            print(f"✅ Asset list export completed: {total_assets_exported} assets exported to {output_file}")
        
    except Exception as e:
        error_msg = f"Error in asset-list-export (parallel): {e}"
        if not quiet_mode:
            print(f"❌ {error_msg}")
        logger.error(error_msg)


def execute_asset_profile_export_parallel(csv_file: str, client, logger: logging.Logger, output_file: str = None, quiet_mode: bool = False, verbose_mode: bool = False):
    """Execute the asset-profile-export command with parallel processing.
    
    Args:
        csv_file: Path to the CSV file containing source-env and target-env mappings
        client: API client instance
        logger: Logger instance
        output_file: Path to output file for writing results
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
    """
    try:
        # Check if CSV file exists
        csv_path = Path(csv_file)
        if not csv_path.exists():
            error_msg = f"CSV file does not exist: {csv_file}"
            print(f"❌ {error_msg}")
            print(f"💡 Please run 'policy-xfr' first to generate the asset_uids.csv file")
            if globals.GLOBAL_OUTPUT_DIR:
                print(f"   Expected location: {globals.GLOBAL_OUTPUT_DIR}/asset-export/asset_uids.csv")
            else:
                print(f"   Expected location: adoc-migration-toolkit-YYYYMMDDHHMM/asset-export/asset_uids.csv")
            logger.error(error_msg)
            return
        
        # Read source-env and target-env mappings from CSV file
        env_mappings = read_csv_uids(csv_file, logger)
        
        if not env_mappings:
            logger.warning("No environment mappings found in CSV file")
            return
        
        # Generate default output file if not provided - use asset-import category
        if not output_file:
            output_file = get_output_file_path(csv_file, "asset-profiles-import-ready.csv", category="asset-import")
        
        if not quiet_mode:
            print(f"\nProcessing {len(env_mappings)} asset profile exports from CSV file (Parallel Mode)")
            print(f"Input file: {csv_file}")
            print(f"Output will be written to: {output_file}")
            if globals.GLOBAL_OUTPUT_DIR:
                print(f"Using global output directory: {globals.GLOBAL_OUTPUT_DIR}")
            if verbose_mode:
                print("🔊 VERBOSE MODE - Detailed output including headers and responses")
            print("="*80)
        
        # Calculate thread configuration
        max_threads = 5
        min_assets_per_thread = 10
        
        if len(env_mappings) < min_assets_per_thread:
            num_threads = 1
            assets_per_thread = len(env_mappings)
        else:
            num_threads = min(max_threads, (len(env_mappings) + min_assets_per_thread - 1) // min_assets_per_thread)
            assets_per_thread = (len(env_mappings) + num_threads - 1) // num_threads
        
        if not quiet_mode:
            print(f"Using {num_threads} threads to process {len(env_mappings)} assets")
            print(f"Assets per thread: {assets_per_thread}")
            print("="*80)
        
        # Process assets in parallel
        thread_results = []
        temp_files = []
        
        # Funny thread names for progress indicators (all same length)
        thread_names = [
            "Rocket Thread     ",
            "Lightning Thread  ", 
            "Unicorn Thread    ",
            "Dragon Thread     ",
            "Shark Thread      "
        ]
        
        def process_asset_chunk(thread_id, start_index, end_index):
            """Process a chunk of assets for a specific thread."""
            # Create a thread-local client instance
            thread_client = type(client)(
                host=client.host,
                access_key=client.access_key,
                secret_key=client.secret_key,
                tenant=getattr(client, 'tenant', None)
            )
            # Copy target credentials to thread client
            thread_client.target_access_key = getattr(client, 'target_access_key', None)
            thread_client.target_secret_key = getattr(client, 'target_secret_key', None)
            thread_client.target_tenant = getattr(client, 'target_tenant', None)
            # Copy host template for tenant substitution
            thread_client.host_template = getattr(client, 'host_template', None)
            
            # Get assets for this thread
            thread_env_mappings = env_mappings[start_index:end_index]
            
            # Create temporary file for this thread
            temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8')
            temp_files.append(temp_file.name)
            
            # Create progress bar for this thread
            progress_bar = create_progress_bar(
                total=len(thread_env_mappings),
                desc=thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}",
                unit="assets",
                disable=quiet_mode,
                position=thread_id,
                leave=False
            )
            
            successful = 0
            failed = 0
            total_assets_processed = 0
            
            # Process each asset in this thread's range
            for i, (source_env, target_env) in enumerate(thread_env_mappings):
                try:
                    if verbose_mode:
                        thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                        print(f"\n{thread_name} - Processing source-env: {source_env}")
                        print(f"Target-env: {target_env}")
                        print("-" * 60)
                    
                    # Step 1: Get asset details by source-env (UID)
                    if verbose_mode:
                        thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                        print(f"\n{thread_name} - Getting asset details for UID: {source_env}")
                        print(f"GET Request Headers:")
                        print(f"  Endpoint: /catalog-server/api/assets?uid={source_env}")
                        print(f"  Method: GET")
                        print(f"  Content-Type: application/json")
                        print(f"  Authorization: Bearer [REDACTED]")
                        if hasattr(thread_client, 'tenant') and thread_client.tenant:
                            print(f"  X-Tenant: {thread_client.tenant}")
                    
                    asset_response = thread_client.make_api_call(
                        endpoint=f"/catalog-server/api/assets?uid={source_env}",
                        method='GET'
                    )
                    
                    if verbose_mode:
                        thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                        print(f"\n{thread_name} - Asset Response:")
                        print(json.dumps(asset_response, indent=2, ensure_ascii=False))
                    
                    # Step 2: Extract the asset ID
                    if not asset_response or 'data' not in asset_response:
                        error_msg = f"No 'data' field found in asset response for UID: {source_env}"
                        if verbose_mode:
                            thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                            print(f"\n{thread_name} - ❌ {error_msg}")
                        logger.error(f"Thread {thread_id}: {error_msg}")
                        failed += 1
                        progress_bar.update(1)
                        continue
                    
                    data_array = asset_response['data']
                    if not data_array or len(data_array) == 0:
                        error_msg = f"Empty 'data' array in asset response for UID: {source_env}"
                        if verbose_mode:
                            thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                            print(f"\n{thread_name} - ❌ {error_msg}")
                        logger.error(f"Thread {thread_id}: {error_msg}")
                        failed += 1
                        progress_bar.update(1)
                        continue
                    
                    first_asset = data_array[0]
                    if 'id' not in first_asset:
                        error_msg = f"No 'id' field found in first asset for UID: {source_env}"
                        if verbose_mode:
                            thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                            print(f"\n{thread_name} - ❌ {error_msg}")
                        logger.error(f"Thread {thread_id}: {error_msg}")
                        failed += 1
                        progress_bar.update(1)
                        continue
                    
                    asset_id = first_asset['id']
                    if verbose_mode:
                        thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                        print(f"{thread_name} - Extracted asset ID: {asset_id}")
                    
                    # Step 3: Get profile configuration for the asset
                    if verbose_mode:
                        thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                        print(f"\n{thread_name} - Getting profile configuration for asset ID: {asset_id}")
                        print(f"GET Request Headers:")
                        print(f"  Endpoint: /catalog-server/api/profile/{asset_id}/config")
                        print(f"  Method: GET")
                        print(f"  Content-Type: application/json")
                        print(f"  Authorization: Bearer [REDACTED]")
                        if hasattr(thread_client, 'tenant') and thread_client.tenant:
                            print(f"  X-Tenant: {thread_client.tenant}")
                    
                    profile_response = thread_client.make_api_call(
                        endpoint=f"/catalog-server/api/profile/{asset_id}/config",
                        method='GET'
                    )
                    
                    if verbose_mode:
                        thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                        print(f"\n{thread_name} - Profile Response:")
                        print(json.dumps(profile_response, indent=2, ensure_ascii=False))
                    
                    # Step 4: Write to temporary CSV file
                    profile_json = json.dumps(profile_response, ensure_ascii=False)
                    with open(temp_file.name, 'a', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                        writer.writerow([target_env, profile_json])
                    
                    if verbose_mode:
                        thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                        print(f"{thread_name} - ✅ Written to file: {target_env}")
                    
                    successful += 1
                    total_assets_processed += 1
                    
                except Exception as e:
                    error_msg = f"Failed to process source-env {source_env}: {e}"
                    if verbose_mode:
                        thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                        print(f"\n{thread_name} - ❌ {error_msg}")
                    logger.error(f"Thread {thread_id}: {error_msg}")
                    failed += 1
                
                # Update progress bar
                progress_bar.update(1)
            
            progress_bar.close()
            
            return {
                'thread_id': thread_id,
                'successful': successful,
                'failed': failed,
                'total_assets_processed': total_assets_processed,
                'temp_file': temp_file.name
            }
        
        # Start threads
        threads = []
        for i in range(num_threads):
            start_index = i * assets_per_thread
            end_index = min(start_index + assets_per_thread, len(env_mappings))
            
            thread = threading.Thread(
                target=lambda tid=i, start=start_index, end=end_index: thread_results.append(
                    process_asset_chunk(tid, start, end)
                )
            )
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Merge temporary files
        if not quiet_mode:
            print("\nMerging temporary files...")
        
        # Create output directory if needed
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Read all rows from temporary files
        all_rows = []
        for temp_file in temp_files:
            try:
                with open(temp_file, 'r', newline='', encoding='utf-8') as temp_csv:
                    reader = csv.reader(temp_csv)
                    for row in reader:
                        if len(row) >= 2:  # Ensure we have target-env and profile_json
                            all_rows.append(row)
            except Exception as e:
                logger.error(f"Error reading temporary file {temp_file}: {e}")
            finally:
                # Clean up temporary file
                try:
                    os.unlink(temp_file)
                except Exception as e:
                    logger.warning(f"Could not delete temporary file {temp_file}: {e}")
        
        # Sort rows by target-env (first column)
        if not quiet_mode:
            print("Sorting results by target-env...")
        
        def sort_key(row):
            target_env = row[0] if len(row) > 0 else ''
            return target_env.lower()  # Case-insensitive sorting
        
        all_rows.sort(key=sort_key)
        
        # Write final output
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            
            # Write header
            writer.writerow(['target-env', 'profile_json'])
            
            # Write sorted data
            writer.writerows(all_rows)
        
        # Print statistics
        if not quiet_mode:
            print("\n" + "="*80)
            print("ASSET PROFILE EXPORT COMPLETED (PARALLEL MODE)")
            print("="*80)
            print(f"Output file: {output_file}")
            print(f"Total assets processed: {len(env_mappings)}")
            print(f"Threads used: {num_threads}")
            
            total_successful = 0
            total_failed = 0
            total_assets_processed = 0
            
            for result in thread_results:
                thread_name = thread_names[result['thread_id']] if result['thread_id'] < len(thread_names) else f"Thread {result['thread_id']}"
                print(f"{thread_name}: {result['successful']} successful, {result['failed']} failed, {result['total_assets_processed']} processed")
                total_successful += result['successful']
                total_failed += result['failed']
                total_assets_processed += result['total_assets_processed']
            
            print(f"\nTotal successful: {total_successful}")
            print(f"Total failed: {total_failed}")
            print(f"Total assets processed: {total_assets_processed}")
            
            # Calculate success rate
            if len(env_mappings) > 0:
                success_rate = (total_successful / len(env_mappings)) * 100
                print(f"Success rate: {success_rate:.1f}%")
            
            # File information
            print(f"\n📁 FILE INFORMATION:")
            print(f"  Input CSV: {csv_file}")
            print(f"  Output CSV: {output_file}")
            print(f"  File size: {output_path.stat().st_size:,} bytes")
            
            # Performance metrics
            if total_successful > 0:
                print(f"\n⚡ PERFORMANCE METRICS:")
                print(f"  Average profiles per asset: {total_assets_processed / total_successful:.1f}")
                print(f"  Total profiles exported: {total_assets_processed}")
            
            # Output format information
            print(f"\n📋 OUTPUT FORMAT:")
            print(f"  CSV columns: target-env, profile_json")
            print(f"  JSON encoding: UTF-8")
            print(f"  CSV quoting: QUOTE_ALL")
            print(f"  Line endings: Platform default")
            
            print("="*80)
            
            if total_failed > 0:
                print("⚠️  Export completed with errors. Check log file for details.")
            else:
                print("✅ Export completed successfully!")
        else:
            print(f"✅ Asset profile export completed: {len(all_rows)} assets processed")
            print(f"Output written to: {output_file}")
        
    except Exception as e:
        error_msg = f"Error in parallel asset-profile-export: {e}"
        if not quiet_mode:
            print(f"❌ {error_msg}")
        logger.error(error_msg)
        raise


def execute_asset_tag_import(csv_file: str, client, logger: logging.Logger, quiet_mode: bool = False, verbose_mode: bool = False, parallel_mode: bool = False):
    """Execute the asset-tag-import command.
    
    Args:
        csv_file: Path to the CSV file containing asset data
        client: API client instance
        logger: Logger instance
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
        parallel_mode: Whether to use parallel processing
    """
    try:
        # Check if CSV file exists
        csv_path = Path(csv_file)
        if not csv_path.exists():
            error_msg = f"CSV file does not exist: {csv_file}"
            print(f"❌ {error_msg}")
            print(f"💡 Please run 'transform-and-merge' first to generate the asset-merged-all.csv file")
            if globals.GLOBAL_OUTPUT_DIR:
                print(f"   Expected location: {globals.GLOBAL_OUTPUT_DIR}/asset-import/asset-merged-all.csv")
            else:
                print(f"   Expected location: adoc-migration-toolkit-YYYYMMDDHHMM/asset-import/asset-merged-all.csv")
            logger.error(error_msg)
            return
        
        # Read CSV data
        asset_data = []
        with open(csv_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # Skip header
            for row in reader:
                if len(row) >= 5:  # Ensure we have at least 5 columns (asset-merged-all.csv format)
                    source_id = row[0]
                    source_uid = row[1]
                    target_id = row[2]
                    target_uid = row[3]
                    tags_str = row[4]  # 5th column contains tags
                    
                    # Parse tags (colon-separated)
                    tags = []
                    if tags_str and tags_str.strip():
                        tags = [tag.strip() for tag in tags_str.split(':') if tag.strip()]
                    
                    asset_data.append({
                        'source_uid': source_uid,
                        'source_id': source_id,
                        'target_uid': target_uid,
                        'tags': tags
                    })
        
        if not asset_data:
            print("❌ No valid asset data found in CSV file")
            logger.warning("No valid asset data found in CSV file")
            return
        
        # Filter out assets with no tags
        assets_with_tags = [asset for asset in asset_data if asset['tags']]
        
        if not assets_with_tags:
            print("ℹ️  No assets with tags found in CSV file")
            logger.info("No assets with tags found in CSV file")
            return
        
        if not quiet_mode:
            print(f"\nImporting tags for {len(assets_with_tags)} assets from CSV file")
            print(f"Input file: {csv_file}")
            if globals.GLOBAL_OUTPUT_DIR:
                print(f"Using global output directory: {globals.GLOBAL_OUTPUT_DIR}")
            if verbose_mode:
                print("🔊 VERBOSE MODE - Detailed output including headers and responses")
            if parallel_mode:
                print("🚀 PARALLEL MODE - Using multiple threads for faster processing")
            print("="*80)
        
        if parallel_mode:
            execute_asset_tag_import_parallel(assets_with_tags, client, logger, quiet_mode, verbose_mode)
        else:
            execute_asset_tag_import_sequential(assets_with_tags, client, logger, quiet_mode, verbose_mode)
        
    except Exception as e:
        error_msg = f"Error in asset-tag-import: {e}"
        if not quiet_mode:
            print(f"❌ {error_msg}")
        logger.error(error_msg)


def execute_asset_tag_import_sequential(assets_with_tags: List[Dict], client, logger: logging.Logger, quiet_mode: bool = False, verbose_mode: bool = False):
    """Execute asset tag import in sequential mode.
    
    Args:
        assets_with_tags: List of asset data dictionaries
        client: API client instance
        logger: Logger instance
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
    """
    total_assets = len(assets_with_tags)
    successful_assets = 0
    failed_assets = 0
    total_tags_imported = 0
    total_tags_failed = 0
    
    # Create progress bar (disable if quiet mode or verbose mode)
    progress_bar = create_progress_bar(
        total=total_assets,
        desc="Importing asset tags",
        unit="assets",
        disable=quiet_mode or verbose_mode
    )
    
    for asset in assets_with_tags:
        try:
            target_uid = asset['target_uid']
            tags = asset['tags']
            
            if verbose_mode:
                print(f"\nProcessing asset: {target_uid}")
                print(f"Tags to import: {tags}")
                print("-" * 60)
            
            # Step 1: Get asset ID from UID
            if verbose_mode:
                print(f"GET Request:")
                print(f"  Endpoint: /catalog-server/api/assets?uid={target_uid}")
                print(f"  Method: GET")
                print(f"  Content-Type: application/json")
                print(f"  Authorization: Bearer [REDACTED]")
                if hasattr(client, 'tenant') and client.tenant:
                    print(f"  X-Tenant: {client.tenant}")
            
            asset_response = client.make_api_call(
                endpoint=f"/catalog-server/api/assets?uid={target_uid}",
                method='GET',
                use_target_auth=True,
                use_target_tenant=True
            )
            
            if verbose_mode:
                print(f"Asset Response:")
                print(json.dumps(asset_response, indent=2, ensure_ascii=False))
            
            # Extract asset ID
            assets_list = []
            if asset_response and 'data' in asset_response:
                if isinstance(asset_response['data'], list):
                    assets_list = asset_response['data']
                elif isinstance(asset_response['data'], dict) and 'assets' in asset_response['data']:
                    assets_list = asset_response['data']['assets']
            if not assets_list:
                error_msg = f"No asset found for UID: {target_uid}"
                if verbose_mode:
                    print(f"❌ {error_msg}")
                logger.error(error_msg)
                failed_assets += 1
                progress_bar.update(1)
                continue
            asset_id = assets_list[0].get('id') if assets_list and isinstance(assets_list[0], dict) else None
            if not asset_id:
                error_msg = f"No asset ID found for UID: {target_uid}"
                if verbose_mode:
                    print(f"❌ {error_msg}")
                logger.error(error_msg)
                failed_assets += 1
                progress_bar.update(1)
                continue
            
            if verbose_mode:
                print(f"Found asset ID: {asset_id}")
            
            # Step 2: Import each tag
            asset_tags_successful = 0
            asset_tags_failed = 0
            
            for tag in tags:
                try:
                    if verbose_mode:
                        print(f"\nPOST Request:")
                        print(f"  Endpoint: /catalog-server/api/assets/{asset_id}/tag")
                        print(f"  Method: POST")
                        print(f"  Content-Type: application/json")
                        print(f"  Authorization: Bearer [REDACTED]")
                        if hasattr(client, 'tenant') and client.tenant:
                            print(f"  X-Tenant: {client.tenant}")
                        print(f"  Request Body: {{\"name\": \"{tag}\"}}")
                    
                    tag_response = client.make_api_call(
                        endpoint=f"/catalog-server/api/assets/{asset_id}/tag",
                        method='POST',
                        json_payload={"name": tag},
                        use_target_auth=True,
                        use_target_tenant=True
                    )
                    
                    if verbose_mode:
                        print(f"Tag Response:")
                        print(json.dumps(tag_response, indent=2, ensure_ascii=False))
                    
                    if tag_response:
                        asset_tags_successful += 1
                        total_tags_imported += 1
                        if verbose_mode:
                            print(f"✅ Successfully imported tag: {tag}")
                    else:
                        asset_tags_failed += 1
                        total_tags_failed += 1
                        if verbose_mode:
                            print(f"❌ Failed to import tag: {tag}")
                
                except Exception as e:
                    error_msg = f"Error importing tag '{tag}' for asset {target_uid}: {e}"
                    if verbose_mode:
                        print(f"❌ {error_msg}")
                    logger.error(error_msg)
                    asset_tags_failed += 1
                    total_tags_failed += 1
            
            # Update asset statistics
            if asset_tags_failed == 0:
                successful_assets += 1
                if verbose_mode:
                    print(f"✅ Successfully processed asset: {target_uid} ({asset_tags_successful} tags)")
            else:
                failed_assets += 1
                if verbose_mode:
                    print(f"⚠️  Partially processed asset: {target_uid} ({asset_tags_successful} successful, {asset_tags_failed} failed)")
            
            progress_bar.update(1)
            
        except Exception as e:
            error_msg = f"Error processing asset {asset.get('target_uid', 'unknown')}: {e}"
            if verbose_mode:
                print(f"❌ {error_msg}")
            logger.error(error_msg)
            failed_assets += 1
            progress_bar.update(1)
    
    progress_bar.close()
    
    # Print statistics
    if not quiet_mode:
        print("\n" + "="*80)
        print("ASSET TAG IMPORT COMPLETED")
        print("="*80)
        print(f"Total assets processed: {total_assets}")
        print(f"Successful assets: {successful_assets}")
        print(f"Failed assets: {failed_assets}")
        print(f"Total tags imported: {total_tags_imported}")
        print(f"Total tags failed: {total_tags_failed}")
        print("="*80)
    else:
        print(f"✅ Asset tag import completed: {successful_assets}/{total_assets} assets successful, {total_tags_imported} tags imported")


def execute_asset_tag_import_parallel(assets_with_tags: List[Dict], client, logger: logging.Logger, quiet_mode: bool = False, verbose_mode: bool = False, max_threads: int = 5):
    """Execute asset tag import in parallel mode.
    Args:
        assets_with_tags: List of asset data dictionaries
        client: API client instance
        logger: Logger instance
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
        max_threads: Maximum number of threads to use (default: 5)
    """
    # Calculate thread configuration
    min_assets_per_thread = 10
    if len(assets_with_tags) < min_assets_per_thread:
        num_threads = 1
        assets_per_thread = len(assets_with_tags)
    else:
        num_threads = min(max_threads, (len(assets_with_tags) + min_assets_per_thread - 1) // min_assets_per_thread)
        assets_per_thread = (len(assets_with_tags) + num_threads - 1) // num_threads
    
    if not quiet_mode:
        print(f"Using {num_threads} threads to process {len(assets_with_tags)} assets")
        print(f"Assets per thread: {assets_per_thread}")
        print("="*80)
    
    # Process assets in parallel
    thread_results = []
    
    # Funny thread names for progress indicators (all same length)
    thread_names = [
        "Rocket Thread     ",
        "Lightning Thread  ", 
        "Unicorn Thread    ",
        "Dragon Thread     ",
        "Shark Thread      "
    ]
    
    def process_asset_chunk(thread_id, start_index, end_index):
        """Process a chunk of assets for a specific thread."""
        # Create a thread-local client instance
        thread_client = type(client)(
            host=client.host,
            access_key=client.access_key,
            secret_key=client.secret_key,
            tenant=getattr(client, 'tenant', None)
        )
        # Copy target credentials to thread client
        thread_client.target_access_key = getattr(client, 'target_access_key', None)
        thread_client.target_secret_key = getattr(client, 'target_secret_key', None)
        thread_client.target_tenant = getattr(client, 'target_tenant', None)
        # Copy host template for tenant substitution
        thread_client.host_template = getattr(client, 'host_template', None)
        
        # Get assets for this thread
        thread_assets = assets_with_tags[start_index:end_index]
        
        # Create progress bar for this thread (disable if quiet mode or verbose mode)
        progress_bar = create_progress_bar(
            total=len(thread_assets),
            desc=thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}",
            unit="assets",
            disable=quiet_mode or verbose_mode,
            position=thread_id,
            leave=False
        )
        
        successful_assets = 0
        failed_assets = 0
        total_tags_imported = 0
        total_tags_failed = 0
        
        # Process each asset in this thread's range
        for asset in thread_assets:
            try:
                target_uid = asset['target_uid']
                tags = asset['tags']
                
                if verbose_mode:
                    thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                    print(f"\n{thread_name} - Processing asset: {target_uid}")
                    print(f"Tags to import: {tags}")
                    print("-" * 60)
                
                # Step 1: Get asset ID from UID
                if verbose_mode:
                    print(f"GET Request:")
                    print(f"  Endpoint: /catalog-server/api/assets?uid={target_uid}")
                    print(f"  Method: GET")
                    print(f"  Content-Type: application/json")
                    print(f"  Authorization: Bearer [REDACTED]")
                    if hasattr(thread_client, 'tenant') and thread_client.tenant:
                        print(f"  X-Tenant: {thread_client.tenant}")
                
                asset_response = thread_client.make_api_call(
                    endpoint=f"/catalog-server/api/assets?uid={target_uid}",
                    method='GET',
                    use_target_auth=True,
                    use_target_tenant=True
                )
                
                if verbose_mode:
                    print(f"Asset Response:")
                    print(json.dumps(asset_response, indent=2, ensure_ascii=False))
                
                # Extract asset ID
                if not asset_response or 'data' not in asset_response:
                    error_msg = f"No 'data' field found in asset response for UID: {target_uid}"
                    if verbose_mode:
                        print(f"❌ {error_msg}")
                    logger.error(error_msg)
                    failed_assets += 1
                    progress_bar.update(1)
                    continue
                
                assets_list = []
                if asset_response and 'data' in asset_response:
                    if isinstance(asset_response['data'], list):
                        assets_list = asset_response['data']
                    elif isinstance(asset_response['data'], dict) and 'assets' in asset_response['data']:
                        assets_list = asset_response['data']['assets']
                if not assets_list:
                    error_msg = f"No asset found for UID: {target_uid}"
                    if verbose_mode:
                        print(f"❌ {error_msg}")
                    logger.error(error_msg)
                    failed_assets += 1
                    progress_bar.update(1)
                    continue
                asset_id = assets_list[0].get('id') if assets_list and isinstance(assets_list[0], dict) else None
                if not asset_id:
                    error_msg = f"No asset ID found for UID: {target_uid}"
                    if verbose_mode:
                        print(f"❌ {error_msg}")
                    logger.error(error_msg)
                    failed_assets += 1
                    progress_bar.update(1)
                    continue
                
                if verbose_mode:
                    print(f"Found asset ID: {asset_id}")
                
                # Step 2: Import each tag
                asset_tags_successful = 0
                asset_tags_failed = 0
                
                for tag in tags:
                    try:
                        if verbose_mode:
                            print(f"\nPOST Request:")
                            print(f"  Endpoint: /catalog-server/api/assets/{asset_id}/tag")
                            print(f"  Method: POST")
                            print(f"  Content-Type: application/json")
                            print(f"  Authorization: Bearer [REDACTED]")
                            if hasattr(thread_client, 'tenant') and thread_client.tenant:
                                print(f"  X-Tenant: {thread_client.tenant}")
                            print(f"  Request Body: {{\"name\": \"{tag}\"}}")
                        
                        tag_response = thread_client.make_api_call(
                            endpoint=f"/catalog-server/api/assets/{asset_id}/tag",
                            method='POST',
                            json_payload={"name": tag},
                            use_target_auth=True,
                            use_target_tenant=True
                        )
                        
                        if verbose_mode:
                            print(f"Tag Response:")
                            print(json.dumps(tag_response, indent=2, ensure_ascii=False))
                        
                        if tag_response:
                            asset_tags_successful += 1
                            total_tags_imported += 1
                            if verbose_mode:
                                print(f"✅ Successfully imported tag: {tag}")
                        else:
                            asset_tags_failed += 1
                            total_tags_failed += 1
                            if verbose_mode:
                                print(f"❌ Failed to import tag: {tag}")
                    
                    except Exception as e:
                        error_msg = f"Error importing tag '{tag}' for asset {target_uid}: {e}"
                        if verbose_mode:
                            print(f"❌ {error_msg}")
                        logger.error(error_msg)
                        asset_tags_failed += 1
                        total_tags_failed += 1
                
                # Update asset statistics
                if asset_tags_failed == 0:
                    successful_assets += 1
                    if verbose_mode:
                        print(f"✅ Successfully processed asset: {target_uid} ({asset_tags_successful} tags)")
                else:
                    failed_assets += 1
                    if verbose_mode:
                        print(f"⚠️  Partially processed asset: {target_uid} ({asset_tags_successful} successful, {asset_tags_failed} failed)")
                
                progress_bar.update(1)
                
            except Exception as e:
                error_msg = f"Error processing asset {asset.get('target_uid', 'unknown')}: {e}"
                if verbose_mode:
                    print(f"❌ {error_msg}")
                logger.error(error_msg)
                failed_assets += 1
                progress_bar.update(1)
        
        progress_bar.close()
        
        return {
            'thread_id': thread_id,
            'successful_assets': successful_assets,
            'failed_assets': failed_assets,
            'total_tags_imported': total_tags_imported,
            'total_tags_failed': total_tags_failed
        }
    
    # Execute parallel processing
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        
        for i in range(num_threads):
            start_index = i * assets_per_thread
            end_index = min(start_index + assets_per_thread, len(assets_with_tags))
            
            future = executor.submit(process_asset_chunk, i, start_index, end_index)
            futures.append(future)
        
        # Collect results
        for future in as_completed(futures):
            try:
                result = future.result()
                thread_results.append(result)
            except Exception as e:
                logger.error(f"Thread execution error: {e}")
    
    # Aggregate results
    total_successful_assets = sum(r['successful_assets'] for r in thread_results)
    total_failed_assets = sum(r['failed_assets'] for r in thread_results)
    total_tags_imported = sum(r['total_tags_imported'] for r in thread_results)
    total_tags_failed = sum(r['total_tags_failed'] for r in thread_results)
    
    # Print statistics
    if not quiet_mode:
        print("\n" + "="*80)
        print("ASSET TAG IMPORT COMPLETED (PARALLEL MODE)")
        print("="*80)
        print(f"Total assets processed: {len(assets_with_tags)}")
        print(f"Successful assets: {total_successful_assets}")
        print(f"Failed assets: {total_failed_assets}")
        print(f"Total tags imported: {total_tags_imported}")
        print(f"Total tags failed: {total_tags_failed}")
        print(f"Threads used: {num_threads}")
        print("="*80)
    else:
        print(f"✅ Asset tag import completed: {total_successful_assets}/{len(assets_with_tags)} assets successful, {total_tags_imported} tags imported")


def execute_asset_config_export_parallel(csv_file: str, client, logger: logging.Logger, output_file: str = None,
                                         quiet_mode: bool = False, verbose_mode: bool = False, max_threads: int = 5):
    """Execute the asset-config-export command with parallel processing.

    Args:
        csv_file: Path to the CSV file containing asset data with 5 columns: source_id, source_uid, target_id, target_uid, tags
        client: API client instance
        logger: Logger instance
        output_file: Path to output file for writing results
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
        max_threads: Maximum number of threads to use for parallel processing
    """
    try:
        # Read asset data from CSV file with 4 columns
        asset_data = read_csv_asset_data(csv_file, logger)

        if not asset_data:
            logger.warning("No asset data found in CSV file")
            return

        # Generate default output file if not provided
        if not output_file:
            output_file = get_output_file_path(csv_file, "asset-config-export.csv", category="asset-export")

        if not quiet_mode:
            print(f"\nProcessing {len(asset_data)} asset config exports from CSV file (Parallel Mode)")
            print(f"Output will be written to: {output_file}")
            if globals.GLOBAL_OUTPUT_DIR:
                print(f"Using global output directory: {globals.GLOBAL_OUTPUT_DIR}")
            if verbose_mode:
                print("🔊 VERBOSE MODE - Detailed output including headers and responses")
            print("🚀 PARALLEL MODE - Using multiple threads for faster processing")
            print("=" * 80)

        # Determine number of threads
        num_threads = min(max_threads, max(1, len(asset_data)))

        if not quiet_mode:
            print(f"Using {num_threads} threads for processing")

        # Thread names for progress indicators
        thread_names = [
            "Rocket Thread     ",
            "Lightning Thread  ",
            "Unicorn Thread    ",
            "Dragon Thread     ",
            "Shark Thread      "
        ]

        # Open output file for writing
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Thread-safe counters
        successful = 0
        failed = 0
        total_assets_processed = 0
        anomaly_config_export_failed = 0
        lock = threading.Lock()
        all_results = []

        def process_asset_chunk(thread_id, start_index, end_index):
            """Process a chunk of assets for a specific thread."""
            nonlocal successful, failed, total_assets_processed, anomaly_config_export_failed
            thread_results = []
            thread_successful = 0
            thread_failed = 0
            thread_anomaly_failed = 0

            # Get thread name
            thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"

            # Create progress bar for this thread if in quiet mode (not verbose)
            if quiet_mode and not verbose_mode:
                progress_bar = tqdm(
                    total=end_index - start_index,
                    desc=thread_name,
                    position=thread_id,
                    leave=False,
                    colour='green'
                )
            else:
                progress_bar = None

            for i in range(start_index, end_index):
                asset = asset_data[i]
                source_uid = asset['source_uid']
                source_id = asset['source_id']
                target_uid = asset['target_uid']
                target_id = asset['target_id']

                if verbose_mode:
                    print(f"\n[Thread {thread_id}] Processing asset - source_uid: {source_uid}, source_id: {source_id}")
                    print("-" * 60)
                elif not quiet_mode:
                    print(
                        f"[Thread {thread_id}] Processing [{i - start_index + 1}/{end_index - start_index}] source_id: {source_id}")

                try:
                    # Get asset configuration using source_id directly
                    if verbose_mode:
                        print(f"[Thread {thread_id}] Getting asset configuration for ID: {source_id}")

                    # Show headers in verbose mode
                    if verbose_mode:
                        print(f"\n[Thread {thread_id}] GET Request Headers:")
                        print(f"  Endpoint: /catalog-server/api/assets/{source_id}/config")
                        print(f"  Method: GET")
                        print(f"  Content-Type: application/json")
                        print(f"  Authorization: Bearer [REDACTED]")
                        if hasattr(client, 'tenant') and client.tenant:
                            print(f"  X-Tenant: {client.tenant}")

                    asset_config = client.make_api_call(
                        endpoint=f"/catalog-server/api/assets/{source_id}/config",
                        method='GET'
                    )

                    # Show response in verbose mode
                    if verbose_mode:
                        print(f"\n[Thread {thread_id}] Config Response:")
                        print(json.dumps(asset_config, indent=2, ensure_ascii=False))

                    asset_config_json = json.dumps(asset_config, ensure_ascii=False, separators=(',', ':'))
                    asset_profile_anomaly_config_json = {}
                    try:
                        asset_profile_anomaly_config = client.make_api_call(
                            endpoint=f"/catalog-server/api/rules/profile-anomaly/byAsset/{source_id}",
                            method='GET'
                        )
                        asset_profile_anomaly_config_json = json.dumps(asset_profile_anomaly_config, ensure_ascii=False,
                                                                       separators=(',', ':'))
                    except Exception as e:
                        error_msg = f"[Thread {thread_id}] Error exporting anomaly details source_id {source_id}: {e}"
                        if verbose_mode or not quiet_mode:
                            print(f"❌ {error_msg}")
                        logger.error(error_msg)
                        thread_anomaly_failed += 1
                    # Get child assets of each asset
                    asset_child_assets_config = client.make_api_call(
                        endpoint=f"/catalog-server/api/assets/{source_id}/childAssets",
                        method='GET'
                    )

                    asset_child_assets_config_json = json.dumps(asset_child_assets_config, ensure_ascii=False,
                                                                separators=(',', ':'))
                    asset_child_assets_config_dict = json.loads(asset_child_assets_config_json)

                    # Build a dictionary of assetId -> uid
                    asset_id_to_uid_map = {item["assetId"]: item["uid"] for item in
                                           asset_child_assets_config_dict["childAssets"]}
                    asset_id_to_uid_map_json = json.dumps(asset_id_to_uid_map, ensure_ascii=False,
                                                          separators=(',', ':'))

                    # Get child assets of each asset
                    asset_target_child_assets_config = client.make_api_call(
                        endpoint=f"/catalog-server/api/assets/{target_id}/childAssets",
                        method='GET',
                        use_target_auth=True,
                        use_target_tenant=True
                    )

                    asset_target_child_assets_config_json = json.dumps(asset_target_child_assets_config,
                                                                       ensure_ascii=False, separators=(',', ':'))
                    asset_target_child_assets_config_dict = json.loads(asset_target_child_assets_config_json)

                    # Build a dictionary of assetId -> uid
                    asset_target_id_to_uid_map = {item["assetId"]: item["uid"] for item in
                                                  asset_target_child_assets_config_dict["childAssets"]}
                    asset_target_id_to_uid_map_json = json.dumps(asset_target_id_to_uid_map, ensure_ascii=False,
                                                                 separators=(',', ':'))

                    # Write the compressed JSON response to CSV with target_uid
                    thread_results.append(
                        [target_uid, asset_config_json, asset_profile_anomaly_config_json, asset_id_to_uid_map_json,
                         asset_target_id_to_uid_map_json])

                    if verbose_mode:
                        print(f"[Thread {thread_id}] ✅ Written to file: {target_uid}")
                    elif not quiet_mode:
                        print(
                            f"[Thread {thread_id}] ✅ [{i - start_index + 1}/{end_index - start_index}] {target_uid}: Config exported successfully")

                    thread_successful += 1

                except Exception as e:
                    error_msg = f"[Thread {thread_id}] Error processing source_id {source_id}: {e}"
                    if verbose_mode or not quiet_mode:
                        print(f"❌ {error_msg}")
                    logger.error(error_msg)
                    thread_failed += 1

                # Update progress bar
                if progress_bar:
                    progress_bar.update(1)

            # Close progress bar
            if progress_bar:
                progress_bar.close()

            # Update global counters thread-safely
            with lock:
                successful += thread_successful
                failed += thread_failed
                total_assets_processed += thread_successful
                anomaly_config_export_failed += thread_anomaly_failed
                all_results.extend(thread_results)

            return {
                'thread_id': thread_id,
                'successful': thread_successful,
                'failed': thread_failed,
                'total_assets': end_index - start_index
            }

        # Calculate chunk sizes
        chunk_size = len(asset_data) // num_threads
        remainder = len(asset_data) % num_threads

        # Create thread pool
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            start_index = 0

            for thread_id in range(num_threads):
                # Calculate end index for this thread
                end_index = start_index + chunk_size
                if thread_id < remainder:
                    end_index += 1

                if start_index < end_index:
                    future = executor.submit(process_asset_chunk, thread_id, start_index, end_index)
                    futures.append(future)

                start_index = end_index

            # Wait for all threads to complete
            thread_results = []
            for future in as_completed(futures):
                thread_results.append(future.result())

        # Write all results to CSV file
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)

            # Write header
            writer.writerow(
                ['target_uid', 'asset_config_json', 'asset_profile_anomaly_config_json', 'asset_id_to_uid_map_json',
                 'asset_target_id_to_uid_map_json'])

            # Write all results
            writer.writerows(all_results)

        # Verify the CSV file can be read correctly
        if verbose_mode or not quiet_mode:
            print("\nVerifying CSV file can be read correctly...")

        try:
            with open(output_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader)
                row_count = 0
                validation_errors = []

                # Validate header
                if len(header) != 4:
                    validation_errors.append(f"Invalid header: expected 4 columns, got {len(header)}")
                elif header[0] != 'target_uid' or header[1] != 'asset_config_json' or header[
                    2] != 'asset_profile_anomaly_config_json' or header[3] != 'asset_id_to_uid_map_json' or header[
                    4] != 'asset_target_id_to_uid_map_json':
                    validation_errors.append(
                        f"Invalid header: expected ['target_uid', 'asset_config_json', 'asset_profile_anomaly_config_json', 'asset_id_to_uid_map_json', 'asset_target_id_to_uid_map_json], got {header}")

                # Validate each row
                for row_num, row in enumerate(reader, start=2):
                    row_count += 1

                    # Check column count
                    if len(row) != 4:
                        validation_errors.append(f"Row {row_num}: Expected 4 columns, got {len(row)}")
                        continue

                    target_uid, config_json_str, asset_profile_anomaly_config_json_str, asset_id_to_uid_map_json_str, asset_target_id_to_uid_map_json_str = row

                    # Check for empty values
                    if not target_uid.strip():
                        validation_errors.append(f"Row {row_num}: Empty target_uid value")

                    if not config_json_str.strip():
                        validation_errors.append(f"Row {row_num}: Empty asset_config_json value")

                    if not asset_profile_anomaly_config_json_str.strip():
                        validation_errors.append(f"Row {row_num}: Empty asset_profile_anomaly_config_json value")
                        continue

                    if not asset_id_to_uid_map_json_str.strip():
                        validation_errors.append(f"Row {row_num}: Empty asset_id_to_uid_map_json value")
                        continue

                    if not asset_target_id_to_uid_map_json_str.strip():
                        validation_errors.append(f"Row {row_num}: Empty asset_target_id_to_uid_map_json value")
                        continue
                    # Verify JSON is parsable
                    try:
                        config_data = json.loads(config_json_str)

                        # Additional validation: check if it's a valid config response
                        if not isinstance(config_data, dict):
                            validation_errors.append(f"Row {row_num}: asset_config_json is not a valid JSON object")
                        elif not config_data:  # Empty object
                            validation_errors.append(f"Row {row_num}: asset_config_json is empty")

                    except json.JSONDecodeError as e:
                        validation_errors.append(f"Row {row_num}: Invalid JSON in asset_config_json - {e}")
                    except Exception as e:
                        validation_errors.append(f"Row {row_num}: Error parsing asset_config_json - {e}")

                    # Verify asset_profile_anomaly_config_json JSON is parsable
                    try:
                        config_data = json.loads(asset_profile_anomaly_config_json_str)

                        # Additional validation: check if it's a valid config response
                        if not isinstance(config_data, dict):
                            validation_errors.append(
                                f"Row {row_num}: asset_profile_anomaly_config_json is not a valid JSON object")
                        elif not config_data:  # Empty object
                            validation_errors.append(f"Row {row_num}: asset_profile_anomaly_config_json is empty")

                    except json.JSONDecodeError as e:
                        validation_errors.append(
                            f"Row {row_num}: Invalid JSON in asset_profile_anomaly_config_json - {e}")
                    except Exception as e:
                        validation_errors.append(
                            f"Row {row_num}: Error parsing asset_profile_anomaly_config_json - {e}")

                    # Verify asset_id_to_uid_map_json JSON is parsable
                    try:
                        config_data = json.loads(asset_id_to_uid_map_json_str)

                        # Additional validation: check if it's a valid config response
                        if not isinstance(config_data, dict):
                            validation_errors.append(
                                f"Row {row_num}: asset_id_to_uid_map_json is not a valid JSON object")
                        elif not config_data:  # Empty object
                            validation_errors.append(f"Row {row_num}: asset_id_to_uid_map_json is empty")

                    except json.JSONDecodeError as e:
                        validation_errors.append(f"Row {row_num}: Invalid JSON in asset_id_to_uid_map_json - {e}")
                    except Exception as e:
                        validation_errors.append(f"Row {row_num}: Error parsing asset_id_to_uid_map_json - {e}")

                        # Verify asset_id_to_uid_map_json JSON is parsable
                    try:
                        config_data = json.loads(asset_target_id_to_uid_map_json_str)

                        # Additional validation: check if it's a valid config response
                        if not isinstance(config_data, dict):
                            validation_errors.append(
                                f"Row {row_num}: asset_target_id_to_uid_map_json is not a valid JSON object")
                        elif not config_data:  # Empty object
                            validation_errors.append(f"Row {row_num}: asset_target_id_to_uid_map_json is empty")

                    except json.JSONDecodeError as e:
                        validation_errors.append(
                            f"Row {row_num}: Invalid JSON in asset_target_id_to_uid_map_json - {e}")
                    except Exception as e:
                        validation_errors.append(f"Row {row_num}: Error parsing asset_target_id_to_uid_map_json - {e}")
                # Report validation results
                if verbose_mode or not quiet_mode:
                    if validation_errors:
                        print(f"❌ CSV validation failed with {len(validation_errors)} errors:")
                        for error in validation_errors[:10]:  # Show first 10 errors
                            print(f"   - {error}")
                        if len(validation_errors) > 10:
                            print(f"   ... and {len(validation_errors) - 10} more errors")
                        logger.error(f"CSV validation failed: {len(validation_errors)} errors found")
                    else:
                        print(f"✅ CSV validation successful: {row_count} data rows read")
                        print(f"   Header: {header}")
                        print(f"   Expected columns: target_uid, asset_config_json")
                        print(f"   All JSON entries are valid and parseable")
                        logger.info(f"CSV validation successful: {row_count} rows validated")

        except FileNotFoundError:
            error_msg = f"Output file not found: {output_path}"
            if verbose_mode or not quiet_mode:
                print(f"❌ {error_msg}")
            logger.error(error_msg)
        except PermissionError:
            error_msg = f"Permission denied reading output file: {output_path}"
            if verbose_mode or not quiet_mode:
                print(f"❌ {error_msg}")
            logger.error(error_msg)
        except Exception as e:
            error_msg = f"CSV verification failed: {e}"
            if verbose_mode or not quiet_mode:
                print(f"❌ {error_msg}")
            logger.error(error_msg)

        # Print summary
        if verbose_mode or not quiet_mode:
            print("\n" + "=" * 80)
            print("ASSET CONFIG EXPORT COMPLETED (PARALLEL MODE)")
            print("=" * 80)
            print(f"Output file: {output_file}")
            print(f"Total assets processed: {len(asset_data)}")
            print(f"Successful: {successful}")
            print(f"Failed: {failed}")
            print(f"Failed anomaly configs export: {anomaly_config_export_failed}")

            print(f"Total assets processed: {total_assets_processed}")
            print(f"Threads used: {num_threads}")

            for result in thread_results:
                thread_name = thread_names[result['thread_id']] if result['thread_id'] < len(
                    thread_names) else f"Thread {result['thread_id']}"
                print(
                    f"{thread_name}: {result['successful']} successful, {result['failed']} failed, {result['total_assets']} assets")

            print("=" * 80)
        else:
            print(
                f"✅ Asset config export completed: {successful} successful, {failed} failed, {anomaly_config_export_failed} anomaly configs failed")
            print(f"Output written to: {output_file}")

    except Exception as e:
        error_msg = f"Error in parallel asset-config-export: {e}"
        if verbose_mode or not quiet_mode:
            print(f"❌ {error_msg}")
        logger.error(error_msg)
        raise


def execute_asset_config_import_parallel(csv_file: str, client, logger: logging.Logger, quiet_mode: bool = False,
                                         verbose_mode: bool = False, dry_run: bool = False, max_threads: int = 5):
    """Execute the asset-config-import command with parallel processing.

    Args:
        csv_file: Path to the CSV file containing target_uid and config_json
        client: API client instance
        logger: Logger instance
        quiet_mode: Whether to show progress bars
        verbose_mode: Whether to enable verbose logging
        dry_run: If True, print the request and payload instead of making the API call

    """
    try:
        # Check if CSV file exists
        csv_path = Path(csv_file)
        if not csv_path.exists():
            error_msg = f"CSV file does not exist: {csv_file}"
            print(f"❌ {error_msg}")
            print(f"💡 Please run 'policy-xfr' first to generate the asset-config-import-ready.csv file")
            if globals.GLOBAL_OUTPUT_DIR:
                print(f"   Expected location: {globals.GLOBAL_OUTPUT_DIR}/asset-import/asset-config-import-ready.csv")
            else:
                print(
                    f"   Expected location: adoc-migration-toolkit-YYYYMMDDHHMM/asset-import/asset-config-import-ready.csv")
            logger.error(error_msg)
            return

        assets_mapped_csv_file = str(globals.GLOBAL_OUTPUT_DIR / "asset-import" / "asset-merged-all.csv")
        assets_mapping = get_source_to_target_asset_id_map(assets_mapped_csv_file, logger)

        # Read CSV data
        asset_data = []
        with open(csv_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            # Skip header row if it exists
            header = next(reader, None)
            if header and len(header) >= 2:
                if 'target_uid' in header[0].lower() or 'config_json' in header[1].lower():
                    pass  # This is a header row, skip
                else:
                    # This might be data, add it back
                    if header[0].strip() and header[1].strip():
                        asset_data.append({
                            'target_uid': header[0].strip(),
                            'config_json': header[1].strip()
                        })
            for row in reader:
                if len(row) >= 2:  # Ensure we have at least target_uid and config_json
                    target_uid = row[0].strip()
                    config_json = row[1].strip()
                    asset_profile_anomaly_config_json = row[2].strip()
                    asset_id_to_uid_map_json = row[3].strip()
                    asset_target_id_to_uid_map_json = row[4].strip()
                    if target_uid and config_json:  # Skip empty rows
                        asset_data.append({
                            'target_uid': target_uid,
                            'config_json': config_json,
                            'asset_profile_anomaly_config_json': asset_profile_anomaly_config_json,
                            'asset_id_to_uid_map_json': asset_id_to_uid_map_json,
                            'asset_target_id_to_uid_map_json': asset_target_id_to_uid_map_json,
                        })

        if not asset_data:
            print("❌ No valid asset data found in CSV file")
            logger.warning("No valid asset data found in CSV file")
            return

        if not quiet_mode:
            print(f"📊 Found {len(asset_data)} assets to process")

        # Determine number of threads (max 5, min 1)
        num_threads = min(max_threads, max(1, len(asset_data)))

        if not quiet_mode:
            print(f"Using {num_threads} threads for processing")

        # Thread names for progress indicators
        thread_names = [
            "Rocket Thread     ",
            "Lightning Thread  ",
            "Unicorn Thread    ",
            "Dragon Thread     ",
            "Shark Thread      "
        ]

        # Thread-safe counters
        successful = 0
        failed = 0
        total_assets_processed = 0
        lock = threading.Lock()
        all_results = []

        # Create progress bar if in quiet mode
        if quiet_mode and not verbose_mode:
            pbar = tqdm(total=len(asset_data), desc="Processing assets", colour='green')

        def process_asset_chunk(thread_id, start_index, end_index):
            nonlocal successful, failed, total_assets_processed
            thread_successful = 0
            thread_failed = 0
            thread_results = []

            # Create progress bar for this thread if in quiet mode
            if quiet_mode and not verbose_mode:
                thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                thread_pbar = tqdm(
                    total=end_index - start_index,
                    desc=thread_name,
                    colour='green',
                    position=thread_id,
                    leave=False
                )

            for i in range(start_index, end_index):
                asset = asset_data[i]
                target_uid = asset['target_uid']
                config_json = asset['config_json']
                # profile_anomaly_config_json = asset['asset_profile_anomaly_config_json']

                try:
                    # Step 1: Get asset ID from target_uid
                    if verbose_mode:
                        print(f"\n[Thread {thread_id}] 🔍 Processing asset {i + 1}/{len(asset_data)}: {target_uid}")
                        print(f"   GET /catalog-server/api/assets?uid={target_uid}")

                    response = client.make_api_call(
                        endpoint=f'/catalog-server/api/assets?uid={target_uid}',
                        method='GET',
                        use_target_auth=True,
                        use_target_tenant=True
                    )

                    if verbose_mode:
                        print(f"   Response: {response}")

                    if not response or 'data' not in response or not response['data']:
                        error_msg = f"No asset found for UID: {target_uid}"
                        if verbose_mode:
                            print(f"   ❌ {error_msg}")
                        thread_failed += 1
                        thread_results.append({'target_uid': target_uid, 'error': error_msg, 'status': 'failed'})
                        continue

                    asset_id = response['data'][0]['id']

                    if verbose_mode:
                        print(f"   Asset ID: {asset_id}")
                        print(f"   PUT /catalog-server/api/assets/{asset_id}/config")
                        print(f"   Data: {config_json}")

                    config_data = json.loads(config_json)
                    transformed_config = transform_config_json_to_asset_configuration(config_data, asset_id)

                    if dry_run:
                        print(
                            f"\n[DRY RUN][Thread {thread_id}] Would send PUT to /catalog-server/api/assets/{asset_id}/config")
                        print(f"[DRY RUN][Thread {thread_id}] Payload:")
                        print(json.dumps(transformed_config, indent=2))
                        thread_successful += 1
                        thread_results.append({'target_uid': target_uid, 'asset_id': asset_id, 'status': 'dry_run'})
                        continue

                    config_response = client.make_api_call(
                        endpoint=f'/catalog-server/api/assets/{asset_id}/config',
                        method='PUT',
                        json_payload=transformed_config,
                        use_target_auth=True,
                        use_target_tenant=True
                    )

                    if verbose_mode:
                        print(f"   Config Response: {config_response}")

                    if config_response:
                        thread_successful += 1
                        thread_results.append({'target_uid': target_uid, 'asset_id': asset_id, 'status': 'success'})
                        if verbose_mode:
                            print(f"   ✅ Successfully updated config for {target_uid}")
                    else:
                        error_msg = f"Failed to update config for asset ID: {asset_id}"
                        if verbose_mode:
                            print(f"   ❌ {error_msg}")
                        thread_failed += 1
                        thread_results.append(
                            {'target_uid': target_uid, 'asset_id': asset_id, 'status': 'failed', 'error': error_msg})

                    import_profile_anomaly_configs(asset_data, assets_mapping, client, logger, quiet_mode, verbose_mode,
                                                   dry_run)
                except Exception as e:
                    error_msg = f"Error processing {target_uid}: {str(e)}"
                    if verbose_mode:
                        print(f"   ❌ {error_msg}")
                    thread_failed += 1
                    thread_results.append({'target_uid': target_uid, 'status': 'failed', 'error': error_msg})

                # Update progress bar
                if quiet_mode and not verbose_mode:
                    thread_pbar.update(1)

            # Close thread progress bar
            if quiet_mode and not verbose_mode:
                thread_pbar.close()

            # Update global counters
            with lock:
                successful += thread_successful
                failed += thread_failed
                total_assets_processed += (end_index - start_index)
                all_results.extend(thread_results)

            return {
                'thread_id': thread_id,
                'successful': thread_successful,
                'failed': thread_failed,
                'total_assets': end_index - start_index,
                'results': thread_results
            }

        # Calculate chunk sizes
        chunk_size = len(asset_data) // num_threads
        remainder = len(asset_data) % num_threads

        # Create and start threads
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            start_index = 0

            for i in range(num_threads):
                # Calculate end index for this thread
                end_index = start_index + chunk_size
                if i < remainder:  # Distribute remainder across first few threads
                    end_index += 1

                if start_index < end_index:  # Only create thread if there's work to do
                    future = executor.submit(process_asset_chunk, i, start_index, end_index)
                    futures.append(future)

                start_index = end_index

            # Collect results
            thread_results = []
            for future in as_completed(futures):
                thread_results.append(future.result())

        # Close main progress bar
        if quiet_mode and not verbose_mode:
            pbar.close()

        # Print summary
        print("\n" + "=" * 60)
        print("ASSET CONFIG IMPORT SUMMARY")
        print("=" * 60)
        print(f"Total assets processed: {total_assets_processed}")
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")

        if thread_results:
            print(f"\nPer-thread breakdown:")
            for result in thread_results:
                thread_name = thread_names[result['thread_id']] if result['thread_id'] < len(
                    thread_names) else f"Thread {result['thread_id']}"
                print(
                    f"{thread_name}: {result['successful']} successful, {result['failed']} failed, {result['total_assets']} assets")

        if failed > 0:
            print(f"\nFailed assets:")
            for result in all_results:
                if result['status'] == 'failed':
                    print(f"  - {result['target_uid']}: {result.get('error', 'Unknown error')}")

        print("=" * 60)

        if failed == 0:
            print("✅ Asset config import completed successfully!")
        else:
            print(f"⚠️  Asset config import completed with {failed} failures. Check the details above.")

    except Exception as e:
        error_msg = f"Error executing asset config import: {e}"
        print(f"❌ {error_msg}")
        logger.error(error_msg)


def transform_config_json_to_asset_configuration(config_data_dict: dict, asset_id: int) -> dict:
    """Transform the raw config_data JSON from CSV into the required assetConfiguration format.
    
    Args:
        config_data_dict: The raw configuration data from the CSV
        asset_id: The asset ID to include in the configuration
        
    Returns:
        dict: The transformed configuration in assetConfiguration format
    """
    # Define the predefined structure fields that should be removed from CSV data
    predefined_fields = {
        "assetId", "profilingType", "scheduled", "schedule", "timeZone",
        "markerConfiguration", "freshnessColumnInfo", "persistencePath",
        "team", "owner", "createdAt", "updatedAt", "notificationChannels",
        "sparkResourceConfig", "isUserMarkedReference", "isReferenceCheckValid",
        "assetReferenceValidationJob", "referenceCheckConfiguration",
        "partitionConfiguration", "isPatternProfile", "patternConfiguration",
        "columnLevel", "profileAnomalyTrainingWindowMinimumInDays",
        "cadenceAnomalyTrainingWindowMinimumInDays", "profileAnomalyModelSensitivity",
        "cadenceAnomalyModelSensitivity", "resourceStrategyType",
        "selectedResourceInventory", "autoRetryEnabled"
    }

    # print(f"config_data::: {json.dumps(config_data_dict, indent=2)}")
    # Update assetId in the main assetConfiguration
    if "assetConfiguration" in config_data_dict:
        config_data_dict["assetConfiguration"]["assetId"] = asset_id
        # Update assetId in freshnessColumnInfo if it exists
        freshness = config_data_dict["assetConfiguration"].get("freshnessColumnInfo")
        if freshness and "assetId" in freshness:
            freshness["assetId"] = asset_id
    # print(f"transform_config_json_to_asset_configuration : {json.dumps(config_data_dict, indent=2)}")

    return config_data_dict














def execute_transform_and_merge(string_transforms: dict, quiet_mode: bool, verbose_mode: bool, logger: logging.Logger):
    """Execute the transform-and-merge command.
    
    Args:
        string_transforms: Dictionary of string transformations {source: target}
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
        logger: Logger instance
    """
    try:
        # Determine input and output directories
        if globals.GLOBAL_OUTPUT_DIR:
            asset_export_dir = globals.GLOBAL_OUTPUT_DIR / "asset-export"
        else:
            # Look for the most recent adoc-migration-toolkit directory
            current_dir = Path.cwd()
            toolkit_dirs = [d for d in current_dir.iterdir() if d.is_dir() and d.name.startswith("adoc-migration-toolkit-")]
            
            if not toolkit_dirs:
                logger.error("No adoc-migration-toolkit directory found")
                print("❌ No adoc-migration-toolkit directory found")
                print("💡 Please run asset-list-export first to generate the required CSV files")
                return
            
            toolkit_dirs.sort(key=lambda x: x.stat().st_ctime, reverse=True)
            latest_toolkit_dir = toolkit_dirs[0]
            asset_export_dir = latest_toolkit_dir / "asset-export"
        
        # Define file paths
        source_file = asset_export_dir / "asset-all-source-export.csv"
        target_file = asset_export_dir / "asset-all-target-export.csv"
        
        # Create asset-import directory for output
        asset_import_dir = asset_export_dir.parent / "asset-import"
        asset_import_dir.mkdir(parents=True, exist_ok=True)
        output_file = asset_import_dir / "asset-merged-all.csv"
        
        # Check if required files exist
        if not source_file.exists():
            logger.error(f"Source file not found: {source_file}")
            print(f"❌ Source file not found: {source_file}")
            print("💡 Please run 'asset-list-export' first to generate the source file")
            return
        
        if not target_file.exists():
            logger.error(f"Target file not found: {target_file}")
            print(f"❌ Target file not found: {target_file}")
            print("💡 Please run 'asset-list-export --target' first to generate the target file")
            return
        
        if not quiet_mode:
            print(f"📁 Asset Export Directory: {asset_export_dir}")
            print(f"📄 Source file: {source_file}")
            print(f"📄 Target file: {target_file}")
            print(f"📄 Output file: {output_file}")
            print(f"🔄 String transformations: {len(string_transforms)} transformations")
            for source, target in string_transforms.items():
                print(f"  '{source}' -> '{target}'")
            print("="*80)
        
        # Read source file
        source_data = []
        with open(source_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                source_data.append(row)

        if verbose_mode:
            print(f"📊 Read {len(source_data)} records from source file")
            
            # Show sample source records
            if source_data:
                print(f"📊 Sample source records:")
                for i, row in enumerate(source_data[:3]):
                    print(f"  {i+1}. target_uid: '{row['target_uid']}'")
        
        # Read target file
        target_data = {}
        with open(target_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Use source_uid as key for matching
                target_data[row['source_uid']] = row

        if verbose_mode:
            print(f"📊 Read {len(target_data)} records from target file")
            
            # Show sample target keys
            if target_data:
                print(f"📊 Sample target keys:")
                target_keys = list(target_data.keys())
                for i, key in enumerate(target_keys[:3]):
                    print(f"  {i+1}. source_uid: '{key}'")
        
        # Create temporary source file with transformed target_uid
        temp_source_file = asset_export_dir / "temp_source_transformed.csv"
        transformed_count = 0

        with open(temp_source_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['source_uid', 'source_id', 'target_uid', 'tags', 'asset_type']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for source_row in source_data:
                # Apply string transformations to target_uid (C -> T)
                original_target_uid = source_row['target_uid']
                transformed_target_uid = original_target_uid
                # Apply all string replacements

                for source_str, target_str in string_transforms.items():
                    if source_str in transformed_target_uid:
                        transformed_target_uid = transformed_target_uid.replace(source_str, target_str)
                        transformed_count += 1
                        if verbose_mode:
                            print(f"🔄 Transformed: '{original_target_uid}' -> '{transformed_target_uid}'")
                            print(f"   Applied: '{source_str}' -> '{target_str}'")

                # Write transformed record to temporary file
                temp_row = {
                    'source_uid': source_row['source_uid'],  # A
                    'source_id': source_row['source_id'],    # B
                    'target_uid': transformed_target_uid,    # T (transformed C)
                    'tags': source_row['tags'],               # D
                    'asset_type': source_row['asset_type']    # E
                }
                writer.writerow(temp_row)
        if verbose_mode:
            print(f"📄 Created temporary source file: {temp_source_file}")
            print(f"🔄 Applied {transformed_count} transformations")
        
        # Read temporary source file and target file for matching
        merged_data = []
        matched_count = 0
        
        with open(temp_source_file, 'r', newline='', encoding='utf-8') as f:
            temp_reader = csv.DictReader(f)
            for temp_row in temp_reader:
                # T from temp source file
                transformed_target_uid = temp_row['target_uid']
                
                if verbose_mode:
                    print(f"🔍 Looking for match: '{transformed_target_uid}' in target data")
                
                # Look for exact match with E (source_uid) in target file
                if transformed_target_uid in target_data:
                    target_row = target_data[transformed_target_uid]
                    matched_count += 1
                    
                    # Create merged record according to specification
                    merged_row = {
                        'source_id': temp_row['source_id'],      # B
                        'source_uid': temp_row['source_uid'],    # A
                        'target_id': target_row['source_id'],    # F
                        'target_uid': target_row['source_uid'],  # E
                        'tags': temp_row['tags'],                # D
                        'source_asset_type': temp_row['asset_type']     # E
                    }
                    merged_data.append(merged_row)
                    
                    if verbose_mode:
                        print(f"✅ Matched: {temp_row['source_uid']} -> {transformed_target_uid}")
                else:
                    if verbose_mode:
                        print(f"❌ No match found for transformed UID: {transformed_target_uid}")
        print(f"merged_data : {merged_data}")
        # Clean up temporary file
        try:
            temp_source_file.unlink()
            if verbose_mode:
                print(f"🗑️  Cleaned up temporary file: {temp_source_file}")
        except Exception as e:
            if verbose_mode:
                print(f"⚠️  Could not delete temporary file {temp_source_file}: {e}")
        
        # Write merged data to output file
        if merged_data:
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                fieldnames = ['source_id', 'source_uid', 'target_id', 'target_uid', 'tags', 'source_asset_type']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(merged_data)
            execute_transform_and_merge_sql_view(quiet_mode, verbose_mode, logger)
            if not quiet_mode:
                print("\n" + "="*80)
                print("TRANSFORM AND MERGE COMPLETED")
                print("="*80)
                print(f"Source file:          {source_file}")
                print(f"Target file:          {target_file}")
                print(f"Output file:          {output_file}")
                print(f"Source records:       {len(source_data)}")
                print(f"Target records:       {len(target_data)}")
                print(f"Transformations applied: {transformed_count}")
                print(f"Matched records:      {matched_count}")
                print(f"Merged records:       {len(merged_data)}")
                print(f"Match rate:           {(matched_count/len(source_data)*100):.1f}%")
                print("="*80)
                
                if matched_count < len(source_data):
                    print("⚠️  Some source records could not be matched with target records")
                    print("💡 This may be due to:")
                    print("   • String transformations not finding exact matches")
                    print("   • Target environment missing some assets")
                    print("   • Different asset UID formats between environments")
                else:
                    print("✅ All source records were successfully matched and merged!")
        else:
            logger.warning("No records were merged")
            print("❌ No records were merged")
            print("💡 Check that:")
            print("   • String transformations are correct")
            print("   • Target file contains matching UIDs")
            print("   • Both files have the expected format")
        
    except Exception as e:
        logger.error(f"Error executing transform-and-merge: {e}")
        print(f"❌ Error executing transform-and-merge: {e}")
        if verbose_mode:
            import traceback
            traceback.print_exc()


def import_profile_anomaly_configs(asset_data, assets_mapping, client, logger, quiet_mode=False, verbose_mode=False, dry_run=False):
    """
    Import profile anomaly configs for a list of assets.
    Args:
        asset_data: List of asset dicts with keys including 'target_uid', 'config_json', 'asset_profile_anomaly_config_json', etc.
        assets_mapping: Dict mapping source_id to target_id.
        client: API client instance.
        logger: Logger instance.
        quiet_mode: Whether to suppress output.
        verbose_mode: Whether to enable verbose output.
        dry_run: If True, do not make actual API calls.
    """
    for asset in asset_data:
        target_uid = asset['target_uid']
        config_json = asset['config_json']
        profile_anomaly_config_json = asset['asset_profile_anomaly_config_json']
        try:
            if profile_anomaly_config_json and profile_anomaly_config_json.strip() and profile_anomaly_config_json.strip() not in ('{}', 'null'):
                if verbose_mode:
                    print("Enter profile_anomaly_config_json")
                profile_anomaly_config_dict = json.loads(profile_anomaly_config_json)

                # Get asset_id from target_uid
                response = client.make_api_call(
                    endpoint=f'/catalog-server/api/assets?uid={target_uid}',
                    method='GET',
                    use_target_auth=True,
                    use_target_tenant=True
                )
                if not response or 'data' not in response or not response['data']:
                    error_msg = f"No asset found for UID: {target_uid}"
                    if verbose_mode:
                        print(f"   ❌ {error_msg}")
                    logger.error(error_msg)
                    continue
                asset_id = response['data'][0]['id']

                asset_profile_anomaly_response = client.make_api_call(
                    endpoint=f'/catalog-server/api/rules/profile-anomaly/byAsset/{asset_id}',
                    method='GET',
                    json_payload=json.loads(config_json),
                    use_target_auth=True,
                    use_target_tenant=True
                )
                rule_id = asset_profile_anomaly_response['rule']['id']
                if verbose_mode:
                    print(f" AssetId : {asset_id} Rule ID: {rule_id} source ruleid :{profile_anomaly_config_dict['rule']['id']}")
                # Extract monitorColumns from details -> items
                source_items = profile_anomaly_config_dict['details']['items']
                each_item_monitor_columns = []
                for each_item in source_items:
                    for monitorColumn in each_item['monitorColumns']:
                        target_info = assets_mapping.get(monitorColumn)
                        if target_info is not None:
                            each_item_monitor_columns.append(int(target_info["target_id"]))
                        else:
                            # Optionally handle missing mapping, e.g., log or skip
                            pass

                items = asset_profile_anomaly_response['details']['items']
                for item in items:
                    item["monitorColumns"] = each_item_monitor_columns

                # Remove 'details' key
                asset_profile_anomaly_response.pop("details", None)
                # Add 'items' with flattened monitorColumns
                asset_profile_anomaly_response["items"] = items
                asset_profile_anomaly_response_json = json.dumps(asset_profile_anomaly_response)
                if verbose_mode:
                    print(f"endpoint /catalog-server/api/rules/profile-anomaly/{rule_id}")
                    print(f"asset_profile_anomaly_response_json = {asset_profile_anomaly_response_json}")
                if not dry_run:
                    client.make_api_call(
                        endpoint=f'/catalog-server/api/rules/profile-anomaly/{rule_id}',
                        method='PUT',
                        json_payload=asset_profile_anomaly_response,
                        use_target_auth=True,
                        use_target_tenant=True
                    )
                # print(f"asset_profile_anomaly_response : {asset_profile_anomaly_response}")
                if verbose_mode:
                    print(f"............Rule {rule_id} done ............")
        except Exception as e:
            error_msg = f"Error processing {target_uid}: {str(e)}"
            if verbose_mode:
                print(f"   ❌ {error_msg}")
            logger.error(error_msg)


def execute_transform_and_merge_sql_view(quiet_mode: bool, verbose_mode: bool, logger: logging.Logger):
    """Generate asset-merged-all_sql_views.csv with SQL views from asset-all-source-export.csv not present in asset-merged-all.csv."""
    try:
        # Determine input and output directories
        if globals.GLOBAL_OUTPUT_DIR:
            asset_export_dir = globals.GLOBAL_OUTPUT_DIR / "asset-export"
        else:
            # Look for the most recent adoc-migration-toolkit directory
            current_dir = Path.cwd()
            toolkit_dirs = [d for d in current_dir.iterdir() if d.is_dir() and d.name.startswith("adoc-migration-toolkit-")]
            if not toolkit_dirs:
                logger.error("No adoc-migration-toolkit directory found")
                print("❌ No adoc-migration-toolkit directory found")
                print("💡 Please run asset-list-export first to generate the required CSV files")
                return
            toolkit_dirs.sort(key=lambda x: x.stat().st_ctime, reverse=True)
            latest_toolkit_dir = toolkit_dirs[0]
            asset_export_dir = latest_toolkit_dir / "asset-export"

        # Define file paths
        source_file = asset_export_dir / "asset-all-source-export.csv"
        asset_import_dir = asset_export_dir.parent / "asset-import"
        asset_import_dir.mkdir(parents=True, exist_ok=True)
        merged_file = asset_import_dir / "asset-merged-all.csv"
        output_file = asset_import_dir / "asset-merged-all_sql_views.csv"

        # Check if required files exist
        if not source_file.exists():
            logger.error(f"Source file not found: {source_file}")
            print(f"❌ Source file not found: {source_file}")
            print("💡 Please run 'asset-list-export' first to generate the source file")
            return
        if not merged_file.exists():
            logger.error(f"Merged file not found: {merged_file}")
            print(f"❌ Merged file not found: {merged_file}")
            print("💡 Please run 'transform-and-merge' first to generate the merged file")
            return

        if not quiet_mode:
            print(f"📁 Asset Export Directory: {asset_export_dir}")
            print(f"📄 Source file: {source_file}")
            print(f"📄 Merged file: {merged_file}")
            print(f"📄 Output file: {output_file}")
            print("="*80)

        # Read merged file to get set of merged source_uids
        merged_source_uids = set()
        with open(merged_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Use source_uid from merged file
                merged_source_uids.add(row['source_uid'])
        if verbose_mode:
            print(f"🔍 Found {len(merged_source_uids)} merged source_uids")

        # Read source file and collect rows not in merged_source_uids
        unmatched_rows = []
        with open(source_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            for row in reader:
                if row['source_uid'] not in merged_source_uids:
                    unmatched_rows.append(row)
        if verbose_mode:
            print(f"🔍 Found {len(unmatched_rows)} unmatched SQL view rows")

        # Write unmatched rows to output file (filter for SQL_VIEW)
        sql_view_rows = [row for row in unmatched_rows if row.get('asset_type') == 'SQL_VIEW' or row.get('source_asset_type') == 'SQL_VIEW']
        if sql_view_rows:
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(sql_view_rows)
            if not quiet_mode:
                print("\n" + "="*80)
                print("SQL VIEW DIFFERENCE EXPORT COMPLETED")
                print("="*80)
                print(f"Source file:   {source_file}")
                print(f"Merged file:   {merged_file}")
                print(f"Output file:   {output_file}")
                print(f"Unmatched SQL views: {len(sql_view_rows)}")
                print(f"Total source records: {sum(1 for _ in open(source_file, 'r', encoding='utf-8')) - 1}")
                print("="*80)
        else:
            logger.info("No unmatched SQL views found.")
            print("✅ All SQL views in source are present in merged file or none found. No output generated.")
    except Exception as e:
        logger.error(f"Error executing transform-and-merge-sql-view: {e}")
        print(f"❌ Error executing transform-and-merge-sql-view: {e}")
        if verbose_mode:
            import traceback
            traceback.print_exc()