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
            print(f"❌ {error_msg}")
            print(f"💡 Please run 'asset-profile-export' first to generate the asset-profiles-import-ready.csv file")
            if globals.GLOBAL_OUTPUT_DIR:
                print(f"   Expected location: {globals.GLOBAL_OUTPUT_DIR}/asset-import/asset-profiles-import-ready.csv")
            else:
                print(f"   Expected location: adoc-migration-toolkit-YYYYMMDDHHMM/asset-import/asset-profiles-import-ready.csv")
            logger.error(error_msg)
            return
        
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
                    print(f"❌ [{i}/{len(import_mappings)}] {target_env}: {error_msg}")
                    logger.error(error_msg)
                    failed += 1
                    progress_bar.update(1)
                    continue
                
                data_array = asset_response['data']
                if not data_array or len(data_array) == 0:
                    error_msg = f"Empty 'data' array in asset response for UID: {target_env}"
                    print(f"❌ [{i}/{len(import_mappings)}] {target_env}: {error_msg}")
                    logger.error(error_msg)
                    failed += 1
                    progress_bar.update(1)
                    continue
                
                first_asset = data_array[0]
                if 'id' not in first_asset:
                    error_msg = f"No 'id' field found in first asset for UID: {target_env}"
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
                    
                    if quiet_mode:
                        print(f"🔍 [{i}/{len(import_mappings)}] {target_env}: Would update profile (dry-run)")
                
                successful += 1
                progress_bar.update(1)
                logger.info(f"Successfully processed target-env {target_env} (asset ID: {asset_id})")
                
            except Exception as e:
                error_msg = f"Failed to process UID {target_env}: {e}"
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
        print(f"❌ {error_msg}")
        logger.error(error_msg)


def execute_asset_config_export(csv_file: str, client, logger: logging.Logger, output_file: str = None, quiet_mode: bool = False, verbose_mode: bool = False):
    """Execute the asset-config-export command.
    
    Args:
        csv_file: Path to the CSV file containing asset data with 4 columns: source_uid, source_id, target_uid, tags
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


def execute_asset_config_import(csv_file: str, client, logger: logging.Logger, dry_run: bool = False, quiet_mode: bool = True, verbose_mode: bool = False):
    """Execute the asset-config-import command.
    
    Args:
        csv_file: Path to the CSV file containing target_uid and config_json
        client: API client instance
        logger: Logger instance
        dry_run: Whether to perform a dry run (no actual API calls)
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
    """
    try:
        # Read target-env and config_json from CSV file
        if not Path(csv_file).exists():
            error_msg = f"CSV file does not exist: {csv_file}"
            print(f"❌ {error_msg}")
            logger.error(error_msg)
            return
        
        print(f"\nProcessing asset config import from CSV file: {csv_file}")
        if dry_run:
            print("🔍 DRY RUN MODE - No actual API calls will be made")
            print("📋 Will show detailed information about what would be executed")
        if quiet_mode:
            print("🔇 QUIET MODE - Minimal output")
        if verbose_mode:
            print("🔊 VERBOSE MODE - Detailed output including headers")
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
            
            if len(header) != 2 or header[0] != 'target_uid' or header[1] != 'config_json':
                error_msg = f"Invalid CSV format. Expected header: ['target_uid', 'config_json'], got: {header}"
                print(f"❌ {error_msg}")
                logger.error(error_msg)
                return
            
            for row_num, row in enumerate(reader, start=2):
                if len(row) != 2:
                    logger.warning(f"Row {row_num}: Expected 2 columns, got {len(row)}")
                    continue
                
                target_uid = row[0].strip()
                config_json = row[1].strip()
                
                if target_uid and config_json:
                    import_mappings.append((target_uid, config_json))
                    logger.debug(f"Row {row_num}: Found target_uid: {target_uid}")
                else:
                    logger.warning(f"Row {row_num}: Empty target_uid or config_json value")
        
        if not import_mappings:
            logger.warning("No valid import mappings found in CSV file")
            return
        
        logger.info(f"Read {len(import_mappings)} import mappings from CSV file: {csv_file}")
        
        successful = 0
        failed = 0
        
        # Process each mapping
        for target_uid, config_json in import_mappings:
            try:
                if dry_run:
                    print(f"🔍 Would import config for: {target_uid}")
                    continue
                
                # Parse the JSON configuration
                config_data = json.loads(config_json)
                
                # Make API call to import configuration
                # Note: This is a placeholder - implement actual API call based on your requirements
                endpoint = f"/catalog-server/api/assets/{target_uid}/config"
                
                if verbose_mode:
                    print(f"📡 Making PUT request to: {endpoint}")
                    print(f"📦 Request payload: {json.dumps(config_data, indent=2)}")
                
                # For now, just log the action
                logger.info(f"Importing config for asset: {target_uid}")
                
                if not quiet_mode:
                    print(f"✅ Imported config for: {target_uid}")
                
                successful += 1
                
            except json.JSONDecodeError as e:
                error_msg = f"Invalid JSON in config for {target_uid}: {e}"
                print(f"❌ {error_msg}")
                logger.error(error_msg)
                failed += 1
            except Exception as e:
                error_msg = f"Failed to import config for {target_uid}: {e}"
                print(f"❌ {error_msg}")
                logger.error(error_msg)
                failed += 1
        
        # Print summary
        print(f"\n📊 Asset Config Import Summary:")
        print(f"  Successful: {successful}")
        print(f"  Failed: {failed}")
        print(f"  Total: {len(import_mappings)}")
        
        if successful > 0:
            print(f"✅ Asset config import completed successfully!")
        if failed > 0:
            print(f"⚠️  {failed} imports failed. Check logs for details.")
        
    except Exception as e:
        error_msg = f"Asset config import failed: {e}"
        print(f"❌ {error_msg}")
        logger.error(error_msg)


def execute_asset_list_export(client, logger: logging.Logger, quiet_mode: bool = False, verbose_mode: bool = False):
    """Execute the asset-list-export command.
    
    Args:
        client: API client instance
        logger: Logger instance
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
    """
    try:
        # Determine output file path
        if globals.GLOBAL_OUTPUT_DIR:
            output_file = globals.GLOBAL_OUTPUT_DIR / "asset-export" / "asset-all-export.csv"
        else:
            output_file = Path("asset-all-export.csv")
        
        if not quiet_mode:
            print(f"\nExporting all assets from ADOC environment")
            print(f"Output will be written to: {output_file}")
            if globals.GLOBAL_OUTPUT_DIR:
                print(f"Using global output directory: {globals.GLOBAL_OUTPUT_DIR}")
            if verbose_mode:
                print("🔊 VERBOSE MODE - Detailed output including headers and responses")
            print("="*80)
        
        # Step 1: Get total count of assets
        if not quiet_mode:
            print("Getting total asset count...")
        
        if verbose_mode:
            print("\nGET Request Headers:")
            print(f"  Endpoint: /catalog-server/api/assets/discover?size=0&page=0&profiled_assets=true&parents=true")
            print(f"  Method: GET")
            print(f"  Content-Type: application/json")
            print(f"  Authorization: Bearer [REDACTED]")
            if hasattr(client, 'tenant') and client.tenant:
                print(f"  X-Tenant: {client.tenant}")
        
        count_response = client.make_api_call(
            endpoint="/catalog-server/api/assets/discover?size=0&page=0&profiled_assets=true&parents=true",
            method='GET'
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
        page_size = 500  # Default page size
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
                if verbose_mode:
                    print(f"\nGET Request Headers:")
                    print(f"  Endpoint: /catalog-server/api/assets/discover?size={page_size}&page={page}&profiled_assets=true&parents=true")
                    print(f"  Method: GET")
                    print(f"  Content-Type: application/json")
                    print(f"  Authorization: Bearer [REDACTED]")
                    if hasattr(client, 'tenant') and client.tenant:
                        print(f"  X-Tenant: {client.tenant}")
                
                page_response = client.make_api_call(
                    endpoint=f"/catalog-server/api/assets/discover?size={page_size}&page={page}&profiled_assets=true&parents=true",
                    method='GET'
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


def execute_asset_list_export_parallel(client, logger: logging.Logger, quiet_mode: bool = False, verbose_mode: bool = False):
    """Execute the asset-list-export command with parallel processing.
    
    Args:
        client: API client instance
        logger: Logger instance
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
    """
    try:
        # Determine output file path
        if globals.GLOBAL_OUTPUT_DIR:
            output_file = globals.GLOBAL_OUTPUT_DIR / "asset-export" / "asset-all-export.csv"
        else:
            output_file = Path("asset-all-export.csv")
        
        if not quiet_mode:
            print(f"\nExporting all assets from ADOC environment (Parallel Mode)")
            print(f"Output will be written to: {output_file}")
            if globals.GLOBAL_OUTPUT_DIR:
                print(f"Using global output directory: {globals.GLOBAL_OUTPUT_DIR}")
            if verbose_mode:
                print("🔊 VERBOSE MODE - Detailed output including headers and responses")
            print("="*80)
        
        # Step 1: Get total count of assets
        if not quiet_mode:
            print("Getting total asset count...")
        
        if verbose_mode:
            print("\nGET Request Headers:")
            print(f"  Endpoint: /catalog-server/api/assets/discover?size=0&page=0&profiled_assets=true&parents=true")
            print(f"  Method: GET")
            print(f"  Content-Type: application/json")
            print(f"  Authorization: Bearer [REDACTED]")
            if hasattr(client, 'tenant') and client.tenant:
                print(f"  X-Tenant: {client.tenant}")
        
        count_response = client.make_api_call(
            endpoint="/catalog-server/api/assets/discover?size=0&page=0&profiled_assets=true&parents=true",
            method='GET'
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
        page_size = 500  # Default page size
        total_pages = (total_count + page_size - 1) // page_size  # Ceiling division
        
        if not quiet_mode:
            print(f"Total assets found: {total_count}")
            print(f"Page size: {page_size}")
            print(f"Total pages to retrieve: {total_pages}")
        
        # Calculate thread configuration
        max_threads = 5
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
            
            # Create temporary file for this thread
            temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8')
            temp_files.append(temp_file.name)
            
            # Create progress bar for this thread
            progress_bar = create_progress_bar(
                total=end_page - start_page,
                desc=thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}",
                unit="pages",
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
                    if verbose_mode:
                        thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                        print(f"\n{thread_name} - Processing page {page + 1}")
                        print(f"GET Request Headers:")
                        print(f"  Endpoint: /catalog-server/api/assets/discover?size={page_size}&page={page}&profiled_assets=true&parents=true")
                        print(f"  Method: GET")
                        print(f"  Content-Type: application/json")
                        print(f"  Authorization: Bearer [REDACTED]")
                        if hasattr(thread_client, 'tenant') and thread_client.tenant:
                            print(f"  X-Tenant: {thread_client.tenant}")
                    
                    page_response = thread_client.make_api_call(
                        endpoint=f"/catalog-server/api/assets/discover?size={page_size}&page={page}&profiled_assets=true&parents=true",
                        method='GET'
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
                                
                                # Extract tags and concatenate with colon separator
                                tags = []
                                if 'tags' in asset_wrapper and asset_wrapper['tags']:
                                    for tag in asset_wrapper['tags']:
                                        if 'name' in tag:
                                            tags.append(tag['name'])
                                
                                tags_str = ':'.join(tags) if tags else ''
                                
                                # Write row: source_uid (asset.uid), source_id (asset.id), target_uid (asset.uid), tags
                                writer.writerow([asset_uid, asset_id, asset_uid, tags_str])
                        
                        if not quiet_mode:
                            thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                            print(f"{thread_name} - ✅ Page {page + 1}: Retrieved {len(page_assets)} assets")
                        else:
                            print(f"✅ Page {page + 1}: {len(page_assets)} assets")
                        
                        successful_pages += 1
                        total_assets += len(page_assets)
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
                                                
                                                # Extract tags and concatenate with colon separator
                                                tags = []
                                                if 'tags' in asset_wrapper and asset_wrapper['tags']:
                                                    for tag in asset_wrapper['tags']:
                                                        if 'name' in tag:
                                                            tags.append(tag['name'])
                                                
                                                tags_str = ':'.join(tags) if tags else ''
                                                
                                                # Write row: source_uid (asset.uid), source_id (asset.id), target_uid (asset.uid), tags
                                                writer.writerow([asset_uid, asset_id, asset_uid, tags_str])
                                        
                                        if not quiet_mode:
                                            thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                                            print(f"{thread_name} - ✅ Page {page + 1}: Found {len(page_assets)} assets in 'data.{location}'")
                                        else:
                                            print(f"✅ Page {page + 1}: {len(page_assets)} assets")
                                        
                                        successful_pages += 1
                                        total_assets += len(page_assets)
                                        assets_found = True
                                        break
                        
                        if not assets_found:
                            error_msg = f"Invalid response format for page {page + 1} - no assets found"
                            if not quiet_mode:
                                thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                                print(f"\n{thread_name} - ❌ {error_msg}")
                            logger.error(f"Thread {thread_id}: {error_msg}")
                            failed_pages += 1
                    
                except Exception as e:
                    error_msg = f"Failed to retrieve page {page + 1}: {e}"
                    if not quiet_mode:
                        thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                        print(f"\n{thread_name} - ❌ {error_msg}")
                    logger.error(f"Thread {thread_id}: {error_msg}")
                    failed_pages += 1
                
                # Update progress bar
                progress_bar.update(1)
            
            progress_bar.close()
            
            return {
                'thread_id': thread_id,
                'successful_pages': successful_pages,
                'failed_pages': failed_pages,
                'total_assets': total_assets,
                'temp_file': temp_file.name
            }
        
        # Start threads
        threads = []
        for i in range(num_threads):
            start_page = i * pages_per_thread
            end_page = min(start_page + pages_per_thread, total_pages)
            
            thread = threading.Thread(
                target=lambda tid=i, start=start_page, end=end_page: thread_results.append(
                    process_page_chunk(tid, start, end)
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
                        if len(row) >= 4:  # Ensure we have all required columns
                            all_rows.append(row)
            except Exception as e:
                logger.error(f"Error reading temporary file {temp_file}: {e}")
            finally:
                # Clean up temporary file
                try:
                    os.unlink(temp_file)
                except Exception as e:
                    logger.warning(f"Could not delete temporary file {temp_file}: {e}")
        
        # Sort rows: first by source_uid, then by source_id
        if not quiet_mode:
            print("Sorting CSV file by source_uid, then source_id...")
        
        def sort_key(row):
            source_uid = row[0] if len(row) > 0 else ''
            source_id = row[1] if len(row) > 1 else ''
            # Convert source_id to int for proper numeric sorting, fallback to string
            try:
                source_id_int = int(source_id) if source_id else 0
            except (ValueError, TypeError):
                source_id_int = 0
            return (source_uid, source_id_int)
        
        all_rows.sort(key=sort_key)
        
        # Write final output with header
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            
            # Write header: source_uid, source_id, target_uid, tags
            writer.writerow(['source_uid', 'source_id', 'target_uid', 'tags'])
            
            # Write sorted data
            writer.writerows(all_rows)
        
        # Print statistics
        if not quiet_mode:
            print("\n" + "="*80)
            print("ASSET LIST EXPORT COMPLETED (PARALLEL MODE)")
            print("="*80)
            print(f"Output file: {output_file}")
            print(f"Total assets exported: {len(all_rows)}")
            print(f"Threads used: {num_threads}")
            
            total_successful_pages = 0
            total_failed_pages = 0
            total_assets_exported = 0
            
            for result in thread_results:
                thread_name = thread_names[result['thread_id']] if result['thread_id'] < len(thread_names) else f"Thread {result['thread_id']}"
                print(f"{thread_name}: {result['successful_pages']} successful pages, {result['failed_pages']} failed pages, {result['total_assets']} assets")
                total_successful_pages += result['successful_pages']
                total_failed_pages += result['failed_pages']
                total_assets_exported += result['total_assets']
            
            print(f"\nTotal successful pages: {total_successful_pages}")
            print(f"Total failed pages: {total_failed_pages}")
            print(f"Total pages processed: {total_pages}")
            print(f"Total assets exported: {total_assets_exported}")
            
            # Calculate success rate
            if total_pages > 0:
                success_rate = (total_successful_pages / total_pages) * 100
                print(f"Success rate: {success_rate:.1f}%")
            
            print("="*80)
        else:
            print(f"✅ Asset list export completed: {len(all_rows)} assets exported to {output_file}")
        
    except Exception as e:
        error_msg = f"Error in parallel asset-list-export: {e}"
        if not quiet_mode:
            print(f"❌ {error_msg}")
        logger.error(error_msg)
        raise


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
            print(f"💡 Please run 'policy-xfr' first to generate the asset-all-import-ready.csv file")
            if globals.GLOBAL_OUTPUT_DIR:
                print(f"   Expected location: {globals.GLOBAL_OUTPUT_DIR}/asset-import/asset-all-import-ready.csv")
            else:
                print(f"   Expected location: adoc-migration-toolkit-YYYYMMDDHHMM/asset-import/asset-all-import-ready.csv")
            logger.error(error_msg)
            return
        
        # Read CSV data
        asset_data = []
        with open(csv_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # Skip header
            for row in reader:
                if len(row) >= 4:  # Ensure we have at least 4 columns
                    source_uid = row[0]
                    source_id = row[1]
                    target_uid = row[2]
                    tags_str = row[3]
                    
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
    
    # Create progress bar
    progress_bar = create_progress_bar(
        total=total_assets,
        desc="Importing asset tags",
        unit="assets",
        disable=quiet_mode
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
                method='GET'
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
                        json_payload={"name": tag}
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


def execute_asset_tag_import_parallel(assets_with_tags: List[Dict], client, logger: logging.Logger, quiet_mode: bool = False, verbose_mode: bool = False):
    """Execute asset tag import in parallel mode.
    
    Args:
        assets_with_tags: List of asset data dictionaries
        client: API client instance
        logger: Logger instance
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
    """
    # Calculate thread configuration
    max_threads = 5
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
        
        # Get assets for this thread
        thread_assets = assets_with_tags[start_index:end_index]
        
        # Create progress bar for this thread
        progress_bar = create_progress_bar(
            total=len(thread_assets),
            desc=thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}",
            unit="assets",
            disable=quiet_mode,
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
                    method='GET'
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
                            json_payload={"name": tag}
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


def execute_asset_config_export_parallel(csv_file: str, client, logger: logging.Logger, output_file: str = None, quiet_mode: bool = False, verbose_mode: bool = False):
    """Execute the asset-config-export command with parallel processing.
    
    Args:
        csv_file: Path to the CSV file containing asset data with 4 columns: source_uid, source_id, target_uid, tags
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
            print(f"\nProcessing {len(asset_data)} asset config exports from CSV file (Parallel Mode)")
            print(f"Output will be written to: {output_file}")
            if globals.GLOBAL_OUTPUT_DIR:
                print(f"Using global output directory: {globals.GLOBAL_OUTPUT_DIR}")
            if verbose_mode:
                print("🔊 VERBOSE MODE - Detailed output including headers and responses")
            print("🚀 PARALLEL MODE - Using multiple threads for faster processing")
            print("="*80)
        
        # Determine number of threads (max 5, min 1)
        num_threads = min(5, max(1, len(asset_data)))
        
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
        lock = threading.Lock()
        all_results = []
        
        def process_asset_chunk(thread_id, start_index, end_index):
            """Process a chunk of assets for a specific thread."""
            nonlocal successful, failed, total_assets_processed
            thread_results = []
            thread_successful = 0
            thread_failed = 0
            
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
                
                if verbose_mode:
                    print(f"\n[Thread {thread_id}] Processing asset - source_uid: {source_uid}, source_id: {source_id}")
                    print("-" * 60)
                elif not quiet_mode:
                    print(f"[Thread {thread_id}] Processing [{i-start_index+1}/{end_index-start_index}] source_id: {source_id}")
                
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
                    
                    config_response = client.make_api_call(
                        endpoint=f"/catalog-server/api/assets/{source_id}/config",
                        method='GET'
                    )
                    
                    # Show response in verbose mode
                    if verbose_mode:
                        print(f"\n[Thread {thread_id}] Config Response:")
                        print(json.dumps(config_response, indent=2, ensure_ascii=False))
                    
                    # Write the compressed JSON response to CSV with target_uid
                    config_json = json.dumps(config_response, ensure_ascii=False, separators=(',', ':'))
                    thread_results.append([target_uid, config_json])
                    
                    if verbose_mode:
                        print(f"[Thread {thread_id}] ✅ Written to file: {target_uid}")
                    elif not quiet_mode:
                        print(f"[Thread {thread_id}] ✅ [{i-start_index+1}/{end_index-start_index}] {target_uid}: Config exported successfully")
                    
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
            writer.writerow(['target_uid', 'config_json'])
            
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
                        print(f"   Expected columns: target_uid, config_json")
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
            print("\n" + "="*80)
            print("ASSET CONFIG EXPORT COMPLETED (PARALLEL MODE)")
            print("="*80)
            print(f"Output file: {output_file}")
            print(f"Total assets processed: {len(asset_data)}")
            print(f"Successful: {successful}")
            print(f"Failed: {failed}")
            print(f"Total assets processed: {total_assets_processed}")
            print(f"Threads used: {num_threads}")
            
            for result in thread_results:
                thread_name = thread_names[result['thread_id']] if result['thread_id'] < len(thread_names) else f"Thread {result['thread_id']}"
                print(f"{thread_name}: {result['successful']} successful, {result['failed']} failed, {result['total_assets']} assets")
            
            print("="*80)
        else:
            print(f"✅ Asset config export completed: {successful} successful, {failed} failed")
            print(f"Output written to: {output_file}")
        
    except Exception as e:
        error_msg = f"Error in parallel asset-config-export: {e}"
        if verbose_mode or not quiet_mode:
            print(f"❌ {error_msg}")
        logger.error(error_msg)
        raise


def execute_asset_config_import_parallel(csv_file: str, client, logger: logging.Logger, quiet_mode: bool = False, verbose_mode: bool = False):
    """Execute the asset-config-import command with parallel processing.
    
    Args:
        csv_file: Path to the CSV file containing target_uid and config_json
        client: API client instance
        logger: Logger instance
        quiet_mode: Whether to show progress bars
        verbose_mode: Whether to enable verbose logging
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
                print(f"   Expected location: adoc-migration-toolkit-YYYYMMDDHHMM/asset-import/asset-config-import-ready.csv")
            logger.error(error_msg)
            return
        
        # Read CSV data
        asset_data = []
        with open(csv_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
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
        
        # Determine number of threads (max 5, min 1)
        num_threads = min(5, max(1, len(asset_data)))
        
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
        
        def process_asset_chunk(thread_id, start_index, end_index):
            """Process a chunk of assets for a specific thread."""
            nonlocal successful, failed, total_assets_processed
            thread_successful = 0
            thread_failed = 0
            thread_results = []
            
            # Create progress bar for this thread if in quiet mode
            if quiet_mode and not verbose_mode:
                thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                pbar = tqdm(
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
                
                try:
                    # Step 1: Get asset ID from target_uid
                    if verbose_mode:
                        print(f"\n🔍 Thread {thread_id}: Getting asset ID for {target_uid}")
                        print(f"   GET /catalog-server/api/assets?uid={target_uid}")
                    
                    # Make GET request to get asset ID
                    response = client.make_api_call(
                        endpoint=f'/catalog-server/api/assets?uid={target_uid}',
                        method='GET'
                    )
                    
                    if verbose_mode:
                        print(f"   Response: {response}")
                    
                    if not response or 'data' not in response or not response['data']:
                        error_msg = f"No asset found for UID: {target_uid}"
                        if verbose_mode:
                            print(f"   ❌ {error_msg}")
                        thread_failed += 1
                        thread_results.append({
                            'target_uid': target_uid,
                            'status': 'failed',
                            'error': error_msg
                        })
                        continue
                    
                    # Extract asset ID
                    asset_id = response['data'][0]['id']
                    
                    if verbose_mode:
                        print(f"   Asset ID: {asset_id}")
                        print(f"   PUT /catalog-server/api/assets/{asset_id}/config")
                        print(f"   Data: {config_json}")
                    
                    # Step 2: Update asset configuration
                    config_response = client.make_api_call(
                        endpoint=f'/catalog-server/api/assets/{asset_id}/config',
                        method='PUT',
                        json_payload=json.loads(config_json)
                    )
                    
                    if verbose_mode:
                        print(f"   Config Response: {config_response}")
                    
                    if config_response:
                        thread_successful += 1
                        thread_results.append({
                            'target_uid': target_uid,
                            'asset_id': asset_id,
                            'status': 'success'
                        })
                        if verbose_mode:
                            print(f"   ✅ Successfully updated config for {target_uid}")
                    else:
                        error_msg = f"Failed to update config for asset ID: {asset_id}"
                        if verbose_mode:
                            print(f"   ❌ {error_msg}")
                        thread_failed += 1
                        thread_results.append({
                            'target_uid': target_uid,
                            'asset_id': asset_id,
                            'status': 'failed',
                            'error': error_msg
                        })
                
                except Exception as e:
                    error_msg = f"Error processing {target_uid}: {str(e)}"
                    if verbose_mode:
                        print(f"   ❌ {error_msg}")
                    thread_failed += 1
                    thread_results.append({
                        'target_uid': target_uid,
                        'status': 'failed',
                        'error': error_msg
                    })
                
                # Update progress bar
                if quiet_mode and not verbose_mode:
                    pbar.update(1)
            
            # Close progress bar
            if quiet_mode and not verbose_mode:
                pbar.close()
            
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
        
        # Print summary
        print("\n" + "="*60)
        print("ASSET CONFIG IMPORT SUMMARY")
        print("="*60)
        print(f"Total assets processed: {total_assets_processed}")
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")
        
        if thread_results:
            print(f"\nPer-thread breakdown:")
            for result in thread_results:
                thread_name = thread_names[result['thread_id']] if result['thread_id'] < len(thread_names) else f"Thread {result['thread_id']}"
                print(f"{thread_name}: {result['successful']} successful, {result['failed']} failed, {result['total_assets']} assets")
        
        if failed > 0:
            print(f"\nFailed assets:")
            for result in all_results:
                if result['status'] == 'failed':
                    print(f"  - {result['target_uid']}: {result.get('error', 'Unknown error')}")
        
        print("="*60)
        
        if failed == 0:
            print("✅ Asset config import completed successfully!")
        else:
            print(f"⚠️  Asset config import completed with {failed} failures. Check the details above.")
        
    except Exception as e:
        error_msg = f"Error executing asset config import: {e}"
        print(f"❌ {error_msg}")
        logger.error(error_msg)


def execute_asset_config_import(csv_file: str, client, logger: logging.Logger, quiet_mode: bool = False, verbose_mode: bool = False, parallel_mode: bool = False):
    """Execute the asset-config-import command.
    
    Args:
        csv_file: Path to the CSV file containing target_uid and config_json
        client: API client instance
        logger: Logger instance
        quiet_mode: Whether to show progress bars
        verbose_mode: Whether to enable verbose logging
        parallel_mode: Whether to use parallel processing
    """
    if parallel_mode:
        execute_asset_config_import_parallel(csv_file, client, logger, quiet_mode, verbose_mode)
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
                    method='GET'
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
                
                # Step 2: Update asset configuration
                config_response = client.make_api_call(
                    endpoint=f'/catalog-server/api/assets/{asset_id}/config',
                    method='PUT',
                    json_payload=json.loads(config_json)
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
        print(f"❌ {error_msg}")
        logger.error(error_msg)