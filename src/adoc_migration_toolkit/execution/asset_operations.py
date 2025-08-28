"""
Asset operations execution functions.

This module contains execution functions for asset-related operations
including profile export/import, config export/import, and list export.
"""

import csv
import json
import logging
import sys
import threading
import tempfile
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

from adoc_migration_toolkit.execution.utils import create_progress_bar, read_csv_uids, read_csv_uids_single_column, read_csv_asset_data, get_thread_names
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


def execute_asset_profile_export(csv_file: str, client, logger: logging.Logger, output_file: str = None, quiet_mode: bool = False, verbose_mode: bool = False, allowed_types: list[str] = ['table', 'sql_view', 'view', 'file', 'kafka_topic'], source_context_id: str = None, target_context_id: str = None):
    """Execute the asset-profile-export command.
    
    Args:
        csv_file: Path to the CSV file containing source-env and target-env mappings
        client: API client instance
        logger: Logger instance
        output_file: Path to output file for writing results
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
        allowed_types: List of asset types to export
        source_context_id: Source context ID for notification mapping (optional)
        target_context_id: Target context ID for notification mapping (optional)
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

        # Load notification ID mapping if context IDs are provided
        notification_id_mapping = {}
        if source_context_id and target_context_id:
            try:
                from .notification_operations import create_notification_id_mapping_csv, load_notification_id_mapping
                if not quiet_mode:
                    print(f"🔄 Creating notification ID mapping for context IDs: {source_context_id} -> {target_context_id}")
                
                # Create the mapping CSV
                mapping_csv_path = create_notification_id_mapping_csv(client, logger, source_context_id, target_context_id, quiet_mode, verbose_mode)
                
                # Load the mapping
                notification_id_mapping = load_notification_id_mapping(mapping_csv_path, quiet_mode, verbose_mode)
                
                if not quiet_mode:
                    print(f"📋 Loaded {len(notification_id_mapping)} notification ID mappings")
                    
            except Exception as e:
                if not quiet_mode:
                    print(f"⚠️  Failed to create notification ID mapping: {e}")
                logger.warning(f"Failed to create notification ID mapping: {e}")

        # Read source-env and target-env mappings from CSV file
        env_mappings = []  # read_csv_uids(csv_file, logger)

        asset_data = read_csv_asset_data(csv_file, logger, allowed_types)
        env_mappings = [
                (entry['source_uid'], entry['target_uid'])
                for entry in asset_data
                if entry.get('source_uid') and entry.get('target_uid')
            ]
        if not env_mappings:
            logger.warning("No environment mappings found in CSV file")
            return
        

        
        # Generate default output file if not provided - use asset-import category
        if not output_file:
            output_file = get_output_file_path(csv_file, "asset-profiles-import-ready.csv", category="asset-import")
        
        if not quiet_mode:
            print(f"\nProcessing {len(env_mappings)} asset profile exports from CSV file: {csv_file}")
            print(f"Output will be written to: {output_file}")
            if source_context_id and target_context_id:
                print(f"🔗 Notification ID mapping enabled: {source_context_id} -> {target_context_id}")
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
        
        # Create progress bar using tqdm utility
        progress_bar = create_progress_bar(
            total=len(env_mappings),
            desc="Exporting asset profiles",
            unit="assets",
            disable=verbose_mode
        )
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            
            # Write header - include source-env for duplicate resolution
            writer.writerow(['target-env', 'profile_json', 'source-env'])
            
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
                    
                    # Transform profile configuration if notification mapping is available
                    if notification_id_mapping:
                        from .notification_operations import transform_profile_configuration
                        profile_response = transform_profile_configuration(
                            profile_response, notification_id_mapping, quiet_mode, verbose_mode
                        )
                    
                    # Step 4: Write to CSV - include source-env for duplicate resolution
                    profile_json = json.dumps(profile_response, ensure_ascii=False)
                    writer.writerow([target_env, profile_json, source_env])
                    
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


def execute_asset_profile_import(csv_file: str, client, logger: logging.Logger, dry_run: bool = False, quiet_mode: bool = True, verbose_mode: bool = False, max_threads: int = 5, notification_mapping_csv: str = None, interactive_duplicate_resolution: bool = True):
    """Execute the asset-profile-import command with parallel processing."""
    import threading
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
        
        # Handle duplicate resolution if enabled
        if interactive_duplicate_resolution and not dry_run:
            if not quiet_mode:
                print("🔍 Checking for duplicate target UIDs...")
            
            resolved_csv_file = detect_and_resolve_duplicates(csv_file, quiet_mode, verbose_mode)
            if resolved_csv_file and resolved_csv_file != csv_file:
                csv_file = resolved_csv_file
                if not quiet_mode:
                    print(f"📄 Using deduplicated file: {csv_file}")
            elif resolved_csv_file is None:
                if not quiet_mode:
                    print("❌ Failed to resolve duplicates. Aborting import.")
                return
        
        # Load notification ID mapping if provided
        notification_id_mapping = {}
        if notification_mapping_csv:
            try:
                from .notification_operations import load_notification_id_mapping
                notification_id_mapping = load_notification_id_mapping(notification_mapping_csv, quiet_mode, verbose_mode)
                if not quiet_mode:
                    print(f"📋 Loaded notification ID mapping with {len(notification_id_mapping)} mappings from: {notification_mapping_csv}")
            except Exception as e:
                if not quiet_mode:
                    print(f"⚠️  Failed to load notification ID mapping: {e}")
                logger.warning(f"Failed to load notification ID mapping: {e}")
        
        if not quiet_mode:
            print(f"\nProcessing asset profile import from CSV file: {csv_file}")
            if notification_mapping_csv:
                print(f"📋 Using notification ID mapping from: {notification_mapping_csv}")
            if dry_run:
                print("🔍 DRY RUN MODE - No actual API calls will be made")
                print("📋 Will show detailed information about what would be executed")
            if quiet_mode:
                print("🔇 QUIET MODE - Minimal output")
            if verbose_mode:
                print("🔊 VERBOSE MODE - Detailed output including headers and responses")
            print("="*80)
        
        # Read CSV file
        import_mappings = []
        with open(csv_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            # Support both old format (2 columns) and new format (3 columns with source-env)
            if len(header) < 2 or header[0] != 'target-env' or header[1] != 'profile_json':
                error_msg = f"Invalid CSV format. Expected header: ['target-env', 'profile_json'] or ['target-env', 'profile_json', 'source-env'], got: {header}"
                if not quiet_mode:
                    print(f"❌ {error_msg}")
                logger.error(error_msg)
                return
            
            # Check if we have the new format with source-env column
            has_source_env = len(header) >= 3 and header[2] == 'source-env'
            if not quiet_mode and has_source_env:
                print(f"📋 Detected CSV format with source-env column (new format)")
            elif not quiet_mode:
                print(f"📋 Detected CSV format without source-env column (legacy format)")
            for row_num, row in enumerate(reader, start=2):
                # Handle both 2-column and 3-column formats
                if len(row) < 2:
                    logger.warning(f"Row {row_num}: Expected at least 2 columns, got {len(row)}")
                    continue
                
                target_env = row[0].strip()
                profile_json = row[1].strip()
                
                if not target_env or not profile_json:
                    logger.warning(f"Row {row_num}: Empty target-env or profile_json value")
                    continue
                
                # Extract source_env if available (for logging purposes)
                source_env = row[2].strip() if len(row) >= 3 and has_source_env else None
                
                import_mappings.append((target_env, profile_json))
                logger.debug(f"Row {row_num}: Found target-env: {target_env}" + (f", source-env: {source_env}" if source_env else ""))
        if not import_mappings:
            logger.warning("No valid import mappings found in CSV file")
            return
        logger.info(f"Read {len(import_mappings)} import mappings from CSV file: {csv_file}")
 
        # Threading setup
        num_threads = max_threads
        min_assets_per_thread = 10
        if len(import_mappings) < min_assets_per_thread:
            num_threads = 1
            assets_per_thread = len(import_mappings)
        else:
            num_threads = min(max_threads, (len(import_mappings) + min_assets_per_thread - 1) // min_assets_per_thread)
            assets_per_thread = (len(import_mappings) + num_threads - 1) // num_threads

        if not quiet_mode:
            print(f"Using {num_threads} threads to process {len(import_mappings)} imports")
            print(f"Assets per thread: {assets_per_thread}")
            print("="*80)
        
        thread_names = get_thread_names()
        
        thread_results = []
        def process_chunk(thread_id, chunk):
            thread_successful = 0
            thread_failed = 0
            thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
            progress_bar = create_progress_bar(
                total=len(chunk),
                desc=thread_name,
                unit="assets",
                disable=quiet_mode,
                position=thread_id,
                leave=False
            )

            for i, (target_env, profile_json) in enumerate(chunk):
                try:
                    if not quiet_mode and verbose_mode:
                        print(f"[Thread {thread_name}] Processing target-env: {target_env}")
                    if not dry_run:
                        asset_response = client.make_api_call(
                            endpoint=f"/catalog-server/api/assets?uid={target_env}",
                            method='GET',
                            use_target_auth=True,
                            use_target_tenant=True
                        )
                    else:
                        asset_response = {
                            "data": [
                                {
                                    "id": 12345,
                                    "name": "MOCK_ASSET",
                                    "uid": target_env
                                }
                            ]
                        }
                        if not quiet_mode:
                            print(f"[Thread {thread_name}] DRY RUN - Would get asset details for UID: {target_env}")
                    if verbose_mode and not dry_run:
                        print(f"[Thread {thread_name}] Asset Response: {json.dumps(asset_response, indent=2, ensure_ascii=False)}")
                    if not asset_response or 'data' not in asset_response:
                        error_msg = f"No 'data' field found in asset response for UID: {target_env}"
                        if not quiet_mode:
                            print(f"❌ [Thread {thread_name}] {target_env}: {error_msg}")
                        logger.error(error_msg)
                        thread_failed += 1
                        progress_bar.update(1)
                        continue
                    data_array = asset_response['data']
                    if not data_array or len(data_array) == 0:
                        error_msg = f"Empty 'data' array in asset response for UID: {target_env}"
                        if not quiet_mode:
                            print(f"❌ [Thread {thread_name}] {target_env}: {error_msg}")
                        logger.error(error_msg)
                        thread_failed += 1
                        progress_bar.update(1)
                        continue
                    first_asset = data_array[0]
                    if 'id' not in first_asset:
                        error_msg = f"No 'id' field found in first asset for UID: {target_env}"
                        if not quiet_mode:
                            print(f"❌ [Thread {thread_name}] {target_env}: {error_msg}")
                        logger.error(error_msg)
                        thread_failed += 1
                        progress_bar.update(1)
                        continue
                    asset_id = first_asset['id']
                    if not quiet_mode:
                        print(f"[Thread {thread_name}] Extracted asset ID: {asset_id}")
                    try:
                        profile_data = json.loads(profile_json)
                        
                        # Transform profile configuration if notification mapping is available
                        if notification_id_mapping:
                            from .notification_operations import transform_profile_configuration
                            profile_data = transform_profile_configuration(
                                profile_data, notification_id_mapping, quiet_mode, verbose_mode
                            )
                    except json.JSONDecodeError as e:
                        error_msg = f"Invalid JSON in profile_json for UID {target_env}: {e}"
                        if not quiet_mode:
                            print(f"❌ [Thread {thread_name}] {target_env}: {error_msg}")
                        logger.error(error_msg)
                        thread_failed += 1
                        progress_bar.update(1)
                        continue
                    if not quiet_mode:
                        print(f"[Thread {thread_name}] Updating profile configuration for asset ID: {asset_id}")
                    if not dry_run:
                        import_response = client.make_api_call(
                            endpoint=f"/catalog-server/api/profile/{asset_id}/config",
                            method='PUT',
                            json_payload=profile_data,
                            use_target_auth=True,
                            use_target_tenant=True
                        )
                        if verbose_mode:
                            print(f"[Thread {thread_name}] Import Response: {json.dumps(import_response, indent=2, ensure_ascii=False)}")
                        if not quiet_mode:
                            print(f"[Thread {thread_name}] ✅ Import successful")
                    else:
                        if not quiet_mode:
                            print(f"[Thread {thread_name}] DRY RUN - Would update profile for asset {asset_id}")
                    thread_successful += 1
                    progress_bar.update(1)
                    logger.info(f"Successfully processed target-env {target_env} (asset ID: {asset_id})")
                except Exception as e:
                    error_msg = f"Failed to process UID {target_env}: {e}"
                    if not quiet_mode:
                        print(f"❌ [Thread {thread_name}] {target_env}: {error_msg}")
                    logger.error(error_msg)
                    thread_failed += 1
                    progress_bar.update(1)

            progress_bar.close()       
            print(f"Thread {thread_name} completed")

            return {
                'thread_id': thread_id,
                'successful': thread_successful,
                'failed': thread_failed
            }
        
        # Split import_mappings into chunks
        threads = []
        
        for i in range(num_threads):
            start_index = i * assets_per_thread
            end_index = min(start_index + assets_per_thread, len(import_mappings))
            chunk = import_mappings[start_index:end_index]
            t = threading.Thread(target=process_chunk, args=(i, chunk))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

                # Execute parallel processing
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Submit tasks for each thread
            futures = []
            for thread_id in range(num_threads):
                start_index = thread_id * assets_per_thread
                end_index = min(start_index + assets_per_thread, len(import_mappings))
                
                if start_index < len(import_mappings):  # Only submit if there are pages to process
                    future = executor.submit(process_chunk, thread_id, chunk)
                    futures.append(future)
            
            # Collect results
            for future in as_completed(futures):
                try:
                    result = future.result()
                    thread_results.append(result)
                except Exception as e:
                    logger.error(f"Thread failed with exception: {e}")
        

        print("thread_results", thread_results)
        total_successful = sum(r['successful'] for r in thread_results)
        total_failed = sum(r['failed'] for r in thread_results)

        if not quiet_mode:
            print("\n" + "="*80)
            print("ASSET PROFILE IMPORT COMPLETED (PARALLEL)")
            print("="*80)
            if dry_run:
                print("🔍 DRY RUN MODE - No actual changes were made")
            print(f"Total mappings processed: {len(import_mappings)}")
            print(f"Successful: {total_successful}")
            print(f"Failed: {total_failed}")
            print("="*80)        
            # Print thread statistics
            print(f"\nThread Statistics:")
            for result in thread_results:
                thread_name = thread_names[result['thread_id']] if result['thread_id'] < len(thread_names) else f"Thread {result['thread_id']}"
                print(f"  {thread_name}: {result['successful']} successful, {result['failed']} failed")

            print("="*80)
        else:
            print(f"✅ Asset profile import completed: {total_successful} successful, {total_failed} failed")
            if dry_run:
                print("🔍 DRY RUN MODE - No actual changes were made")

    except Exception as e:
        error_msg = f"Error in asset-profile-import (parallel): {e}"
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
            writer.writerow(['target_uid', 'config_json', 'source_uid'])
            
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
                    
                    # Write the compressed JSON response to CSV with target_uid and source_uid
                    config_json = json.dumps(config_response, ensure_ascii=False, separators=(',', ':'))
                    writer.writerow([target_uid, config_json, source_uid])
                    
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
                if len(header) != 3:
                    validation_errors.append(f"Invalid header: expected 3 columns, got {len(header)}")
                elif header[0] != 'target_uid' or header[1] != 'config_json' or header[2] != 'source_uid':
                    validation_errors.append(f"Invalid header: expected ['target_uid', 'config_json', 'source_uid'], got {header}")
                
                # Validate each row
                for row_num, row in enumerate(reader, start=2):
                    row_count += 1
                    
                    # Check column count
                    if len(row) != 3:
                        validation_errors.append(f"Row {row_num}: Expected 3 columns, got {len(row)}")
                        continue
                    
                    target_uid, config_json_str, source_uid = row
                    
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
            if header and len(header) >= 6:
                if 'target_uid' in header[0].lower() or 'asset_config_json' in header[1].lower() or 'source_uid' in header[5].lower():
                    pass  # This is a header row, skip
                else:
                    # This might be data, add it back
                    if header[0].strip() and header[1].strip():
                        asset_data.append({
                            'target_uid': header[0].strip(),
                            'config_json': header[1].strip(),
                            'source_uid': header[5].strip() if len(header) > 5 else 'Unknown'
                        })
            for row in reader:
                if len(row) >= 6:  # Ensure we have all 6 columns
                    target_uid = row[0].strip()
                    config_json = row[1].strip()  # asset_config_json column
                    source_uid = row[5].strip()  # source_uid column (6th column)
                    if target_uid and config_json:  # Skip empty rows
                        asset_data.append({
                            'target_uid': target_uid,
                            'config_json': config_json,
                            'source_uid': source_uid
                        })
                elif len(row) >= 3:  # Fallback for 3-column format
                    target_uid = row[0].strip()
                    config_json = row[1].strip()
                    source_uid = row[2].strip() if len(row) > 2 else 'Unknown'
                    if target_uid and config_json:  # Skip empty rows
                        asset_data.append({
                            'target_uid': target_uid,
                            'config_json': config_json,
                            'source_uid': source_uid
                        })
        
        if not asset_data:
            print("❌ No valid asset data found in CSV file")
            logger.warning("No valid asset data found in CSV file")
            return
        
        if not quiet_mode:
            print(f"📊 Found {len(asset_data)} assets to process")
        
        # Check for and resolve duplicates before processing
        duplicates_found = check_for_duplicates_in_asset_data(asset_data)
        if duplicates_found:
            if not quiet_mode:
                print("🔍 Resolving duplicates interactively...")
            
            # Resolve duplicates interactively
            resolved_asset_data = resolve_duplicates_interactively(asset_data, duplicates_found, quiet_mode, verbose_mode)
            if resolved_asset_data is None:
                print("❌ Duplicate resolution was cancelled by user")
                return
            else:
                asset_data = resolved_asset_data
                if not quiet_mode:
                    print(f"✅ Duplicate resolution completed. Processing {len(asset_data)} unique configurations")
        
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
        if not quiet_mode:
            print("\n" + "="*80)
            print("ASSET CONFIG IMPORT COMPLETED")
            print("="*80)
            if dry_run:
                print("🔍 DRY RUN MODE - No actual changes were made")
            print(f"Total mappings processed: {len(asset_data)}")
            print(f"Successful: {successful}")
            print(f"Failed: {failed}")
            print("="*80)
        else:
            print(f"✅ Asset config import completed: {successful} successful, {failed} failed")
            if dry_run:
                print("🔍 DRY RUN MODE - No actual changes were made")
        
    except Exception as e:
        error_msg = f"Error in asset-config-import: {e}"
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
        thread_names = get_thread_names()
        
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


def execute_asset_profile_export_parallel(csv_file: str, client, logger: logging.Logger, output_file: str = None, quiet_mode: bool = False, verbose_mode: bool = False, allowed_types: list[str] = ['table', 'sql_view', 'view', 'file', 'kafka_topic'], max_threads: int = 5, source_context_id: str = None, target_context_id: str = None):
    """Execute the asset-profile-export command with parallel processing.
    
    Args:
        csv_file: Path to the CSV file containing source-env and target-env mappings
        client: API client instance
        logger: Logger instance
        output_file: Path to output file for writing results
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
        allowed_types: List of asset types to export
    """
    try:
        print(f"Assset profile export parallel starting with threads : {max_threads}")
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
        env_mappings = [] #read_csv_uids(csv_file, logger)

        asset_data = read_csv_asset_data(csv_file, logger, allowed_types)

        env_mappings = [
                (entry['source_uid'], entry['target_uid'])
                for entry in asset_data
                if entry.get('source_uid') and entry.get('target_uid')
            ]

        if not env_mappings:
            logger.warning("No environment mappings found in CSV file")
            return
        

        
        # Load notification ID mapping if context IDs are provided
        notification_id_mapping = {}
        if source_context_id and target_context_id:
            try:
                from .notification_operations import create_notification_id_mapping_csv, load_notification_id_mapping
                if not quiet_mode:
                    print(f"🔄 Creating notification ID mapping for context IDs: {source_context_id} -> {target_context_id}")
                
                # Create the mapping CSV
                mapping_csv_path = create_notification_id_mapping_csv(client, logger, source_context_id, target_context_id, quiet_mode, verbose_mode)
                
                # Load the mapping
                notification_id_mapping = load_notification_id_mapping(mapping_csv_path, quiet_mode, verbose_mode)
                
                if not quiet_mode:
                    print(f"📋 Loaded {len(notification_id_mapping)} notification ID mappings")
                    
            except Exception as e:
                if not quiet_mode:
                    print(f"⚠️  Failed to create notification ID mapping: {e}")
                logger.warning(f"Failed to create notification ID mapping: {e}")
        
        # Generate default output file if not provided - use asset-import category
        if not output_file:
            output_file = get_output_file_path(csv_file, "asset-profiles-import-ready.csv", category="asset-import")
        
        print(f"quiet_mode : {quiet_mode}")
        if not quiet_mode:
            print(f"\nProcessing {len(env_mappings)} asset profile exports from CSV file (Parallel Mode)")
            print(f"Input file: {csv_file}")
            print(f"Output will be written to: {output_file}")
            if source_context_id and target_context_id:
                print(f"🔗 Notification ID mapping enabled: {source_context_id} -> {target_context_id}")
            if globals.GLOBAL_OUTPUT_DIR:
                print(f"Using global output directory: {globals.GLOBAL_OUTPUT_DIR}")
            if verbose_mode:
                print("🔊 VERBOSE MODE - Detailed output including headers and responses")
            print("="*80)
        
        # Calculate thread configuration
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
        thread_names = get_thread_names()
        
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
            thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
            # Create progress bar for this thread
            progress_bar = create_progress_bar(
                total=end_page - start_page,
                desc= thread_name,
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
                        print(f"\n{thread_name} - Processing source-env: {source_env}")
                        print(f"Target-env: {target_env}")
                        print("-" * 60)
                    
                    # Step 1: Get asset details by source-env (UID)
                    if verbose_mode:
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
                        print(f"\n{thread_name} - Asset Response:")
                        print(json.dumps(asset_response, indent=2, ensure_ascii=False))
                    
                    # Step 2: Extract the asset ID
                    if not asset_response or 'data' not in asset_response:
                        error_msg = f"No 'data' field found in asset response for UID: {source_env}"
                        if verbose_mode:
                            print(f"\n{thread_name} - ❌ {error_msg}")
                        logger.error(f"Thread {thread_id}: {error_msg}")
                        failed += 1
                        progress_bar.update(1)
                        continue
                    
                    data_array = asset_response['data']
                    if not data_array or len(data_array) == 0:
                        error_msg = f"Empty 'data' array in asset response for UID: {source_env}"
                        if verbose_mode:
                            print(f"\n{thread_name} - ❌ {error_msg}")
                        logger.error(f"Thread {thread_id}: {error_msg}")
                        failed += 1
                        progress_bar.update(1)
                        continue
                    
                    first_asset = data_array[0]
                    if 'id' not in first_asset:
                        error_msg = f"No 'id' field found in first asset for UID: {source_env}"
                        if verbose_mode:
                            print(f"\n{thread_name} - ❌ {error_msg}")
                        logger.error(f"Thread {thread_id}: {error_msg}")
                        failed += 1
                        progress_bar.update(1)
                        continue
                    
                    asset_id = first_asset['id']
                    if verbose_mode:
                        print(f"{thread_name} - Extracted asset ID: {asset_id}")
                    
                    # Step 3: Get profile configuration for the asset
                    if verbose_mode:
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
                        print(f"\n{thread_name} - Profile Response:")
                        print(json.dumps(profile_response, indent=2, ensure_ascii=False))
                    
                    # Transform profile configuration if notification mapping is available
                    if notification_id_mapping:
                        from .notification_operations import transform_profile_configuration
                        profile_response = transform_profile_configuration(
                            profile_response, notification_id_mapping, quiet_mode, verbose_mode
                        )
                    
                    # Step 4: Write to temporary CSV file - include source-env for duplicate resolution
                    profile_json = json.dumps(profile_response, ensure_ascii=False)
                    with open(temp_file.name, 'a', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                        writer.writerow([target_env, profile_json, source_env])
                    
                    if verbose_mode:
                        print(f"{thread_name} - ✅ Written to file: {target_env}")
                    
                    successful += 1
                    total_assets_processed += 1
                    
                except Exception as e:
                    error_msg = f"Failed to process source-env {source_env}: {e}"
                    if verbose_mode:
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
        
        # Execute parallel processing
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Submit tasks for each thread
            futures = []
            for thread_id in range(num_threads):
                start_page = thread_id * assets_per_thread
                end_page = min(start_page + assets_per_thread, len(env_mappings))
                
                if start_page < len(env_mappings):  # Only submit if there are pages to process
                    future = executor.submit(process_asset_chunk, thread_id, start_page, end_page)
                    futures.append(future)
            
            # Collect results
            for future in as_completed(futures):
                try:
                    result = future.result()
                    thread_results.append(result)
                except Exception as e:
                    logger.error(f"Thread failed with exception: {e}")

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
            
            # Write header - include source-env for duplicate resolution
            writer.writerow(['target-env', 'profile_json', 'source-env'])
            
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
    thread_names = get_thread_names()
    
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
                                         quiet_mode: bool = False, verbose_mode: bool = False, max_threads: int = 5, allowed_types: list[str] = ['table', 'sql_view', 'view', 'file', 'kafka_topic']):
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
        asset_data = read_csv_asset_data(csv_file, logger, allowed_types)
        if not asset_data:
            logger.warning("No asset data found in CSV file")
            return

        # Generate default output file if not provided
        if not output_file:
            output_file = get_output_file_path(csv_file, "asset-config-export.csv", category="asset-export")

        if not quiet_mode:
            print(f"\nReading asset config exports from CSV file :{csv_file}")
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
        thread_names = get_thread_names()

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
                        print(f"❌ {error_msg}")
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

                    # Write the compressed JSON response to CSV with target_uid and source_uid
                    thread_results.append(
                        [target_uid, asset_config_json, asset_profile_anomaly_config_json, asset_id_to_uid_map_json,
                         asset_target_id_to_uid_map_json, source_uid])

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
                 'asset_target_id_to_uid_map_json', 'source_uid'])

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
                if len(header) != 6:
                    validation_errors.append(f"Invalid header: expected 6 columns, got {len(header)}")
                elif header[0] != 'target_uid' or header[1] != 'asset_config_json' or header[2] != 'asset_profile_anomaly_config_json' or header[3] != 'asset_id_to_uid_map_json' or header[4] != 'asset_target_id_to_uid_map_json' or header[5] != 'source_uid':
                    validation_errors.append(f"Invalid header: expected ['target_uid', 'asset_config_json', 'asset_profile_anomaly_config_json', 'asset_id_to_uid_map_json', 'asset_target_id_to_uid_map_json', 'source_uid'], got {header}")

                # Validate each row
                for row_num, row in enumerate(reader, start=2):
                    row_count += 1

                    # Check column count
                    if len(row) != 6:
                        validation_errors.append(f"Row {row_num}: Expected 6 columns, got {len(row)}")
                        continue

                    target_uid, asset_config_json_str, asset_profile_anomaly_config_json_str, asset_id_to_uid_map_json_str, asset_target_id_to_uid_map_json_str, source_uid = row

                    # Check for empty values
                    if not target_uid.strip():
                        validation_errors.append(f"Row {row_num}: Empty target_uid value")

                    if not asset_config_json_str.strip():
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
        # The below limit is set to fix the error: field larger than field limit (131072), python csv read has a limitation.
        csv.field_size_limit(sys.maxsize)
        with open(csv_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # Skip header
            
            # Determine CSV format (3-column or 6-column)
            if len(header) == 3:
                # 3-column format: target_uid, config_json, source_uid
                for row in reader:
                    if len(row) >= 3:
                        asset_data.append({
                            'target_uid': row[0],
                            'config_json': row[1],
                            'source_uid': row[2]
                        })
            elif len(header) >= 6:
                # 6-column format: target_uid, asset_config_json, asset_profile_anomaly_config_json, asset_id_to_uid_map_json, asset_target_id_to_uid_map_json, source_uid
                for row in reader:
                    if len(row) >= 6:
                        asset_data.append({
                            'target_uid': row[0],
                            'config_json': row[1],
                            'source_uid': row[5]
                        })
            else:
                # Fallback for other formats
                for row in reader:
                    if len(row) >= 2:
                        asset_data.append({
                            'target_uid': row[0],
                            'config_json': row[1],
                            'source_uid': row[2] if len(row) > 2 else 'Unknown'
                        })

        if not asset_data:
            print("❌ No asset data found in CSV file")
            return

        if not quiet_mode:
            print(f"📊 Found {len(asset_data)} assets to process")

        # Check for and resolve duplicates before processing
        duplicates_found = check_for_duplicates_in_asset_data(asset_data)
        if duplicates_found:
            if not quiet_mode:
                print("🔍 Resolving duplicates interactively...")
            
            # Resolve duplicates interactively
            resolved_asset_data = resolve_duplicates_interactively(asset_data, duplicates_found, quiet_mode, verbose_mode)
            if resolved_asset_data is None:
                print("❌ Duplicate resolution was cancelled by user")
                return
            else:
                asset_data = resolved_asset_data
                if not quiet_mode:
                    print(f"✅ Duplicate resolution completed. Processing {len(asset_data)} unique configurations")

        # Pre-analyze assets to provide better visibility
        print(f"\n📊 ASSET CONFIG IMPORT ANALYSIS")
        print("=" * 60)
        print(f"📋 Total assets in CSV: {len(asset_data)}")
        
        # Categorize assets before processing
        assets_with_config = 0
        assets_without_config = 0
        assets_with_null_config = 0
        assets_with_empty_config = 0
        
        for asset in asset_data:
            config_json = asset['config_json']
            try:
                config_data = json.loads(config_json)
                if "assetConfiguration" in config_data:
                    if config_data["assetConfiguration"] is not None:
                        assets_with_config += 1
                    else:
                        assets_with_null_config += 1
                else:
                    assets_without_config += 1
            except (json.JSONDecodeError, TypeError):
                assets_with_empty_config += 1
        
        print(f"🔧 Assets with custom configuration: {assets_with_config}")
        print(f"⚙️  Assets with null configuration: {assets_with_null_config}")
        print(f"📄 Assets without configuration field: {assets_without_config}")
        print(f"❌ Assets with invalid JSON: {assets_with_empty_config}")
        print("=" * 60)
        
        if assets_with_config == 0:
            print("⚠️  No assets with custom configurations found. All assets will be skipped.")
            print("💡 This is normal if all assets have default configurations.")
            return

        # Determine number of threads (max 5, min 1)
        num_threads = min(max_threads, max(1, len(asset_data)))

        if not quiet_mode:
            print(f"Using {num_threads} threads for processing")

        # Thread names for progress indicators
        thread_names = get_thread_names()

        # Thread-safe counters
        successful = 0
        failed = 0
        total_assets_processed = 0
        asset_configs_not_found = 0
        asset_not_found = 0
        lock = threading.Lock()
        all_results = []

        # Create progress bar if in quiet mode
        if quiet_mode and not verbose_mode:
            pbar = tqdm(total=len(asset_data), desc="Processing assets", colour='green')

        def process_asset_chunk(thread_id, start_index, end_index):
            nonlocal successful, failed, total_assets_processed, asset_configs_not_found, asset_not_found
            thread_successful = 0
            thread_failed = 0
            asset_configs_not_per_thread = 0
            asset_not_found_thread = 0
            thread_results = []

            # Create progress bar for this thread (show in normal mode and quiet mode, but not in verbose mode)
            if not verbose_mode:
                thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                thread_pbar = tqdm(
                    total=end_index - start_index,
                    desc=thread_name,
                    colour='green',
                    position=thread_id,
                    leave=False
                )
            else:
                thread_pbar = None

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
                        asset_not_found_thread += 1
                        thread_results.append({'target_uid': target_uid, 'error': error_msg, 'status': 'failed', 'reason': 'Asset not found in target'})
                        continue

                    asset_id = response['data'][0]['id']

                    if verbose_mode:
                        print(f"   Asset ID: {asset_id}")
                        print(f"   PUT /catalog-server/api/assets/{asset_id}/config")
                        print(f"   Data: {config_json}")

                    config_data = json.loads(config_json)
                    if "assetConfiguration" in config_data and config_data["assetConfiguration"] is not None:
                        transformed_config = transform_config_json_to_asset_configuration(config_data, asset_id)
                        if dry_run:
                            print(
                                f"\n[DRY RUN][Thread {thread_id}] Would send PUT to /catalog-server/api/assets/{asset_id}/config")
                            print(f"[DRY RUN][Thread {thread_id}] Payload:")
                            print(json.dumps(transformed_config, indent=2))
                            thread_successful += 1
                            thread_results.append({'target_uid': target_uid, 'asset_id': asset_id, 'status': 'dry_run', 'reason': 'Dry run mode'})
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
                            thread_results.append({'target_uid': target_uid, 'asset_id': asset_id, 'status': 'success', 'reason': 'Configuration imported successfully'})
                            if verbose_mode:
                                print(f"   ✅ Successfully updated config for {target_uid}")
                        else:
                            error_msg = f"Failed to update config for asset ID: {asset_id}"
                            if verbose_mode:
                                print(f"   ❌ {error_msg}")
                            thread_failed += 1
                            thread_results.append(
                                {'target_uid': target_uid, 'asset_id': asset_id, 'status': 'failed', 'error': error_msg, 'reason': 'API call failed'})
                    else:
                        # Enhanced reason for skipping
                        if "assetConfiguration" not in config_data:
                            reason = "No assetConfiguration field in config"
                        elif config_data["assetConfiguration"] is None:
                            reason = "assetConfiguration is null (default config)"
                        else:
                            reason = "Invalid assetConfiguration format"
                        
                        if verbose_mode:
                            print(f"   ⏭️ Skipping {target_uid}: {reason}")
                        thread_failed += 1
                        asset_configs_not_per_thread += 1
                        thread_results.append({'target_uid': target_uid, 'asset_id': asset_id, 'status': 'skipped', 'reason': reason})
                        continue

                    # Note: Profile anomaly configs are handled separately if needed
                    # import_profile_anomaly_configs(asset_data, assets_mapping, client, logger, quiet_mode, verbose_mode, dry_run)
                except Exception as e:
                    error_msg = f"Error processing {target_uid}: {str(e)}"
                    if verbose_mode:
                        print(f"   ❌ {error_msg}")
                    thread_failed += 1
                    thread_results.append({'target_uid': target_uid, 'status': 'failed', 'error': error_msg, 'reason': 'Exception occurred'})

                # Update progress bar
                if thread_pbar:
                    thread_pbar.update(1)

            # Close thread progress bar
            if thread_pbar:
                thread_pbar.close()

            # Update global counters
            with lock:
                successful += thread_successful
                failed += thread_failed
                total_assets_processed += (end_index - start_index)
                all_results.extend(thread_results)
                asset_configs_not_found += asset_configs_not_per_thread
                asset_not_found += asset_not_found_thread

        # Create and start threads
        threads = []
        chunk_size = len(asset_data) // num_threads
        remainder = len(asset_data) % num_threads

        start_index = 0
        for i in range(num_threads):
            end_index = start_index + chunk_size + (1 if i < remainder else 0)
            thread = threading.Thread(target=process_asset_chunk, args=(i, start_index, end_index))
            threads.append(thread)
            thread.start()
            start_index = end_index

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Close main progress bar
        if quiet_mode and not verbose_mode:
            pbar.close()

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

        # Enhanced summary with detailed breakdown
        print("\n" + "=" * 60)
        print("ASSET CONFIG IMPORT SUMMARY")
        print("=" * 60)
        print(f"📋 Total assets in CSV: {len(asset_data)}")
        print(f"🔧 Assets with custom configuration: {assets_with_config}")
        print(f"⚙️  Assets with null configuration: {assets_with_null_config}")
        print(f"📄 Assets without configuration field: {assets_without_config}")
        print(f"❌ Assets with invalid JSON: {assets_with_empty_config}")
        print("-" * 60)
        print(f"🔄 Total assets processed: {total_assets_processed}")
        print(f"✅ Successfully imported: {successful}")
        print(f"❌ Failed to import: {failed}")
        print(f"🔍 Asset not found in target: {asset_not_found}")
        print(f"⏭️  Assets skipped (default config): {asset_configs_not_found}")
        print("=" * 60)
        
        # Show detailed reasons for skipped assets
        if asset_configs_not_found > 0:
            print(f"\n📋 DETAILED BREAKDOWN OF SKIPPED ASSETS:")
            print("-" * 60)
            skipped_reasons = {}
            for result in all_results:
                if result['status'] == 'skipped':
                    reason = result.get('reason', 'Unknown reason')
                    skipped_reasons[reason] = skipped_reasons.get(reason, 0) + 1
            
            for reason, count in skipped_reasons.items():
                print(f"  • {reason}: {count} assets")
            print("-" * 60)

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
    if "assetConfiguration" in config_data_dict and config_data_dict["assetConfiguration"] is not None:
        config_data_dict["assetConfiguration"]["assetId"] = asset_id
        # Update assetId in freshnessColumnInfo if it exists
        freshness = config_data_dict["assetConfiguration"].get("freshnessColumnInfo")
        if freshness and "assetId" in freshness:
            freshness["assetId"] = asset_id
    else:
        print(f"Asset configuration not found: {asset_id}")

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
                    # Check if the source_str provided by user exists in source uid (transformed_target_uid)
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
        # Safely get profile_anomaly_config_json, defaulting to None if not present
        profile_anomaly_config_json = asset.get('asset_profile_anomaly_config_json', None)
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
                    logger.info(f" AssetId : {asset_id} Rule ID: {rule_id} source ruleid :{profile_anomaly_config_dict['rule']['id']}")
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
                    logger.info(f"Asset config anomaly Rule {rule_id} done")
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

def detect_and_resolve_duplicates(csv_file: str, quiet_mode: bool = False, verbose_mode: bool = False):
    """
    Detect duplicate target UIDs in the asset-profiles-import-ready.csv file and let user choose which one to keep.
    
    Args:
        csv_file: Path to the asset-profiles-import-ready.csv file
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
        
    Returns:
        str: Path to the deduplicated CSV file
    """
    import csv
    from pathlib import Path
    
    if not Path(csv_file).exists():
        print(f"❌ CSV file not found: {csv_file}")
        return None
    
    # Read all entries
    entries = []
    with open(csv_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)  # Skip header
        for row_num, row in enumerate(reader, 2):
            if len(row) >= 2:
                target_env = row[0].strip()
                profile_json = row[1].strip()
                entries.append({
                    'row_num': row_num,
                    'target_env': target_env,
                    'profile_json': profile_json,
                    'raw_row': row  # Store the full row data
                })
    
    # Group by target_env to find duplicates
    target_groups = {}
    for entry in entries:
        target_env = entry['target_env']
        if target_env not in target_groups:
            target_groups[target_env] = []
        target_groups[target_env].append(entry)
    
    # Find duplicates
    duplicates = {target_env: entries for target_env, entries in target_groups.items() if len(entries) > 1}
    
    if not duplicates:
        if not quiet_mode:
            print("✅ No duplicate target UIDs found. Proceeding with import...")
        return csv_file
    
    if not quiet_mode:
        print(f"\n🔍 Found {len(duplicates)} Source UIDs which are pointing to single Target UIDs. So we have duplicate configurations present:")
        print("="*80)
    
    # Let user choose for each duplicate
    selected_entries = []
    skipped_targets = set()
    
    for target_env, entries in duplicates.items():
        if not quiet_mode:
            print(f"\n📋 Target UID: {target_env}")
            print(f"   Found {len(entries)} configurations:")
            
            for i, entry in enumerate(entries, 1):
                # Extract source info from profile JSON for display
                try:
                    import json
                    profile_data = json.loads(entry['profile_json'])
                    profile_settings = profile_data.get("profileSettingsConfigs", {})
                    
                    # Check for notification channels
                    notification_channels = profile_settings.get("profileNotificationChannels", {})
                    has_notifications = bool(notification_channels and notification_channels.get("configuredNotificationGroupIds"))
                    
                    # Check for schedule
                    has_schedule = bool(profile_settings.get("schedule"))
                    
                    # Check for profiling enabled
                    is_enabled = profile_settings.get("enabled", False)
                    
                    # Try to extract source UID from the CSV row if it has 3 columns
                    source_uid = "Unknown"
                    if len(entry.get('raw_row', [])) >= 3:
                        source_uid = entry['raw_row'][2].strip()
                    
                    print(f"   Option {i}:")
                    print(f"     - Source UID: {source_uid}")
                    print(f"     - Notifications: {'✅' if has_notifications else '❌'}")
                    print(f"     - Schedule: {'✅' if has_schedule else '❌'}")
                    print(f"     - Profiling Enabled: {'✅' if is_enabled else '❌'}")
                    
                except Exception as e:
                    print(f"   Option {i}: Row {entry['row_num']} (Could not parse configuration: {e})")
        
        # Get user input
        while True:
            try:
                choice = input(f"\n🤔 Which configuration do you want to keep for '{target_env}'? (1-{len(entries)}, or 'skip' to skip this target): ").strip()
                
                if choice.lower() == 'skip':
                    skipped_targets.add(target_env)
                    if not quiet_mode:
                        print(f"   ⏭️  Skipping {target_env}")
                    break
                
                choice_num = int(choice)
                if 1 <= choice_num <= len(entries):
                    selected_entry = entries[choice_num - 1]
                    selected_entries.append(selected_entry)
                    if not quiet_mode:
                        print(f"   ✅ Selected Option {choice_num}")
                    break
                else:
                    print(f"   ❌ Please enter a number between 1 and {len(entries)}, or 'skip'")
            except ValueError:
                print(f"   ❌ Please enter a valid number between 1 and {len(entries)}, or 'skip'")
    
    # Add non-duplicate entries
    for target_env, entries in target_groups.items():
        if len(entries) == 1 and target_env not in skipped_targets:
            selected_entries.append(entries[0])
    
    # Create deduplicated CSV - use the same format as the input file
    output_file = csv_file.replace('.csv', '_deduplicated.csv')
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(header)  # Write header (preserves format)
        for entry in selected_entries:
            # Write the same format as input (with or without source-env)
            if len(entry['raw_row']) >= 3:
                writer.writerow([entry['target_env'], entry['profile_json'], entry['raw_row'][2]])
            else:
                writer.writerow([entry['target_env'], entry['profile_json']])
    
    if not quiet_mode:
        print(f"\n✅ Deduplication complete!")
        print(f"   📊 Original entries: {len(entries)}")
        print(f"   📊 Selected entries: {len(selected_entries)}")
        print(f"   📊 Skipped targets: {len(skipped_targets)}")
        print(f"   📄 Output file: {output_file}")
    
    return output_file


def verify_profile_configurations_after_import(csv_file: str, client, logger: logging.Logger, quiet_mode: bool = False, verbose_mode: bool = False, max_threads: int = 5):
    """
    Verify that profile configurations were successfully updated in the target environment.
    
    Args:
        csv_file: Path to the CSV file used for import (to get target UIDs)
        client: API client for making requests
        logger: Logger instance
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
        max_threads: Maximum number of threads for parallel processing
    
    Returns:
        dict: Verification results with success/failure counts
    """
    import csv
    from pathlib import Path
    
    if not Path(csv_file).exists():
        if not quiet_mode:
            print(f"❌ CSV file not found: {csv_file}")
        return None
    
    # Check if a deduplicated version exists and use it for verification
    csv_path = Path(csv_file)
    deduplicated_csv = csv_path.parent / f"{csv_path.stem}_deduplicated{csv_path.suffix}"
    
    if deduplicated_csv.exists():
        csv_file = str(deduplicated_csv)
        if not quiet_mode:
            print(f"📄 Using deduplicated CSV for verification: {csv_file}")
    else:
        if not quiet_mode:
            print(f"📄 Using original CSV for verification: {csv_file}")
    
    if not quiet_mode:
        print(f"\n🔍 Verifying profile configurations in target environment...")
        print(f"📄 Reading target UIDs from: {csv_file}")
        print("="*80)
    
    # Read target UIDs from CSV
    target_uids = []
    with open(csv_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)  # Skip header
        for row_num, row in enumerate(reader, start=2):
            if len(row) >= 1:
                target_uid = row[0].strip()
                if target_uid:
                    target_uids.append(target_uid)
    
    if not target_uids:
        if not quiet_mode:
            print("❌ No target UIDs found in CSV file")
        return None
    
    if not quiet_mode:
        print(f"📊 Found {len(target_uids)} target UIDs to verify")
    
    # Threading setup
    num_threads = max_threads
    min_assets_per_thread = 10
    if len(target_uids) < min_assets_per_thread:
        num_threads = 1
        assets_per_thread = len(target_uids)
    else:
        num_threads = min(max_threads, (len(target_uids) + min_assets_per_thread - 1) // min_assets_per_thread)
        assets_per_thread = (len(target_uids) + num_threads - 1) // num_threads

    if not quiet_mode:
        print(f"Using {num_threads} threads to verify {len(target_uids)} profiles")
        print(f"Assets per thread: {assets_per_thread}")
        print("="*80)
    
    # Verification results
    verification_results = {
        'total_checked': 0,
        'successful': 0,
        'failed': 0,
        'asset_not_found': 0,
        'profile_not_found': 0,
        'details': []
    }
    
    # Thread synchronization
    lock = threading.Lock()
    thread_results = []
    
    def process_verification_chunk(thread_id, chunk):
        thread_successful = 0
        thread_failed = 0
        thread_asset_not_found = 0
        thread_profile_not_found = 0
        thread_details = []
        
        # Get thread names for consistent naming across commands
        thread_names = get_thread_names()
        thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
        if not verbose_mode:
            thread_pbar = create_progress_bar(
                total=len(chunk),
                desc=thread_name,
                unit="profiles",
                disable=quiet_mode,
                position=thread_id,
                leave=False
            )
        else:
            thread_pbar = None
        
        for i, target_uid in enumerate(chunk):
            if verbose_mode:
                print(f"\n[{thread_id}] [{i+1}/{len(chunk)}] Verifying: {target_uid}")
            
            try:
                # Step 1: Get asset ID from target UID
                if verbose_mode:
                    print(f"   GET /catalog-server/api/assets?uid={target_uid}")
                
                asset_response = client.make_api_call(
                    endpoint=f"/catalog-server/api/assets?uid={target_uid}",
                    method='GET',
                    use_target_auth=True,
                    use_target_tenant=True
                )
                
                if not asset_response or 'data' not in asset_response or not asset_response['data']:
                    error_msg = f"Asset not found for UID: {target_uid}"
                    if verbose_mode:
                        print(f"   ❌ {error_msg}")
                    thread_asset_not_found += 1
                    thread_details.append({
                        'target_uid': target_uid,
                        'status': 'asset_not_found',
                        'error': error_msg
                    })
                    if thread_pbar:
                        thread_pbar.update(1)
                    continue
                
                asset_id = asset_response['data'][0]['id']
                if verbose_mode:
                    print(f"   ✅ Found asset ID: {asset_id}")
                
                # Step 2: Get profile configuration from target
                if verbose_mode:
                    print(f"   GET /catalog-server/api/profile/{asset_id}/config")
                
                profile_response = client.make_api_call(
                    endpoint=f"/catalog-server/api/profile/{asset_id}/config",
                    method='GET',
                    use_target_auth=True,
                    use_target_tenant=True
                )
                
                if not profile_response:
                    error_msg = f"Profile configuration not found for asset ID: {asset_id}"
                    if verbose_mode:
                        print(f"   ❌ {error_msg}")
                    thread_profile_not_found += 1
                    thread_details.append({
                        'target_uid': target_uid,
                        'asset_id': asset_id,
                        'status': 'profile_not_found',
                        'error': error_msg
                    })
                    if thread_pbar:
                        thread_pbar.update(1)
                    continue
                
                # Step 3: Verify profile configuration has expected structure
                profile_settings = profile_response.get("profileSettingsConfigs", {})
                notification_channels = profile_settings.get("profileNotificationChannels", {})
                
                # Check for key configuration elements
                has_notifications = bool(notification_channels and notification_channels.get("configuredNotificationGroupIds"))
                has_schedule = bool(profile_settings.get("schedule"))
                is_enabled = profile_settings.get("enabled", False)
                
                if verbose_mode:
                    print(f"   📋 Configuration details:")
                    print(f"      - Notifications: {'✅' if has_notifications else '❌'}")
                    print(f"      - Schedule: {'✅' if has_schedule else '❌'}")
                    print(f"      - Enabled: {'✅' if is_enabled else '❌'}")
                    if has_notifications:
                        notification_ids = notification_channels.get("configuredNotificationGroupIds", [])
                        print(f"      - Notification IDs: {notification_ids}")
                
                # Consider it successful if we can retrieve the configuration
                thread_successful += 1
                thread_details.append({
                    'target_uid': target_uid,
                    'asset_id': asset_id,
                    'status': 'success',
                    'has_notifications': has_notifications,
                    'has_schedule': has_schedule,
                    'is_enabled': is_enabled,
                    'notification_ids': notification_channels.get("configuredNotificationGroupIds", []) if has_notifications else []
                })
                
                if verbose_mode:
                    print(f"   ✅ Configuration verified successfully")
                
            except Exception as e:
                error_msg = f"Error verifying {target_uid}: {str(e)}"
                if verbose_mode:
                    print(f"   ❌ {error_msg}")
                thread_failed += 1
                thread_details.append({
                    'target_uid': target_uid,
                    'status': 'error',
                    'error': error_msg
                })
            
            if thread_pbar:
                thread_pbar.update(1)
        
        if thread_pbar:
            thread_pbar.close()
        
        # Update global results with thread lock
        with lock:
            verification_results['total_checked'] += len(chunk)
            verification_results['successful'] += thread_successful
            verification_results['failed'] += thread_failed
            verification_results['asset_not_found'] += thread_asset_not_found
            verification_results['profile_not_found'] += thread_profile_not_found
            verification_results['details'].extend(thread_details)
            thread_results.append({
                'thread_id': thread_id,
                'successful': thread_successful,
                'failed': thread_failed,
                'asset_not_found': thread_asset_not_found,
                'profile_not_found': thread_profile_not_found,
                'total_assets': len(chunk)
            })
    
    # Create and start threads
    threads = []
    for thread_id in range(num_threads):
        start_index = thread_id * assets_per_thread
        end_index = min(start_index + assets_per_thread, len(target_uids))
        chunk = target_uids[start_index:end_index]
        
        if chunk:  # Only create thread if there are assets to process
            thread = threading.Thread(target=process_verification_chunk, args=(thread_id, chunk))
            threads.append(thread)
            thread.start()
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    # Print thread statistics
    if thread_results and not quiet_mode:
        print(f"\nThread Statistics:")
        for result in thread_results:
            thread_name = f"Thread {result['thread_id']}"
            print(f"  {thread_name}: {result['successful']} successful, {result['failed']} failed, {result['asset_not_found']} asset not found, {result['profile_not_found']} profile not found, {result['total_assets']} assets")
    
    # Generate CSV report
    csv_report_path = generate_verification_csv_report(verification_results, csv_file, quiet_mode, verbose_mode)
    
    # Print summary
    if not quiet_mode:
        print(f"\n" + "="*80)
        print("PROFILE CONFIGURATION VERIFICATION COMPLETED")
        print("="*80)
        print(f"📊 Total checked: {verification_results['total_checked']}")
        print(f"✅ Successful: {verification_results['successful']}")
        print(f"❌ Failed: {verification_results['failed']}")
        print(f"🔍 Asset not found: {verification_results['asset_not_found']}")
        print(f"📋 Profile not found: {verification_results['profile_not_found']}")
        
        if verification_results['successful'] > 0:
            success_rate = (verification_results['successful'] / verification_results['total_checked']) * 100
            print(f"📈 Success rate: {success_rate:.1f}%")
        
        # Enhanced detailed breakdown
        print(f"\n🔍 DETAILED BREAKDOWN:")
        print("=" * 60)
        
        # Categorize results by status
        status_counts = {}
        for detail in verification_results['details']:
            status = detail.get('status', 'unknown')
            status_counts[status] = status_counts.get(status, 0) + 1
        
        for status, count in status_counts.items():
            status_icon = {
                'success': '✅',
                'asset_not_found': '❌',
                'profile_not_found': '❌',
                'error': '❌'
            }.get(status, '❓')
            print(f"   {status_icon} {status.replace('_', ' ').title()}: {count} assets")
        
        # Show specific failure details
        failed_results = [d for d in verification_results['details'] if d.get('status') != 'success']
        if failed_results:
            print(f"\n❌ FAILURE DETAILS:")
            print("-" * 60)
            failure_reasons = {}
            for result in failed_results:
                error_msg = result.get('error', 'Unknown error')
                failure_reasons[error_msg] = failure_reasons.get(error_msg, 0) + 1
            
            for reason, count in failure_reasons.items():
                print(f"   • {reason}: {count} assets")
            
            # Show examples of failed assets
            print(f"\n📋 Examples of failed assets:")
            for i, result in enumerate(failed_results[:3]):  # Show first 3 examples
                print(f"   {i+1}. {result.get('target_uid', 'N/A')}: {result.get('error', 'Unknown error')}")
            if len(failed_results) > 3:
                print(f"   ... and {len(failed_results) - 3} more")
        
        print("=" * 60)
        
        if csv_report_path:
            print(f"📄 Detailed CSV report: {csv_report_path}")
        
        # Show some successful configurations as examples
        successful_configs = [d for d in verification_results['details'] if d['status'] == 'success']
        if successful_configs:
            print(f"\n📋 Sample successful configurations:")
            for i, config in enumerate(successful_configs[:3], 1):  # Show first 3
                print(f"   {i}. {config['target_uid']}")
                print(f"      - Asset ID: {config['asset_id']}")
                print(f"      - Notifications: {'✅' if config['has_notifications'] else '❌'}")
                print(f"      - Schedule: {'✅' if config['has_schedule'] else '❌'}")
                print(f"      - Enabled: {'✅' if config['is_enabled'] else '❌'}")
                if config['has_notifications']:
                    print(f"      - Notification IDs: {config['notification_ids']}")
    
    return verification_results


def generate_verification_csv_report(verification_results: dict, input_csv_file: str, quiet_mode: bool = False, verbose_mode: bool = False) -> str:
    """
    Generate a detailed CSV report of verification results.
    
    Args:
        verification_results: Results from verify_profile_configurations_after_import
        input_csv_file: Original CSV file path (for naming the report)
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
    
    Returns:
        str: Path to the generated CSV report file
    """
    import csv
    from pathlib import Path
    from datetime import datetime
    
    if not verification_results or not verification_results['details']:
        if not quiet_mode:
            print("❌ No verification results to generate CSV report")
        return None
    
    # Generate output file path
    input_path = Path(input_csv_file)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create output directory if it doesn't exist
    output_dir = input_path.parent / "verification-reports"
    output_dir.mkdir(exist_ok=True)
    
    # Generate report filename
    report_filename = f"profile_verification_report_{timestamp}.csv"
    report_path = output_dir / report_filename
    
    if not quiet_mode:
        print(f"\n📄 Generating detailed CSV report: {report_path}")
    
    # Write CSV report
    with open(report_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        
        # Write header
        writer.writerow([
            'Target_UID',
            'Asset_ID',
            'Verification_Status',
            'Has_Notifications',
            'Has_Schedule',
            'Profiling_Enabled',
            'Notification_Group_IDs',
            'Error_Message',
            'Verification_Details'
        ])
        
        # Write data rows
        for detail in verification_results['details']:
            target_uid = detail.get('target_uid', 'N/A')
            asset_id = detail.get('asset_id', 'N/A')
            status = detail.get('status', 'unknown')
            
            # Format status for better readability
            status_display = {
                'success': '✅ PASSED',
                'asset_not_found': '❌ ASSET_NOT_FOUND',
                'profile_not_found': '❌ PROFILE_NOT_FOUND',
                'error': '❌ ERROR'
            }.get(status, status.upper())
            
            # Extract configuration details for successful verifications
            has_notifications = 'Yes' if detail.get('has_notifications', False) else 'No'
            has_schedule = 'Yes' if detail.get('has_schedule', False) else 'No'
            profiling_enabled = 'Yes' if detail.get('is_enabled', False) else 'No'
            notification_ids = detail.get('notification_ids', [])
            notification_ids_str = ', '.join(map(str, notification_ids)) if notification_ids else 'None'
            
            # Error message for failed verifications
            error_message = detail.get('error', '')
            
            # Additional verification details
            verification_details = []
            if status == 'success':
                verification_details.append(f"Asset ID: {asset_id}")
                verification_details.append(f"Notifications: {has_notifications}")
                verification_details.append(f"Schedule: {has_schedule}")
                verification_details.append(f"Enabled: {profiling_enabled}")
                if notification_ids:
                    verification_details.append(f"Notification IDs: {notification_ids_str}")
            else:
                verification_details.append(f"Error: {error_message}")
            
            verification_details_str = ' | '.join(verification_details)
            
            # Write row
            writer.writerow([
                target_uid,
                asset_id,
                status_display,
                has_notifications,
                has_schedule,
                profiling_enabled,
                notification_ids_str,
                error_message,
                verification_details_str
            ])
    
    if not quiet_mode:
        print(f"✅ CSV report generated successfully!")
        print(f"   📊 Total records: {len(verification_results['details'])}")
        
        # Show detailed summary statistics
        successful_count = len([d for d in verification_results['details'] if d['status'] == 'success'])
        mismatch_count = len([d for d in verification_results['details'] if d['status'] == 'mismatch'])
        error_count = len([d for d in verification_results['details'] if d['status'] == 'error'])
        asset_not_found_count = len([d for d in verification_results['details'] if d['status'] == 'asset_not_found'])
        config_not_found_count = len([d for d in verification_results['details'] if d['status'] == 'config_not_found'])
        
        print(f"   ✅ Passed: {successful_count}")
        print(f"   ⚠️  Mismatch: {mismatch_count}")
        print(f"   ❌ Error: {error_count}")
        print(f"   🔍 Asset not found: {asset_not_found_count}")
        print(f"   ⚙️  Config not found: {config_not_found_count}")
        
        # Show CSV report insights
        if mismatch_count > 0:
            print(f"\n📋 CSV Report Insights:")
            print(f"   📄 Report file: {report_path}")
            print(f"   🔍 Check 'Verification_Details' column for specific mismatch reasons")
            print(f"   📊 Use 'Verification_Status' column to filter results")
            print(f"   💡 Mismatched assets may need manual review or re-import")
        
        if verbose_mode:
            print(f"   📁 Report location: {report_path}")
    
    return str(report_path)
def verify_asset_configurations_after_import(input_csv_file: str, client, logger: logging.Logger, quiet_mode: bool = False, verbose_mode: bool = False, max_threads: int = 5):
    """
    Verify that asset configurations were successfully imported by checking the target environment.
    
    Args:
        input_csv_file: Path to the asset-config-import-ready.csv file
        client: API client instance
        logger: Logger instance
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
        max_threads: Maximum number of threads for parallel processing
    
    Returns:
        dict: Verification results with summary and details
    """
    try:
        # Check if CSV file exists
        csv_path = Path(input_csv_file)
        if not csv_path.exists():
            error_msg = f"CSV file does not exist: {input_csv_file}"
            print(f"❌ {error_msg}")
            logger.error(error_msg)
            return None
        
        # Read CSV data
        asset_data = []
        with open(input_csv_file, 'r', newline='', encoding='utf-8') as f:
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
            return None
        
        if not quiet_mode:
            print(f"🔍 Verifying {len(asset_data)} asset configurations...")
        
        # Determine number of threads
        num_threads = min(max_threads, max(1, len(asset_data)))
        
        if not quiet_mode:
            print(f"🔄 Using {num_threads} threads for verification")
        
        # Thread-safe counters
        successful = 0
        failed = 0
        asset_not_found = 0
        config_not_found = 0
        lock = threading.Lock()
        all_results = []
        
        # Create progress bar if in quiet mode
        if quiet_mode and not verbose_mode:
            pbar = tqdm(total=len(asset_data), desc="Verifying configs", colour='blue')
        
        def verify_asset_config_chunk(thread_id, start_index, end_index):
            nonlocal successful, failed, asset_not_found, config_not_found
            thread_successful = 0
            thread_failed = 0
            thread_asset_not_found = 0
            thread_config_not_found = 0
            thread_results = []
            
            thread_name = f"Thread-{thread_id}"
            
            for i in range(start_index, end_index):
                if i >= len(asset_data):
                    break
                
                asset = asset_data[i]
                target_uid = asset['target_uid']
                expected_config = asset['config_json']
                
                try:
                    # Step 1: Get asset ID from target_uid
                    if verbose_mode:
                        print(f"\n🔍 {thread_name}: Processing asset {i+1}/{len(asset_data)}: {target_uid}")
                        print(f"   GET /catalog-server/api/assets?uid={target_uid}")
                    
                    # Make GET request to get asset ID
                    response = client.make_api_call(
                        endpoint=f'/catalog-server/api/assets?uid={target_uid}',
                        method='GET',
                        use_target_auth=True,
                        use_target_tenant=True
                    )
                    
                    if not response or 'data' not in response or not response['data']:
                        error_msg = f"No asset found for UID: {target_uid}"
                        if verbose_mode:
                            print(f"   ❌ {thread_name}: {error_msg}")
                        thread_asset_not_found += 1
                        thread_results.append({
                            'target_uid': target_uid,
                            'asset_id': None,
                            'status': 'asset_not_found',
                            'error': error_msg,
                            'has_config': False,
                            'config_details': {}
                        })
                        continue
                    
                    asset_id = response['data'][0].get('id')
                    if not asset_id:
                        error_msg = f"No asset ID found for UID: {target_uid}"
                        if verbose_mode:
                            print(f"   ❌ {thread_name}: {error_msg}")
                        thread_asset_not_found += 1
                        thread_results.append({
                            'target_uid': target_uid,
                            'asset_id': None,
                            'status': 'asset_not_found',
                            'error': error_msg,
                            'has_config': False,
                            'config_details': {}
                        })
                        continue
                    
                    # Step 2: Get asset configuration
                    if verbose_mode:
                        print(f"   GET /catalog-server/api/assets/{asset_id}/config")
                    
                    config_response = client.make_api_call(
                        endpoint=f'/catalog-server/api/assets/{asset_id}/config',
                        method='GET',
                        use_target_auth=True,
                        use_target_tenant=True
                    )
                    
                    if not config_response or 'assetConfiguration' not in config_response:
                        error_msg = f"No configuration found for asset ID: {asset_id}"
                        if verbose_mode:
                            print(f"   ❌ {thread_name}: {error_msg}")
                        thread_config_not_found += 1
                        thread_results.append({
                            'target_uid': target_uid,
                            'asset_id': asset_id,
                            'status': 'config_not_found',
                            'error': error_msg,
                            'has_config': False,
                            'config_details': {}
                        })
                        continue
                    
                    # Step 3: Compare configurations
                    actual_config = config_response.get('assetConfiguration') or {}
                    
                    # Parse expected config JSON
                    try:
                        expected_config_dict = json.loads(expected_config)
                        expected_asset_config = expected_config_dict.get('assetConfiguration') or {}
                    except json.JSONDecodeError as e:
                        error_msg = f"Invalid expected config JSON: {e}"
                        if verbose_mode:
                            print(f"   ❌ {thread_name}: {error_msg}")
                        thread_failed += 1
                        thread_results.append({
                            'target_uid': target_uid,
                            'asset_id': asset_id,
                            'status': 'error',
                            'error': error_msg,
                            'has_config': True,
                            'config_details': actual_config
                        })
                        continue
                    
                    # Comprehensive configuration verification
                    config_verification = {
                        'has_schedule': actual_config.get('scheduled', False),
                        'has_timezone': bool(actual_config.get('timeZone')),
                        'has_spark_config': bool(actual_config.get('sparkResourceConfig')),
                        'incremental_strategy': bool(actual_config.get('markerConfiguration')),
                        'has_notifications': bool(actual_config.get('notificationChannels')),
                        'is_pattern_profile': actual_config.get('isPatternProfile', False),
                        'column_level': actual_config.get('columnLevel'),
                        'resource_strategy': actual_config.get('resourceStrategyType'),
                        'auto_retry_enabled': actual_config.get('autoRetryEnabled', False),
                        'is_user_marked_reference': actual_config.get('isUserMarkedReference', False),
                        'is_reference_check_valid': actual_config.get('isReferenceCheckValid', False),
                        'has_reference_check_config': bool(actual_config.get('referenceCheckConfiguration')),
                        'profile_anomaly_sensitivity': actual_config.get('profileAnomalyModelSensitivity'),
                        'cadence_anomaly_training_window': actual_config.get('cadenceAnomalyTrainingWindowMinimumInDays')
                    }
                    
                    # Check if key configurations match
                    verification_passed = True
                    verification_details = []
                    detailed_mismatches = []
                    
                    # Compare key fields with detailed mismatch information and default config detection
                    expected_scheduled = expected_asset_config.get('scheduled')
                    actual_scheduled = actual_config.get('scheduled')
                    if expected_scheduled != actual_scheduled:
                        verification_passed = False
                        verification_details.append("Schedule mismatch")
                        if expected_scheduled and not actual_scheduled:
                            detailed_mismatches.append(f"Schedule: Default config present in source but missing in target")
                        elif not expected_scheduled and actual_scheduled:
                            detailed_mismatches.append(f"Schedule: Default config missing in source but present in target")
                        else:
                            detailed_mismatches.append(f"Schedule: Expected={expected_scheduled}, Actual={actual_scheduled}")
                    
                    expected_timezone = expected_asset_config.get('timeZone')
                    actual_timezone = actual_config.get('timeZone')
                    if expected_timezone != actual_timezone:
                        verification_passed = False
                        verification_details.append("Timezone mismatch")
                        if expected_timezone and not actual_timezone:
                            detailed_mismatches.append(f"Timezone: Default config present in source but missing in target")
                        elif not expected_timezone and actual_timezone:
                            detailed_mismatches.append(f"Timezone: Default config missing in source but present in target")
                        else:
                            detailed_mismatches.append(f"Timezone: Expected={expected_timezone}, Actual={actual_timezone}")
                    
                    expected_pattern = expected_asset_config.get('isPatternProfile')
                    actual_pattern = actual_config.get('isPatternProfile')
                    if expected_pattern != actual_pattern:
                        verification_passed = False
                        verification_details.append("Pattern profile mismatch")
                        if expected_pattern and not actual_pattern:
                            detailed_mismatches.append(f"Pattern Profile: Default config present in source but missing in target")
                        elif not expected_pattern and actual_pattern:
                            detailed_mismatches.append(f"Pattern Profile: Default config missing in source but present in target")
                        else:
                            detailed_mismatches.append(f"Pattern Profile: Expected={expected_pattern}, Actual={actual_pattern}")
                    
                    expected_column_level = expected_asset_config.get('columnLevel')
                    actual_column_level = actual_config.get('columnLevel')
                    if expected_column_level != actual_column_level:
                        verification_passed = False
                        verification_details.append("Column level mismatch")
                        if expected_column_level and not actual_column_level:
                            detailed_mismatches.append(f"Column Level: Default config present in source but missing in target")
                        elif not expected_column_level and actual_column_level:
                            detailed_mismatches.append(f"Column Level: Default config missing in source but present in target")
                        else:
                            detailed_mismatches.append(f"Column Level: Expected={expected_column_level}, Actual={actual_column_level}")
                    
                    expected_resource_strategy = expected_asset_config.get('resourceStrategyType')
                    actual_resource_strategy = actual_config.get('resourceStrategyType')
                    if expected_resource_strategy != actual_resource_strategy:
                        verification_passed = False
                        verification_details.append("Resource strategy mismatch")
                        if expected_resource_strategy and not actual_resource_strategy:
                            detailed_mismatches.append(f"Resource Strategy: Default config present in source but missing in target")
                        elif not expected_resource_strategy and actual_resource_strategy:
                            detailed_mismatches.append(f"Resource Strategy: Default config missing in source but present in target")
                        else:
                            detailed_mismatches.append(f"Resource Strategy: Expected={expected_resource_strategy}, Actual={actual_resource_strategy}")
                    
                    expected_user_reference = expected_asset_config.get('isUserMarkedReference')
                    actual_user_reference = actual_config.get('isUserMarkedReference')
                    if expected_user_reference != actual_user_reference:
                        verification_passed = False
                        verification_details.append("User marked reference mismatch")
                        if expected_user_reference and not actual_user_reference:
                            detailed_mismatches.append(f"User Marked Reference: Default config present in source but missing in target")
                        elif not expected_user_reference and actual_user_reference:
                            detailed_mismatches.append(f"User Marked Reference: Default config missing in source but present in target")
                        else:
                            detailed_mismatches.append(f"User Marked Reference: Expected={expected_user_reference}, Actual={actual_user_reference}")
                    
                    expected_sensitivity = expected_asset_config.get('profileAnomalyModelSensitivity')
                    actual_sensitivity = actual_config.get('profileAnomalyModelSensitivity')
                    if expected_sensitivity != actual_sensitivity:
                        verification_passed = False
                        verification_details.append("Profile anomaly sensitivity mismatch")
                        if expected_sensitivity and not actual_sensitivity:
                            detailed_mismatches.append(f"Profile Anomaly Sensitivity: Default config present in source but missing in target")
                        elif not expected_sensitivity and actual_sensitivity:
                            detailed_mismatches.append(f"Profile Anomaly Sensitivity: Default config missing in source but present in target")
                        else:
                            detailed_mismatches.append(f"Profile Anomaly Sensitivity: Expected={expected_sensitivity}, Actual={actual_sensitivity}")
                    
                    expected_training_window = expected_asset_config.get('cadenceAnomalyTrainingWindowMinimumInDays')
                    actual_training_window = actual_config.get('cadenceAnomalyTrainingWindowMinimumInDays')
                    if expected_training_window != actual_training_window:
                        verification_passed = False
                        verification_details.append("Cadence anomaly training window mismatch")
                        if expected_training_window and not actual_training_window:
                            detailed_mismatches.append(f"Training Window: Default config present in source but missing in target")
                        elif not expected_training_window and actual_training_window:
                            detailed_mismatches.append(f"Training Window: Default config missing in source but present in target")
                        else:
                            detailed_mismatches.append(f"Training Window: Expected={expected_training_window}, Actual={actual_training_window}")
                    
                    expected_auto_retry = expected_asset_config.get('autoRetryEnabled')
                    actual_auto_retry = actual_config.get('autoRetryEnabled')
                    if expected_auto_retry != actual_auto_retry:
                        verification_passed = False
                        verification_details.append("Auto retry enabled mismatch")
                        if expected_auto_retry and not actual_auto_retry:
                            detailed_mismatches.append(f"Auto Retry: Default config present in source but missing in target")
                        elif not expected_auto_retry and actual_auto_retry:
                            detailed_mismatches.append(f"Auto Retry: Default config missing in source but present in target")
                        else:
                            detailed_mismatches.append(f"Auto Retry: Expected={expected_auto_retry}, Actual={actual_auto_retry}")
                    
                    # Determine config status based on expected vs actual configuration
                    config_status = "Default Config Present"
                    if expected_asset_config:
                        # Check if the expected config has any non-default values
                        has_custom_config = False
                        for key, expected_value in expected_asset_config.items():
                            if expected_value is not None and expected_value != "":
                                has_custom_config = True
                                break
                        
                        if has_custom_config:
                            config_status = "Config Changed"
                    
                    if verification_passed:
                        if verbose_mode:
                            print(f"   ✅ {thread_name}: Configuration verified successfully for {target_uid}")
                        thread_successful += 1
                        thread_results.append({
                            'target_uid': target_uid,
                            'asset_id': asset_id,
                            'status': 'success',
                            'error': None,
                            'has_config': True,
                            'config_details': config_verification,
                            'verification_details': 'All configurations match',
                            'config_status': config_status
                        })
                    else:
                        if verbose_mode:
                            print(f"   ⚠️ {thread_name}: Configuration mismatch for {target_uid}: {', '.join(verification_details)}")
                        thread_failed += 1
                        thread_results.append({
                            'target_uid': target_uid,
                            'asset_id': asset_id,
                            'status': 'mismatch',
                            'error': f"Configuration mismatch: {', '.join(verification_details)}",
                            'has_config': True,
                            'config_details': config_verification,
                            'verification_details': ', '.join(verification_details),
                            'detailed_mismatches': detailed_mismatches,
                            'config_status': config_status
                        })
                
                except Exception as e:
                    error_msg = f"Error verifying asset {target_uid}: {str(e)}"
                    if verbose_mode:
                        print(f"   ❌ {thread_name}: {error_msg}")
                    thread_failed += 1
                    thread_results.append({
                        'target_uid': target_uid,
                        'asset_id': None,
                        'status': 'error',
                        'error': error_msg,
                        'has_config': False,
                        'config_details': {}
                    })
                
                # Update progress bar
                if quiet_mode and not verbose_mode:
                    pbar.update(1)
            
            # Update global counters
            with lock:
                successful += thread_successful
                failed += thread_failed
                asset_not_found += thread_asset_not_found
                config_not_found += thread_config_not_found
                all_results.extend(thread_results)
            
            if verbose_mode:
                print(f"🔍 {thread_name}: Completed - {thread_successful} success, {thread_failed} failed, {thread_asset_not_found} asset not found, {thread_config_not_found} config not found")
        
        # Create and start threads
        threads = []
        chunk_size = len(asset_data) // num_threads
        remainder = len(asset_data) % num_threads
        
        start_index = 0
        for thread_id in range(num_threads):
            end_index = start_index + chunk_size + (1 if thread_id < remainder else 0)
            thread = threading.Thread(
                target=verify_asset_config_chunk,
                args=(thread_id + 1, start_index, end_index)
            )
            threads.append(thread)
            thread.start()
            start_index = end_index
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Close progress bar
        if quiet_mode and not verbose_mode:
            pbar.close()
        
        # Compile results
        verification_results = {
            'summary': {
                'total_assets': len(asset_data),
                'successful': successful,
                'failed': failed,
                'asset_not_found': asset_not_found,
                'config_not_found': config_not_found
            },
            'details': all_results
        }
        
        # Display summary
        if not quiet_mode:
            print(f"\n📊 Asset Configuration Verification Summary:")
            print(f"   📋 Total assets processed: {len(asset_data)}")
            print(f"   ✅ Successfully verified: {successful}")
            print(f"   ❌ Failed verification: {failed}")
            print(f"   🔍 Asset not found: {asset_not_found}")
            print(f"   ⚙️ Config not found: {config_not_found}")
            
            if successful > 0:
                success_rate = (successful / len(asset_data)) * 100
                print(f"   📈 Success rate: {success_rate:.1f}%")
            
            # Enhanced detailed breakdown
            print(f"\n🔍 DETAILED BREAKDOWN:")
            print("=" * 60)
            
            # Categorize results by status
            status_counts = {}
            for result in all_results:
                status = result.get('status', 'unknown')
                status_counts[status] = status_counts.get(status, 0) + 1
            
            for status, count in status_counts.items():
                status_icon = {
                    'success': '✅',
                    'mismatch': '⚠️',
                    'asset_not_found': '❌',
                    'config_not_found': '❌',
                    'error': '❌'
                }.get(status, '❓')
                print(f"   {status_icon} {status.replace('_', ' ').title()}: {count} assets")
            
            # Show specific mismatch details
            mismatch_results = [r for r in all_results if r.get('status') == 'mismatch']
            if mismatch_results:
                print(f"\n⚠️  CONFIGURATION MISMATCHES DETAILS:")
                print("-" * 60)
                mismatch_reasons = {}
                for result in mismatch_results:
                    error_msg = result.get('error', 'Unknown mismatch')
                    mismatch_reasons[error_msg] = mismatch_reasons.get(error_msg, 0) + 1
                
                for reason, count in mismatch_reasons.items():
                    print(f"   • {reason}: {count} assets")
                
                # Show detailed mismatch reasons
                print(f"\n🔍 DETAILED MISMATCH REASONS:")
                print("-" * 60)
                detailed_reasons = {}
                for result in mismatch_results:
                    detailed_mismatches = result.get('detailed_mismatches', [])
                    for mismatch in detailed_mismatches:
                        detailed_reasons[mismatch] = detailed_reasons.get(mismatch, 0) + 1
                
                # Group by configuration field for better readability
                field_groups = {}
                for reason, count in detailed_reasons.items():
                    # Extract field name (everything before the colon)
                    field_name = reason.split(':')[0].strip()
                    if field_name not in field_groups:
                        field_groups[field_name] = []
                    field_groups[field_name].append((reason, count))
                
                for field_name, reasons in field_groups.items():
                    print(f"   📋 {field_name}:")
                    for reason, count in reasons:
                        # Extract the specific reason (everything after the colon)
                        specific_reason = reason.split(':', 1)[1].strip()
                        print(f"      • {specific_reason}: {count} assets")
                
                # Show examples of mismatched assets with specific details
                print(f"\n📋 Examples of mismatched assets:")
                for i, result in enumerate(mismatch_results[:3]):  # Show first 3 examples with details
                    print(f"   {i+1}. {result.get('target_uid', 'N/A')}")
                    detailed_mismatches = result.get('detailed_mismatches', [])
                    for mismatch in detailed_mismatches:
                        print(f"      - {mismatch}")
                if len(mismatch_results) > 3:
                    print(f"   ... and {len(mismatch_results) - 3} more assets with similar mismatches")
            
            # Show error details
            error_results = [r for r in all_results if r.get('status') == 'error']
            if error_results:
                print(f"\n❌ ERROR DETAILS:")
                print("-" * 60)
                error_types = {}
                for result in error_results:
                    error_msg = result.get('error', 'Unknown error')
                    error_types[error_msg] = error_types.get(error_msg, 0) + 1
                
                for error_type, count in error_types.items():
                    print(f"   • {error_type}: {count} assets")
            
            print("=" * 60)
        
        return verification_results
    
    except Exception as e:
        error_msg = f"Error in asset configuration verification: {e}"
        print(f"❌ {error_msg}")
        logger.error(error_msg)
        return None

def generate_config_verification_csv_report(verification_results: dict, input_csv_file: str, quiet_mode: bool = False, verbose_mode: bool = False):
    """
    Generate a detailed CSV report of asset configuration verification results.
    
    Args:
        verification_results: Results from verify_asset_configurations_after_import
        input_csv_file: Path to the input CSV file
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
    
    Returns:
        str: Path to the generated CSV report
    """
    if not verification_results or 'details' not in verification_results:
        if not quiet_mode:
            print("❌ No verification results to report")
        return None
    
    # Generate output file path
    input_path = Path(input_csv_file)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create output directory if it doesn't exist
    output_dir = input_path.parent / "verification-reports"
    output_dir.mkdir(exist_ok=True)
    
    # Generate report filename
    report_filename = f"config_verification_report_{timestamp}.csv"
    report_path = output_dir / report_filename
    
    if not quiet_mode:
        print(f"\n📄 Generating detailed CSV report: {report_path}")
    
    # Write CSV report
    with open(report_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        
        # Write header
        writer.writerow([
            'Target_UID',
            'Asset_ID',
            'Verification_Status',
            'Config_Status',
            'Has_Spark_Config',
            'Incremental_Strategy',
            'Resource_Strategy',
            'Auto_Retry_Enabled',
            'Is_User_Marked_Reference',
            'Is_Reference_Check_Valid',
            'Has_Reference_Check_Config',
            'Error_Message',
            'Verification_Details'
        ])
        
        # Write data rows
        for detail in verification_results['details']:
            target_uid = detail.get('target_uid', 'N/A')
            asset_id = detail.get('asset_id', 'N/A')
            status = detail.get('status', 'unknown')
            
            # Format status for better readability
            status_display = {
                'success': '✅ PASSED',
                'mismatch': '⚠️ MISMATCH',
                'asset_not_found': '❌ ASSET_NOT_FOUND',
                'config_not_found': '❌ CONFIG_NOT_FOUND',
                'error': '❌ ERROR'
            }.get(status, status.upper())
            
            # Extract configuration details
            config_details = detail.get('config_details', {})
            config_status = detail.get('config_status', 'Unknown')
            has_spark_config = 'Yes' if config_details.get('has_spark_config', False) else 'No'
            incremental_strategy = 'Yes' if config_details.get('incremental_strategy', False) else 'No'
            resource_strategy = config_details.get('resource_strategy', 'N/A')
            auto_retry_enabled = 'Yes' if config_details.get('auto_retry_enabled', False) else 'No'
            is_user_marked_reference = 'Yes' if config_details.get('is_user_marked_reference', False) else 'No'
            is_reference_check_valid = 'Yes' if config_details.get('is_reference_check_valid', False) else 'No'
            has_reference_check_config = 'Yes' if config_details.get('has_reference_check_config', False) else 'No'
            
            # Error message for failed verifications
            error_message = detail.get('error', '')
            
            # Verification details
            verification_details = detail.get('verification_details', '')
            
            # Write row
            writer.writerow([
                target_uid,
                asset_id,
                status_display,
                config_status,
                has_spark_config,
                incremental_strategy,
                resource_strategy,
                auto_retry_enabled,
                is_user_marked_reference,
                is_reference_check_valid,
                has_reference_check_config,
                error_message,
                verification_details
            ])
    
    if not quiet_mode:
        print(f"✅ CSV report generated successfully!")
        print(f"   📊 Total records: {len(verification_results['details'])}")
        
        # Show summary statistics
        successful_count = len([d for d in verification_results['details'] if d['status'] == 'success'])
        failed_count = len([d for d in verification_results['details'] if d['status'] != 'success'])
        
        print(f"   ✅ Passed: {successful_count}")
        print(f"   ❌ Failed: {failed_count}")
        
        if verbose_mode:
            print(f"   📁 Report location: {report_path}")
    
    return str(report_path)

def check_for_duplicates_in_asset_data(asset_data: List[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    """
    Check for duplicate target UIDs in asset data.
    
    Args:
        asset_data: List of asset dictionaries with target_uid and config_json
    
    Returns:
        Dictionary mapping target_uid to list of duplicate configurations, empty if no duplicates
    """
    uid_groups = {}
    
    for asset in asset_data:
        target_uid = asset['target_uid']
        if target_uid not in uid_groups:
            uid_groups[target_uid] = []
        uid_groups[target_uid].append(asset)
    
    # Return only groups with more than one configuration
    duplicates = {uid: configs for uid, configs in uid_groups.items() if len(configs) > 1}
    return duplicates

def resolve_duplicates_interactively(asset_data: List[Dict[str, str]], duplicates: Dict[str, List[Dict[str, str]]], quiet_mode: bool = False, verbose_mode: bool = False) -> Optional[List[Dict[str, str]]]:
    """
    Resolve duplicates interactively and return the resolved asset data.
    
    Args:
        asset_data: Original list of asset dictionaries
        duplicates: Dictionary of duplicate configurations
        quiet_mode: Whether to suppress output
        verbose_mode: Whether to enable verbose logging
    
    Returns:
        Resolved asset data list, or None if user cancels
    """
    try:
        resolved_assets = []
        processed_uids = set()
        
        if not quiet_mode:
            print(f"\n⚠️  Found {len(duplicates)} Source UIDs which are pointing to single Target UIDs. So we have duplicate configurations present:")
            
            # Check if we're using legacy format (no source UIDs available)
            legacy_format_detected = False
            for target_uid, configs in duplicates.items():
                for config in configs:
                    if config.get('source_uid', 'Unknown') == 'Unknown':
                        legacy_format_detected = True
                        break
                if legacy_format_detected:
                    break
            
            if legacy_format_detected:
                print("💡 Note: Source UID information is not available (using legacy export format).")
                print("   For better duplicate resolution, consider re-running 'asset-config-export' to get source UID information.")
        
        for target_uid, configs in duplicates.items():
            if not quiet_mode:
                print(f"\n🔍 Target UID: {target_uid}")
                print(f"   Found {len(configs)} configurations:")
                
                for j, config in enumerate(configs):
                    try:
                        config_data = json.loads(config['config_json'])
                        asset_config = config_data.get('assetConfiguration') or {}
                        resource_strategy = asset_config.get('resourceStrategyType', 'N/A')
                        auto_retry = asset_config.get('autoRetryEnabled', 'N/A')
                        is_user_marked = asset_config.get('isUserMarkedReference', 'N/A')
                        
                        # Extract source information from the asset data
                        source_uid = config.get('source_uid', 'Unknown')
                        
                        # If source_uid is Unknown or empty, show a message
                        if source_uid == 'Unknown' or not source_uid or source_uid.strip() == '':
                            source_uid = "Source UID: Not available (from legacy export)"
                        else:
                            # If we have a source_uid, display it properly
                            source_uid = f"Source UID: {source_uid}"
                        
                        print(f"   Option {j+1}:")
                        print(f"      {source_uid}")
                        print(f"      Resource Strategy: {resource_strategy}")
                        print(f"      Auto Retry Enabled: {auto_retry}")
                        print(f"      User Marked Reference: {is_user_marked}")
                    except json.JSONDecodeError:
                        print(f"   Option {j+1}: (Invalid JSON)")
                
                # Ask user to choose
                while True:
                    try:
                        choice = input(f"\n   Which configuration would you like to keep? (1-{len(configs)}): ").strip()
                        choice_num = int(choice)
                        if 1 <= choice_num <= len(configs):
                            selected_config = configs[choice_num - 1]
                            resolved_assets.append(selected_config)
                            processed_uids.add(target_uid)
                            if not quiet_mode:
                                print(f"   ✅ Selected Option {choice_num}")
                            break
                        else:
                            print(f"   ❌ Please enter a number between 1 and {len(configs)}")
                    except ValueError:
                        print("   ❌ Please enter a valid number")
                    except KeyboardInterrupt:
                        if not quiet_mode:
                            print("\n❌ Operation cancelled by user")
                        return None
        
        # Add non-duplicate assets
        for asset in asset_data:
            target_uid = asset['target_uid']
            if target_uid not in processed_uids:
                resolved_assets.append(asset)
        
        if not quiet_mode:
            print(f"\n✅ Duplicate resolution completed!")
            print(f"   📊 Original configurations: {len(asset_data)}")
            print(f"   📊 Resolved configurations: {len(resolved_assets)}")
            print(f"   📊 Duplicates resolved: {len(asset_data) - len(resolved_assets)}")
        
        return resolved_assets
        
    except Exception as e:
        if not quiet_mode:
            print(f"❌ Error resolving duplicates: {e}")
        return None
