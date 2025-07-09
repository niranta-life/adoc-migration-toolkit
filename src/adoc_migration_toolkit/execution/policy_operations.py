"""
Policy operations execution functions.

This module contains execution functions for policy-related operations
including policy export/import, list export, and rule tag export.
"""

import csv
import json
import logging
import os
import threading
import tempfile
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Tuple
from tqdm import tqdm
from glob import glob
from concurrent.futures import ThreadPoolExecutor, as_completed

from .utils import create_progress_bar
from ..shared.file_utils import get_output_file_path
from ..shared import globals

def execute_policy_list_export(client, logger: logging.Logger, quiet_mode: bool = False, verbose_mode: bool = False):
    """Execute the policy-list-export command.
    
    Args:
        client: API client instance
        logger: Logger instance
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
    """
    try:
        # Determine output file path using the policy-export category
        output_file = get_output_file_path("", "policies-all-export.csv", category="policy-export")
        
        if not quiet_mode:
            print(f"\nExporting all rules from ADOC environment")
            print(f"Output will be written to: {output_file}")

            if verbose_mode:
                print("🔊 VERBOSE MODE - Detailed output including headers and responses")
            print("="*80)
        
        # Step 1: Get total count of policies
        if not quiet_mode:
            print("Getting total rules count...")
        
        if verbose_mode:
            print("\nGET Request Headers:")
            print(f"  Endpoint: /catalog-server/api/rules?page=0&size=0&ruleStatus=ACTIVE")
            print(f"  Method: GET")
            print(f"  Content-Type: application/json")
            print(f"  Authorization: Bearer [REDACTED]")
            if hasattr(client, 'tenant') and client.tenant:
                print(f"  X-Tenant: {client.tenant}")
        
        count_response = client.make_api_call(
            endpoint="/catalog-server/api/rules?page=0&size=0&ruleStatus=ACTIVE",
            method='GET'
        )
        
        if verbose_mode:
            print("\nCount Response:")
            print(json.dumps(count_response, indent=2, ensure_ascii=False))
        
        # Extract total count
        if not count_response or 'meta' not in count_response or 'count' not in count_response['meta']:
            error_msg = "Failed to get total rules count from response"
            print(f"❌ {error_msg}")
            logger.error(error_msg)
            return
        
        total_count = count_response['meta']['count']
        page_size = 1000  # Default page size
        total_pages = (total_count + page_size - 1) // page_size  # Ceiling division
        
        if not quiet_mode:
            print(f"Total rules found: {total_count}")
            print(f"Page size: {page_size}")
            print(f"Total pages to retrieve: {total_pages}")
            print("="*80)
        
        # Step 2: Retrieve all pages and collect policies
        all_policies = []
        successful_pages = 0
        failed_pages = 0
        
        for page in range(total_pages):
            if not quiet_mode:
                print(f"\n[Page {page + 1}/{total_pages}] Retrieving rules...")
            
            try:
                if verbose_mode:
                    print(f"\nGET Request Headers:")
                    print(f"  Endpoint: /catalog-server/api/rules?page={page}&size={page_size}&ruleStatus=ACTIVE")
                    print(f"  Method: GET")
                    print(f"  Content-Type: application/json")
                    print(f"  Authorization: Bearer [REDACTED]")
                    if hasattr(client, 'tenant') and client.tenant:
                        print(f"  X-Tenant: {client.tenant}")
                
                page_response = client.make_api_call(
                    endpoint=f"/catalog-server/api/rules?page={page}&size={page_size}&ruleStatus=ACTIVE",
                    method='GET'
                )
                
                if verbose_mode:
                    print(f"\nPage {page + 1} Response:")
                    print(json.dumps(page_response, indent=2, ensure_ascii=False))
                
                # Extract policies from response
                if page_response and 'rules' in page_response:
                    page_policies = page_response['rules']
                    # Extract the actual policy objects from the nested structure
                    actual_policies = []
                    for policy_wrapper in page_policies:
                        if 'rule' in policy_wrapper:
                            actual_policies.append(policy_wrapper['rule'])
                        else:
                            # Fallback: if no 'rule' wrapper, use the object directly
                            actual_policies.append(policy_wrapper)
                    
                    all_policies.extend(actual_policies)
                    
                    if not quiet_mode:
                        print(f"✅ Page {page + 1}: Retrieved {len(actual_policies)} rules")
                    else:
                        print(f"✅ Page {page + 1}/{total_pages}: {len(actual_policies)} rules")
                    
                    successful_pages += 1
                else:
                    error_msg = f"Invalid response format for page {page + 1} - no rules found"
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
        
        # Step 3: Process each policy to get asset details
        if not quiet_mode:
            print(f"\nProcessing {len(all_policies)} rules to extract asset information...")
        
        processed_policies = []
        total_asset_calls = 0
        successful_asset_calls = 0
        failed_asset_calls = 0
        failed_rules = []  # Track rules that failed to retrieve assemblies
        
        # Create progress bar using tqdm utility
        progress_bar = create_progress_bar(
            total=len(all_policies),
            desc="Processing rule",
            unit="rules",
            disable=verbose_mode
        )
        
        for i, policy in enumerate(all_policies, 1):
            # Update progress bar with current policy ID using set_postfix
            progress_bar.set_postfix(rule_id=policy.get('id', 'unknown'))
            
            # Extract tableAssetIds from backingAssets for this policy
            table_asset_ids = []
            backing_assets = policy.get('backingAssets', [])
            for asset in backing_assets:
                table_asset_id = asset.get('tableAssetId')
                if table_asset_id:
                    table_asset_ids.append(table_asset_id)
            
            # Get asset details for this policy's tableAssetIds
            asset_details = {}
            assembly_details = {}
            
            if table_asset_ids:
                # Convert to comma-separated string for this policy
                table_asset_ids_str = ','.join(map(str, table_asset_ids))
                total_asset_calls += 1
                
                if verbose_mode:
                    print(f"\nGET Request Headers:")
                    print(f"  Endpoint: /catalog-server/api/assets/search?ids={table_asset_ids_str}")
                    print(f"  Method: GET")
                    print(f"  Content-Type: application/json")
                    print(f"  Authorization: Bearer [REDACTED]")
                    if hasattr(client, 'tenant') and client.tenant:
                        print(f"  X-Tenant: {client.tenant}")
                
                try:
                    assets_response = client.make_api_call(
                        endpoint=f"/catalog-server/api/assets/search?ids={table_asset_ids_str}",
                        method='GET'
                    )
                    
                    if verbose_mode:
                        print("\nAssets Response:")
                        print(json.dumps(assets_response, indent=2, ensure_ascii=False))
                    
                    # Extract asset details for this policy
                    if assets_response and 'assets' in assets_response:
                        for asset in assets_response['assets']:
                            asset_id = asset.get('id')
                            if asset_id:
                                asset_details[asset_id] = asset
                    
                    # Extract assembly details for this policy
                    if assets_response and 'assemblies' in assets_response:
                        for assembly in assets_response['assemblies']:
                            assembly_id = assembly.get('id')
                            if assembly_id:
                                assembly_details[assembly_id] = assembly
                    
                    successful_asset_calls += 1
                    if verbose_mode:
                        print(f"✅ Retrieved details for {len(asset_details)} assets and {len(assembly_details)} assemblies for policy {policy.get('id')}")
                        
                except Exception as e:
                    error_msg = f"Failed to retrieve asset details for policy {policy.get('id')}: {e}"
                    if verbose_mode:
                        print(f"❌ {error_msg}")
                    logger.error(error_msg)
                    failed_asset_calls += 1
                    
                    # Track failed rule
                    failed_rules.append({
                        'policy_id': policy.get('id', 'unknown'),
                        'policy_type': policy.get('type', 'unknown'),
                        'error': str(e),
                        'table_asset_ids': table_asset_ids
                    })
            
            # Add asset and assembly details to the policy for later processing
            policy['_asset_details'] = asset_details
            policy['_assembly_details'] = assembly_details
            processed_policies.append(policy)
            
            # Update progress bar
            progress_bar.update(1)
        
        # Close progress bar
        progress_bar.close()
        
        # Show completion summary
        if not quiet_mode:
            print(f"\nAsset API calls completed:")
            print(f"  Total API calls made: {total_asset_calls}")
            print(f"  Successful calls: {successful_asset_calls}")
            print(f"  Failed calls: {failed_asset_calls}")
            
            # Show failed rules summary
            if failed_rules:
                print(f"\n❌ Failed Rules Summary ({len(failed_rules)} rules):")
                print("="*80)
                for failed_rule in failed_rules:
                    print(f"Rule ID: {failed_rule['policy_id']}")
                    print(f"Rule Type: {failed_rule['policy_type']}")
                    print(f"Table Asset IDs: {', '.join(map(str, failed_rule['table_asset_ids']))}")
                    print(f"Error: {failed_rule['error']}")
                    print("-" * 40)
                print("="*80)
        
        # Step 4: Write policies to CSV file with additional columns
        if not quiet_mode:
            print(f"\nWriting {len(processed_policies)} rules to CSV file...")
        
        # Create output directory if needed
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            
            # Write header with additional columns
            writer.writerow(['id', 'type', 'engineType', 'tableAssetIds', 'assemblyIds', 'assemblyNames', 'sourceTypes'])
            
            # Write policy data
            for policy in processed_policies:
                policy_id = policy.get('id', '')
                policy_type = policy.get('type', '') or ''  # Convert None to empty string
                engine_type = policy.get('engineType', '') or ''  # Convert None to empty string
                
                # Extract tableAssetIds from backingAssets
                table_asset_ids = []
                assembly_ids = set()
                assembly_names = set()
                source_types = set()
                
                backing_assets = policy.get('backingAssets', [])
                asset_details = policy.get('_asset_details', {})
                assembly_details = policy.get('_assembly_details', {})
                
                for asset in backing_assets:
                    table_asset_id = asset.get('tableAssetId')
                    if table_asset_id:
                        table_asset_ids.append(str(table_asset_id))
                        
                        # Get assembly information from asset details
                        if table_asset_id in asset_details:
                            asset_detail = asset_details[table_asset_id]
                            assembly_id = asset_detail.get('assemblyId')
                            if assembly_id and assembly_id in assembly_details:
                                assembly = assembly_details[assembly_id]
                                assembly_ids.add(str(assembly_id))
                                assembly_name = assembly.get('name', '')
                                if assembly_name:
                                    assembly_names.add(assembly_name)
                                source_type = assembly.get('sourceType', {}).get('name', '')
                                if source_type:
                                    source_types.add(source_type)
                
                # Convert sets to comma-separated strings
                table_asset_ids_str = ','.join(table_asset_ids)
                assembly_ids_str = ','.join(sorted(assembly_ids))
                assembly_names_str = ','.join(sorted(assembly_names))
                source_types_str = ','.join(sorted(source_types))
                
                writer.writerow([
                    policy_id, 
                    policy_type, 
                    engine_type, 
                    table_asset_ids_str,
                    assembly_ids_str,
                    assembly_names_str,
                    source_types_str
                ])
        
        # Step 5: Sort the CSV file by id
        if not quiet_mode:
            print("Sorting CSV file by id...")
        
        # Read all rows
        rows = []
        with open(output_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # Skip header
            rows = list(reader)
        
        # Sort rows by id
        def sort_key(row):
            policy_id = row[0] if len(row) > 0 else ''
            # Convert policy_id to int for proper numeric sorting, fallback to string
            try:
                policy_id_int = int(policy_id) if policy_id else 0
            except (ValueError, TypeError):
                policy_id_int = 0
            return policy_id_int
        
        rows.sort(key=sort_key)
        
        # Write sorted data back to file
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(header)
            writer.writerows(rows)
        
        # Step 6: Print statistics
        if not quiet_mode:
            print("\n" + "="*80)
            print("RULES LIST EXPORT SUMMARY")
            print("="*80)
            print(f"Output file: {output_file}")
            print(f"Total rules exported: {len(processed_policies)}")
            print(f"Successful pages: {successful_pages}")
            print(f"Failed pages: {failed_pages}")
            print(f"Total pages processed: {total_pages}")
            print(f"Total table assets found: {total_asset_calls}")
            print(f"Total asset API calls made: {total_asset_calls}")
            
            # Calculate total assemblies found
            total_assemblies = 0
            for policy in processed_policies:
                total_assemblies += len(policy.get('_assembly_details', {}))
            print(f"Total assemblies found: {total_assemblies}")
            
            # Additional statistics
            type_counts = {}
            engine_type_counts = {}
            assembly_name_counts = {}
            source_type_counts = {}
            
            for policy in processed_policies:
                policy_type = policy.get('type', '') or 'UNKNOWN'
                engine_type = policy.get('engineType', '') or 'UNKNOWN'
                
                type_counts[policy_type] = type_counts.get(policy_type, 0) + 1
                engine_type_counts[engine_type] = engine_type_counts.get(engine_type, 0) + 1
                
                # Extract assembly names and source types from backingAssets
                backing_assets = policy.get('backingAssets', [])
                asset_details = policy.get('_asset_details', {})
                assembly_details = policy.get('_assembly_details', {})
                
                for asset in backing_assets:
                    table_asset_id = asset.get('tableAssetId')
                    if table_asset_id and table_asset_id in asset_details:
                        asset_detail = asset_details[table_asset_id]
                        assembly_id = asset_detail.get('assemblyId')
                        if assembly_id and assembly_id in assembly_details:
                            assembly = assembly_details[assembly_id]
                            assembly_name = assembly.get('name', '')
                            if assembly_name:
                                assembly_name_counts[assembly_name] = assembly_name_counts.get(assembly_name, 0) + 1
                            source_type = assembly.get('sourceType', {}).get('name', '')
                            if source_type:
                                source_type_counts[source_type] = source_type_counts.get(source_type, 0) + 1
            
            print(f"\n📊 DETAILED STATISTICS SUMMARY")
            print("-" * 50)
            print(f"Total rules processed: {len(processed_policies)}")
            print(f"Total asset API calls: {total_asset_calls}")
            print(f"Successful asset calls: {successful_asset_calls}")
            print(f"Failed asset calls: {failed_asset_calls}")
            print(f"Total assemblies found: {total_assemblies}")
            
            # Calculate success rate
            if total_asset_calls > 0:
                success_rate = (successful_asset_calls / total_asset_calls) * 100
                print(f"Asset API success rate: {success_rate:.1f}%")
            
            # Rule Type Statistics
            if type_counts:
                print(f"\n🔧 RULE TYPES ({len(type_counts)} types):")
                print("-" * 50)
                total_rules = len(processed_policies)
                for rule_type, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
                    percentage = (count / total_rules) * 100
                    print(f"  {rule_type:<20} {count:>5} rules ({percentage:>5.1f}%)")
            
            # Engine Type Statistics
            if engine_type_counts:
                print(f"\n⚙️  ENGINE TYPES ({len(engine_type_counts)} types):")
                print("-" * 50)
                for engine_type, count in sorted(engine_type_counts.items(), key=lambda x: x[1], reverse=True):
                    percentage = (count / len(processed_policies)) * 100
                    print(f"  {engine_type:<20} {count:>5} rules ({percentage:>5.1f}%)")
            
            # Assembly Statistics
            if assembly_name_counts:
                print(f"\n🏗️  ASSEMBLIES ({len(assembly_name_counts)} assemblies):")
                print("-" * 50)
                total_assembly_rules = sum(assembly_name_counts.values())
                for assembly_name, count in sorted(assembly_name_counts.items(), key=lambda x: x[1], reverse=True):
                    percentage = (count / total_assembly_rules) * 100
                    print(f"  {assembly_name:<30} {count:>5} rules ({percentage:>5.1f}%)")
            else:
                print(f"\n🏗️  ASSEMBLIES: No assemblies found")
            
            # Source Type Statistics
            if source_type_counts:
                print(f"\n📡 SOURCE TYPES ({len(source_type_counts)} types):")
                print("-" * 50)
                total_source_rules = sum(source_type_counts.values())
                for source_type, count in sorted(source_type_counts.items(), key=lambda x: x[1], reverse=True):
                    percentage = (count / total_source_rules) * 100
                    print(f"  {source_type:<20} {count:>5} rules ({percentage:>5.1f}%)")
            else:
                print(f"\n📡 SOURCE TYPES: No source types found")
            
            # Failed Rules Summary (if any)
            if failed_rules:
                print(f"\n❌ FAILED RULES SUMMARY ({len(failed_rules)} rules):")
                print("="*80)
                for failed_rule in failed_rules:
                    print(f"Policy ID: {failed_rule['policy_id']}")
                    print(f"Policy Type: {failed_rule['policy_type']}")
                    print(f"Table Asset IDs: {', '.join(map(str, failed_rule['table_asset_ids']))}")
                    print(f"Error: {failed_rule['error']}")
                    print("-" * 40)
                print("="*80)
            
            print("="*80)
        else:
            print(f"✅ Policies list export completed: {len(processed_policies)} policies exported to {output_file}")
        
    except Exception as e:
        error_msg = f"Error in policy-list-export: {e}"
        if not quiet_mode:
            print(f"❌ {error_msg}")
        logger.error(error_msg)


def execute_policy_list_export_parallel(client, logger: logging.Logger, quiet_mode: bool = False, verbose_mode: bool = False):
    """Execute the policy-list-export command with parallel processing.
    
    Args:
        client: API client instance
        logger: Logger instance
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
    """
    try:
        # Determine output file path using the policy-export category
        output_file = get_output_file_path("", "policies-all-export.csv", category="policy-export")
        
        if not quiet_mode:
            print(f"\nExporting all rules from ADOC environment (Parallel Mode)")
            print(f"Output will be written to: {output_file}")
            if verbose_mode:
                print("🔊 VERBOSE MODE - Detailed output including headers and responses")
            print("="*80)
        
        # Step 1: Get total count of policies
        if not quiet_mode:
            print("Getting total rules count...")
        
        count_response = client.make_api_call(
            endpoint="/catalog-server/api/rules?page=0&size=0&ruleStatus=ACTIVE",
            method='GET'
        )
        
        if not count_response or 'meta' not in count_response or 'count' not in count_response['meta']:
            error_msg = "Failed to get total rules count from response"
            print(f"❌ {error_msg}")
            logger.error(error_msg)
            return
        
        total_count = count_response['meta']['count']
        
        if not quiet_mode:
            print(f"Total rules found: {total_count}")
        
        # Step 2: Get all policies first (sequential - this is fast)
        if not quiet_mode:
            print("Retrieving all policies...")
        
        all_policies = []
        page_size = 1000
        total_pages = (total_count + page_size - 1) // page_size
        
        for page in range(total_pages):
            if not quiet_mode:
                print(f"  Retrieving page {page + 1}/{total_pages}...")
            
            page_response = client.make_api_call(
                endpoint=f"/catalog-server/api/rules?page={page}&size={page_size}&ruleStatus=ACTIVE",
                method='GET'
            )
            
            if page_response and 'rules' in page_response:
                page_policies = page_response['rules']
                actual_policies = []
                for policy_wrapper in page_policies:
                    if 'rule' in policy_wrapper:
                        actual_policies.append(policy_wrapper['rule'])
                    else:
                        actual_policies.append(policy_wrapper)
                
                all_policies.extend(actual_policies)
        
        if not quiet_mode:
            print(f"Retrieved {len(all_policies)} policies")
        
        # Step 3: Calculate thread configuration for asset processing
        max_threads = 5
        min_policies_per_thread = 10
        
        if len(all_policies) < min_policies_per_thread:
            num_threads = 1
            policies_per_thread = len(all_policies)
        else:
            num_threads = min(max_threads, (len(all_policies) + min_policies_per_thread - 1) // min_policies_per_thread)
            policies_per_thread = (len(all_policies) + num_threads - 1) // num_threads
        
        if not quiet_mode:
            print(f"Using {num_threads} threads to process {len(all_policies)} policies")
            print(f"Policies per thread: {policies_per_thread}")
            print("="*80)
        
        # Step 4: Process asset details in parallel
        temp_files = []
        thread_results = []
        
        def process_policy_chunk(thread_id, start_index, end_index):
            """Process a chunk of policies for a specific thread."""
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
            
            # Get policies for this thread
            thread_policies = all_policies[start_index:end_index]
            
            # Create progress bar for this thread using utility function
            progress_bar = create_progress_bar(
                total=len(thread_policies),
                desc=f"Thread {thread_id}",
                unit="policies",
                disable=quiet_mode,
                position=thread_id,
                leave=False
            )
            
            processed_policies = []
            total_asset_calls = 0
            successful_asset_calls = 0
            failed_asset_calls = 0
            
            # Process each policy in this thread's range
            for policy in thread_policies:
                # Extract tableAssetIds from backingAssets
                table_asset_ids = []
                backing_assets = policy.get('backingAssets', [])
                for asset in backing_assets:
                    table_asset_id = asset.get('tableAssetId')
                    if table_asset_id:
                        table_asset_ids.append(table_asset_id)
                
                # Get asset details for this policy's tableAssetIds
                asset_details = {}
                assembly_details = {}
                
                if table_asset_ids:
                    table_asset_ids_str = ','.join(map(str, table_asset_ids))
                    total_asset_calls += 1
                    
                    try:
                        assets_response = thread_client.make_api_call(
                            endpoint=f"/catalog-server/api/assets/search?ids={table_asset_ids_str}",
                            method='GET'
                        )
                        
                        if assets_response and 'assets' in assets_response:
                            for asset in assets_response['assets']:
                                asset_id = asset.get('id')
                                if asset_id:
                                    asset_details[asset_id] = asset
                        
                        if assets_response and 'assemblies' in assets_response:
                            for assembly in assets_response['assemblies']:
                                assembly_id = assembly.get('id')
                                if assembly_id:
                                    assembly_details[assembly_id] = assembly
                        
                        successful_asset_calls += 1
                        
                    except Exception as e:
                        failed_asset_calls += 1
                        logger.error(f"Thread {thread_id}: Failed to retrieve asset details for policy {policy.get('id')}: {e}")
                
                # Add asset and assembly details to the policy
                policy['_asset_details'] = asset_details
                policy['_assembly_details'] = assembly_details
                processed_policies.append(policy)
                
                # Update progress bar
                progress_bar.update(1)
            
            progress_bar.close()
            
            # Write processed policies to temporary CSV file
            with open(temp_file.name, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                writer.writerow(['id', 'type', 'engineType', 'tableAssetIds', 'assemblyIds', 'assemblyNames', 'sourceTypes'])
                
                for policy in processed_policies:
                    policy_id = policy.get('id', '')
                    policy_type = policy.get('type', '') or ''
                    engine_type = policy.get('engineType', '') or ''
                    
                    # Extract tableAssetIds from backingAssets
                    table_asset_ids = []
                    assembly_ids = set()
                    assembly_names = set()
                    source_types = set()
                    
                    backing_assets = policy.get('backingAssets', [])
                    asset_details = policy.get('_asset_details', {})
                    assembly_details = policy.get('_assembly_details', {})
                    
                    for asset in backing_assets:
                        table_asset_id = asset.get('tableAssetId')
                        if table_asset_id:
                            table_asset_ids.append(str(table_asset_id))
                            
                            # Get assembly information from asset details
                            if table_asset_id in asset_details:
                                asset_detail = asset_details[table_asset_id]
                                assembly_id = asset_detail.get('assemblyId')
                                if assembly_id and assembly_id in assembly_details:
                                    assembly = assembly_details[assembly_id]
                                    assembly_ids.add(str(assembly_id))
                                    assembly_name = assembly.get('name', '')
                                    if assembly_name:
                                        assembly_names.add(assembly_name)
                                    source_type = assembly.get('sourceType', {}).get('name', '')
                                    if source_type:
                                        source_types.add(source_type)
                    
                    # Convert sets to comma-separated strings
                    table_asset_ids_str = ','.join(table_asset_ids)
                    assembly_ids_str = ','.join(sorted(assembly_ids))
                    assembly_names_str = ','.join(sorted(assembly_names))
                    source_types_str = ','.join(sorted(source_types))
                    
                    writer.writerow([
                        policy_id, 
                        policy_type, 
                        engine_type, 
                        table_asset_ids_str,
                        assembly_ids_str,
                        assembly_names_str,
                        source_types_str
                    ])
            
            return {
                'thread_id': thread_id,
                'processed': len(processed_policies),
                'total_asset_calls': total_asset_calls,
                'successful_asset_calls': successful_asset_calls,
                'failed_asset_calls': failed_asset_calls,
                'temp_file': temp_file.name
            }
        
        # Step 5: Start threads
        threads = []
        for i in range(num_threads):
            start_index = i * policies_per_thread
            end_index = min(start_index + policies_per_thread, len(all_policies))
            
            thread = threading.Thread(
                target=lambda tid=i, start=start_index, end=end_index: thread_results.append(
                    process_policy_chunk(tid, start, end)
                )
            )
            threads.append(thread)
            thread.start()
        
        # Step 6: Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Step 7: Merge temporary files
        if not quiet_mode:
            print("\nMerging temporary files...")
        
        # Create output directory if needed
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w', newline='', encoding='utf-8') as output_csv:
            writer = csv.writer(output_csv, quoting=csv.QUOTE_ALL)
            
            # Write header
            writer.writerow(['id', 'type', 'engineType', 'tableAssetIds', 'assemblyIds', 'assemblyNames', 'sourceTypes'])
            
            # Merge all temporary files
            for temp_file in temp_files:
                try:
                    with open(temp_file, 'r', newline='', encoding='utf-8') as temp_csv:
                        reader = csv.reader(temp_csv)
                        next(reader)  # Skip header
                        for row in reader:
                            writer.writerow(row)
                except Exception as e:
                    logger.error(f"Error reading temporary file {temp_file}: {e}")
                finally:
                    # Clean up temporary file
                    try:
                        os.unlink(temp_file)
                    except Exception as e:
                        logger.warning(f"Could not delete temporary file {temp_file}: {e}")
        
        # Step 8: Sort the CSV file by id
        if not quiet_mode:
            print("Sorting CSV file by id...")
        
        # Read all rows
        rows = []
        with open(output_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # Skip header
            rows = list(reader)
        
        # Sort rows by id
        def sort_key(row):
            policy_id = row[0] if len(row) > 0 else ''
            # Convert policy_id to int for proper numeric sorting, fallback to string
            try:
                policy_id_int = int(policy_id) if policy_id else 0
            except (ValueError, TypeError):
                policy_id_int = 0
            return policy_id_int
        
        rows.sort(key=sort_key)
        
        # Write sorted data back to file
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(header)
            writer.writerows(rows)
        
        # Step 9: Print statistics
        if not quiet_mode:
            print("\n" + "="*80)
            print("RULES LIST EXPORT SUMMARY (PARALLEL MODE)")
            print("="*80)
            print(f"Output file: {output_file}")
            
            total_processed = 0
            total_asset_calls = 0
            total_successful_asset_calls = 0
            total_failed_asset_calls = 0
            
            for result in thread_results:
                total_processed += result['processed']
                total_asset_calls += result['total_asset_calls']
                total_successful_asset_calls += result['successful_asset_calls']
                total_failed_asset_calls += result['failed_asset_calls']
                
                print(f"Thread {result['thread_id']}: {result['processed']} policies, "
                      f"{result['successful_asset_calls']}/{result['total_asset_calls']} asset calls successful")
            
            print(f"\nTotal rules exported: {total_processed}")
            print(f"Total asset API calls made: {total_asset_calls}")
            print(f"Successful asset calls: {total_successful_asset_calls}")
            print(f"Failed asset calls: {total_failed_asset_calls}")
            print("="*80)
        
    except Exception as e:
        error_msg = f"Failed to execute parallel policy list export: {e}"
        print(f"❌ {error_msg}")
        logger.error(error_msg)
        raise


def execute_policy_export(client, logger: logging.Logger, quiet_mode: bool = False, verbose_mode: bool = False, batch_size: int = 50, export_type: str = None, filter_value: str = None):
    """Execute the policy-export command.
    
    Args:
        client: API client instance
        logger: Logger instance
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
        batch_size: Number of policies to export in each batch
        export_type: Type of export (rule-types, engine-types, assemblies, source-types)
        filter_value: Optional filter value within the export type
    """
    try:
        # Determine input and output file paths
        if globals.GLOBAL_OUTPUT_DIR:
            input_file = globals.GLOBAL_OUTPUT_DIR / "policy-export" / "policies-all-export.csv"
            output_dir = globals.GLOBAL_OUTPUT_DIR / "policy-export"
        else:
            # Use the same logic as policy-list-export to find the input file
            # Look for the most recent adoc-migration-toolkit-YYYYMMDDHHMM directory
            current_dir = Path.cwd()
            toolkit_dirs = list(current_dir.glob("adoc-migration-toolkit-*"))
            
            if not toolkit_dirs:
                error_msg = "No adoc-migration-toolkit directory found. Please run 'policy-list-export' first."
                print(f"❌ {error_msg}")
                logger.error(error_msg)
                return
            
            # Sort by creation time and use the most recent
            toolkit_dirs.sort(key=lambda x: x.stat().st_ctime, reverse=True)
            latest_toolkit_dir = toolkit_dirs[0]
            
            input_file = latest_toolkit_dir / "policy-export" / "policies-all-export.csv"
            output_dir = latest_toolkit_dir / "policy-export"
        
        if not quiet_mode:
            print(f"\nExporting policy definitions by type")
            print(f"Input file: {input_file}")
            print(f"Output directory: {output_dir}")
            if verbose_mode:
                print("🔊 VERBOSE MODE - Detailed output including headers and responses")
            print("="*80)
        
        # Check if input file exists
        if not input_file.exists():
            error_msg = f"Input file does not exist: {input_file}"
            print(f"❌ {error_msg}")
            print(f"💡 Please run 'policy-list-export' first to generate the input file")
            logger.error(error_msg)
            return
        
        # Read policies from CSV file
        if not quiet_mode:
            print("Reading policies from CSV file...")
        
        policies_by_category = {}
        total_policies = 0
        
        with open(input_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # Skip header
            
            # Check if this is the new format with additional columns
            if len(header) >= 7 and header[0] == 'id' and header[1] == 'type' and header[2] == 'engineType':
                # New format with additional columns
                expected_columns = ['id', 'type', 'engineType', 'tableAssetIds', 'assemblyIds', 'assemblyNames', 'sourceTypes']
                if len(header) != len(expected_columns):
                    error_msg = f"Invalid CSV format. Expected {len(expected_columns)} columns, got {len(header)}"
                    print(f"❌ {error_msg}")
                    logger.error(error_msg)
                    return
            elif len(header) == 3 and header[0] == 'id' and header[1] == 'type' and header[2] == 'engineType':
                # Old format - only basic columns
                error_msg = "CSV file is in old format. Please run 'policy-list-export' first to generate the new format with additional columns."
                print(f"❌ {error_msg}")
                logger.error(error_msg)
                return
            else:
                error_msg = f"Invalid CSV format. Expected header: ['id', 'type', 'engineType', ...], got: {header}"
                print(f"❌ {error_msg}")
                logger.error(error_msg)
                return
            
            for row_num, row in enumerate(reader, start=2):
                if len(row) != len(header):
                    logger.warning(f"Row {row_num}: Expected {len(header)} columns, got {len(row)}")
                    continue
                
                policy_id = row[0].strip()
                policy_type = row[1].strip()
                engine_type = row[2].strip()
                table_asset_ids = row[3].strip()
                assembly_ids = row[4].strip()
                assembly_names = row[5].strip()
                source_types = row[6].strip()
                
                if not policy_id:
                    logger.warning(f"Row {row_num}: Empty policy ID")
                    continue
                
                # Determine the category based on export_type
                category = None
                category_value = None
                
                if export_type == 'rule-types':
                    category = policy_type
                    category_value = policy_type
                elif export_type == 'engine-types':
                    category = engine_type
                    category_value = engine_type
                elif export_type == 'assemblies':
                    if assembly_names:
                        # Split by comma and use the first assembly name
                        assembly_name_list = [name.strip() for name in assembly_names.split(',') if name.strip()]
                        if assembly_name_list:
                            category = assembly_name_list[0]
                            category_value = assembly_names
                elif export_type == 'source-types':
                    if source_types:
                        # Split by comma and use the first source type
                        source_type_list = [stype.strip() for stype in source_types.split(',') if stype.strip()]
                        if source_type_list:
                            category = source_type_list[0]
                            category_value = source_types
                else:
                    # Default: group by policy type (original behavior)
                    category = policy_type
                    category_value = policy_type
                
                # Apply filter if specified
                if filter_value and category != filter_value:
                    continue
                
                if category:
                    if category not in policies_by_category:
                        policies_by_category[category] = []
                    policies_by_category[category].append(policy_id)
                    total_policies += 1
                else:
                    logger.warning(f"Row {row_num}: No valid category found for export type '{export_type}'")
        
        if not policies_by_category:
            error_msg = "No valid policies found matching the specified criteria"
            print(f"❌ {error_msg}")
            logger.error(error_msg)
            return
        
        # Determine output filename based on export type and filter
        if export_type:
            base_filename = export_type.replace('-', '_')
            if filter_value:
                # Sanitize filter value for filename
                safe_filter = "".join(c for c in filter_value if c.isalnum() or c in (' ', '-', '_')).rstrip()
                safe_filter = safe_filter.replace(' ', '_').lower()
                filename = f"{base_filename}_{safe_filter}"
            else:
                filename = base_filename
        else:
            filename = "policy_types"
        
        if not quiet_mode:
            export_type_display = export_type if export_type else "policy types"
            filter_display = f" (filtered by: {filter_value})" if filter_value else ""
            print(f"Found {total_policies} policies across {len(policies_by_category)} {export_type_display}{filter_display}")
            print(f"Output filename base: {filename}")
            print("="*80)
        
        # Generate timestamp for all files
        timestamp = datetime.now().strftime("%m-%d-%Y-%H-%M")
        
        # Export policies by type in batches
        successful_exports = 0
        failed_exports = 0
        export_results = {}
        
        # Calculate total batches for progress bar
        total_batches = 0
        for policy_ids in policies_by_category.values():
            total_batches += (len(policy_ids) + batch_size - 1) // batch_size
        
        current_batch = 0
        failed_batch_indices = set()
        
        # Print initial progress bar and status line
        if not quiet_mode:
            print(f"Exporting: [{'░' * 50}] 0/{total_batches} (0.0%)")
            print(f"Status: Initializing...")
        
        batch_idx = 0
        for policy_type, policy_ids in policies_by_category.items():
            if not quiet_mode:
                # Update status line for new policy type (but don't reset progress bar)
                # Calculate current progress
                percentage = (current_batch / total_batches) * 100
                bar_width = 50
                filled_blocks = int((current_batch / total_batches) * bar_width)
                
                # Build the current bar state
                bar = ''
                for i in range(bar_width):
                    if i < filled_blocks:
                        batch_index_for_block = int((i / bar_width) * total_batches)
                        if batch_index_for_block in failed_batch_indices:
                            bar += '\033[31m█\033[0m'  # Red for failed
                        else:
                            bar += '\033[32m█\033[0m'  # Green for success
                    else:
                        bar += '░'  # Empty block
                
                print(f"\033[2F\033[KExporting: [{bar}] {current_batch}/{total_batches} ({percentage:.1f}%)")
                print(f"\033[KStatus: Processing {policy_type} ({len(policy_ids)} policies)")
            else:
                print(f"Processing {policy_type}: {len(policy_ids)} policies")
            
            type_total_batches = (len(policy_ids) + batch_size - 1) // batch_size
            for batch_num in range(type_total_batches):
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, len(policy_ids))
                batch_ids = policy_ids[start_idx:end_idx]
                current_batch += 1
                
                # Generate filename with range information
                # Use the actual policy type name (category) for the filename
                safe_category = "".join(c for c in policy_type if c.isalnum() or c in (' ', '-', '_')).rstrip()
                safe_category = safe_category.replace(' ', '_').lower()
                batch_filename = f"{safe_category}-{timestamp}-{start_idx}-{end_idx-1}.zip"
                output_file = output_dir / batch_filename
                
                # Prepare query parameters
                ids_param = ','.join(batch_ids)
                query_params = {
                    'ruleStatus': 'ALL',
                    'includeTags': 'true',
                    'ids': ids_param,
                    'filename': batch_filename
                }
                
                # Build endpoint with query parameters
                endpoint = "/catalog-server/api/rules/export/policy-definitions"
                query_string = '&'.join([f"{k}={v}" for k, v in query_params.items()])
                full_endpoint = f"{endpoint}?{query_string}"
                
                if verbose_mode:
                    print(f"\nGET Request Headers:")
                    print(f"  Endpoint: {full_endpoint}")
                    print(f"  Method: GET")
                    print(f"  Content-Type: application/zip")
                    print(f"  Authorization: Bearer [REDACTED]")
                    if hasattr(client, 'tenant') and client.tenant:
                        print(f"  X-Tenant: {client.tenant}")
                    print(f"  Query Parameters:")
                    for k, v in query_params.items():
                        if k == 'ids':
                            print(f"    {k}: {len(batch_ids)} IDs (first few: {', '.join(batch_ids[:3])}{'...' if len(batch_ids) > 3 else ''})")
                        else:
                            print(f"    {k}: {v}")
                
                try:
                    # Make API call to get ZIP file
                    response = client.make_api_call(
                        endpoint=full_endpoint,
                        method='GET',
                        return_binary=True
                    )
                    
                    if verbose_mode:
                        print(f"\nResponse:")
                        print(f"  Status: Success")
                        print(f"  Content-Type: application/zip")
                        print(f"  File size: {len(response) if response else 0} bytes")
                    
                    # Write ZIP file to output directory
                    if response:
                        with open(output_file, 'wb') as f:
                            f.write(response)
                        
                        # Store result for this batch
                        batch_key = f"{policy_type}_batch_{batch_num + 1}"
                        export_results[batch_key] = {
                            'success': True,
                            'filename': batch_filename,
                            'count': len(batch_ids),
                            'file_size': len(response),
                            'range': f"{start_idx}-{end_idx-1}"
                        }
                        successful_exports += 1
                    else:
                        error_msg = f"Empty response for {policy_type} batch {batch_num + 1}"
                        if verbose_mode:
                            print(f"\n❌ {error_msg}")
                        logger.error(error_msg)
                        
                        # Mark this batch as failed
                        failed_batch_indices.add(batch_idx)
                        
                        batch_key = f"{policy_type}_batch_{batch_num + 1}"
                        export_results[batch_key] = {
                            'success': False,
                            'filename': batch_filename,
                            'count': len(batch_ids),
                            'error': error_msg,
                            'range': f"{start_idx}-{end_idx-1}"
                        }
                        failed_exports += 1
                        
                except Exception as e:
                    error_msg = f"Failed to export {policy_type} batch {batch_num + 1}: {e}"
                    if verbose_mode:
                        print(f"\n❌ {error_msg}")
                    logger.error(error_msg)
                    
                    # Mark this batch as failed
                    failed_batch_indices.add(batch_idx)
                    
                    batch_key = f"{policy_type}_batch_{batch_num + 1}"
                    export_results[batch_key] = {
                        'success': False,
                        'filename': batch_filename,
                        'count': len(batch_ids),
                        'error': str(e),
                        'range': f"{start_idx}-{end_idx-1}"
                    }
                    failed_exports += 1
                
                # Update progress bar and status in place
                if not quiet_mode:
                    percentage = (current_batch / total_batches) * 100
                    bar_width = 50
                    
                    # Calculate how many blocks should be filled based on current progress
                    filled_blocks = int((current_batch / total_batches) * bar_width)
                    
                    # Build the bar with exactly 50 characters
                    bar = ''
                    for i in range(bar_width):
                        if i < filled_blocks:
                            # This block should be filled - check if it's a failed batch
                            # Map the block index back to batch index
                            batch_index_for_block = int((i / bar_width) * total_batches)
                            if batch_index_for_block in failed_batch_indices:
                                bar += '\033[31m█\033[0m'  # Red for failed
                            else:
                                bar += '\033[32m█\033[0m'  # Green for success
                        else:
                            bar += '░'  # Empty block
                    
                    # Move cursor up 2 lines and update both progress bar and status
                    print(f"\033[2F\033[KExporting: [{bar}] {current_batch}/{total_batches} ({percentage:.1f}%)")
                    print(f"\033[KStatus: Processing {policy_type} batch {batch_num + 1}")
                else:
                    print(f"  Batch {batch_num + 1}/{type_total_batches}: {len(batch_ids)} policies")
                    if response:
                        print(f"✅ {batch_filename}")
                    else:
                        print(f"❌ Failed")
                
                batch_idx += 1
        
        # Print final progress bar and status
        if not quiet_mode:
            bar_width = 50
            bar = ''
            for i in range(bar_width):
                # Map the block index back to batch index
                batch_index_for_block = int((i / bar_width) * total_batches)
                if batch_index_for_block in failed_batch_indices:
                    bar += '\033[31m█\033[0m'  # Red for failed
                else:
                    bar += '\033[32m█\033[0m'  # Green for success
            
            print(f"\033[2F\033[KExporting: [{bar}] {total_batches}/{total_batches} (100.0%)")
            print(f"\033[KStatus: Completed.")
            print()  # Add a blank line after completion
        
        # Print summary
        print("\n" + "="*80)
        print("POLICY EXPORT SUMMARY")
        print("="*80)
        print(f"Output directory: {output_dir}")
        print(f"Timestamp: {timestamp}")
        print(f"Batch size: {batch_size}")
        print(f"Total policy types processed: {len(policies_by_category)}")
        print(f"Successful exports: {successful_exports}")
        print(f"Failed exports: {failed_exports}")
        
        print(f"\nExport Results:")
        # Group results by policy type for better display
        results_by_type = {}
        for batch_key, result in export_results.items():
            policy_type = batch_key.split('_batch_')[0]
            if policy_type not in results_by_type:
                results_by_type[policy_type] = []
            results_by_type[policy_type].append(result)
        
        for policy_type, batch_results in results_by_type.items():
            print(f"  {policy_type}:")
            for result in batch_results:
                if result['success']:
                    print(f"    ✅ Batch {result['range']}: {result['count']} policies -> {result['filename']} ({result['file_size']} bytes)")
                else:
                    print(f"    ❌ Batch {result['range']}: {result['count']} policies -> {result['error']}")
        
        print("="*80)
        
        if failed_exports > 0:
            print("⚠️  Export completed with errors. Check log file for details.")
        else:
            print("✅ Export completed successfully!")
            
    except Exception as e:
        error_msg = f"Error executing policy export: {e}"
        print(f"❌ {error_msg}")
        logger.error(error_msg)


def execute_policy_import(client, logger: logging.Logger, file_pattern: str, quiet_mode: bool = False, verbose_mode: bool = False):
    """Execute the policy-import command.
    
    Args:
        client: API client instance
        logger: Logger instance
        file_pattern: File path or glob pattern for ZIP files
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
    """
    try:
        if not quiet_mode:
            print(f"\nImporting policy definitions from ZIP files")
            print(f"File pattern: {file_pattern}")
            print("="*80)
            
            # Show target environment information
            print(f"\n🌍 TARGET ENVIRONMENT INFORMATION:")
            target_host = client._build_host_url(use_target_tenant=True)
            print(f"  Host: {target_host}")
            if hasattr(client, 'target_tenant') and client.target_tenant:
                print(f"  Target Tenant: {client.target_tenant}")
            else:
                print(f"  Source Tenant: {client.tenant} (will be used as target)")
            print(f"  Authentication: Target access key and secret key")
            print("="*80)
        
        # Determine the search directory
        # If file_pattern is an absolute path, use it as is
        # Otherwise, look in the output-dir/policy-import directory
        if os.path.isabs(file_pattern):
            search_pattern = file_pattern
            search_dir = os.path.dirname(file_pattern) if os.path.dirname(file_pattern) else "."
        else:
            # Use the global output directory or default to current directory
            if globals.GLOBAL_OUTPUT_DIR:
                search_dir = globals.GLOBAL_OUTPUT_DIR / "policy-import"
            else:
                # Fallback to current directory with timestamped subdirectory
                from datetime import datetime
                timestamp = datetime.now().strftime("%Y%m%d%H%M")
                search_dir = Path(f"adoc-migration-toolkit-{timestamp}/policy-import")
            
            # Ensure the search directory exists
            if not search_dir.exists():
                error_msg = f"Policy import directory does not exist: {search_dir}"
                print(f"❌ {error_msg}")
                print(f"💡 Expected location: {search_dir}")
                print(f"💡 Use 'policy-export' first to generate ZIP files, or specify an absolute path")
                logger.error(error_msg)
                return
            
            search_pattern = str(search_dir / file_pattern)
        
        if not quiet_mode:
            print(f"📁 Searching for files in: {search_dir}")
            print(f"🔍 Search pattern: {search_pattern}")
            print("="*80)
        
        # Find all matching ZIP files
        zip_files = glob(search_pattern)
        if not zip_files:
            error_msg = f"No ZIP files found matching pattern: {file_pattern}"
            print(f"❌ {error_msg}")
            print(f"📁 Searched in: {search_dir}")
            print(f"💡 Expected location: {search_dir}")
            print(f"💡 Use 'policy-export' first to generate ZIP files, or specify an absolute path")
            logger.error(error_msg)
            return
        
        # Filter to only ZIP files
        zip_files = [f for f in zip_files if f.lower().endswith('.zip')]
        if not zip_files:
            error_msg = f"No ZIP files found matching pattern: {file_pattern}"
            print(f"❌ {error_msg}")
            print(f"📁 Searched in: {search_dir}")
            print(f"💡 Expected location: {search_dir}")
            print(f"💡 Use 'policy-export' first to generate ZIP files, or specify an absolute path")
            logger.error(error_msg)
            return
        
        if not quiet_mode:
            print(f"Found {len(zip_files)} ZIP files to import")
            print("="*80)
        
        # Statistics aggregation
        aggregated_stats = {
            'conflictingAssemblies': 0,
            'conflictingPolicies': 0,
            'conflictingSqlViews': 0,
            'conflictingVisualViews': 0,
            'preChecks': 0,
            'totalAssetUDFVariablesPerAsset': 0,
            'totalBusinessRules': 0,
            'totalDataCadencePolicyCount': 0,
            'totalDataDriftPolicyCount': 0,
            'totalDataQualityPolicyCount': 0,
            'totalDataSourceCount': 0,
            'totalPolicyCount': 0,
            'totalProfileAnomalyPolicyCount': 0,
            'totalReconciliationPolicyCount': 0,
            'totalReferenceAssets': 0,
            'totalSchemaDriftPolicyCount': 0,
            'totalUDFPackages': 0,
            'totalUDFTemplates': 0,
            'files_processed': 0,
            'files_failed': 0,
            'upload_configs_successful': 0,
            'upload_configs_failed': 0,
            'apply_configs_successful': 0,
            'apply_configs_failed': 0,
            'uuids': []
        }
        
        successful_imports = 0
        failed_imports = 0
        
        # PHASE 1: Upload all ZIP files and collect UUIDs
        if not quiet_mode:
            print(f"\n📤 PHASE 1: Uploading all ZIP files")
            print("="*80)
        
        upload_results = []  # List to store (zip_file, upload_response, upload_uuid) tuples
        
        for i, zip_file in enumerate(zip_files, 1):
            if not quiet_mode:
                print(f"Uploading file {i}/{len(zip_files)}: {zip_file}")
            
            try:
                # Validate file exists and is readable
                if not os.path.exists(zip_file):
                    error_msg = f"File does not exist: {zip_file}"
                    if not quiet_mode:
                        print(f"❌ {error_msg}")
                    logger.error(error_msg)
                    failed_imports += 1
                    aggregated_stats['files_failed'] += 1
                    continue
                
                if not os.path.isfile(zip_file):
                    error_msg = f"Path is not a file: {zip_file}"
                    if not quiet_mode:
                        print(f"❌ {error_msg}")
                    logger.error(error_msg)
                    failed_imports += 1
                    aggregated_stats['files_failed'] += 1
                    continue
                
                # Prepare the multipart form data
                # Read the file content first, then prepare the files dictionary
                with open(zip_file, 'rb') as f:
                    file_content = f.read()
                
                # Prepare files dictionary with proper format for multipart upload
                files = {
                    'policy-config-file': (
                        os.path.basename(zip_file),  # filename
                        file_content,                # file content as bytes
                        'application/zip'            # content type
                    )
                }
                
                if verbose_mode:
                    print(f"\nPOST Request Headers:")
                    print(f"  Endpoint: /catalog-server/api/rules/import/policy-definitions/upload-config")
                    print(f"  Method: POST")
                    print(f"  Content-Type: multipart/form-data")
                    print(f"  Accept: application/json")
                    print(f"  Authorization: Bearer [REDACTED] (Target credentials)")
                    if hasattr(client, 'target_tenant') and client.target_tenant:
                        print(f"  X-Tenant: {client.target_tenant} (Target tenant)")
                    else:
                        print(f"  X-Tenant: {client.tenant} (Source tenant - will be used as target)")
                    print(f"  File: {zip_file}")
                
                # Upload config
                if verbose_mode:
                    print(f"\n📤 Upload Config")
                    print(f"  Endpoint: /catalog-server/api/rules/import/policy-definitions/upload-config")
                    print(f"  Method: POST")
                    print(f"  Content-Type: multipart/form-data")
                    print(f"  File: {zip_file}")
                
                upload_response = client.make_api_call(
                    endpoint="/catalog-server/api/rules/import/policy-definitions/upload-config",
                    method='POST',
                    files=files,
                    use_target_auth=True,
                    use_target_tenant=True
                )
                
                if not upload_response:
                    error_msg = f"Upload config failed - empty response for {zip_file}"
                    if not quiet_mode:
                        print(f"❌ {error_msg}")
                    logger.error(error_msg)
                    failed_imports += 1
                    aggregated_stats['files_failed'] += 1
                    aggregated_stats['upload_configs_failed'] += 1
                    continue
                
                # Extract UUID from upload response
                upload_uuid = upload_response.get('uuid')
                if not upload_uuid:
                    error_msg = f"Upload config failed - no UUID returned for {zip_file}"
                    if not quiet_mode:
                        print(f"❌ {error_msg}")
                    logger.error(error_msg)
                    failed_imports += 1
                    aggregated_stats['files_failed'] += 1
                    aggregated_stats['upload_configs_failed'] += 1
                    continue
                
                # Store successful upload result
                upload_results.append((zip_file, upload_response, upload_uuid))
                aggregated_stats['upload_configs_successful'] += 1
                
                if verbose_mode:
                    print(f"  ✅ Upload config successful")
                    print(f"  UUID: {upload_uuid}")
                    print(f"  Total Policies: {upload_response.get('totalPolicyCount', 0)}")
                    print(f"  Data Quality Policies: {upload_response.get('totalDataQualityPolicyCount', 0)}")
                    print(f"  Data Sources: {upload_response.get('totalDataSourceCount', 0)}")
                else:
                    print(f"✅ [{i}/{len(zip_files)}] {zip_file}: Upload successful (UUID: {upload_uuid})")
                    
            except Exception as e:
                error_msg = f"Failed to upload {zip_file}: {e}"
                if not quiet_mode:
                    print(f"❌ {error_msg}")
                logger.error(error_msg)
                failed_imports += 1
                aggregated_stats['files_failed'] += 1
        
        # PHASE 2: Apply config for each uploaded UUID
        if not quiet_mode:
            print(f"\n📥 PHASE 2: Applying config for {len(upload_results)} uploaded files")
            print("="*80)
        
        for i, (zip_file, upload_response, upload_uuid) in enumerate(upload_results, 1):
            if not quiet_mode:
                print(f"Applying config {i}/{len(upload_results)}: {zip_file} (UUID: {upload_uuid})")
            
            try:
                if verbose_mode:
                    print(f"\n📥 Apply Config")
                    print(f"  Endpoint: /catalog-server/api/rules/import/policy-definitions/apply-config")
                    print(f"  Method: POST")
                    print(f"  Content-Type: application/json")
                    print(f"  UUID: {upload_uuid}")
                
                # Prepare apply config payload
                apply_payload = {
                    "assemblyMap": {},
                    "policyOverride": True,
                    "sqlViewOverride": True,
                    "visualViewOverride": False,
                    "uuid": upload_uuid
                }
                
                apply_response = client.make_api_call(
                    endpoint="/catalog-server/api/rules/import/policy-definitions/apply-config",
                    method='POST',
                    json_payload=apply_payload,
                    use_target_auth=True,
                    use_target_tenant=True
                )
                
                if apply_response:
                    aggregated_stats['apply_configs_successful'] += 1
                    aggregated_stats['files_processed'] += 1
                    successful_imports += 1
                    
                    # Aggregate statistics from upload response
                    for key in aggregated_stats.keys():
                        if key in upload_response and isinstance(upload_response[key], (int, float)):
                            if key == 'uuids':
                                if isinstance(upload_response[key], list):
                                    aggregated_stats[key].extend(upload_response[key])
                            else:
                                aggregated_stats[key] += upload_response[key]
                    
                    # Add UUID to list
                    aggregated_stats['uuids'].append(upload_uuid)
                    
                    if not quiet_mode:
                        print(f"✅ Successfully applied config: {zip_file}")
                        if verbose_mode:
                            print(f"  UUID: {upload_uuid}")
                            print(f"  Total Policies: {upload_response.get('totalPolicyCount', 0)}")
                            print(f"  Data Quality Policies: {upload_response.get('totalDataQualityPolicyCount', 0)}")
                            print(f"  Data Sources: {upload_response.get('totalDataSourceCount', 0)}")
                    else:
                        print(f"✅ [{i}/{len(upload_results)}] {zip_file}: Apply successful")
                else:
                    error_msg = f"Apply config failed for {zip_file} (UUID: {upload_uuid})"
                    if not quiet_mode:
                        print(f"❌ {error_msg}")
                    logger.error(error_msg)
                    failed_imports += 1
                    aggregated_stats['files_failed'] += 1
                    aggregated_stats['apply_configs_failed'] += 1
                    
            except Exception as e:
                error_msg = f"Failed to apply config for {zip_file}: {e}"
                if not quiet_mode:
                    print(f"❌ {error_msg}")
                logger.error(error_msg)
                failed_imports += 1
                aggregated_stats['files_failed'] += 1
        
        # Print summary
        print("\n" + "="*80)
        print("POLICY IMPORT SUMMARY")
        print("="*80)
        print(f"Files processed: {successful_imports}")
        print(f"Files failed: {failed_imports}")
        print(f"Total files: {len(zip_files)}")
        print(f"Upload configs successful: {aggregated_stats['upload_configs_successful']}")
        print(f"Upload configs failed: {aggregated_stats['upload_configs_failed']}")
        print(f"Apply configs successful: {aggregated_stats['apply_configs_successful']}")
        print(f"Apply configs failed: {aggregated_stats['apply_configs_failed']}")
        
        if successful_imports > 0:
            print(f"\n📊 AGGREGATED STATISTICS")
            print("-" * 50)
            print(f"Total Policies: {aggregated_stats['totalPolicyCount']}")
            print(f"Data Quality Policies: {aggregated_stats['totalDataQualityPolicyCount']}")
            print(f"Data Sources: {aggregated_stats['totalDataSourceCount']}")
            print(f"Business Rules: {aggregated_stats['totalBusinessRules']}")
            print(f"Data Cadence Policies: {aggregated_stats['totalDataCadencePolicyCount']}")
            print(f"Data Drift Policies: {aggregated_stats['totalDataDriftPolicyCount']}")
            print(f"Profile Anomaly Policies: {aggregated_stats['totalProfileAnomalyPolicyCount']}")
            print(f"Reconciliation Policies: {aggregated_stats['totalReconciliationPolicyCount']}")
            print(f"Schema Drift Policies: {aggregated_stats['totalSchemaDriftPolicyCount']}")
            print(f"Reference Assets: {aggregated_stats['totalReferenceAssets']}")
            print(f"UDF Packages: {aggregated_stats['totalUDFPackages']}")
            print(f"UDF Templates: {aggregated_stats['totalUDFTemplates']}")
            print(f"Asset UDF Variables: {aggregated_stats['totalAssetUDFVariablesPerAsset']}")
            
            print(f"\n⚠️  CONFLICTS DETECTED")
            print("-" * 30)
            print(f"Conflicting Assemblies: {aggregated_stats['conflictingAssemblies']}")
            print(f"Conflicting Policies: {aggregated_stats['conflictingPolicies']}")
            print(f"Conflicting SQL Views: {aggregated_stats['conflictingSqlViews']}")
            print(f"Conflicting Visual Views: {aggregated_stats['conflictingVisualViews']}")
            
            if aggregated_stats['uuids']:
                print(f"\n🔑 IMPORTED UUIDs")
                print("-" * 20)
                for uuid in aggregated_stats['uuids']:
                    print(f"  {uuid}")
        
        print("="*80)
        
        if failed_imports > 0:
            print("⚠️  Import completed with errors. Check log file for details.")
        else:
            print("✅ Import completed successfully!")
            
    except Exception as e:
        error_msg = f"Error executing policy import: {e}"
        print(f"❌ {error_msg}")
        logger.error(error_msg)


def execute_rule_tag_export(client, logger: logging.Logger, quiet_mode: bool = False, verbose_mode: bool = False):
    """Execute the rule-tag-export command.
    
    Args:
        client: API client instance
        logger: Logger instance
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
    """
    try:
        # Determine output file path using the policy-export category
        output_file = get_output_file_path("", "rule-tags-export.csv", category="policy-export")
        
        if not quiet_mode:
            print(f"\nExporting rule tags from ADOC environment")
            print(f"Output will be written to: {output_file}")
            if verbose_mode:
                print("🔊 VERBOSE MODE - Detailed output including headers and responses")
            print("="*80)
        
        # Check if policies-all-export.csv exists
        if globals.GLOBAL_OUTPUT_DIR:
            policies_file = globals.GLOBAL_OUTPUT_DIR / "policy-export" / "policies-all-export.csv"
        else:
            # Look for the most recent adoc-migration-toolkit-YYYYMMDDHHMM directory
            current_dir = Path.cwd()
            toolkit_dirs = list(current_dir.glob("adoc-migration-toolkit-*"))
            
            if not toolkit_dirs:
                error_msg = "No adoc-migration-toolkit directory found. Please run 'policy-list-export' first."
                print(f"❌ {error_msg}")
                logger.error(error_msg)
                return
            
            # Sort by creation time and use the most recent
            toolkit_dirs.sort(key=lambda x: x.stat().st_ctime, reverse=True)
            latest_toolkit_dir = toolkit_dirs[0]
            policies_file = latest_toolkit_dir / "policy-export" / "policies-all-export.csv"
        
        # Check if policies file exists
        if not policies_file.exists():
            if not quiet_mode:
                print(f"❌ Policy list file not found: {policies_file}")
                print("💡 Running policy-list-export first to generate the required file...")
                print("="*80)
            
            # Run policy-list-export internally
            execute_policy_list_export(client, logger, quiet_mode, verbose_mode)
            
            # Check again if the file was created
            if not policies_file.exists():
                error_msg = "Failed to generate policies-all-export.csv file"
                print(f"❌ {error_msg}")
                logger.error(error_msg)
                return
        
        # Read policies from CSV file
        if not quiet_mode:
            print(f"Reading policies from: {policies_file}")
        
        rule_ids = []
        try:
            with open(policies_file, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader)  # Skip header
                
                for row in reader:
                    if len(row) > 0 and row[0].strip():  # First column should be the rule ID
                        try:
                            rule_id = int(row[0].strip())
                            rule_ids.append(rule_id)
                        except ValueError:
                            # Skip non-numeric IDs
                            continue
        except Exception as e:
            error_msg = f"Failed to read policies file: {e}"
            print(f"❌ {error_msg}")
            logger.error(error_msg)
            return
        
        if not rule_ids:
            error_msg = "No valid rule IDs found in policies file"
            print(f"❌ {error_msg}")
            logger.error(error_msg)
            return
        
        if not quiet_mode:
            print(f"Found {len(rule_ids)} rules to process")
            print("="*80)
        
        # Create output directory if needed
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Process each rule to get tags
        successful_calls = 0
        failed_calls = 0
        rule_tags_data = []
        
        # Create progress bar
        progress_bar = create_progress_bar(
            total=len(rule_ids),
            desc="Exporting rule tags",
            unit="rules",
            disable=verbose_mode
        )
        
        for i, rule_id in enumerate(rule_ids, 1):
            # Update progress bar with current rule ID using set_postfix
            progress_bar.set_postfix(rule_id=rule_id)
            
            try:
                if verbose_mode:
                    print(f"\n\n[Rule {i}/{len(rule_ids)}] Processing rule ID: {rule_id}")
                    print(f"\nGET Request Headers:")
                    print(f"  Endpoint: /catalog-server/api/rules/{rule_id}/tags")
                    print(f"  Method: GET")
                    print(f"  Content-Type: application/json")
                    print(f"  Authorization: Bearer [REDACTED]")
                    if hasattr(client, 'tenant') and client.tenant:
                        print(f"  X-Tenant: {client.tenant}")
                
                # Make API call to get tags for this rule
                response = client.make_api_call(
                    endpoint=f"/catalog-server/api/rules/{rule_id}/tags",
                    method='GET'
                )
                
                if verbose_mode:
                    print(f"\nResponse:")
                    print(json.dumps(response, indent=2, ensure_ascii=False))
                
                # Extract tag names from response
                tag_names = []
                if response and 'ruleTags' in response:
                    for tag in response['ruleTags']:
                        tag_name = tag.get('name')
                        if tag_name:
                            tag_names.append(tag_name)
                
                # Only store the data if there are tags
                if tag_names:
                    rule_tags_data.append({
                        'rule_id': rule_id,
                        'tag_names': tag_names
                    })
                
                successful_calls += 1
                
                # Update progress bar
                progress_bar.update(1)
                
                if verbose_mode:
                    print(f"✅ Rule {rule_id}: Found {len(tag_names)} tags")
                    if tag_names:
                        print(f"   Tags: {', '.join(tag_names)}")
                    else:
                        print(f"   No tags found - skipping output")
                
            except Exception as e:
                error_msg = f"Failed to get tags for rule {rule_id}: {e}"
                if verbose_mode:
                    print(f"❌ {error_msg}")
                logger.error(error_msg)
                failed_calls += 1
                
                # Don't store rules that failed to retrieve tags
                # Update progress bar
                progress_bar.update(1)
        
        # Close progress bar
        progress_bar.close()
        
        # Write results to CSV file
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            
            # Write header
            writer.writerow(['rule_id', 'tags'])
            
            # Write data
            for data in rule_tags_data:
                rule_id = data['rule_id']
                tag_names = data['tag_names']
                tags_str = ','.join(tag_names) if tag_names else ''
                writer.writerow([rule_id, tags_str])
        
        # Print statistics
        if not quiet_mode:
            print("\n" + "="*80)
            print("RULE TAG EXPORT COMPLETED")
            print("="*80)
            print(f"Output file: {output_file}")
            print(f"Total rules processed: {len(rule_ids)}")
            print(f"Successful API calls: {successful_calls}")
            print(f"Failed API calls: {failed_calls}")
            
            # Calculate statistics
            rules_with_tags = len(rule_tags_data)  # All stored rules have tags
            rules_without_tags = len(rule_ids) - rules_with_tags
            
            print(f"Rules with tags (written to output): {rules_with_tags}")
            print(f"Rules without tags (skipped): {rules_without_tags}")
            
            # Calculate success rate
            if len(rule_ids) > 0:
                success_rate = (successful_calls / len(rule_ids)) * 100
                print(f"API success rate: {success_rate:.1f}%")
            
            # Show tag statistics
            all_tags = []
            for data in rule_tags_data:
                all_tags.extend(data['tag_names'])
            
            if all_tags:
                tag_counts = {}
                for tag in all_tags:
                    tag_counts[tag] = tag_counts.get(tag, 0) + 1
                
                print(f"\n📊 TAG STATISTICS")
                print("-" * 50)
                print(f"Total unique tags: {len(tag_counts)}")
                print(f"Total tag occurrences: {len(all_tags)}")
                
                # Show top 10 most common tags
                if tag_counts:
                    print(f"\n🏷️  TOP 10 MOST COMMON TAGS:")
                    print("-" * 40)
                    sorted_tags = sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)
                    for tag_name, count in sorted_tags[:10]:
                        percentage = (count / len(rule_ids)) * 100
                        print(f"  {tag_name:<30} {count:>5} rules ({percentage:>5.1f}%)")
            else:
                print(f"\n📊 TAG STATISTICS")
                print("-" * 50)
                print("No tags found in any rules")
            
            print("="*80)
        else:
            print(f"✅ Rule tag export completed: {len(rule_ids)} rules processed, {len(rule_tags_data)} rules with tags written to output")
        
    except Exception as e:
        error_msg = f"Error in rule-tag-export: {e}"
        if not quiet_mode:
            print(f"❌ {error_msg}")
        logger.error(error_msg)


def execute_policy_export_parallel(client, logger: logging.Logger, quiet_mode: bool = False, verbose_mode: bool = False, batch_size: int = 50, export_type: str = None, filter_value: str = None):
    """Execute the policy-export command with parallel processing.
    
    Args:
        client: API client instance
        logger: Logger instance
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
        batch_size: Number of policies to export in each batch
        export_type: Type of export (rule-types, engine-types, assemblies, source-types)
        filter_value: Optional filter value within the export type
    """
    try:
        # Determine input and output file paths
        if globals.GLOBAL_OUTPUT_DIR:
            input_file = globals.GLOBAL_OUTPUT_DIR / "policy-export" / "policies-all-export.csv"
            output_dir = globals.GLOBAL_OUTPUT_DIR / "policy-export"
        else:
            # Use the same logic as policy-list-export to find the input file
            # Look for the most recent adoc-migration-toolkit-YYYYMMDDHHMM directory
            current_dir = Path.cwd()
            toolkit_dirs = list(current_dir.glob("adoc-migration-toolkit-*"))
            
            if not toolkit_dirs:
                error_msg = "No adoc-migration-toolkit directory found. Please run 'policy-list-export' first."
                print(f"❌ {error_msg}")
                logger.error(error_msg)
                return
            
            # Sort by creation time and use the most recent
            toolkit_dirs.sort(key=lambda x: x.stat().st_ctime, reverse=True)
            latest_toolkit_dir = toolkit_dirs[0]
            
            input_file = latest_toolkit_dir / "policy-export" / "policies-all-export.csv"
            output_dir = latest_toolkit_dir / "policy-export"
        
        if not quiet_mode:
            print(f"\nExporting policy definitions by type (Parallel Mode)")
            print(f"Input file: {input_file}")
            print(f"Output directory: {output_dir}")
            if verbose_mode:
                print("🔊 VERBOSE MODE - Detailed output including headers and responses")
            print("="*80)
        
        # Check if input file exists
        if not input_file.exists():
            error_msg = f"Input file does not exist: {input_file}"
            print(f"❌ {error_msg}")
            print(f"💡 Please run 'policy-list-export' first to generate the input file")
            logger.error(error_msg)
            return
        
        # Read policies from CSV file
        if not quiet_mode:
            print("Reading policies from CSV file...")
        
        policies_by_category = {}
        total_policies = 0
        
        with open(input_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # Skip header
            
            # Check if this is the new format with additional columns
            if len(header) >= 7 and header[0] == 'id' and header[1] == 'type' and header[2] == 'engineType':
                # New format with additional columns
                expected_columns = ['id', 'type', 'engineType', 'tableAssetIds', 'assemblyIds', 'assemblyNames', 'sourceTypes']
                if len(header) != len(expected_columns):
                    error_msg = f"Invalid CSV format. Expected {len(expected_columns)} columns, got {len(header)}"
                    print(f"❌ {error_msg}")
                    logger.error(error_msg)
                    return
            elif len(header) == 3 and header[0] == 'id' and header[1] == 'type' and header[2] == 'engineType':
                # Old format - only basic columns
                error_msg = "CSV file is in old format. Please run 'policy-list-export' first to generate the new format with additional columns."
                print(f"❌ {error_msg}")
                logger.error(error_msg)
                return
            else:
                error_msg = f"Invalid CSV format. Expected header: ['id', 'type', 'engineType', ...], got: {header}"
                print(f"❌ {error_msg}")
                logger.error(error_msg)
                return
            
            for row_num, row in enumerate(reader, start=2):
                if len(row) != len(header):
                    logger.warning(f"Row {row_num}: Expected {len(header)} columns, got {len(row)}")
                    continue
                
                policy_id = row[0].strip()
                policy_type = row[1].strip()
                engine_type = row[2].strip()
                table_asset_ids = row[3].strip()
                assembly_ids = row[4].strip()
                assembly_names = row[5].strip()
                source_types = row[6].strip()
                
                if not policy_id:
                    logger.warning(f"Row {row_num}: Empty policy ID")
                    continue
                
                # Determine the category based on export_type
                category = None
                category_value = None
                
                if export_type == 'rule-types':
                    category = policy_type
                    category_value = policy_type
                elif export_type == 'engine-types':
                    category = engine_type
                    category_value = engine_type
                elif export_type == 'assemblies':
                    if assembly_names:
                        # Split by comma and use the first assembly name
                        assembly_name_list = [name.strip() for name in assembly_names.split(',') if name.strip()]
                        if assembly_name_list:
                            category = assembly_name_list[0]
                            category_value = assembly_names
                elif export_type == 'source-types':
                    if source_types:
                        # Split by comma and use the first source type
                        source_type_list = [stype.strip() for stype in source_types.split(',') if stype.strip()]
                        if source_type_list:
                            category = source_type_list[0]
                            category_value = source_types
                else:
                    # Default: group by policy type (original behavior)
                    category = policy_type
                    category_value = policy_type
                
                # Apply filter if specified
                if filter_value and category != filter_value:
                    continue
                
                if category:
                    if category not in policies_by_category:
                        policies_by_category[category] = []
                    policies_by_category[category].append(policy_id)
                    total_policies += 1
                else:
                    logger.warning(f"Row {row_num}: No valid category found for export type '{export_type}'")
        
        if not policies_by_category:
            error_msg = "No valid policies found matching the specified criteria"
            print(f"❌ {error_msg}")
            logger.error(error_msg)
            return
        
        # Determine output filename based on export type and filter
        if export_type:
            base_filename = export_type.replace('-', '_')
            if filter_value:
                # Sanitize filter value for filename
                safe_filter = "".join(c for c in filter_value if c.isalnum() or c in (' ', '-', '_')).rstrip()
                safe_filter = safe_filter.replace(' ', '_').lower()
                filename = f"{base_filename}_{safe_filter}"
            else:
                filename = base_filename
        else:
            filename = "policy_types"
        
        if not quiet_mode:
            export_type_display = export_type if export_type else "policy types"
            filter_display = f" (filtered by: {filter_value})" if filter_value else ""
            print(f"Found {total_policies} policies across {len(policies_by_category)} {export_type_display}{filter_display}")
            print(f"Output filename base: {filename}")
            print("="*80)
        
        # Generate timestamp for all files
        timestamp = datetime.now().strftime("%m-%d-%Y-%H-%M")
        
        # Step 3: Calculate thread configuration
        max_threads = 5
        min_categories_per_thread = 1
        
        if len(policies_by_category) < min_categories_per_thread:
            num_threads = 1
            categories_per_thread = len(policies_by_category)
        else:
            num_threads = min(max_threads, (len(policies_by_category) + min_categories_per_thread - 1) // min_categories_per_thread)
            categories_per_thread = (len(policies_by_category) + num_threads - 1) // num_threads
        
        if not quiet_mode:
            print(f"Using {num_threads} threads to process {len(policies_by_category)} categories")
            print(f"Categories per thread: {categories_per_thread}")
            print("="*80)
        
        # Step 4: Process categories in parallel
        thread_results = []
        
        # Funny thread names for progress indicators (all same length)
        thread_names = [
            "Rocket Thread     ",
            "Lightning Thread  ", 
            "Unicorn Thread    ",
            "Dragon Thread     ",
            "Shark Thread      "
        ]
        
        def process_category_chunk(thread_id, start_index, end_index):
            """Process a chunk of categories for a specific thread."""
            # Create a thread-local client instance
            thread_client = type(client)(
                host=client.host,
                access_key=client.access_key,
                secret_key=client.secret_key,
                tenant=getattr(client, 'tenant', None)
            )
            
            # Get categories for this thread
            category_items = list(policies_by_category.items())[start_index:end_index]
            
            # Create progress bar for this thread
            total_batches = 0
            for _, policy_ids in category_items:
                total_batches += (len(policy_ids) + batch_size - 1) // batch_size
            
            progress_bar = create_progress_bar(
                total=total_batches,
                desc=thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}",
                unit="batches",
                disable=quiet_mode,
                position=thread_id,
                leave=False
            )
            
            successful_exports = 0
            failed_exports = 0
            export_results = {}
            
            # Process each category in this thread's range
            for policy_type, policy_ids in category_items:
                type_total_batches = (len(policy_ids) + batch_size - 1) // batch_size
                
                for batch_num in range(type_total_batches):
                    start_idx = batch_num * batch_size
                    end_idx = min((batch_num + 1) * batch_size, len(policy_ids))
                    batch_ids = policy_ids[start_idx:end_idx]
                    
                    # Generate filename with range information
                    safe_category = "".join(c for c in policy_type if c.isalnum() or c in (' ', '-', '_')).rstrip()
                    safe_category = safe_category.replace(' ', '_').lower()
                    batch_filename = f"{safe_category}-{timestamp}-{start_idx}-{end_idx-1}.zip"
                    output_file = output_dir / batch_filename
                    
                    # Prepare query parameters
                    ids_param = ','.join(batch_ids)
                    query_params = {
                        'ruleStatus': 'ALL',
                        'includeTags': 'true',
                        'ids': ids_param,
                        'filename': batch_filename
                    }
                    
                    # Build endpoint with query parameters
                    endpoint = "/catalog-server/api/rules/export/policy-definitions"
                    query_string = '&'.join([f"{k}={v}" for k, v in query_params.items()])
                    full_endpoint = f"{endpoint}?{query_string}"
                    
                    if verbose_mode:
                        thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                        print(f"\n{thread_name} - GET Request Headers:")
                        print(f"  Endpoint: {full_endpoint}")
                        print(f"  Method: GET")
                        print(f"  Content-Type: application/zip")
                        print(f"  Authorization: Bearer [REDACTED]")
                        if hasattr(thread_client, 'tenant') and thread_client.tenant:
                            print(f"  X-Tenant: {thread_client.tenant}")
                        print(f"  Query Parameters:")
                        for k, v in query_params.items():
                            if k == 'ids':
                                print(f"    {k}: {len(batch_ids)} IDs (first few: {', '.join(batch_ids[:3])}{'...' if len(batch_ids) > 3 else ''})")
                            else:
                                print(f"    {k}: {v}")
                    
                    try:
                        # Make API call to get ZIP file
                        response = thread_client.make_api_call(
                            endpoint=full_endpoint,
                            method='GET',
                            return_binary=True
                        )
                        
                        if verbose_mode:
                            thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                            print(f"\n{thread_name} - Response:")
                            print(f"  Status: Success")
                            print(f"  Content-Type: application/zip")
                            print(f"  File size: {len(response) if response else 0} bytes")
                        
                        # Write ZIP file to output directory
                        if response:
                            with open(output_file, 'wb') as f:
                                f.write(response)
                            
                            # Store result for this batch
                            batch_key = f"{policy_type}_batch_{batch_num + 1}"
                            export_results[batch_key] = {
                                'success': True,
                                'filename': batch_filename,
                                'count': len(batch_ids),
                                'file_size': len(response),
                                'range': f"{start_idx}-{end_idx-1}"
                            }
                            successful_exports += 1
                        else:
                            error_msg = f"Empty response for {policy_type} batch {batch_num + 1}"
                            if verbose_mode:
                                thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                                print(f"\n{thread_name} - ❌ {error_msg}")
                            logger.error(f"Thread {thread_id}: {error_msg}")
                            
                            batch_key = f"{policy_type}_batch_{batch_num + 1}"
                            export_results[batch_key] = {
                                'success': False,
                                'filename': batch_filename,
                                'count': len(batch_ids),
                                'error': error_msg,
                                'range': f"{start_idx}-{end_idx-1}"
                            }
                            failed_exports += 1
                            
                    except Exception as e:
                        error_msg = f"Failed to export {policy_type} batch {batch_num + 1}: {e}"
                        if verbose_mode:
                            thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                            print(f"\n{thread_name} - ❌ {error_msg}")
                        logger.error(f"Thread {thread_id}: {error_msg}")
                        
                        batch_key = f"{policy_type}_batch_{batch_num + 1}"
                        export_results[batch_key] = {
                            'success': False,
                            'filename': batch_filename,
                            'count': len(batch_ids),
                            'error': str(e),
                            'range': f"{start_idx}-{end_idx-1}"
                        }
                        failed_exports += 1
                    
                    # Update progress bar
                    progress_bar.update(1)
            
            progress_bar.close()
            
            return {
                'thread_id': thread_id,
                'successful_exports': successful_exports,
                'failed_exports': failed_exports,
                'export_results': export_results
            }
        
        # Step 5: Start threads
        threads = []
        for i in range(num_threads):
            start_index = i * categories_per_thread
            end_index = min(start_index + categories_per_thread, len(policies_by_category))
            
            thread = threading.Thread(
                target=lambda tid=i, start=start_index, end=end_index: thread_results.append(
                    process_category_chunk(tid, start, end)
                )
            )
            threads.append(thread)
            thread.start()
        
        # Step 6: Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Step 7: Consolidate results
        total_successful_exports = 0
        total_failed_exports = 0
        all_export_results = {}
        
        for result in thread_results:
            total_successful_exports += result['successful_exports']
            total_failed_exports += result['failed_exports']
            all_export_results.update(result['export_results'])
        
        # Print summary
        print("\n" + "="*80)
        print("POLICY EXPORT SUMMARY (PARALLEL MODE)")
        print("="*80)
        print(f"Output directory: {output_dir}")
        print(f"Timestamp: {timestamp}")
        print(f"Batch size: {batch_size}")
        print(f"Total policy types processed: {len(policies_by_category)}")
        print(f"Threads used: {num_threads}")
        
        for result in thread_results:
            thread_name = thread_names[result['thread_id']] if result['thread_id'] < len(thread_names) else f"Thread {result['thread_id']}"
            print(f"{thread_name}: {result['successful_exports']} successful, {result['failed_exports']} failed")
        
        print(f"\nTotal successful exports: {total_successful_exports}")
        print(f"Total failed exports: {total_failed_exports}")
        
        print(f"\nExport Results:")
        # Group results by policy type for better display
        results_by_type = {}
        for batch_key, result in all_export_results.items():
            policy_type = batch_key.split('_batch_')[0]
            if policy_type not in results_by_type:
                results_by_type[policy_type] = []
            results_by_type[policy_type].append(result)
        
        for policy_type, batch_results in results_by_type.items():
            print(f"  {policy_type}:")
            for result in batch_results:
                if result['success']:
                    print(f"    ✅ Batch {result['range']}: {result['count']} policies -> {result['filename']} ({result['file_size']} bytes)")
                else:
                    print(f"    ❌ Batch {result['range']}: {result['count']} policies -> {result['error']}")
        
        print("="*80)
        
        if total_failed_exports > 0:
            print("⚠️  Export completed with errors. Check log file for details.")
        else:
            print("✅ Export completed successfully!")
            
    except Exception as e:
        error_msg = f"Error executing parallel policy export: {e}"
        print(f"❌ {error_msg}")
        logger.error(error_msg)
        raise 


def execute_rule_tag_export_parallel(client, logger: logging.Logger, quiet_mode: bool = False, verbose_mode: bool = False):
    """Execute the rule-tag-export command with parallel processing.
    
    Args:
        client: API client instance
        logger: Logger instance
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
    """
    try:
        # Determine output file path using the policy-export category
        output_file = get_output_file_path("", "rule-tags-export.csv", category="policy-export")
        
        if not quiet_mode:
            print(f"\nExporting rule tags from ADOC environment (Parallel Mode)")
            print(f"Output will be written to: {output_file}")
            if verbose_mode:
                print("🔊 VERBOSE MODE - Detailed output including headers and responses")
            print("="*80)
        
        # Check if policies-all-export.csv exists
        if globals.GLOBAL_OUTPUT_DIR:
            policies_file = globals.GLOBAL_OUTPUT_DIR / "policy-export" / "policies-all-export.csv"
        else:
            # Look for the most recent adoc-migration-toolkit-YYYYMMDDHHMM directory
            current_dir = Path.cwd()
            toolkit_dirs = list(current_dir.glob("adoc-migration-toolkit-*"))
            
            if not toolkit_dirs:
                error_msg = "No adoc-migration-toolkit directory found. Please run 'policy-list-export' first."
                print(f"❌ {error_msg}")
                logger.error(error_msg)
                return
            
            # Sort by creation time and use the most recent
            toolkit_dirs.sort(key=lambda x: x.stat().st_ctime, reverse=True)
            latest_toolkit_dir = toolkit_dirs[0]
            policies_file = latest_toolkit_dir / "policy-export" / "policies-all-export.csv"
        
        # Check if policies file exists
        if not policies_file.exists():
            if not quiet_mode:
                print(f"❌ Policy list file not found: {policies_file}")
                print("💡 Running policy-list-export first to generate the required file...")
                print("="*80)
            
            # Run policy-list-export internally
            execute_policy_list_export(client, logger, quiet_mode, verbose_mode)
            
            # Check again if the file was created
            if not policies_file.exists():
                error_msg = "Failed to generate policies-all-export.csv file"
                print(f"❌ {error_msg}")
                logger.error(error_msg)
                return
        
        # Read policies from CSV file
        if not quiet_mode:
            print(f"Reading policies from: {policies_file}")
        
        rule_ids = []
        try:
            with open(policies_file, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader)  # Skip header
                
                for row in reader:
                    if len(row) > 0 and row[0].strip():  # First column should be the rule ID
                        try:
                            rule_id = int(row[0].strip())
                            rule_ids.append(rule_id)
                        except ValueError:
                            # Skip non-numeric IDs
                            continue
        except Exception as e:
            error_msg = f"Failed to read policies file: {e}"
            print(f"❌ {error_msg}")
            logger.error(error_msg)
            return
        
        if not rule_ids:
            error_msg = "No valid rule IDs found in policies file"
            print(f"❌ {error_msg}")
            logger.error(error_msg)
            return
        
        if not quiet_mode:
            print(f"Found {len(rule_ids)} rules to process")
        
        # Step 3: Calculate thread configuration
        max_threads = 5
        min_rules_per_thread = 10
        
        if len(rule_ids) < min_rules_per_thread:
            num_threads = 1
            rules_per_thread = len(rule_ids)
        else:
            num_threads = min(max_threads, (len(rule_ids) + min_rules_per_thread - 1) // min_rules_per_thread)
            rules_per_thread = (len(rule_ids) + num_threads - 1) // num_threads
        
        if not quiet_mode:
            print(f"Using {num_threads} threads to process {len(rule_ids)} rules")
            print(f"Rules per thread: {rules_per_thread}")
            print("="*80)
        
        # Step 4: Process rules in parallel
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
        
        def process_rule_chunk(thread_id, start_index, end_index):
            """Process a chunk of rules for a specific thread."""
            # Create a thread-local client instance
            thread_client = type(client)(
                host=client.host,
                access_key=client.access_key,
                secret_key=client.secret_key,
                tenant=getattr(client, 'tenant', None)
            )
            
            # Get rules for this thread
            thread_rule_ids = rule_ids[start_index:end_index]
            
            # Create temporary file for this thread
            temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8')
            temp_files.append(temp_file.name)
            
            # Create progress bar for this thread
            progress_bar = create_progress_bar(
                total=len(thread_rule_ids),
                desc=thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}",
                unit="rules",
                disable=quiet_mode,
                position=thread_id,
                leave=False
            )
            
            successful_calls = 0
            failed_calls = 0
            rule_tags_data = []
            
            # Process each rule in this thread's range
            for i, rule_id in enumerate(thread_rule_ids):
                try:
                    if verbose_mode:
                        thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                        print(f"\n{thread_name} - Processing rule ID: {rule_id}")
                        print(f"GET Request Headers:")
                        print(f"  Endpoint: /catalog-server/api/rules/{rule_id}/tags")
                        print(f"  Method: GET")
                        print(f"  Content-Type: application/json")
                        print(f"  Authorization: Bearer [REDACTED]")
                        if hasattr(thread_client, 'tenant') and thread_client.tenant:
                            print(f"  X-Tenant: {thread_client.tenant}")
                    
                    # Make API call to get tags for this rule
                    response = thread_client.make_api_call(
                        endpoint=f"/catalog-server/api/rules/{rule_id}/tags",
                        method='GET'
                    )
                    
                    if verbose_mode:
                        thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                        print(f"\n{thread_name} - Response:")
                        print(json.dumps(response, indent=2, ensure_ascii=False))
                    
                    # Extract tag names from response
                    tag_names = []
                    if response and 'ruleTags' in response:
                        for tag in response['ruleTags']:
                            tag_name = tag.get('name')
                            if tag_name:
                                tag_names.append(tag_name)
                    
                    # Only store the data if there are tags
                    if tag_names:
                        rule_tags_data.append({
                            'rule_id': rule_id,
                            'tag_names': tag_names
                        })
                    
                    successful_calls += 1
                    
                    if verbose_mode:
                        thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                        print(f"{thread_name} - ✅ Rule {rule_id}: Found {len(tag_names)} tags")
                        if tag_names:
                            print(f"   Tags: {', '.join(tag_names)}")
                        else:
                            print(f"   No tags found - skipping output")
                    
                except Exception as e:
                    error_msg = f"Failed to get tags for rule {rule_id}: {e}"
                    if verbose_mode:
                        thread_name = thread_names[thread_id] if thread_id < len(thread_names) else f"Thread {thread_id}"
                        print(f"\n{thread_name} - ❌ {error_msg}")
                    logger.error(f"Thread {thread_id}: {error_msg}")
                    failed_calls += 1
                
                # Update progress bar
                progress_bar.update(1)
            
            progress_bar.close()
            
            # Write results to temporary CSV file
            with open(temp_file.name, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                
                # Write header
                writer.writerow(['rule_id', 'tags'])
                
                # Write data
                for data in rule_tags_data:
                    rule_id = data['rule_id']
                    tag_names = data['tag_names']
                    tags_str = ','.join(tag_names) if tag_names else ''
                    writer.writerow([rule_id, tags_str])
            
            return {
                'thread_id': thread_id,
                'successful_calls': successful_calls,
                'failed_calls': failed_calls,
                'rules_with_tags': len(rule_tags_data),
                'temp_file': temp_file.name
            }
        
        # Step 5: Start threads
        threads = []
        for i in range(num_threads):
            start_index = i * rules_per_thread
            end_index = min(start_index + rules_per_thread, len(rule_ids))
            
            thread = threading.Thread(
                target=lambda tid=i, start=start_index, end=end_index: thread_results.append(
                    process_rule_chunk(tid, start, end)
                )
            )
            threads.append(thread)
            thread.start()
        
        # Step 6: Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Step 7: Merge temporary files
        if not quiet_mode:
            print("\nMerging temporary files...")
        
        # Create output directory if needed
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Read all rows from temporary files
        all_rows = []
        for temp_file in temp_files:
            try:
                with open(temp_file, 'r', newline='', encoding='utf-8') as temp_csv:
                    reader = csv.reader(temp_csv)
                    next(reader)  # Skip header
                    for row in reader:
                        if len(row) >= 2:  # Ensure we have rule_id and tags
                            all_rows.append(row)
            except Exception as e:
                logger.error(f"Error reading temporary file {temp_file}: {e}")
            finally:
                # Clean up temporary file
                try:
                    os.unlink(temp_file)
                except Exception as e:
                    logger.warning(f"Could not delete temporary file {temp_file}: {e}")
        
        # Step 8: Sort rows by rule_id
        if not quiet_mode:
            print("Sorting results by rule ID...")
        
        def sort_key(row):
            rule_id = row[0] if len(row) > 0 else ''
            # Convert rule_id to int for proper numeric sorting, fallback to string
            try:
                rule_id_int = int(rule_id) if rule_id else 0
            except (ValueError, TypeError):
                rule_id_int = 0
            return rule_id_int
        
        all_rows.sort(key=sort_key)
        
        # Step 9: Write final output
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            
            # Write header
            writer.writerow(['rule_id', 'tags'])
            
            # Write sorted data
            writer.writerows(all_rows)
        
        # Step 10: Print statistics
        if not quiet_mode:
            print("\n" + "="*80)
            print("RULE TAG EXPORT COMPLETED (PARALLEL MODE)")
            print("="*80)
            print(f"Output file: {output_file}")
            print(f"Total rules processed: {len(rule_ids)}")
            print(f"Threads used: {num_threads}")
            
            total_successful_calls = 0
            total_failed_calls = 0
            total_rules_with_tags = 0
            
            for result in thread_results:
                thread_name = thread_names[result['thread_id']] if result['thread_id'] < len(thread_names) else f"Thread {result['thread_id']}"
                print(f"{thread_name}: {result['successful_calls']} successful, {result['failed_calls']} failed, {result['rules_with_tags']} with tags")
                total_successful_calls += result['successful_calls']
                total_failed_calls += result['failed_calls']
                total_rules_with_tags += result['rules_with_tags']
            
            print(f"\nTotal successful API calls: {total_successful_calls}")
            print(f"Total failed API calls: {total_failed_calls}")
            print(f"Rules with tags (written to output): {total_rules_with_tags}")
            print(f"Rules without tags (skipped): {len(rule_ids) - total_rules_with_tags}")
            
            # Calculate success rate
            if len(rule_ids) > 0:
                success_rate = (total_successful_calls / len(rule_ids)) * 100
                print(f"API success rate: {success_rate:.1f}%")
            
            # Show tag statistics
            all_tags = []
            for row in all_rows:
                if len(row) >= 2 and row[1].strip():
                    tags = [tag.strip() for tag in row[1].split(',') if tag.strip()]
                    all_tags.extend(tags)
            
            if all_tags:
                tag_counts = {}
                for tag in all_tags:
                    tag_counts[tag] = tag_counts.get(tag, 0) + 1
                
                print(f"\n📊 TAG STATISTICS")
                print("-" * 50)
                print(f"Total unique tags: {len(tag_counts)}")
                print(f"Total tag occurrences: {len(all_tags)}")
                
                # Show top 10 most common tags
                if tag_counts:
                    print(f"\n🏷️  TOP 10 MOST COMMON TAGS:")
                    print("-" * 40)
                    sorted_tags = sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)
                    for tag_name, count in sorted_tags[:10]:
                        percentage = (count / len(rule_ids)) * 100
                        print(f"  {tag_name:<30} {count:>5} rules ({percentage:>5.1f}%)")
            else:
                print(f"\n📊 TAG STATISTICS")
                print("-" * 50)
                print("No tags found in any rules")
            
            print("="*80)
        else:
            print(f"✅ Rule tag export completed: {len(rule_ids)} rules processed, {len(all_rows)} rules with tags written to output")
        
    except Exception as e:
        error_msg = f"Error in parallel rule-tag-export: {e}"
        if not quiet_mode:
            print(f"❌ {error_msg}")
        logger.error(error_msg)
        raise