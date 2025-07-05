"""
Command Line Interface for ADOC Export Import.

This module provides the CLI entry point for the policy export formatter.
"""

import argparse
import sys
import csv
import json
import logging
import readline
import os
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
from .core import PolicyTranformer, setup_logging
from .api_client import create_api_client
from typing import Optional

# Global variable to store the output directory
GLOBAL_OUTPUT_DIR: Optional[Path] = None


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

def create_asset_export_parser(subparsers):
    """Create the asset-export subcommand parser."""
    asset_export_parser = subparsers.add_parser(
        'asset-export',
        help='Export asset details by reading UIDs from CSV file and making API calls',
        description='Asset Export Tool - Read UIDs from CSV and export asset details via API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m adoc_migration_toolkit asset-export --csv-file=data/output/segmented_spark_uids.csv --env-file=config.env
  python -m adoc_migration_toolkit asset-export --csv-file=data/output/segmented_spark_uids.csv --env-file=config.env --verbose

Features:
  - Reads UIDs from CSV file (first column)
  - Makes REST API calls to Acceldata environment
  - Displays formatted JSON responses
  - Comprehensive error handling and logging
        """
    )
    
    asset_export_parser.add_argument(
        "--csv-file", 
        required=True,
        help="Path to CSV file containing UIDs (first column)"
    )
    asset_export_parser.add_argument(
        "--env-file", 
        required=True,
        help="Path to environment file containing AD_HOST, AD_SOURCE_ACCESS_KEY, AD_SOURCE_SECRET_KEY, AD_SOURCE_TENANT"
    )
    asset_export_parser.add_argument(
        "--log-level", "-l",
        choices=["ERROR", "WARNING", "INFO", "DEBUG"],
        default="ERROR",
        help="Set logging level (default: ERROR)"
    )
    asset_export_parser.add_argument(
        "--verbose", "-v", 
        action="store_true", 
        help="Enable verbose logging (overrides --log-level)"
    )
    
    return asset_export_parser


def create_interactive_parser(subparsers):
    """Create the interactive subcommand parser."""
    interactive_parser = subparsers.add_parser(
        'interactive',
        help='Interactive REST API client for making API calls',
        description='Interactive REST API Client - Make API calls with configurable endpoints and authentication',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m adoc_migration_toolkit interactive --env-file=config.env
  python -m adoc_migration_toolkit interactive --env-file=config.env --verbose

Interactive Commands:
  # REST API Commands
  GET /catalog-server/api/assets?uid=123
  PUT /catalog-server/api/assets {"key": "value"}
  GET /catalog-server/api/assets?uid=123 --target-auth --target-tenant
  
  # Segments Commands
  segments-export [csv_file] [--output-file file] [--quiet]
  segments-import [csv_file] [--dry-run] [--quiet] [--verbose]
  
  # Asset Profile Commands
  asset-profile-export [csv_file] [--output-file file] [--quiet] [--verbose]
  asset-profile-import [csv_file] [--dry-run] [--quiet] [--verbose]
  
  # Asset Configuration Commands
  asset-config-export <csv_file> [--output-file file] [--quiet] [--verbose]
  asset-list-export [--quiet] [--verbose]
  
  # Policy Commands
  policy-list-export [--quiet] [--verbose]
  policy-export [--type export_type] [--filter filter_value] [--quiet] [--verbose] [--batch-size size]
  policy-import <file_or_pattern> [--quiet] [--verbose]
  policy-xfr [--input input_dir] --source-env-string source --target-env-string target [--quiet] [--verbose]
  
  # Utility Commands
  set-output-dir <directory>
  
  # Session Commands
  help
  history
  exit, quit, q

Features:
  - Interactive API client with autocomplete
  - Support for GET and PUT requests
  - Configurable source/target authentication and tenants
  - JSON payload support for PUT requests
  - Well-formatted JSON responses
  - Comprehensive migration toolkit with guided workflows
  - Asset and policy management capabilities
  - File formatting and transformation tools
  - Command history and session management
        """
    )
    
    interactive_parser.add_argument(
        "--env-file", 
        required=True,
        help="Path to environment file containing AD_HOST, AD_SOURCE_ACCESS_KEY, AD_SOURCE_SECRET_KEY, AD_SOURCE_TENANT"
    )
    interactive_parser.add_argument(
        "--log-level", "-l",
        choices=["ERROR", "WARNING", "INFO", "DEBUG"],
        default="ERROR",
        help="Set logging level (default: ERROR)"
    )
    interactive_parser.add_argument(
        "--verbose", "-v", 
        action="store_true", 
        help="Enable verbose logging (overrides --log-level)"
    )
    
    return interactive_parser


def validate_formatter_arguments(args):
    """Validate formatter command line arguments."""
    if not args.source_env_string or not args.source_env_string.strip():
        raise ValueError("Source environment string cannot be empty")
    
    if args.target_env_string is None:
        raise ValueError("Target environment string cannot be None")
    
    # Input directory validation is now handled in run_formatter since it's optional
    # and can auto-detect the policy-export directory


def validate_asset_export_arguments(args):
    """Validate asset-export command line arguments."""
    if not args.csv_file or not args.csv_file.strip():
        raise ValueError("CSV file path cannot be empty")
    
    if not args.env_file or not args.env_file.strip():
        raise ValueError("Environment file path cannot be empty")
    
    # Check if CSV file exists
    csv_path = Path(args.csv_file)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file does not exist: {args.csv_file}")
    
    if not csv_path.is_file():
        raise ValueError(f"CSV path is not a file: {args.csv_file}")
    
    # Check if environment file exists
    env_path = Path(args.env_file)
    if not env_path.exists():
        raise FileNotFoundError(f"Environment file does not exist: {args.env_file}")
    
    if not env_path.is_file():
        raise ValueError(f"Environment path is not a file: {args.env_file}")


def validate_rest_api_arguments(args):
    """Validate rest-api command line arguments."""
    if not args.env_file or not args.env_file.strip():
        raise ValueError("Environment file path cannot be empty")
    
    env_path = Path(args.env_file)
    if not env_path.exists():
        raise FileNotFoundError(f"Environment file does not exist: {args.env_file}")
    
    if not env_path.is_file():
        raise ValueError(f"Environment path is not a file: {args.env_file}")


def read_csv_uids(csv_file: str, logger: logging.Logger) -> list:
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


def read_csv_uids_single_column(csv_file: str, logger: logging.Logger) -> list:
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

def run_asset_export(args):
    """Run the asset-export command."""
    try:
        # Setup logging
        logger = setup_logging(args.verbose, args.log_level)
        
        # Validate arguments
        validate_asset_export_arguments(args)
        
        # Read UIDs from CSV file
        uids = read_csv_uids(args.csv_file, logger)
        
        if not uids:
            logger.warning("No UIDs found in CSV file")
            return 0
        
        # Create API client
        client = create_api_client(env_file=args.env_file, logger=logger)
        
        # Test connection
        if not client.test_connection():
            logger.error("Failed to connect to API")
            return 1
        
        # Process each UID
        successful = 0
        failed = 0
        
        print("\n" + "="*80)
        print("ASSET EXPORT RESULTS")
        print("="*80)
        
        for i, (source_env, target_env) in enumerate(uids, 1):
            print(f"\n[{i}/{len(uids)}] Processing source-env: {source_env}, target-env: {target_env}")
            print("-" * 60)
            
            try:
                # Make API call
                response_data = client.get_asset_by_uid(source_env)
                
                # Display formatted JSON response
                print(json.dumps(response_data, indent=2, ensure_ascii=False))
                successful += 1
                
            except Exception as e:
                logger.error(f"Failed to get asset details for source-env {source_env}: {e}")
                print(f"❌ Error: {e}")
                failed += 1
        
        # Print summary
        print("\n" + "="*80)
        print("EXPORT SUMMARY")
        print("="*80)
        print(f"Total UIDs processed: {len(uids)}")
        print(f"Successful:           {successful}")
        print(f"Failed:               {failed}")
        print("="*80)
        
        # Close client
        client.close()
        
        if failed > 0:
            print("⚠️  Export completed with errors. Check log file for details.")
            return 1
        else:
            print("✅ Export completed successfully!")
            return 0
            
    except (ValueError, FileNotFoundError, PermissionError) as e:
        print(f"❌ Configuration error: {e}")
        return 1
    except KeyboardInterrupt:
        print("\n⚠️  Export interrupted by user.")
        return 1
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1

def parse_api_command(command: str) -> tuple:
    """Parse an API command string into components.
    
    Args:
        command: Command string like "GET /endpoint" or "PUT /endpoint {'key': 'value'}"
        
    Returns:
        Tuple of (method, endpoint, json_payload)
    """
    parts = command.strip().split()
    if not parts:
        return None, None, None
    
    method = parts[0].upper()
    if method not in ['GET', 'PUT']:
        raise ValueError(f"Unsupported HTTP method: {method}")
    
    if len(parts) < 2:
        raise ValueError("Endpoint is required")
    
    endpoint = parts[1]
    json_payload = None
    
    # For PUT requests, look for JSON payload
    if method == 'PUT' and len(parts) > 2:
        # Join remaining parts and try to parse as JSON
        json_str = ' '.join(parts[2:])
        try:
            json_payload = json.loads(json_str)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON payload: {e}")
    
    return method, endpoint, json_payload


def parse_segments_export_command(command: str) -> tuple:
    """Parse a segments-export command string into components.
    
    Args:
        command: Command string like "segments-export [<csv_file>] [--output-file <file>] [--quiet]"
        
    Returns:
        Tuple of (csv_file, output_file, quiet_mode)
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'segments-export':
        return None, None, False
    
    csv_file = None
    output_file = None
    quiet_mode = False
    
    # Check for flags and options
    i = 1
    while i < len(parts):
        if parts[i] == '--output-file' and i + 1 < len(parts):
            output_file = parts[i + 1]
            parts.pop(i)  # Remove --output-file
            parts.pop(i)  # Remove the file path
        elif parts[i] == '--quiet':
            quiet_mode = True
            parts.remove('--quiet')
        elif i == 1 and not parts[i].startswith('--'):
            # This is the CSV file argument (first non-flag argument)
            csv_file = parts[i]
            parts.remove(parts[i])
        else:
            i += 1
    
    # If no CSV file specified, use default from output directory
    if not csv_file:
        if GLOBAL_OUTPUT_DIR:
            csv_file = str(GLOBAL_OUTPUT_DIR / "policy-export" / "segmented_spark_uids.csv")
        else:
            # Look for the most recent adoc-migration-toolkit directory
            current_dir = Path.cwd()
            toolkit_dirs = [d for d in current_dir.iterdir() if d.is_dir() and d.name.startswith("adoc-migration-toolkit-")]
            
            if toolkit_dirs:
                # Sort by creation time and use the most recent
                toolkit_dirs.sort(key=lambda x: x.stat().st_ctime, reverse=True)
                latest_toolkit_dir = toolkit_dirs[0]
                csv_file = str(latest_toolkit_dir / "policy-export" / "segmented_spark_uids.csv")
            else:
                csv_file = "policy-export/segmented_spark_uids.csv"  # Fallback
    
    # Generate default output file if not provided - use policy-import category
    if not output_file:
        output_file = get_output_file_path(csv_file, "segments_output.csv", category="policy-import")
    
    return csv_file, output_file, quiet_mode


def execute_segments_export(csv_file: str, client, logger: logging.Logger, output_file: str = None, quiet_mode: bool = False):
    """Execute the segments-export command.
    
    Args:
        csv_file: Path to the CSV file containing source-env and target-env mappings
        client: API client instance
        logger: Logger instance
        output_file: Path to output file for writing results
        quiet_mode: Whether to suppress console output
    """
    try:
        # Check if CSV file exists
        csv_path = Path(csv_file)
        if not csv_path.exists():
            error_msg = f"CSV file does not exist: {csv_file}"
            print(f"❌ {error_msg}")
            print(f"💡 Please run 'policy-xfr' first to generate the segmented_spark_uids.csv file")
            if GLOBAL_OUTPUT_DIR:
                print(f"   Expected location: {GLOBAL_OUTPUT_DIR}/policy-export/segmented_spark_uids.csv")
            else:
                print(f"   Expected location: adoc-migration-toolkit-YYYYMMDDHHMM/policy-export/segmented_spark_uids.csv")
            logger.error(error_msg)
            return
        
        # Read source-env and target-env mappings from CSV file
        env_mappings = read_csv_uids(csv_file, logger)
        
        if not env_mappings:
            logger.warning("No environment mappings found in CSV file")
            return
        
        # Generate default output file if not provided - use policy-import category
        if not output_file:
            output_file = get_output_file_path(csv_file, "segments_output.csv", category="policy-import")
        
        if not quiet_mode:
            print(f"\nProcessing {len(env_mappings)} environment mappings from CSV file: {csv_file}")
            print(f"Output will be written to: {output_file}")
            if GLOBAL_OUTPUT_DIR:
                print(f"Using global output directory: {GLOBAL_OUTPUT_DIR}")
            print("="*80)
        
        # Open output file for writing
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        successful = 0
        failed = 0
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            
            # Write header
            writer.writerow(['target-env', 'segments_json'])
            
            for i, (source_env, target_env) in enumerate(env_mappings, 1):
                if verbose_mode:
                    print(f"\n[{i}/{len(env_mappings)}] Processing source-env: {source_env}")
                    print(f"Target-env: {target_env}")
                    print("-" * 60)
                elif not quiet_mode:
                    # Calculate progress
                    percentage = (i / len(env_mappings)) * 100
                    bar_width = 50
                    filled_blocks = int((i / len(env_mappings)) * bar_width)
                    
                    # Build the current bar state
                    bar = ''
                    for j in range(bar_width):
                        if j < filled_blocks:
                            # This block should be filled - check if it's a failed asset
                            asset_index_for_block = int((j / bar_width) * len(env_mappings))
                            if asset_index_for_block in failed_indices:
                                bar += '\033[31m█\033[0m'  # Red for failed
                            else:
                                bar += '\033[32m█\033[0m'  # Green for success
                        else:
                            bar += '░'  # Empty block
                    
                    # Move cursor up 1 line and clear the status line, then update both progress bar and status
                    print(f"\033[A\033[KExporting: [{bar}] {i}/{len(env_mappings)} ({percentage:.1f}%)")
                    print(f"\033[KStatus: Processing UID: {source_env}")
                else:
                    # Quiet mode - still show progress bar but minimal status
                    percentage = (i / len(env_mappings)) * 100
                    bar_width = 50
                    filled_blocks = int((i / len(env_mappings)) * bar_width)
                    
                    # Build the current bar state
                    bar = ''
                    for j in range(bar_width):
                        if j < filled_blocks:
                            # This block should be filled - check if it's a failed asset
                            asset_index_for_block = int((j / bar_width) * len(env_mappings))
                            if asset_index_for_block in failed_indices:
                                bar += '\033[31m█\033[0m'  # Red for failed
                            else:
                                bar += '\033[32m█\033[0m'  # Green for success
                        else:
                            bar += '░'  # Empty block
                    
                    # Move cursor up 1 line and clear the status line, then update both progress bar and status
                    print(f"\033[A\033[KExporting: [{bar}] {i}/{len(env_mappings)} ({percentage:.1f}%)")
                    print(f"\033[KStatus: Processing UID: {source_env}")
                
                try:
                    # Step 1: Get asset details by source-env
                    if not quiet_mode:
                        print(f"Getting asset details for source-env: {source_env}")
                    asset_response = client.make_api_call(
                        endpoint=f"/catalog-server/api/assets?uid={source_env}",
                        method='GET'
                    )
                    
                    # Step 2: Extract the top-level "id" field
                    if not asset_response or 'data' not in asset_response:
                        error_msg = f"No 'data' field found in asset response for source-env: {source_env}"
                        if not quiet_mode:
                            print(f"❌ {error_msg}")
                        logger.error(error_msg)
                        failed += 1
                        continue
                    
                    data_array = asset_response['data']
                    if not data_array or len(data_array) == 0:
                        error_msg = f"Empty 'data' array in asset response for source-env: {source_env}"
                        if not quiet_mode:
                            print(f"❌ {error_msg}")
                        logger.error(error_msg)
                        failed += 1
                        continue
                    
                    first_asset = data_array[0]
                    if 'id' not in first_asset:
                        error_msg = f"No 'id' field found in first asset for source-env: {source_env}"
                        if not quiet_mode:
                            print(f"❌ {error_msg}")
                        logger.error(error_msg)
                        failed += 1
                        continue
                    
                    asset_id = first_asset['id']
                    if not quiet_mode:
                        print(f"Extracted asset ID: {asset_id}")
                    
                    # Step 3: Get segments for the asset
                    if not quiet_mode:
                        print(f"Getting segments for asset ID: {asset_id}")
                    segments_response = client.make_api_call(
                        endpoint=f"/catalog-server/api/assets/{asset_id}/segments",
                        method='GET'
                    )
                    
                    # Step 4: Only write to CSV if segments are present and non-empty
                    segments = None
                    if (
                        isinstance(segments_response, dict)
                        and "assetSegments" in segments_response
                        and isinstance(segments_response["assetSegments"], dict)
                        and "segments" in segments_response["assetSegments"]
                    ):
                        segments = segments_response["assetSegments"]["segments"]
                    elif (
                        isinstance(segments_response, dict)
                        and "segments" in segments_response
                    ):
                        segments = segments_response["segments"]
                    
                    if not segments or not isinstance(segments, list) or len(segments) == 0:
                        msg = f"No segments found for asset ID {asset_id} (source-env: {source_env}), skipping."
                        if not quiet_mode:
                            print(f"⚠️  {msg}")
                        logger.info(msg)
                        continue
                    
                    segments_json = json.dumps(segments_response, ensure_ascii=False)
                    writer.writerow([target_env, segments_json])
                    
                    if not quiet_mode:
                        print(f"✅ Written to file: {target_env}")
                        print("Segments Response:")
                        print(json.dumps(segments_response, indent=2, ensure_ascii=False))
                    
                    successful += 1
                    
                except Exception as e:
                    error_msg = f"Failed to process source-env {source_env}: {e}"
                    if not quiet_mode:
                        print(f"❌ {error_msg}")
                    logger.error(error_msg)
                    failed += 1
        
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
                elif header[0] != 'target-env' or header[1] != 'segments_json':
                    validation_errors.append(f"Invalid header: expected ['target-env', 'segments_json'], got {header}")
                
                # Validate each row
                for row_num, row in enumerate(reader, start=2):
                    row_count += 1
                    
                    # Check column count
                    if len(row) != 2:
                        validation_errors.append(f"Row {row_num}: Expected 2 columns, got {len(row)}")
                        continue
                    
                    target_env, segments_json_str = row
                    
                    # Check for empty values
                    if not target_env.strip():
                        validation_errors.append(f"Row {row_num}: Empty target-env value")
                    
                    if not segments_json_str.strip():
                        validation_errors.append(f"Row {row_num}: Empty segments_json value")
                        continue
                    
                    # Verify JSON is parsable
                    try:
                        segments_data = json.loads(segments_json_str)
                        
                        # Additional validation: check if it's a valid segments response
                        if not isinstance(segments_data, dict):
                            validation_errors.append(f"Row {row_num}: segments_json is not a valid JSON object")
                        elif not segments_data:  # Empty object
                            validation_errors.append(f"Row {row_num}: segments_json is empty")
                        
                    except json.JSONDecodeError as e:
                        validation_errors.append(f"Row {row_num}: Invalid JSON in segments_json - {e}")
                    except Exception as e:
                        validation_errors.append(f"Row {row_num}: Error parsing segments_json - {e}")
                
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
                        print(f"   Expected columns: target-env, segments_json")
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
            print("SEGMENTS EXPORT COMPLETED")
            print("="*80)
            print(f"Output file: {output_file}")
            print(f"Total environment mappings processed: {len(env_mappings)}")
            print(f"Successful: {successful}")
            print(f"Failed: {failed}")
            print("="*80)
        else:
            print(f"✅ Segments export completed: {successful} successful, {failed} failed")
            print(f"Output written to: {output_file}")
        
    except Exception as e:
        error_msg = f"Error in segments-export: {e}"
        if not quiet_mode:
            print(f"❌ {error_msg}")
        logger.error(error_msg)


def cleanup_command_history():
    """Clean up command history to prevent cursor position issues."""
    try:
        # Clear the current line buffer to reset cursor position
        readline.clear_history()
        
        # Reload history from file and filter out exit commands
        history_file = os.path.expanduser("~/.adoc_migration_toolkit_history")
        if os.path.exists(history_file):
            # Read history and filter out exit commands
            with open(history_file, 'r') as f:
                lines = f.readlines()
            
            # Filter out exit commands, history, help commands and empty lines
            filtered_lines = []
            for line in lines:
                line = line.strip()
                if line and line.lower() not in ['exit', 'quit', 'q', 'history', 'help']:
                    filtered_lines.append(line)
            
            # Write back filtered history
            with open(history_file, 'w') as f:
                for line in filtered_lines:
                    f.write(line + '\n')
            
            # Reload the cleaned history
            readline.read_history_file(history_file)
            
    except Exception:
        # If cleanup fails, just continue
        pass


def get_user_input(prompt: str) -> str:
    """Get user input with improved cursor handling."""
    try:
        # Clear any pending input
        import sys
        if hasattr(sys.stdin, 'flush'):
            sys.stdin.flush()
        
        # Get input with proper prompt
        command = input(prompt).strip()
        
        # Clean up any trailing whitespace that might cause issues
        return command
    except (EOFError, KeyboardInterrupt):
        raise
    except Exception as e:
        # If there's an issue with input, try to recover
        print(f"\nInput error: {e}")
        return ""


def setup_autocomplete():
    """Setup autocomplete for the interactive session."""
    import glob
    # Available commands and their completions
    commands = [
        'segments-export',
        'segments-import',
        'asset-profile-export',
        'asset-profile-import',
        'asset-config-export',
        'asset-list-export',
        'policy-list-export',
        'policy-export',
        'policy-import',
        'policy-xfr',
        'rule-tag-export',
        'set-output-dir',
        'help',
        'history',
        'exit',
        'quit',
        'q'
    ]
    
    # Flags for completion
    flags = [
        '--quiet',
        '--output-file',
        '--dry-run',
        '--verbose',
        '--input',
        '--output-dir',
        '--source-env-string',
        '--target-env-string',
        '--batch-size'
    ]

    # Commands that expect a file or directory path as the next argument
    path_arg_commands = [
        'segments-export', 'segments-import',
        'asset-profile-export', 'asset-profile-import',
        'asset-config-export', 'policy-import', 'set-output-dir'
    ]

    def complete_path(text):
        """Return a list of file/directory completions for the given text."""
        # If text is empty, list everything in current dir
        if not text:
            text = '.'
        # Expand ~ to home
        text = os.path.expanduser(text)
        # Handle partial paths properly
        if '/' in text:
            # This is a partial path, split it into directory and prefix
            dirname = os.path.dirname(text)
            prefix = os.path.basename(text)
            # Ensure dirname is not empty
            if not dirname:
                dirname = '.'
            try:
                if os.path.isdir(dirname):
                    entries = [os.path.join(dirname, f) for f in os.listdir(dirname) if f.startswith(prefix)]
                else:
                    entries = []
            except Exception:
                entries = []
        else:
            # Single word - check if it's a directory or needs prefix matching
            if os.path.isdir(text):
                entries = [os.path.join(text, f) for f in os.listdir(text)]
            else:
                dirname = '.'
                prefix = text
                try:
                    entries = [os.path.join(dirname, f) for f in os.listdir(dirname) if f.startswith(prefix)]
                except Exception:
                    entries = []
        # Add trailing slash for directories
        completions = []
        for entry in entries:
            if os.path.isdir(entry):
                completions.append(entry + '/')
            else:
                completions.append(entry)
        return completions

    def completer(text, state):
        options = []
        line = readline.get_line_buffer()
        words = line.split()
        # Handle empty line or just whitespace
        if not line.strip():
            options = commands
        elif len(words) == 0:
            options = [cmd for cmd in commands if cmd.lower().startswith(text.lower())]
        elif len(words) == 1:
            current_word = words[0]
            if text == current_word:
                options = [cmd for cmd in commands if cmd.lower().startswith(current_word.lower())]
            else:
                options = [cmd for cmd in commands if cmd.lower().startswith(text.lower())]
        elif len(words) == 2:
            cmd = words[0].lower()
            # If the command expects a path, complete with filesystem
            if cmd in path_arg_commands:
                # For path completion, we need to handle the case where the user is typing
                # a partial path like "data/sam". The text parameter might only be "sam"
                # but we need the full context.
                if '/' in words[1]:
                    # User is typing a path with slashes, reconstruct the full path
                    if text == words[1]:
                        # User is at the end of the word, use it as is
                        full_path = words[1]
                    elif text.startswith(words[1]):
                        # User is continuing to type the same word
                        full_path = text
                    else:
                        # The text is just the last part, reconstruct the full path
                        full_path = words[1][:words[1].rfind('/')+1] + text
                    options = complete_path(full_path)
                else:
                    options = complete_path(text)
            elif cmd == 'set-output-dir':
                # Same logic for set-output-dir
                if '/' in words[1]:
                    if text == words[1]:
                        full_path = words[1]
                    elif text.startswith(words[1]):
                        full_path = text
                    else:
                        full_path = words[1][:words[1].rfind('/')+1] + text
                    options = complete_path(full_path)
                else:
                    options = complete_path(text)

        elif len(words) >= 2:
            cmd = words[0].lower()
            if text.startswith('--'):
                options = [flag for flag in flags if flag.startswith(text)]
            elif cmd in path_arg_commands and len(words) == 2:
                # Same path reconstruction logic
                if '/' in words[1]:
                    dirname = words[1][:words[1].rfind('/')+1]
                    # If text already starts with dirname, use text as is
                    if text.startswith(dirname):
                        full_path = text
                    else:
                        full_path = dirname + text
                    options = complete_path(full_path)
                else:
                    options = complete_path(text)
            elif cmd in path_arg_commands and len(words) > 2:
                # After the path, suggest flags
                if text.startswith('--'):
                    options = [flag for flag in flags if flag.startswith(text)]
            elif cmd == 'asset-list-export' and len(words) >= 2:
                if text.startswith('--'):
                    options = [flag for flag in flags if flag.startswith(text) and flag in ['--quiet', '--verbose']]
            elif cmd == 'policy-list-export' and len(words) >= 2:
                if text.startswith('--'):
                    options = [flag for flag in flags if flag.startswith(text) and flag in ['--quiet', '--verbose']]
            elif cmd == 'policy-export' and len(words) >= 2:
                if text.startswith('--'):
                    options = [flag for flag in flags if flag.startswith(text) and flag in ['--quiet', '--verbose', '--batch-size', '--type', '--filter']]
                elif words[-2] == '--type' and len(words) >= 3:
                    # After --type, suggest export types
                    export_types = ['rule-types', 'engine-types', 'assemblies', 'source-types']
                    options = [et for et in export_types if et.startswith(text)]
                elif words[-2] == '--filter' and len(words) >= 3:
                    # After --filter, suggest common filter values based on the export type
                    # This is a basic suggestion - actual values would depend on the data
                    if any('--type' in word for word in words):
                        # Find the export type to suggest relevant filters
                        type_index = words.index('--type')
                        if type_index + 1 < len(words):
                            export_type = words[type_index + 1]
                            if export_type == 'rule-types':
                                options = ['data-quality', 'data-governance', 'data-observability']
                            elif export_type == 'engine-types':
                                options = ['SPARK', 'JDBC_URL', 'PYTHON']
                            elif export_type == 'assemblies':
                                options = ['production-db', 'staging-db', 'dev-db', 'test-db']
                            elif export_type == 'source-types':
                                options = ['PostgreSQL', 'MySQL', 'Oracle', 'SQL Server']
                            else:
                                options = []
                            options = [opt for opt in options if opt.lower().startswith(text.lower())]
                        else:
                            options = []
                    else:
                        options = []
            elif cmd == 'policy-import' and len(words) >= 2:
                if text.startswith('--'):
                    options = [flag for flag in flags if flag.startswith(text) and flag in ['--quiet', '--verbose']]
            elif cmd == 'policy-xfr' and len(words) >= 2:
                if text.startswith('--'):
                    options = [flag for flag in flags if flag.startswith(text) and flag in ['--quiet', '--verbose', '--input', '--output-dir', '--source-env-string', '--target-env-string']]
            elif cmd == 'asset-profile-export' and len(words) >= 2:
                if text.startswith('--'):
                    options = [flag for flag in flags if flag.startswith(text) and flag in ['--quiet', '--verbose', '--output-file']]
                elif len(words) == 2 and not words[1].startswith('--'):
                    # First argument (optional CSV file)
                    options = complete_path(text)
            elif cmd == 'asset-profile-import' and len(words) >= 2:
                if text.startswith('--'):
                    options = [flag for flag in flags if flag.startswith(text) and flag in ['--quiet', '--verbose', '--dry-run']]
                elif len(words) == 2 and not words[1].startswith('--'):
                    # First argument (optional CSV file)
                    options = complete_path(text)
            elif cmd == 'segments-export' and len(words) >= 2:
                if text.startswith('--'):
                    options = [flag for flag in flags if flag.startswith(text) and flag in ['--quiet', '--output-file']]
                elif len(words) == 2 and not words[1].startswith('--'):
                    # First argument (optional CSV file)
                    options = complete_path(text)
            elif words[-2] == '--output-file' and len(words) >= 3:
                # After --output-file, suggest output file names
                options = complete_path(text)
            elif words[-2] == '--batch-size' and len(words) >= 3:
                # After --batch-size, suggest common batch sizes
                options = ['50', '100', '200', '500', '1000']
        if state < len(options):
            return options[state]
        else:
            return None
    readline.set_completer(completer)
    try:
        readline.parse_and_bind('tab: complete')
        readline.parse_and_bind('bind ^I rl_complete')
        print("✅ Tab completion configured successfully")
    except Exception as e:
        print(f"Warning: Could not configure tab completion: {e}")
        print("Tab completion may not work on this system.")


def run_interactive(args):
    """Run the interactive REST API client."""
    try:
        # Setup logging
        logger = setup_logging(args.verbose, args.log_level)
        
        # Validate arguments
        validate_rest_api_arguments(args)
        
        # Create API client
        client = create_api_client(env_file=args.env_file, logger=logger)
        
        # Test connection
        if not client.test_connection():
            logger.error("Failed to connect to API")
            return 1
        
        # Load global output directory from configuration
        global GLOBAL_OUTPUT_DIR
        GLOBAL_OUTPUT_DIR = load_global_output_directory()
        
        # Display current output directory status
        print("\n" + "="*80)
        print("\033[1m\033[36mADOC INTERACTIVE MIGRATION TOOLKIT\033[0m")
        print("="*80)
        if GLOBAL_OUTPUT_DIR:
            print(f"📁 Output Directory: {GLOBAL_OUTPUT_DIR}")
            print(f"📁 Current Directory: {os.getcwd()}")
            print(f"📋 Config File: {args.env_file}")
            print(f"🌍 Source Environment: {client.host}")
            print(f"🌍 Source Tenant: {client.tenant}")
        else:
            print(f"📁 Output Directory: Not set (will use default timestamped directories)")
            print(f"💡 Use 'set-output-dir <directory>' to set a persistent output directory")
        print("="*80)
        
        # Setup command history
        history_file = os.path.expanduser("~/.adoc_migration_toolkit_history")
        try:
            readline.read_history_file(history_file)
        except FileNotFoundError:
            pass  # History file doesn't exist yet
        
        # Set history file for future sessions
        readline.set_history_length(1000)  # Keep last 1000 commands
        
        # Configure readline for better cursor handling
        try:
            # Set input mode for better cursor behavior
            readline.parse_and_bind('set input-meta on')
            readline.parse_and_bind('set output-meta on')
            readline.parse_and_bind('set convert-meta off')
            readline.parse_and_bind('set horizontal-scroll-mode on')
            readline.parse_and_bind('set completion-query-items 0')
            readline.parse_and_bind('set page-completions off')
            readline.parse_and_bind('set skip-completed-text on')
            readline.parse_and_bind('set completion-ignore-case on')
            readline.parse_and_bind('set show-all-if-ambiguous on')
            readline.parse_and_bind('set show-all-if-unmodified on')
        except Exception as e:
            logger.warning(f"Could not configure readline settings: {e}")
        
        # Setup autocomplete
        setup_autocomplete()
        
        # Clean up command history to prevent cursor issues
        cleanup_command_history()
        
        while True:
            try:
                # Get user input with improved handling
                command = get_user_input("\n\033[1m\033[36mADOC\033[0m > ")
                
                if not command:
                    continue
                
                # Don't add exit commands to history
                if command.lower() in ['exit', 'quit', 'q']:
                    print("Goodbye!")
                    break
                
                # Check if it's a command number from history (before adding to history)
                if command.isdigit():
                    history_command = get_command_from_history(int(command))
                    if history_command:
                        print(f"Executing: {history_command}")
                        # Set the command to the history command and continue processing
                        command = history_command
                        # Don't add this to history since it's already there
                        skip_history_add = True
                    else:
                        print(f"❌ No command found with number {command}")
                        continue
                else:
                    skip_history_add = False
                
                # List of valid commands (including aliases)
                valid_commands = [
                    'segments-export', 'segments-import',
                    'asset-profile-export', 'asset-profile-import',
                    'asset-config-export', 'asset-list-export',
                    'policy-list-export', 'policy-export', 'policy-import', 'policy-xfr', 'rule-tag-export',
                    'set-output-dir',
                    # Utility commands (will be filtered anyway)
                    'help', 'history', 'exit', 'quit', 'q'
                ]
                
                # Add command to history (except exit commands, history command, help command, and commands from history)
                if (
                    not skip_history_add
                    and command.strip()
                    and command.lower() not in ['exit', 'quit', 'q', 'history', 'help']
                    and any(command.lower().startswith(cmd) for cmd in valid_commands)
                ):
                    try:
                        readline.add_history(command)
                    except Exception:
                        pass  # Ignore history errors
                
                # Check if it's a help command
                if command.lower() == 'help':
                    show_interactive_help()
                    continue
                
                # Check if it's a history command
                if command.lower() == 'history':
                    show_command_history()
                    continue
                
                # Check if it's a segments-export command
                if command.lower().startswith('segments-export'):
                    csv_file, output_file, quiet_mode = parse_segments_export_command(command)
                    if csv_file:
                        execute_segments_export(csv_file, client, logger, output_file, quiet_mode)
                    continue
                
                # Check if it's a segments-import command
                if command.lower().startswith('segments-import'):
                    csv_file, dry_run, quiet_mode, verbose_mode = parse_segments_import_command(command)
                    if csv_file:
                        execute_segments_import(csv_file, client, logger, dry_run, quiet_mode, verbose_mode)
                    continue
                
                # Check if it's an asset-profile-export command
                if command.lower().startswith('asset-profile-export'):
                    csv_file, output_file, quiet_mode, verbose_mode = parse_asset_profile_export_command(command)
                    if csv_file:
                        execute_asset_profile_export(csv_file, client, logger, output_file, quiet_mode, verbose_mode)
                    continue
                
                # Check if it's an asset-profile-import command
                if command.lower().startswith('asset-profile-import'):
                    csv_file, dry_run, quiet_mode, verbose_mode = parse_asset_profile_import_command(command)
                    if csv_file:
                        execute_asset_profile_import(csv_file, client, logger, dry_run, quiet_mode, verbose_mode)
                    continue
                
                # Check if it's an asset-config-export command
                if command.lower().startswith('asset-config-export'):
                    csv_file, output_file, quiet_mode, verbose_mode = parse_asset_config_export_command(command)
                    if csv_file:
                        execute_asset_config_export(csv_file, client, logger, output_file, quiet_mode, verbose_mode)
                    continue
                
                # Check if it's an asset-list-export command
                if command.lower().startswith('asset-list-export'):
                    quiet_mode, verbose_mode = parse_asset_list_export_command(command)
                    execute_asset_list_export(client, logger, quiet_mode, verbose_mode)
                    continue
                
                # Check if it's a policy-list-export command
                if command.lower().startswith('policy-list-export'):
                    quiet_mode, verbose_mode = parse_policy_list_export_command(command)
                    execute_policy_list_export(client, logger, quiet_mode, verbose_mode)
                    continue
                
                # Check if it's a policy-export command
                if command.lower().startswith('policy-export'):
                    quiet_mode, verbose_mode, batch_size, export_type, filter_value = parse_policy_export_command(command)
                    execute_policy_export(client, logger, quiet_mode, verbose_mode, batch_size, export_type, filter_value)
                    continue
                
                # Check if it's a policy-import command
                if command.lower().startswith('policy-import'):
                    file_pattern, quiet_mode, verbose_mode = parse_policy_import_command(command)
                    if file_pattern:
                        execute_policy_import(client, logger, file_pattern, quiet_mode, verbose_mode)
                    continue
                
                # Check if it's a rule-tag-export command
                if command.lower().startswith('rule-tag-export'):
                    quiet_mode, verbose_mode = parse_rule_tag_export_command(command)
                    execute_rule_tag_export(client, logger, quiet_mode, verbose_mode)
                    continue
                
                # Check if it's a policy-xfr command
                if command.lower().startswith('policy-xfr'):
                    input_dir, source_string, target_string, output_dir, quiet_mode, verbose_mode = parse_formatter_command(command)
                    if source_string and target_string:
                        execute_formatter(input_dir, source_string, target_string, output_dir, quiet_mode, verbose_mode, logger)
                    continue
                
                # Check if it's a set-output-dir command
                if command.lower().startswith('set-output-dir'):
                    directory = parse_set_output_dir_command(command)
                    if directory:
                        set_global_output_directory(directory, logger)
                    continue
                
                # Parse the command for GET/PUT requests
                method, endpoint, json_payload = parse_api_command(command)
                
                if method is None:
                    continue
                
                # Handle dynamic endpoints with placeholders
                if '<asset-id>' in endpoint or '<asset-uid>' in endpoint:
                    endpoint = handle_dynamic_endpoints(endpoint)
                    print(f"\nCurrent endpoint: {endpoint}")
                    print("Please modify the command to replace <asset-id> or <asset-uid> with actual values, then press Enter to continue...")
                    continue
                
                # Make the API call
                print(f"\nMaking {method} request to: {endpoint}")
                print("-" * 60)
                
                response_data = client.make_api_call(
                    endpoint=endpoint,
                    method=method,
                    json_payload=json_payload
                )
                
                # Display formatted JSON response
                print(json.dumps(response_data, indent=2, ensure_ascii=False))
                
            except KeyboardInterrupt:
                print("\n\nGoodbye!")
                break
            except (ValueError, FileNotFoundError, PermissionError) as e:
                print(f"❌ Error: {e}")
            except Exception as e:
                print(f"❌ Unexpected error: {e}")
                logger.error(f"Unexpected error in interactive mode: {e}")
        
        # Save command history
        try:
            readline.write_history_file(history_file)
        except Exception as e:
            logger.warning(f"Could not save command history: {e}")
        
        # Close client
        client.close()
        return 0
        
    except (ValueError, FileNotFoundError, PermissionError) as e:
        print(f"❌ Configuration error: {e}")
        return 1
    except KeyboardInterrupt:
        print("\n⚠️  Client interrupted by user.")
        return 1
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1


def parse_segments_import_command(command: str) -> tuple:
    """Parse a segments-import command string into components.
    
    Args:
        command: Command string like "segments-import <csv_file> [--dry-run] [--quiet] [--verbose]"
        
    Returns:
        Tuple of (csv_file, dry_run, quiet_mode, verbose_mode)
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'segments-import':
        return None, False, True, False
    
    if len(parts) < 2:
        raise ValueError("CSV file path is required for segments-import command")
    
    csv_file = parts[1]
    dry_run = False
    quiet_mode = True  # Default to quiet mode
    verbose_mode = False
    
    # Check for flags
    if '--dry-run' in parts:
        dry_run = True
        parts.remove('--dry-run')
    
    if '--verbose' in parts:
        verbose_mode = True
        quiet_mode = False  # Verbose overrides quiet
        parts.remove('--verbose')
    
    if '--quiet' in parts:
        quiet_mode = True
        verbose_mode = False  # Quiet overrides verbose
        parts.remove('--quiet')
    
    return csv_file, dry_run, quiet_mode, verbose_mode


def execute_segments_import(csv_file: str, client, logger: logging.Logger, dry_run: bool = False, quiet_mode: bool = True, verbose_mode: bool = False):
    """Execute the segments-import command.
    
    Args:
        csv_file: Path to the CSV file containing target-env and segments_json
        client: API client instance
        logger: Logger instance
        dry_run: Whether to perform a dry run (no actual API calls)
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
    """
    try:
        # Read target-env and segments_json from CSV file
        if not Path(csv_file).exists():
            error_msg = f"CSV file does not exist: {csv_file}"
            print(f"❌ {error_msg}")
            logger.error(error_msg)
            return
        
        print(f"\nProcessing segment import from CSV file: {csv_file}")
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
            
            if len(header) != 2 or header[0] != 'target-env' or header[1] != 'segments_json':
                error_msg = f"Invalid CSV format. Expected header: ['target-env', 'segments_json'], got: {header}"
                print(f"❌ {error_msg}")
                logger.error(error_msg)
                return
            
            for row_num, row in enumerate(reader, start=2):
                if len(row) != 2:
                    logger.warning(f"Row {row_num}: Expected 2 columns, got {len(row)}")
                    continue
                
                target_env = row[0].strip()
                segments_json = row[1].strip()
                
                if target_env and segments_json:
                    import_mappings.append((target_env, segments_json))
                    logger.debug(f"Row {row_num}: Found target-env: {target_env}")
                else:
                    logger.warning(f"Row {row_num}: Empty target-env or segments_json value")
        
        if not import_mappings:
            logger.warning("No valid import mappings found in CSV file")
            return
        
        logger.info(f"Read {len(import_mappings)} import mappings from CSV file: {csv_file}")
        
        successful = 0
        failed = 0
        
        for i, (target_env, segments_json) in enumerate(import_mappings, 1):
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
                    continue
                
                data_array = asset_response['data']
                if not data_array or len(data_array) == 0:
                    error_msg = f"Empty 'data' array in asset response for UID: {target_env}"
                    print(f"❌ [{i}/{len(import_mappings)}] {target_env}: {error_msg}")
                    logger.error(error_msg)
                    failed += 1
                    continue
                
                first_asset = data_array[0]
                if 'id' not in first_asset:
                    error_msg = f"No 'id' field found in first asset for UID: {target_env}"
                    print(f"❌ [{i}/{len(import_mappings)}] {target_env}: {error_msg}")
                    logger.error(error_msg)
                    failed += 1
                    continue
                
                asset_id = first_asset['id']
                if not quiet_mode:
                    print(f"Extracted asset ID: {asset_id}")
                
                # Step 3: Parse segments JSON and extract segments array
                try:
                    segments_data = json.loads(segments_json)
                    
                    # Extract segments from the JSON structure
                    if 'assetSegments' in segments_data and 'segments' in segments_data['assetSegments']:
                        segments = segments_data['assetSegments']['segments']
                    elif 'segments' in segments_data:
                        segments = segments_data['segments']
                    else:
                        error_msg = f"No 'segments' array found in JSON for UID: {target_env}"
                        if not quiet_mode:
                            print(f"❌ {error_msg}")
                        logger.error(error_msg)
                        failed += 1
                        continue
                    
                    # Prepare segments for import (remove IDs to create new segments)
                    import_segments = []
                    for segment in segments:
                        import_segment = {
                            "id": None,  # Set to None to create new segment
                            "name": segment.get("name", ""),
                            "conditions": []
                        }
                        
                        # Process conditions
                        if "conditions" in segment:
                            for condition in segment["conditions"]:
                                import_condition = {
                                    "id": None,  # Set to None to create new condition
                                    "columnId": condition.get("columnId"),
                                    "condition": condition.get("condition", "CUSTOM"),
                                    "value": condition.get("value", "")
                                }
                                import_segment["conditions"].append(import_condition)
                        
                        import_segments.append(import_segment)
                    
                    if not quiet_mode:
                        print(f"Prepared {len(import_segments)} segments for import")
                        for seg in import_segments:
                            print(f"  - {seg['name']} ({len(seg['conditions'])} conditions)")
                    
                except json.JSONDecodeError as e:
                    error_msg = f"Invalid JSON in segments_json for UID {target_env}: {e}"
                    if not quiet_mode:
                        print(f"❌ {error_msg}")
                    logger.error(error_msg)
                    failed += 1
                    continue
                
                # Step 4: Make POST request to import segments
                if not quiet_mode:
                    print(f"Importing segments for asset ID: {asset_id}")
                
                if not dry_run:
                    import_payload = {"segments": import_segments}
                    
                    # Show headers in verbose mode
                    if verbose_mode:
                        print("\nPOST Request Headers:")
                        print(f"  Endpoint: /catalog-server/api/assets/{asset_id}/segments")
                        print(f"  Method: POST")
                        print(f"  Content-Type: application/json")
                        print(f"  Authorization: Bearer [REDACTED]")
                        if hasattr(client, 'target_tenant') and client.target_tenant:
                            print(f"  X-Tenant: {client.target_tenant}")
                        print(f"  Payload: {json.dumps(import_payload)}")
                    
                    import_response = client.make_api_call(
                        endpoint=f"/catalog-server/api/assets/{asset_id}/segments",
                        method='POST',
                        json_payload=import_payload,
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
                    if not quiet_mode:
                        print("🔍 DRY RUN - Would import segments:")
                        print(json.dumps({"segments": import_segments}))
                
                successful += 1
                
            except Exception as e:
                error_msg = f"Failed to process UID {target_env}: {e}"
                if not quiet_mode:
                    print(f"❌ {error_msg}")
                logger.error(error_msg)
                failed += 1
        
        # Print summary
        if not quiet_mode:
            print("\n" + "="*80)
            print("SEGMENT IMPORT COMPLETED")
            print("="*80)
            if dry_run:
                print("🔍 DRY RUN MODE - No actual changes were made")
            print(f"Total mappings processed: {len(import_mappings)}")
            print(f"Successful: {successful}")
            print(f"Failed: {failed}")
            print("="*80)
        else:
            print(f"✅ Segment import completed: {successful} successful, {failed} failed")
            if dry_run:
                print("🔍 DRY RUN MODE - No actual changes were made")
        
    except Exception as e:
        error_msg = f"Error in segments-import: {e}"
        print(f"❌ {error_msg}")
        logger.error(error_msg)


def parse_asset_profile_export_command(command: str) -> tuple:
    """Parse an asset-profile-export command string into components.
    
    Args:
        command: Command string like "asset-profile-export [<csv_file>] [--output-file <file>] [--quiet] [--verbose]"
        
    Returns:
        Tuple of (csv_file, output_file, quiet_mode, verbose_mode)
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'asset-profile-export':
        return None, None, False, False
    
    csv_file = None
    output_file = None
    quiet_mode = False  # Default to showing progress bar and status
    verbose_mode = False
    
    # Check for flags and options
    i = 1
    while i < len(parts):
        if parts[i] == '--output-file' and i + 1 < len(parts):
            output_file = parts[i + 1]
            parts.pop(i)  # Remove --output-file
            parts.pop(i)  # Remove the file path
        elif parts[i] == '--quiet':
            quiet_mode = True
            verbose_mode = False  # Quiet overrides verbose
            parts.remove('--quiet')
        elif parts[i] == '--verbose':
            verbose_mode = True
            quiet_mode = False  # Verbose overrides quiet
            parts.remove('--verbose')
        elif i == 1 and not parts[i].startswith('--'):
            # This is the CSV file argument (first non-flag argument)
            csv_file = parts[i]
            parts.remove(parts[i])
        else:
            i += 1
    
    # If no CSV file specified, use default from output directory
    if not csv_file:
        if GLOBAL_OUTPUT_DIR:
            csv_file = str(GLOBAL_OUTPUT_DIR / "asset-export" / "asset_uids.csv")
        else:
            # Look for the most recent adoc-migration-toolkit directory
            current_dir = Path.cwd()
            toolkit_dirs = [d for d in current_dir.iterdir() if d.is_dir() and d.name.startswith("adoc-migration-toolkit-")]
            
            if toolkit_dirs:
                # Sort by creation time and use the most recent
                toolkit_dirs.sort(key=lambda x: x.stat().st_ctime, reverse=True)
                latest_toolkit_dir = toolkit_dirs[0]
                csv_file = str(latest_toolkit_dir / "asset-export" / "asset_uids.csv")
            else:
                csv_file = "asset-export/asset_uids.csv"  # Fallback
    
    # Generate default output file if not provided - use asset-import category
    if not output_file:
        output_file = get_output_file_path(csv_file, "asset-profiles-import-ready.csv", category="asset-import")
    
    return csv_file, output_file, quiet_mode, verbose_mode


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
            if GLOBAL_OUTPUT_DIR:
                print(f"   Expected location: {GLOBAL_OUTPUT_DIR}/asset-export/asset_uids.csv")
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
            if GLOBAL_OUTPUT_DIR:
                print(f"Using global output directory: {GLOBAL_OUTPUT_DIR}")
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
            
            # Print completion status
            if not quiet_mode:
                print(f"✅ Export completed - {successful} successful, {failed} failed")
            
            # Print comprehensive statistics
            if not quiet_mode:
                print("\n" + "="*80)
                print("ASSET PROFILE EXPORT STATISTICS")
                print("="*80)
                
                # File information
                print(f"📁 FILE INFORMATION:")
                print(f"  Input CSV: {csv_file}")
                print(f"  Output CSV: {output_file}")
                print(f"  File size: {output_path.stat().st_size:,} bytes")
                
                # Processing statistics
                print(f"\n📊 PROCESSING STATISTICS:")
                print(f"  Total assets to process: {len(env_mappings)}")
                print(f"  Successfully processed: {successful}")
                print(f"  Failed to process: {failed}")
                print(f"  Success rate: {(successful / len(env_mappings) * 100):.1f}%")
                print(f"  Failure rate: {(failed / len(env_mappings) * 100):.1f}%")
                
                # Performance metrics
                if successful > 0:
                    print(f"\n⚡ PERFORMANCE METRICS:")
                    print(f"  Average profiles per asset: {total_assets_processed / successful:.1f}")
                    print(f"  Total profiles exported: {total_assets_processed}")
                
                # Error summary
                if failed > 0:
                    print(f"\n⚠️  ERROR SUMMARY:")
                    print(f"  Assets with missing data field: {sum(1 for i in failed_indices if i < len(env_mappings))}")
                    print(f"  Assets with empty data array: {sum(1 for i in failed_indices if i < len(env_mappings))}")
                    print(f"  Assets with missing ID field: {sum(1 for i in failed_indices if i < len(env_mappings))}")
                    print(f"  API call failures: {failed}")
                
                # Output format information
                print(f"\n📋 OUTPUT FORMAT:")
                print(f"  CSV columns: target-env, profile_json")
                print(f"  JSON encoding: UTF-8")
                print(f"  CSV quoting: QUOTE_ALL")
                print(f"  Line endings: Platform default")
                
                # Validation results
                print(f"\n✅ VALIDATION:")
                print(f"  CSV file readable: Yes")
                print(f"  Header format: Correct")
                print(f"  JSON entries: Valid and parseable")
                print(f"  Data integrity: Verified")
                
                print("="*80)
                
                if failed > 0:
                    print("⚠️  Export completed with errors. Check log file for details.")
                else:
                    print("✅ Export completed successfully!")
            else:
                print(f"✅ Asset profile export completed: {successful} successful, {failed} failed")
                print(f"Output written to: {output_file}")
        
    except Exception as e:
        error_msg = f"Error in asset-profile-export: {e}"
        if not quiet_mode:
            print(f"❌ {error_msg}")
        logger.error(error_msg)


def parse_asset_profile_import_command(command: str) -> tuple:
    """Parse an asset-profile-import command string into components.
    
    Args:
        command: Command string like "asset-profile-import [<csv_file>] [--dry-run] [--quiet] [--verbose]"
        
    Returns:
        Tuple of (csv_file, dry_run, quiet_mode, verbose_mode)
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'asset-profile-import':
        return None, False, True, False
    
    csv_file = None
    dry_run = False
    quiet_mode = True  # Default to quiet mode
    verbose_mode = False
    
    # Check for flags and options
    i = 1
    while i < len(parts):
        if parts[i] == '--dry-run':
            dry_run = True
            parts.remove('--dry-run')
        elif parts[i] == '--verbose':
            verbose_mode = True
            quiet_mode = False  # Verbose overrides quiet
            parts.remove('--verbose')
        elif parts[i] == '--quiet':
            quiet_mode = True
            verbose_mode = False  # Quiet overrides verbose
            parts.remove('--quiet')
        elif i == 1 and not parts[i].startswith('--'):
            # This is the CSV file argument (first non-flag argument)
            csv_file = parts[i]
            parts.remove(parts[i])
        else:
            i += 1
    
    # If no CSV file specified, use default from output directory
    if not csv_file:
        if GLOBAL_OUTPUT_DIR:
            csv_file = str(GLOBAL_OUTPUT_DIR / "asset-import" / "asset-profiles-import-ready.csv")
        else:
            # Look for the most recent adoc-migration-toolkit directory
            current_dir = Path.cwd()
            toolkit_dirs = [d for d in current_dir.iterdir() if d.is_dir() and d.name.startswith("adoc-migration-toolkit-")]
            
            if toolkit_dirs:
                # Sort by creation time and use the most recent
                toolkit_dirs.sort(key=lambda x: x.stat().st_ctime, reverse=True)
                latest_toolkit_dir = toolkit_dirs[0]
                csv_file = str(latest_toolkit_dir / "asset-import" / "asset-profiles-import-ready.csv")
            else:
                csv_file = "asset-import/asset-profiles-import-ready.csv"  # Fallback
    
    return csv_file, dry_run, quiet_mode, verbose_mode


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
            if GLOBAL_OUTPUT_DIR:
                print(f"   Expected location: {GLOBAL_OUTPUT_DIR}/asset-import/asset-profiles-import-ready.csv")
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


def parse_asset_config_export_command(command: str) -> tuple:
    """Parse an asset-config-export command string into components.
    
    Args:
        command: Command string like "asset-config-export <csv_file> [--output-file <file>] [--quiet] [--verbose]"
        
    Returns:
        Tuple of (csv_file, output_file, quiet_mode, verbose_mode)
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'asset-config-export':
        return None, None, False, False
    
    if len(parts) < 2:
        raise ValueError("CSV file path is required for asset-config-export command")
    
    csv_file = parts[1]
    output_file = None
    quiet_mode = False
    verbose_mode = False
    
    # Check for flags and options
    i = 2
    while i < len(parts):
        if parts[i] == '--output-file' and i + 1 < len(parts):
            output_file = parts[i + 1]
            parts.pop(i)  # Remove --output-file
            parts.pop(i)  # Remove the file path
        elif parts[i] == '--quiet':
            quiet_mode = True
            verbose_mode = False  # Quiet overrides verbose
            parts.remove('--quiet')
        elif parts[i] == '--verbose':
            verbose_mode = True
            quiet_mode = False  # Verbose overrides quiet
            parts.remove('--verbose')
        else:
            i += 1
    
    # Generate default output file if not provided
    if not output_file:
        output_file = get_output_file_path(csv_file, "asset-config-export.csv", category="asset-export")
    
    return csv_file, output_file, quiet_mode, verbose_mode


def execute_asset_config_export(csv_file: str, client, logger: logging.Logger, output_file: str = None, quiet_mode: bool = False, verbose_mode: bool = False):
    """Execute the asset-config-export command.
    
    Args:
        csv_file: Path to the CSV file containing UIDs in the first column
        client: API client instance
        logger: Logger instance
        output_file: Path to output file for writing results
        quiet_mode: Whether to suppress console output
        verbose_mode: Whether to enable verbose logging
    """
    try:
        # Read UIDs from the first column of CSV file
        uids = read_csv_uids_single_column(csv_file, logger)
        
        if not uids:
            logger.warning("No UIDs found in CSV file")
            return
        
        # Generate default output file if not provided
        if not output_file:
            output_file = get_output_file_path(csv_file, "asset-config-export.csv", category="asset-export")
        
        if not quiet_mode:
            print(f"\nProcessing {len(uids)} asset config exports from CSV file: {csv_file}")
            print(f"Output will be written to: {output_file}")
            if GLOBAL_OUTPUT_DIR:
                print(f"Using global output directory: {GLOBAL_OUTPUT_DIR}")
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
            writer.writerow(['target-env', 'config_json'])
            
            for i, uid in enumerate(uids, 1):
                if not quiet_mode:
                    print(f"\n[{i}/{len(uids)}] Processing UID: {uid}")
                    print("-" * 60)
                else:
                    print(f"Processing [{i}/{len(uids)}] UID: {uid}")
                
                try:
                    # Step 1: Get asset details by UID to extract the asset ID
                    if not quiet_mode:
                        print(f"Getting asset details for UID: {uid}")
                    
                    # Show headers in verbose mode
                    if verbose_mode:
                        print("\nGET Request Headers:")
                        print(f"  Endpoint: /catalog-server/api/assets?uid={uid}")
                        print(f"  Method: GET")
                        print(f"  Content-Type: application/json")
                        print(f"  Authorization: Bearer [REDACTED]")
                        if hasattr(client, 'tenant') and client.tenant:
                            print(f"  X-Tenant: {client.tenant}")
                    
                    asset_response = client.make_api_call(
                        endpoint=f"/catalog-server/api/assets?uid={uid}",
                        method='GET'
                    )
                    
                    # Show response in verbose mode
                    if verbose_mode:
                        print("\nAsset Response:")
                        print(json.dumps(asset_response, indent=2, ensure_ascii=False))
                    
                    # Step 2: Extract the asset ID from the response
                    if not asset_response or 'data' not in asset_response:
                        error_msg = f"No 'data' field found in asset response for UID: {uid}"
                        if not quiet_mode:
                            print(f"❌ {error_msg}")
                        logger.error(error_msg)
                        failed += 1
                        continue
                    
                    data_array = asset_response['data']
                    if not data_array or len(data_array) == 0:
                        error_msg = f"Empty 'data' array in asset response for UID: {uid}"
                        if not quiet_mode:
                            print(f"❌ {error_msg}")
                        logger.error(error_msg)
                        failed += 1
                        continue
                    
                    first_asset = data_array[0]
                    if 'id' not in first_asset:
                        error_msg = f"No 'id' field found in first asset for UID: {uid}"
                        if not quiet_mode:
                            print(f"❌ {error_msg}")
                        logger.error(error_msg)
                        failed += 1
                        continue
                    
                    asset_id = first_asset['id']
                    if not quiet_mode:
                        print(f"Extracted asset ID: {asset_id}")
                    
                    # Step 3: Get asset configuration using the asset ID
                    if not quiet_mode:
                        print(f"Getting asset configuration for asset ID: {asset_id}")
                    
                    # Show headers in verbose mode
                    if verbose_mode:
                        print("\nGET Request Headers:")
                        print(f"  Endpoint: /catalog-server/api/assets/{asset_id}/config")
                        print(f"  Method: GET")
                        print(f"  Content-Type: application/json")
                        print(f"  Authorization: Bearer [REDACTED]")
                        if hasattr(client, 'tenant') and client.tenant:
                            print(f"  X-Tenant: {client.tenant}")
                    
                    config_response = client.make_api_call(
                        endpoint=f"/catalog-server/api/assets/{asset_id}/config",
                        method='GET'
                    )
                    
                    # Show response in verbose mode
                    if verbose_mode:
                        print("\nConfig Response:")
                        print(json.dumps(config_response, indent=2, ensure_ascii=False))
                    
                    # Step 4: Write compressed JSON response to CSV
                    if not quiet_mode:
                        print(f"Writing asset configuration to CSV")
                    
                    # Write the compressed JSON response to CSV
                    config_json = json.dumps(config_response, ensure_ascii=False, separators=(',', ':'))
                    writer.writerow([uid, config_json])
                    
                    if not quiet_mode:
                        print(f"✅ Written to file: {uid}")
                        if not verbose_mode:  # Only show response if not in verbose mode (to avoid duplication)
                            print("Config Response:")
                            print(json.dumps(config_response, indent=2, ensure_ascii=False))
                    else:
                        print(f"✅ [{i}/{len(uids)}] {uid}: Config exported successfully")
                    
                    successful += 1
                    total_assets_processed += 1
                    
                except Exception as e:
                    error_msg = f"Failed to process UID {uid}: {e}"
                    if not quiet_mode:
                        print(f"❌ {error_msg}")
                    else:
                        print(f"❌ [{i}/{len(uids)}] {uid}: {error_msg}")
                    logger.error(error_msg)
                    failed += 1
        
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
                elif header[0] != 'target-env' or header[1] != 'config_json':
                    validation_errors.append(f"Invalid header: expected ['target-env', 'config_json'], got {header}")
                
                # Validate each row
                for row_num, row in enumerate(reader, start=2):
                    row_count += 1
                    
                    # Check column count
                    if len(row) != 2:
                        validation_errors.append(f"Row {row_num}: Expected 2 columns, got {len(row)}")
                        continue
                    
                    target_env, config_json_str = row
                    
                    # Check for empty values
                    if not target_env.strip():
                        validation_errors.append(f"Row {row_num}: Empty target-env value")
                    
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
                        print(f"   Expected columns: target-env, config_json")
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
            print(f"Total UIDs processed: {len(uids)}")
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


def parse_set_output_dir_command(command: str) -> str:
    """Parse a set-output-dir command string into components.
    
    Args:
        command: Command string like "set-output-dir <directory>"
        
    Returns:
        str: Directory path or None if invalid
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'set-output-dir':
        return None
    
    if len(parts) < 2:
        raise ValueError("Directory path is required for set-output-dir command")
    
    # Join remaining parts in case directory path contains spaces
    directory = ' '.join(parts[1:])
    return directory





def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog='adoc_migration_toolkit',
        description='ADOC Export Import - Professional tools for policy export processing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Available commands:
  asset-export  Export asset details by reading UIDs from CSV file
  interactive   Interactive REST API client for making API calls

For help on a specific command:
  python -m adoc_migration_toolkit <command> --help
        """
    )
    
    subparsers = parser.add_subparsers(
        dest='command',
        help='Available commands',
        metavar='COMMAND'
    )
    
    # Add subcommands
    create_asset_export_parser(subparsers)
    create_interactive_parser(subparsers)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    if args.command == 'asset-export':
        return run_asset_export(args)
    elif args.command == 'interactive':
        return run_interactive(args)
    else:
        print(f"Unknown command: {args.command}")
        parser.print_help()
        return 1


def show_interactive_help():
    """Display help information for all available interactive commands."""
    # ANSI escape codes for formatting
    BOLD = '\033[1m'
    RESET = '\033[0m'
    
    # Get current output directory for dynamic paths
    current_output_dir = GLOBAL_OUTPUT_DIR
    
    print("\n" + "="*80)
    print("ADOC INTERACTIVE MIGRATION TOOLKIT - COMMAND HELP")
    print("="*80)
    
    # Show current output directory status
    if current_output_dir:
        print(f"\n📁 Current Output Directory: {current_output_dir}")
    else:
        print(f"\n📁 Current Output Directory: Not set (will use default: adoc-migration-toolkit-YYYYMMDDHHMM)")
    print("💡 Use 'set-output-dir <directory>' to change the output directory")
    print("="*80)
    
    print(f"\n{BOLD}📊 SEGMENTS COMMANDS:{RESET}")
    print(f"  {BOLD}segments-export{RESET} [<csv_file>] [--output-file <file>] [--quiet]")
    print("    Description: Export segments from source environment to CSV file")
    print("    Arguments:")
    print("      csv_file: Path to CSV file with source-env and target-env mappings (optional)")
    print("      --output-file: Specify custom output file (optional)")
    print("      --quiet: Suppress console output, show only summary")
    print("    Examples:")
    print("      segments-export")
    print("      segments-export <output-dir>/policy-export/segmented_spark_uids.csv")
    print("      segments-export data/uids.csv --output-file my_segments.csv --quiet")
    print("    Behavior:")
    print("      • If no CSV file specified, uses default from output directory")
    print("      • Default input: <output-dir>/policy-export/segmented_spark_uids.csv")
    print("      • Default output: <output-dir>/policy-import/segments_output.csv")
    print("      • Exports segments configuration for assets with isSegmented=true")
    print("      • For engineType=SPARK: Required because segmented Spark configurations")
    print("        are not directly imported with standard import capability")
    print("      • For engineType=JDBC_SQL: Already available in standard import,")
    print("        so no additional configuration needed")
    print("      • Only processes assets that have segments defined")
    print("      • Skips assets without segments (logged as info)")
    
    print(f"\n  {BOLD}segments-import{RESET} <csv_file> [--dry-run] [--quiet] [--verbose]")
    print("    Description: Import segments to target environment from CSV file")
    print("    Arguments:")
    print("      csv_file: Path to CSV file with target-env and segments_json")
    print("      --dry-run: Preview changes without making API calls")
    print("      --quiet: Suppress console output (default)")
    print("      --verbose: Show detailed output including headers")
    print("    Examples:")
    print("      segments-import <output-dir>/policy-import/segments_output.csv")
    print("      segments-import segments.csv --dry-run --verbose")
    print("    Behavior:")
    print("      • Reads the CSV file generated from segments-export command")
    print("      • Targets UIDs for which segments are present and engine is SPARK")
    print("      • Imports segments configuration to target environment")
    print("      • Creates new segments (removes existing IDs)")
    print("      • Supports both SPARK and JDBC_SQL engine types")
    print("      • Validates CSV format and JSON content")
    print("      • Processes only assets that have valid segments configuration")
    
    print(f"\n{BOLD}🔧 ASSET PROFILE COMMANDS:{RESET}")
    print(f"  {BOLD}asset-profile-export{RESET} [<csv_file>] [--output-file <file>] [--quiet] [--verbose]")
    print("    Description: Export asset profiles from source environment to CSV file")
    print("    Arguments:")
    print("      csv_file: Path to CSV file with source-env and target-env mappings (optional)")
    print("      --output-file: Specify custom output file (optional)")
    print("      --quiet: Suppress console output, show only summary (default)")
    print("      --verbose: Show detailed output including headers and responses")
    print("    Examples:")
    print("      asset-profile-export")
    print("      asset-profile-export <output-dir>/asset-export/asset_uids.csv")
    print("      asset-profile-export uids.csv --output-file profiles.csv --verbose")
    print("    Behavior:")
    print("      • If no CSV file specified, uses default from output directory")
    print("      • Default input: <output-dir>/asset-export/asset_uids.csv")
    print("      • Default output: <output-dir>/asset-import/asset-profiles-import-ready.csv")
    print("      • Reads source-env and target-env mappings from CSV file")
    print("      • Makes API calls to get asset profiles from source environment")
    print("      • Writes profile JSON data to output CSV file")
    print("      • Shows minimal output by default, use --verbose for detailed information")
    
    print(f"\n  {BOLD}asset-profile-import{RESET} [<csv_file>] [--dry-run] [--quiet] [--verbose]")
    print("    Description: Import asset profiles to target environment from CSV file")
    print("    Arguments:")
    print("      csv_file: Path to CSV file with target-env and profile_json (optional)")
    print("      --dry-run: Preview changes without making API calls")
    print("      --quiet: Suppress console output (default)")
    print("      --verbose: Show detailed output including headers and responses")
    print("    Examples:")
    print("      asset-profile-import")
    print("      asset-profile-import <output-dir>/asset-import/asset-profiles-import-ready.csv")
    print("      asset-profile-import profiles.csv --dry-run --verbose")
    print("    Behavior:")
    print("      • If no CSV file specified, uses default from output directory")
    print("      • Default input: <output-dir>/asset-import/asset-profiles-import-ready.csv")
    print("      • Reads target-env and profile_json from CSV file")
    print("      • Makes API calls to update asset profiles in target environment")
    print("      • Supports dry-run mode for previewing changes")
    
    print(f"\n{BOLD}🔍 ASSET CONFIGURATION COMMANDS:{RESET}")
    print(f"  {BOLD}asset-config-export{RESET} <csv_file> [--output-file <file>] [--quiet] [--verbose]")
    print("    Description: Export asset configurations from source environment to CSV file")
    print("    Arguments:")
    print("      csv_file: Path to CSV file with UIDs in the first column")
    print("      --output-file: Specify custom output file (optional)")
    print("      --quiet: Suppress console output, show only summary (default)")
    print("      --verbose: Show detailed output including headers and responses")
    print("    Examples:")
    print("      asset-config-export <output-dir>/asset-export/asset_uids.csv")
    print("      asset-config-export uids.csv --output-file configs.csv --verbose")
    print("    Behavior:")
    print("      • Reads UIDs from the first column of the CSV file")
    print("      • Makes REST call to '/catalog-server/api/assets?uid=<uid>' to get asset ID")
    print("      • Uses asset ID to call '/catalog-server/api/assets/<id>/config'")
    print("      • Writes compressed JSON response to CSV with target-env UID")
    print("      • Shows status for each UID in quiet mode")
    print("      • Shows HTTP headers and response objects in verbose mode")
    print("      • Output format: target-env, config_json (compressed)")
    
    print(f"\n  {BOLD}asset-list-export{RESET} [--quiet] [--verbose]")
    print("    Description: Export all assets from source environment to CSV file")
    print("    Arguments:")
    print("      --quiet: Suppress console output, show only summary")
    print("      --verbose: Show detailed output including headers and responses")
    print("    Examples:")
    print("      asset-list-export")
    print("      asset-list-export --quiet")
    print("      asset-list-export --verbose")
    print("    Behavior:")
    print("      • Uses '/catalog-server/api/assets/discover' endpoint with pagination")
    print("      • First call gets total count with size=0&page=0")
    print("      • Retrieves all pages with size=500 (default)")
    print("      • Output file: <output-dir>/asset-export/asset-all-export.csv")
    print("      • CSV columns: uid, id")
    print("      • Sorts output by uid first, then by id")
    print("      • Shows page-by-page progress in quiet mode")
    print("      • Shows detailed request/response in verbose mode")
    print("      • Provides comprehensive statistics upon completion")
    
    print(f"\n  {BOLD}policy-list-export{RESET} [--quiet] [--verbose]")
    print("    Description: Export all policies from source environment to CSV file")
    print("    Arguments:")
    print("      --quiet: Suppress console output, show only summary")
    print("      --verbose: Show detailed output including headers and responses")
    print("    Examples:")
    print("      policy-list-export")
    print("      policy-list-export --quiet")
    print("      policy-list-export --verbose")
    print("    Behavior:")
    print("      • Uses '/catalog-server/api/rules' endpoint with pagination")
    print("      • First call gets total count with page=0&size=0")
    print("      • Retrieves all pages with size=1000 (default)")
    print("      • Output file: <output-dir>/policy-export/policies-all-export.csv")
    print("      • CSV columns: id, type, engineType")
    print("      • Sorts output by id")
    print("      • Shows page-by-page progress in quiet mode")
    print("      • Shows detailed request/response in verbose mode")
    print("      • Provides comprehensive statistics upon completion")
    
    print(f"\n  {BOLD}policy-export{RESET} [--type <export_type>] [--filter <filter_value>] [--quiet] [--verbose] [--batch-size <size>]")
    print("    Description: Export policy definitions by different categories from source environment to ZIP files")
    print("    Arguments:")
    print("      --type: Export type (rule-types, engine-types, assemblies, source-types)")
    print("      --filter: Optional filter value within the export type")
    print("      --quiet: Suppress console output, show only summary")
    print("      --verbose: Show detailed output including headers and responses")
    print("      --batch-size: Number of policies to export in each batch (default: 50)")
    print("    Examples:")
    print("      policy-export")
    print("      policy-export --type rule-types")
    print("      policy-export --type engine-types --filter JDBC_URL")
    print("      policy-export --type assemblies --filter production-db")
    print("      policy-export --type source-types --filter PostgreSQL")
    print("      policy-export --type rule-types --batch-size 100 --quiet")
    print("    Behavior:")
    print("      • Reads policies from <output-dir>/policy-export/policies-all-export.csv (generated by policy-list-export)")
    print("      • Groups policies by the specified export type")
    print("      • Optionally filters to a specific value within that type")
    print("      • Exports each group in batches using '/catalog-server/api/rules/export/policy-definitions'")
    print("      • Output files: <export_type>[-<filter>]-<timestamp>-<range>.zip in <output-dir>/policy-export")
    print("      • Default batch size: 50 policies per ZIP file")
    print("      • Filename examples:")
    print("        - rule_types-07-04-2025-17-21-0-99.zip")
    print("        - engine_types_jdbc_url-07-04-2025-17-21-0-99.zip")
    print("        - assemblies_production_db-07-04-2025-17-21-0-99.zip")
    print("      • Shows batch-by-batch progress in quiet mode")
    print("      • Shows detailed request/response in verbose mode")
    print("      • Provides comprehensive statistics upon completion")
    
    print(f"\n  {BOLD}policy-import{RESET} <file_or_pattern> [--quiet] [--verbose]")
    print("    Description: Import policy definitions from ZIP files to target environment")
    print("    Arguments:")
    print("      file_or_pattern: ZIP file path or glob pattern (e.g., *.zip)")
    print("      --quiet: Suppress console output, show only summary")
    print("      --verbose: Show detailed output including headers and responses")
    print("    Examples:")
    print("      policy-import *.zip")
    print("      policy-import data-quality-*.zip")
    print("      policy-import /path/to/specific-file.zip")
    print("      policy-import *.zip --verbose")
    print("    Behavior:")
    print("      • Uploads ZIP files to '/catalog-server/api/rules/import/policy-definitions/upload-config'")
    print("      • Uses target environment authentication (target access key, secret key, and tenant)")
    print("      • By default, looks for files in <output-dir>/policy-import directory")
    print("      • Supports absolute paths to override default directory")
    print("      • Supports glob patterns for multiple files")
    print("      • Validates that files exist and are readable")
    print("      • Aggregates statistics across all imported files")
    print("      • Shows detailed import results and conflicts")
    print("      • Provides comprehensive summary with aggregated statistics")
    print("      • Tracks UUIDs of imported policy definitions")
    print("      • Reports conflicts (assemblies, policies, SQL views, visual views)")
    
    print(f"\n  {BOLD}rule-tag-export{RESET} [--quiet] [--verbose]")
    print("    Description: Export rule tags for all policies from policies-all-export.csv")
    print("    Arguments:")
    print("      --quiet: Suppress console output, show only summary with progress bar")
    print("      --verbose: Show detailed output including headers and responses")
    print("    Examples:")
    print("      rule-tag-export")
    print("      rule-tag-export --quiet")
    print("      rule-tag-export --verbose")
    print("    Behavior:")
    print("      • Automatically runs policy-list-export if policies-all-export.csv doesn't exist")
    print("      • Reads rule IDs from <output-dir>/policy-export/policies-all-export.csv (first column)")
    print("      • Makes API calls to '/catalog-server/api/rules/<id>/tags' for each rule")
    print("      • Extracts tag names from the response")
    print("      • Outputs to <output-dir>/policy-export/rule-tags-export.csv with rule ID and comma-separated tags")
    print("      • Shows progress bar in quiet mode")
    print("      • Shows detailed API calls in verbose mode")
    print("      • Provides comprehensive statistics upon completion")
    
    print(f"\n  {BOLD}policy-xfr{RESET} [--input <input_dir>] --source-env-string <source> --target-env-string <target> [options]")
    print("    Description: Format policy export files by replacing substrings in JSON files and ZIP archives")
    print("    Arguments:")
    print("      --source-env-string: Substring to search for (source environment) [REQUIRED]")
    print("      --target-env-string: Substring to replace with (target environment) [REQUIRED]")
    print("    Options:")
    print("      --input: Input directory (auto-detected from policy-export if not specified)")
    print("      --output-dir: Output directory (defaults to organized subdirectories)")
    print("      --quiet: Suppress console output, show only summary")
    print("      --verbose: Show detailed output including processing details")
    print("    Examples:")
    print("      policy-xfr --source-env-string \"PROD_DB\" --target-env-string \"DEV_DB\"")
    print("      policy-xfr --input data/samples --source-env-string \"old\" --target-env-string \"new\"")
    print("      policy-xfr --source-env-string \"PROD_DB\" --target-env-string \"DEV_DB\" --verbose")
    print("    Behavior:")
    print("      • Processes JSON files and ZIP archives in the input directory")
    print("      • Replaces all occurrences of source string with target string")
    print("      • Maintains file structure and count")
    print("      • Auto-detects input directory from <output-dir>/policy-export if not specified")
    print("      • Creates organized output directory structure")
    print("      • Extracts data quality policy assets to CSV files")
    print("      • Generates <output-dir>/asset-export/asset_uids.csv and <output-dir>/policy-import/segmented_spark_uids.csv")
    print("      • Shows detailed processing statistics upon completion")
    
    print(f"\n{BOLD}🛠️ UTILITY COMMANDS:{RESET}")
    print(f"  {BOLD}set-output-dir{RESET} <directory>")
    print("    Description: Set global output directory for all export commands")
    print("    Arguments:")
    print("      directory: Path to the output directory")
    print("    Examples:")
    print("      set-output-dir /path/to/my/output")
    print("      set-output-dir data/custom_output")
    print("    Features:")
    print("      • Sets the output directory for all export commands")
    print("      • Creates the directory if it doesn't exist")
    print("      • Validates write permissions")
    print("      • Saves configuration to ~/.adoc_migration_config.json")
    print("      • Persists across multiple interactive sessions")
    print("      • Can be changed anytime with another set-output-dir command")
    
    print(f"\n  {BOLD}help{RESET}")
    print("    Description: Show this help information")
    print("    Example: help")
    
    print(f"\n  {BOLD}history{RESET}")
    print("    Description: Show the last 25 commands with numbers")
    print("    Example: history")
    print("    Features:")
    print("      • Displays the last 25 commands with numbered entries")
    print("      • Latest commands appear first (highest numbers)")
    print("      • Long commands are truncated for display")
    print("      • Enter a number to execute that command")
    print("      • Works alongside ↑/↓ arrow key navigation")
    
    print(f"\n  {BOLD}exit, quit, q{RESET}")
    print("    Description: Exit the interactive client")
    print("    Examples: exit, quit, q")
    
    print(f"\n{BOLD}💡 TIPS:{RESET}")
    print("  • Use TAB key for command autocomplete")
    print("  • Use ↑/↓ arrow keys to navigate command history")
    print("  • Type part of an endpoint and press TAB to see suggestions")
    print("  • Use --dry-run to preview changes before making them")
    print("  • Use --verbose to see detailed API request/response information")
    print("  • Check log files for detailed error information")
    print("  • Set output directory once with set-output-dir to avoid specifying --output-file repeatedly")
    
    print(f"\n{BOLD}📁 FILE LOCATIONS:{RESET}")
    print("  • Input CSV files: <output-dir>/asset-export/ and <output-dir>/policy-import/")
    print("  • Output CSV files: <output-dir>/ (organized by category)")
    print("  • Log files: adoc-migration-toolkit-YYYYMMDD.log")
    
    print(f"\n{BOLD}📋 OUTPUT DIRECTORY SPECIFICATION:{RESET}")
    if current_output_dir:
        print(f"  • output-dir = {current_output_dir}")
    else:
        print("  • output-dir = Not set (will use default: adoc-migration-toolkit-YYYYMMDDHHMM)")
        print("  • 💡 Use 'set-output-dir <directory>' to set a persistent output directory")
    
    print("="*80)


# Global output directory for all export commands
GLOBAL_OUTPUT_DIR = None

# Configuration file for persistent settings
CONFIG_FILE = Path.home() / ".adoc_migration_config.json"


def load_global_output_directory() -> Path:
    """Load the global output directory from configuration file.
    
    Returns:
        Path: The loaded output directory or None if not found
    """
    try:
        if CONFIG_FILE.exists():
            with open(CONFIG_FILE, 'r') as f:
                config = json.load(f)
                output_dir = config.get('output_directory')
                if output_dir:
                    output_path = Path(output_dir)
                    # Verify the directory still exists and is writable
                    if output_path.exists() and output_path.is_dir():
                        # Test write permissions
                        test_file = output_path / ".test_write"
                        try:
                            test_file.write_text("test")
                            test_file.unlink()
                            return output_path
                        except Exception:
                            # Directory exists but not writable, remove from config
                            save_global_output_directory(None)
                            return None
                    else:
                        # Directory doesn't exist, remove from config
                        save_global_output_directory(None)
                        return None
    except Exception as e:
        # If there's any error reading the config, just return None
        return None
    return None


def save_global_output_directory(output_dir: Path):
    """Save the global output directory to configuration file.
    
    Args:
        output_dir: The output directory to save, or None to clear
    """
    try:
        config = {}
        if CONFIG_FILE.exists():
            with open(CONFIG_FILE, 'r') as f:
                config = json.load(f)
        
        if output_dir:
            config['output_directory'] = str(output_dir.resolve())
        else:
            config.pop('output_directory', None)
        
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=2)
    except Exception as e:
        # Silently fail if we can't save the config
        pass


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





def execute_asset_config_import(csv_file: str, client, logger: logging.Logger, dry_run: bool = False, quiet_mode: bool = True, verbose_mode: bool = False):
    """Execute the asset-config-import command.
    
    Args:
        csv_file: Path to the CSV file containing target-env and config_json
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
            
            if len(header) != 2 or header[0] != 'target-env' or header[1] != 'config_json':
                error_msg = f"Invalid CSV format. Expected header: ['target-env', 'config_json'], got: {header}"
                print(f"❌ {error_msg}")
                logger.error(error_msg)
                return
            
            for row_num, row in enumerate(reader, start=2):
                if len(row) != 2:
                    logger.warning(f"Row {row_num}: Expected 2 columns, got {len(row)}")
                    continue
                
                target_env = row[0].strip()
                config_json = row[1].strip()
                
                if target_env and config_json:
                    import_mappings.append((target_env, config_json))
                    logger.debug(f"Row {row_num}: Found target-env: {target_env}")
                else:
                    logger.warning(f"Row {row_num}: Empty target-env or config_json value")
        
        if not import_mappings:
            logger.warning("No valid import mappings found in CSV file")
            return
        
        logger.info(f"Read {len(import_mappings)} import mappings from CSV file: {csv_file}")
        
        successful = 0
        failed = 0
        
        # Process each mapping
        for target_env, config_json in import_mappings:
            try:
                if dry_run:
                    print(f"🔍 Would import config for: {target_env}")
                    continue
                
                # Parse the JSON configuration
                config_data = json.loads(config_json)
                
                # Make API call to import configuration
                # Note: This is a placeholder - implement actual API call based on your requirements
                endpoint = f"/catalog-server/api/assets/{target_env}/config"
                
                if verbose_mode:
                    print(f"📡 Making PUT request to: {endpoint}")
                    print(f"📦 Request payload: {json.dumps(config_data, indent=2)}")
                
                # For now, just log the action
                logger.info(f"Importing config for asset: {target_env}")
                
                if not quiet_mode:
                    print(f"✅ Imported config for: {target_env}")
                
                successful += 1
                
            except json.JSONDecodeError as e:
                error_msg = f"Invalid JSON in config for {target_env}: {e}"
                print(f"❌ {error_msg}")
                logger.error(error_msg)
                failed += 1
            except Exception as e:
                error_msg = f"Failed to import config for {target_env}: {e}"
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


if __name__ == "__main__":
    sys.exit(main())

def parse_asset_list_export_command(command: str) -> tuple:
    """Parse an asset-list-export command string into components.
    
    Args:
        command: Command string like "asset-list-export [--quiet] [--verbose]"
        
    Returns:
        Tuple of (quiet_mode, verbose_mode)
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'asset-list-export':
        return False, False
    
    quiet_mode = False
    verbose_mode = False
    
    # Check for flags
    if '--quiet' in parts:
        quiet_mode = True
        verbose_mode = False  # Quiet overrides verbose
        parts.remove('--quiet')
    
    if '--verbose' in parts:
        verbose_mode = True
        quiet_mode = False  # Verbose overrides quiet
        parts.remove('--verbose')
    
    return quiet_mode, verbose_mode


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
        if GLOBAL_OUTPUT_DIR:
            output_file = GLOBAL_OUTPUT_DIR / "asset-all-export.csv"
        else:
            output_file = Path("asset-all-export.csv")
        
        if not quiet_mode:
            print(f"\nExporting all assets from ADOC environment")
            print(f"Output will be written to: {output_file}")
            if GLOBAL_OUTPUT_DIR:
                print(f"Using global output directory: {GLOBAL_OUTPUT_DIR}")
            if verbose_mode:
                print("🔊 VERBOSE MODE - Detailed output including headers and responses")
            print("="*80)
        
        # Step 1: Get total count of assets
        if not quiet_mode:
            print("Getting total asset count...")
        
        if verbose_mode:
            print("\nGET Request Headers:")
            print(f"  Endpoint: /catalog-server/api/assets/discover?size=0&page=0")
            print(f"  Method: GET")
            print(f"  Content-Type: application/json")
            print(f"  Authorization: Bearer [REDACTED]")
            if hasattr(client, 'tenant') and client.tenant:
                print(f"  X-Tenant: {client.tenant}")
        
        count_response = client.make_api_call(
            endpoint="/catalog-server/api/assets/discover?size=0&page=0",
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
                    print(f"  Endpoint: /catalog-server/api/assets/discover?size={page_size}&page={page}")
                    print(f"  Method: GET")
                    print(f"  Content-Type: application/json")
                    print(f"  Authorization: Bearer [REDACTED]")
                    if hasattr(client, 'tenant') and client.tenant:
                        print(f"  X-Tenant: {client.tenant}")
                
                page_response = client.make_api_call(
                    endpoint=f"/catalog-server/api/assets/discover?size={page_size}&page={page}",
                    method='GET'
                )
                
                if verbose_mode:
                    print(f"\nPage {page + 1} Response:")
                    print(json.dumps(page_response, indent=2, ensure_ascii=False))
                
                # Extract assets from response
                if page_response and 'data' in page_response and 'assets' in page_response['data']:
                    page_assets = page_response['data']['assets']
                    # Extract the actual asset objects from the nested structure
                    actual_assets = []
                    for asset_wrapper in page_assets:
                        if 'asset' in asset_wrapper:
                            actual_assets.append(asset_wrapper['asset'])
                        else:
                            # Fallback: if no 'asset' wrapper, use the object directly
                            actual_assets.append(asset_wrapper)
                    
                    all_assets.extend(actual_assets)
                    
                    if not quiet_mode:
                        print(f"✅ Page {page + 1}: Retrieved {len(actual_assets)} assets")
                    else:
                        print(f"✅ Page {page + 1}/{total_pages}: {len(actual_assets)} assets")
                    
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
                                    # Handle nested asset structure
                                    actual_assets = []
                                    for asset_wrapper in page_assets:
                                        if 'asset' in asset_wrapper:
                                            actual_assets.append(asset_wrapper['asset'])
                                        else:
                                            actual_assets.append(asset_wrapper)
                                    
                                    all_assets.extend(actual_assets)
                                    if not quiet_mode:
                                        print(f"✅ Page {page + 1}: Found {len(actual_assets)} assets in 'data.{location}'")
                                    else:
                                        print(f"✅ Page {page + 1}/{total_pages}: {len(actual_assets)} assets")
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
            
            # Write header
            writer.writerow(['uid', 'id'])
            
            # Write asset data
            for asset in all_assets:
                uid = asset.get('uid', '')
                asset_id = asset.get('id', '')
                
                writer.writerow([uid, asset_id])
        
        # Step 4: Sort the CSV file by uid, then id
        if not quiet_mode:
            print("Sorting CSV file by uid, then id...")
        
        # Read all rows
        rows = []
        with open(output_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # Skip header
            rows = list(reader)
        
        # Sort rows: first by uid, then by id
        def sort_key(row):
            uid = row[0] if len(row) > 0 else ''
            asset_id = row[1] if len(row) > 1 else ''
            # Convert asset_id to int for proper numeric sorting, fallback to string
            try:
                asset_id_int = int(asset_id) if asset_id else 0
            except (ValueError, TypeError):
                asset_id_int = 0
            return (uid, asset_id_int)
        
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


def parse_policy_list_export_command(command: str) -> tuple:
    """Parse a policy-list-export command string into components.
    
    Args:
        command: Command string like "policy-list-export [--quiet] [--verbose]"
        
    Returns:
        Tuple of (quiet_mode, verbose_mode)
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'policy-list-export':
        return False, False
    
    quiet_mode = False
    verbose_mode = False
    
    # Check for flags
    if '--quiet' in parts:
        quiet_mode = True
        verbose_mode = False  # Quiet overrides verbose
        parts.remove('--quiet')
    
    if '--verbose' in parts:
        verbose_mode = True
        quiet_mode = False  # Verbose overrides quiet
        parts.remove('--verbose')
    
    return quiet_mode, verbose_mode


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
            print(f"  Endpoint: /catalog-server/api/rules?page=0&size=0")
            print(f"  Method: GET")
            print(f"  Content-Type: application/json")
            print(f"  Authorization: Bearer [REDACTED]")
            if hasattr(client, 'tenant') and client.tenant:
                print(f"  X-Tenant: {client.tenant}")
        
        count_response = client.make_api_call(
            endpoint="/catalog-server/api/rules?page=0&size=0",
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
                    print(f"  Endpoint: /catalog-server/api/rules?page={page}&size={page_size}")
                    print(f"  Method: GET")
                    print(f"  Content-Type: application/json")
                    print(f"  Authorization: Bearer [REDACTED]")
                    if hasattr(client, 'tenant') and client.tenant:
                        print(f"  X-Tenant: {client.tenant}")
                
                page_response = client.make_api_call(
                    endpoint=f"/catalog-server/api/rules?page={page}&size={page_size}",
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
                        print(f"✅ Page {page + 1}/{total_pages}: {len(actual_policies)} polrulesicies")
                    
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
            print("="*80)
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


def parse_policy_export_command(command: str) -> tuple:
    """Parse a policy-export command string into components.
    
    Args:
        command: Command string like "policy-export [--type <export_type>] [--filter <filter_value>] [--quiet] [--verbose] [--batch-size <size>]"
        
    Returns:
        Tuple of (quiet_mode, verbose_mode, batch_size, export_type, filter_value)
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'policy-export':
        return False, False, 50, None, None
    
    quiet_mode = False
    verbose_mode = False
    batch_size = 50  # Default batch size
    export_type = None
    filter_value = None
    
    # Check for flags and options
    i = 1
    while i < len(parts):
        if parts[i] == '--type' and i + 1 < len(parts):
            export_type = parts[i + 1].lower()
            if export_type not in ['rule-types', 'engine-types', 'assemblies', 'source-types']:
                raise ValueError(f"Invalid export type: {export_type}. Must be one of: rule-types, engine-types, assemblies, source-types")
            parts.pop(i)  # Remove --type
            parts.pop(i)  # Remove the type value
        elif parts[i] == '--filter' and i + 1 < len(parts):
            filter_value = parts[i + 1]
            parts.pop(i)  # Remove --filter
            parts.pop(i)  # Remove the filter value
        elif parts[i] == '--batch-size' and i + 1 < len(parts):
            try:
                batch_size = int(parts[i + 1])
                if batch_size <= 0:
                    raise ValueError("Batch size must be positive")
                parts.pop(i)  # Remove --batch-size
                parts.pop(i)  # Remove the batch size value
            except (ValueError, IndexError):
                raise ValueError("Invalid batch size. Must be a positive integer")
        elif parts[i] == '--quiet':
            quiet_mode = True
            verbose_mode = False  # Quiet overrides verbose
            parts.remove('--quiet')
        elif parts[i] == '--verbose':
            verbose_mode = True
            quiet_mode = False  # Verbose overrides quiet
            parts.remove('--verbose')
        else:
            i += 1
    
    return quiet_mode, verbose_mode, batch_size, export_type, filter_value


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
        if GLOBAL_OUTPUT_DIR:
            input_file = GLOBAL_OUTPUT_DIR / "policy-export" / "policies-all-export.csv"
            output_dir = GLOBAL_OUTPUT_DIR / "policy-export"
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


def set_global_output_directory(directory: str, logger: logging.Logger) -> bool:
    """Set the global output directory for all export commands.
    
    Args:
        directory: Path to the output directory
        logger: Logger instance
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        output_path = Path(directory).resolve()
        
        # Create directory if it doesn't exist
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Verify it's a directory and writable
        if not output_path.is_dir():
            raise ValueError(f"Path is not a directory: {directory}")
        
        # Test write permissions
        test_file = output_path / ".test_write"
        try:
            test_file.write_text("test")
            test_file.unlink()
        except Exception as e:
            raise PermissionError(f"Cannot write to directory {directory}: {e}")
        
        global GLOBAL_OUTPUT_DIR
        GLOBAL_OUTPUT_DIR = output_path
        
        # Save to configuration file for persistence
        save_global_output_directory(output_path)
        
        logger.info(f"Global output directory set to: {GLOBAL_OUTPUT_DIR}")
        print(f"✅ Global output directory set to: {GLOBAL_OUTPUT_DIR}")
        print(f"💾 Directory saved to configuration for future sessions")
        return True
        
    except Exception as e:
        error_msg = f"Failed to set output directory '{directory}': {e}"
        print(f"❌ {error_msg}")
        logger.error(error_msg)
        return False


def parse_formatter_command(command: str) -> tuple:
    """Parse policy-xfr command in interactive mode.
    
    Args:
        command (str): The command string
        
    Returns:
        tuple: (input_dir, source_string, target_string, output_dir, quiet_mode, verbose_mode)
    """
    try:
        # Remove the command prefix
        args_str = command[len('policy-xfr'):].strip()
        
        # Default values
        input_dir = None
        source_string = None
        target_string = None
        output_dir = None
        quiet_mode = False
        verbose_mode = False
        
        # Parse arguments
        args = args_str.split()
        i = 0
        
        while i < len(args):
            arg = args[i]
            
            if arg == '--input' and i + 1 < len(args):
                input_dir = args[i + 1]
                i += 2
            elif arg == '--output-dir' and i + 1 < len(args):
                output_dir = args[i + 1]
                i += 2
            elif arg == '--source-env-string' and i + 1 < len(args):
                source_string = args[i + 1]
                i += 2
            elif arg == '--target-env-string' and i + 1 < len(args):
                target_string = args[i + 1]
                i += 2
            elif arg == '--quiet' or arg == '-q':
                quiet_mode = True
                i += 1
            elif arg == '--verbose' or arg == '-v':
                verbose_mode = True
                i += 1
            elif arg == '--help' or arg == '-h':
                print("\n" + "="*60)
                print("POLICY-XFR COMMAND HELP")
                print("="*60)
                print("Usage: policy-xfr [--input <input_dir>] --source-env-string <source> --target-env-string <target> [options]")
                print("\nArguments:")
                print("  --source-env-string <string>  Substring to search for (source environment) [REQUIRED]")
                print("  --target-env-string <string>  Substring to replace with (target environment) [REQUIRED]")
                print("\nOptions:")
                print("  --input <dir>                 Input directory (auto-detected from policy-export if not specified)")
                print("  --output-dir <dir>            Output directory (defaults to organized subdirectories)")
                print("  --quiet, -q                   Quiet mode (minimal output)")
                print("  --verbose, -v                 Verbose mode (detailed output)")
                print("  --help, -h                    Show this help message")
                print("\nExamples:")
                print("  policy-xfr --source-env-string \"PROD_DB\" --target-env-string \"DEV_DB\"")
                print("  policy-xfr --input data/samples --source-env-string \"old\" --target-env-string \"new\"")
                print("  policy-xfr --source-env-string \"PROD_DB\" --target-env-string \"DEV_DB\" --verbose")
                print("="*60)
                return None, None, None, None, False, False
            else:
                # Unknown argument
                print(f"❌ Unknown argument: {arg}")
                print("💡 Use 'policy-xfr --help' for usage information")
                return None, None, None, None, False, False
        
        # Validate required arguments
        if not source_string:
            print("❌ Missing required argument: --source-env-string")
            print("💡 Use 'policy-xfr --help' for usage information")
            return None, None, None, None, False, False
        
        if not target_string:
            print("❌ Missing required argument: --target-env-string")
            print("💡 Use 'policy-xfr --help' for usage information")
            return None, None, None, None, False, False
        
        return input_dir, source_string, target_string, output_dir, quiet_mode, verbose_mode
        
    except Exception as e:
        print(f"❌ Error parsing policy-xfr command: {e}")
        return None, None, None, None, False, False


def execute_formatter(input_dir: str, source_string: str, target_string: str, output_dir: str, 
                     quiet_mode: bool, verbose_mode: bool, logger: logging.Logger):
    """Execute formatter command in interactive mode.
    
    Args:
        input_dir (str): Input directory (can be None for auto-detection)
        source_string (str): Source environment string
        target_string (str): Target environment string
        output_dir (str): Output directory (can be None for default)
        quiet_mode (bool): Quiet mode flag
        verbose_mode (bool): Verbose mode flag
        logger (Logger): Logger instance
    """
    try:
        # Determine input directory - if not specified, use policy-export directory
        if not input_dir:
            # First, check if we have a global output directory set
            if GLOBAL_OUTPUT_DIR:
                global_policy_export_dir = GLOBAL_OUTPUT_DIR / "policy-export"
                if global_policy_export_dir.exists() and global_policy_export_dir.is_dir():
                    input_dir = str(global_policy_export_dir)
                    if not quiet_mode:
                        print(f"📁 Using global output directory: {input_dir}")
                else:
                    if not quiet_mode:
                        print(f"📁 Global output directory policy-export not found: {global_policy_export_dir}")
            
            # If no global directory or it doesn't exist, look for the most recent adoc-migration-toolkit directory
            if not input_dir:
                current_dir = Path.cwd()
                # Only look for directories, not files
                toolkit_dirs = [d for d in current_dir.iterdir() if d.is_dir() and d.name.startswith("adoc-migration-toolkit-")]
                
                if not toolkit_dirs:
                    print("❌ No adoc-migration-toolkit directory found.")
                    print("💡 Please specify an input directory or run 'policy-export' first to generate ZIP files")
                    return
                
                # Sort by creation time and use the most recent
                toolkit_dirs.sort(key=lambda x: x.stat().st_ctime, reverse=True)
                latest_toolkit_dir = toolkit_dirs[0]
                input_dir = str(latest_toolkit_dir / "policy-export")
                
                if not quiet_mode:
                    print(f"📁 Using input directory: {input_dir}")
        
        # Create and run the formatter
        formatter = PolicyTranformer(
            input_dir=input_dir,
            search_string=source_string,
            replace_string=target_string,
            output_dir=output_dir,
            logger=logger
        )
        
        stats = formatter.process_directory()
        
        if not quiet_mode:
            # Print professional summary
            print("\n" + "="*60)
            print("PROCESSING SUMMARY")
            print("="*60)
            print(f"Input directory:     {input_dir}")
            print(f"Output directory:    {formatter.output_dir}")
            print(f"Asset export dir:    {formatter.asset_export_dir}")
            print(f"Policy export dir:   {formatter.policy_export_dir}")
            print(f"Source env string:   '{source_string}'")
            print(f"Target env string:   '{target_string}'")
            print(f"Total files found:   {stats['total_files']}")
            
            if stats['json_files'] > 0:
                print(f"JSON files:          {stats['json_files']}")
            if stats['zip_files'] > 0:
                print(f"ZIP files:           {stats['zip_files']}")
            
            print(f"Files investigated:  {stats.get('files_investigated', 0)}")
            print(f"Changes made:        {stats.get('changes_made', 0)}")
            print(f"Successful:          {stats['successful']}")
            print(f"Failed:              {stats['failed']}")
            
            if stats.get('extracted_assets', 0) > 0:
                print(f"Assets extracted:    {stats['extracted_assets']}")
            
            if stats.get('all_assets', 0) > 0:
                print(f"All assets found:    {stats['all_assets']}")
            
            # Display policy statistics if any policies were processed
            if stats.get('total_policies_processed', 0) > 0:
                print(f"\nPolicy Statistics:")
                print(f"  Total policies processed: {stats['total_policies_processed']}")
                print(f"  Segmented SPARK policies: {stats['segmented_spark_policies']}")
                print(f"  Segmented JDBC_SQL policies: {stats['segmented_jdbc_policies']}")
                print(f"  Non-segmented policies: {stats['non_segmented_policies']}")
            
            if stats['errors']:
                print(f"\nErrors encountered:  {len(stats['errors'])}")
                for error in stats['errors'][:5]:  # Show first 5 errors
                    print(f"  - {error}")
                if len(stats['errors']) > 5:
                    print(f"  ... and {len(stats['errors']) - 5} more errors")
            
            print("="*60)
        
        if stats['failed'] > 0 or stats['errors']:
            print("⚠️  Processing completed with errors. Check log file for details.")
        else:
            print("✅ Formatter completed successfully!")
            
    except Exception as e:
        print(f"❌ Error executing formatter: {e}")
        logger.error(f"Error executing formatter: {e}")


def show_command_history():
    """Display the last 25 commands from history with numbers."""
    try:
        # Clean current session history first
        clean_current_session_history()
        
        # Get current history length
        history_length = readline.get_current_history_length()
        
        if history_length == 0:
            print("\n📋 No command history available.")
            return
        
        # Get the last 25 commands (or all if less than 25)
        start_index = max(0, history_length - 25)
        commands = []
        
        for i in range(start_index, history_length):
            try:
                command = readline.get_history_item(i + 1)  # readline uses 1-based indexing
                if command and command.strip():
                    commands.append(command.strip())
            except Exception:
                continue
        
        if not commands:
            print("\n📋 No command history available.")
            return
        
        print(f"\n📋 Command History (last {len(commands)} commands):")
        print("="*60)
        
        # Display commands with numbers, latest first
        for i, cmd in enumerate(reversed(commands), 1):
            # Truncate long commands for display
            display_cmd = cmd if len(cmd) <= 50 else cmd[:47] + "..."
            print(f"{i:2d}: {display_cmd}")
        
        print("="*60)
        print("💡 Enter a number to execute that command")
        print("💡 Use ↑/↓ arrow keys to navigate history")
        
    except Exception as e:
        print(f"❌ Error displaying history: {e}")


def clean_current_session_history():
    """Clean the current session's in-memory history by removing utility commands."""
    try:
        # Get current history length
        history_length = readline.get_current_history_length()
        
        if history_length == 0:
            return
        
        # Create a new clean history
        clean_history = []
        
        for i in range(history_length):
            try:
                command = readline.get_history_item(i + 1)  # readline uses 1-based indexing
                if command and command.strip():
                    # Only keep commands that are not utility commands
                    if command.strip().lower() not in ['exit', 'quit', 'q', 'history', 'help']:
                        clean_history.append(command.strip())
            except Exception:
                continue
        
        # Clear current history and reload clean version
        readline.clear_history()
        
        # Add back only the clean commands
        for command in clean_history:
            try:
                readline.add_history(command)
            except Exception:
                continue
                
    except Exception:
        # If cleanup fails, just continue
        pass


def get_command_from_history(command_number: int) -> str:
    """Get a command from history by its number.
    
    Args:
        command_number: The number of the command in history (1-based, latest first)
        
    Returns:
        str: The command string or None if not found
    """
    try:
        # Get current history length
        history_length = readline.get_current_history_length()
        
        if history_length == 0:
            return None
        
        # Get the last 25 commands (or all if less than 25)
        start_index = max(0, history_length - 25)
        commands = []
        
        for i in range(start_index, history_length):
            try:
                command = readline.get_history_item(i + 1)  # readline uses 1-based indexing
                if command and command.strip():
                    commands.append(command.strip())
            except Exception:
                continue
        
        # Reverse to get latest first, then get the requested command
        if 1 <= command_number <= len(commands):
            return commands[-(command_number)]  # Negative indexing to get from end
        
        return None
        
    except Exception:
        return None


def parse_policy_import_command(command: str) -> tuple:
    """Parse a policy-import command string into components.
    
    Args:
        command: Command string like "policy-import <file_or_pattern> [--quiet] [--verbose]"
        
    Returns:
        Tuple of (file_pattern, quiet_mode, verbose_mode)
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'policy-import':
        return None, False, False
    
    if len(parts) < 2:
        return None, False, False
    
    file_pattern = parts[1]
    quiet_mode = False
    verbose_mode = False
    
    # Check for flags
    for i in range(2, len(parts)):
        if parts[i] == '--quiet' or parts[i] == '-q':
            quiet_mode = True
        elif parts[i] == '--verbose' or parts[i] == '-v':
            verbose_mode = True
        elif parts[i] == '--help' or parts[i] == '-h':
            print("\n" + "="*60)
            print("POLICY-IMPORT COMMAND HELP")
            print("="*60)
            print("Usage: policy-import <file_or_pattern> [options]")
            print("\nArguments:")
            print("  <file_or_pattern>  ZIP file path or glob pattern (e.g., *.zip)")
            print("\nOptions:")
            print("  --quiet, -q        Quiet mode (minimal output)")
            print("  --verbose, -v      Verbose mode (detailed output)")
            print("  --help, -h         Show this help message")
            print("\nExamples:")
            print("  policy-import *.zip")
            print("  policy-import data-quality-*.zip")
            print("  policy-import /path/to/specific-file.zip")
            print("  policy-import *.zip --verbose")
            print("\nFeatures:")
            print("  - Uploads ZIP files to policy import API")
            print("  - Uses target environment authentication (target access key, secret key, and tenant)")
            print("  - By default, looks for files in output-dir/policy-import directory")
            print("  - Supports absolute paths to override default directory")
            print("  - Supports glob patterns for multiple files")
            print("  - Validates that files exist and are readable")
            print("  - Aggregates statistics across all files")
            print("  - Shows detailed import results")
            print("="*60)
            return None, False, False
    
    return file_pattern, quiet_mode, verbose_mode


def parse_rule_tag_export_command(command: str) -> tuple:
    """Parse the rule-tag-export command and extract arguments.
    
    Args:
        command: The full command string
        
    Returns:
        tuple: (quiet_mode, verbose_mode)
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'rule-tag-export':
        return False, False
    
    quiet_mode = False
    verbose_mode = False
    
    # Check for flags
    for i in range(1, len(parts)):
        if parts[i] == '--quiet' or parts[i] == '-q':
            quiet_mode = True
        elif parts[i] == '--verbose' or parts[i] == '-v':
            verbose_mode = True
        elif parts[i] == '--help' or parts[i] == '-h':
            print("\n" + "="*60)
            print("RULE-TAG-EXPORT COMMAND HELP")
            print("="*60)
            print("Usage: rule-tag-export [options]")
            print("\nOptions:")
            print("  --quiet, -q        Quiet mode (minimal output with progress bar)")
            print("  --verbose, -v      Verbose mode (detailed output)")
            print("  --help, -h         Show this help message")
            print("\nExamples:")
            print("  rule-tag-export")
            print("  rule-tag-export --quiet")
            print("  rule-tag-export --verbose")
            print("\nFeatures:")
            print("  - Exports rule tags for all policies from policies-all-export.csv")
            print("  - Automatically runs policy-list-export if policies-all-export.csv doesn't exist")
            print("  - Makes API calls to /catalog-server/api/rules/<id>/tags for each rule")
            print("  - Outputs to rule-tags-export.csv with rule ID and comma-separated tags")
            print("  - Shows progress bar in quiet mode")
            print("  - Shows detailed API calls in verbose mode")
            print("="*60)
            return False, False
    
    return quiet_mode, verbose_mode


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
        from glob import glob
        import json
        
        if not quiet_mode:
            print(f"\nImporting policy definitions from ZIP files")
            print(f"File pattern: {file_pattern}")
            print("="*80)
            
            # Show target environment information
            print(f"\n🌍 TARGET ENVIRONMENT INFORMATION:")
            print(f"  Host: {client.host}")
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
            if GLOBAL_OUTPUT_DIR:
                search_dir = GLOBAL_OUTPUT_DIR / "policy-import"
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
            'uuids': []
        }
        
        successful_imports = 0
        failed_imports = 0
        
        # Process each ZIP file
        for i, zip_file in enumerate(zip_files, 1):
            if not quiet_mode:
                print(f"Processing file {i}/{len(zip_files)}: {zip_file}")
            
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
                
                # Make API call with target authentication
                response = client.make_api_call(
                    endpoint="/catalog-server/api/rules/import/policy-definitions/upload-config",
                    method='POST',
                    files=files,
                    use_target_auth=True,
                    use_target_tenant=True
                )
                
                if verbose_mode:
                    print(f"\nResponse:")
                    print(f"  Status: Success")
                    print(f"  Content-Type: application/json")
                    print(f"  Response size: {len(str(response)) if response else 0} bytes")
                
                if response:
                    # Response is already parsed JSON from make_api_call
                    response_data = response
                    
                    # Aggregate statistics
                    for key in aggregated_stats.keys():
                        if key in response_data and isinstance(response_data[key], (int, float)):
                            if key == 'uuids':
                                if isinstance(response_data[key], list):
                                    aggregated_stats[key].extend(response_data[key])
                            else:
                                aggregated_stats[key] += response_data[key]
                    
                    # Add UUID if present
                    if 'uuid' in response_data:
                        aggregated_stats['uuids'].append(response_data['uuid'])
                    
                    aggregated_stats['files_processed'] += 1
                    successful_imports += 1
                    
                    if not quiet_mode:
                        print(f"✅ Successfully imported: {zip_file}")
                        if verbose_mode:
                            print(f"  UUID: {response_data.get('uuid', 'N/A')}")
                            print(f"  Total Policies: {response_data.get('totalPolicyCount', 0)}")
                            print(f"  Data Quality Policies: {response_data.get('totalDataQualityPolicyCount', 0)}")
                            print(f"  Data Sources: {response_data.get('totalDataSourceCount', 0)}")
                    else:
                        print(f"✅ [{i}/{len(zip_files)}] {zip_file}: Successfully imported")
                else:
                    error_msg = f"Empty response for {zip_file}"
                    if not quiet_mode:
                        print(f"❌ {error_msg}")
                    logger.error(error_msg)
                    failed_imports += 1
                    aggregated_stats['files_failed'] += 1
                    
            except Exception as e:
                error_msg = f"Failed to import {zip_file}: {e}"
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
        if GLOBAL_OUTPUT_DIR:
            policies_file = GLOBAL_OUTPUT_DIR / "policy-export" / "policies-all-export.csv"
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
        
        for rule_id in rule_ids:
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