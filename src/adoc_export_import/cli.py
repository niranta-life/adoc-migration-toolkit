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
import pickle
from pathlib import Path
from datetime import datetime
from .core import PolicyExportFormatter, setup_logging
from .api_client import create_api_client
from .guided_migration import GuidedMigration, MigrationState


def create_formatter_parser(subparsers):
    """Create the formatter subcommand parser."""
    formatter_parser = subparsers.add_parser(
        'formatter',
        help='Format policy export files by replacing substrings in JSON files and ZIP archives',
        description='JSON String Replacer - Replace substrings in JSON files and ZIP archives',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m adoc_export_import formatter --input=data/samples --source-env-string="PROD_DB" --target-env-string="DEV_DB"
  python -m adoc_export_import formatter --input=data/samples --source-env-string="old" --target-env-string="new" --output-dir=data/output
  python -m adoc_export_import formatter --input=data/samples --source-env-string="COMM_APAC_ETL_PROD_DB" --target-env-string="NEW_DB_NAME" --verbose

Features:
  - Processes JSON files and ZIP archives
  - Maintains file structure and count
  - Comprehensive error handling and logging
  - Professional output formatting
  - Extracts data quality policy assets to CSV
        """
    )
    
    formatter_parser.add_argument(
        "--input", 
        required=True,
        help="Directory containing JSON files and ZIP files to process"
    )
    formatter_parser.add_argument(
        "--source-env-string", 
        required=True,
        help="Substring to search for (source environment)"
    )
    formatter_parser.add_argument(
        "--target-env-string", 
        required=True,
        help="Substring to replace with (target environment)"
    )
    formatter_parser.add_argument(
        "--output-dir", 
        help="Output directory (defaults to input_dir with '_import_ready' suffix)"
    )
    formatter_parser.add_argument(
        "--log-level", "-l",
        choices=["ERROR", "WARNING", "INFO", "DEBUG"],
        default="ERROR",
        help="Set logging level (default: ERROR)"
    )
    formatter_parser.add_argument(
        "--verbose", "-v", 
        action="store_true", 
        help="Enable verbose logging (overrides --log-level)"
    )
    
    return formatter_parser


def create_asset_export_parser(subparsers):
    """Create the asset-export subcommand parser."""
    asset_export_parser = subparsers.add_parser(
        'asset-export',
        help='Export asset details by reading UIDs from CSV file and making API calls',
        description='Asset Export Tool - Read UIDs from CSV and export asset details via API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m adoc_export_import asset-export --csv-file=data/output/segmented_spark_uids.csv --env-file=config.env
  python -m adoc_export_import asset-export --csv-file=data/output/segmented_spark_uids.csv --env-file=config.env --verbose

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
  python -m adoc_export_import interactive --env-file=config.env
  python -m adoc_export_import interactive --env-file=config.env --verbose

Interactive Commands:
  GET /catalog-server/api/assets?uid=123
  PUT /catalog-server/api/assets {"key": "value"}
  GET /catalog-server/api/assets?uid=123 --target-auth --target-tenant
  segments-export data/output/segmented_spark_uids.csv
  segments-export data/output/segmented_spark_uids.csv --output-file my_segments.csv --quiet
  segments-export data/output/segmented_spark_uids.csv --target-auth --target-tenant
  exit

Features:
  - Interactive API client
  - Support for GET and PUT requests
  - Configurable source/target authentication and tenants
  - JSON payload support for PUT requests
  - Well-formatted JSON responses
  - Segments export with CSV file output and quiet mode
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
    if not args.input or not args.input.strip():
        raise ValueError("Input directory cannot be empty")
    
    if not args.source_env_string or not args.source_env_string.strip():
        raise ValueError("Source environment string cannot be empty")
    
    if args.target_env_string is None:
        raise ValueError("Target environment string cannot be None")
    
    # Check if input directory exists
    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input directory does not exist: {args.input}")
    
    if not input_path.is_dir():
        raise ValueError(f"Input path is not a directory: {args.input}")


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


def run_formatter(args):
    """Run the formatter command."""
    try:
        # Setup logging
        logger = setup_logging(args.verbose, args.log_level)
        
        # Validate arguments
        validate_formatter_arguments(args)
        
        # Create and run the formatter
        formatter = PolicyExportFormatter(
            input_dir=args.input,
            search_string=args.source_env_string,
            replace_string=args.target_env_string,
            output_dir=args.output_dir,
            logger=logger
        )
        
        stats = formatter.process_directory()
        
        # Print professional summary
        print("\n" + "="*60)
        print("PROCESSING SUMMARY")
        print("="*60)
        print(f"Input directory:     {args.input}")
        print(f"Output directory:    {formatter.output_dir}")
        print(f"Source env string:   '{args.source_env_string}'")
        print(f"Target env string:   '{args.target_env_string}'")
        print(f"Total files found:   {stats['total_files']}")
        
        if stats['json_files'] > 0:
            print(f"JSON files:          {stats['json_files']}")
        if stats['zip_files'] > 0:
            print(f"ZIP files:           {stats['zip_files']}")
        
        print(f"Files investigated:  {stats['files_investigated']}")
        print(f"Changes made:        {stats['changes_made']}")
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
            return 1
        else:
            print("✅ Processing completed successfully!")
            return 0
            
    except (ValueError, FileNotFoundError, PermissionError) as e:
        print(f"❌ Configuration error: {e}")
        return 1
    except KeyboardInterrupt:
        print("\n⚠️  Processing interrupted by user.")
        return 1
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1


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


def handle_dynamic_endpoints(endpoint: str) -> str:
    """Handle dynamic endpoints with placeholders and provide user guidance.
    
    Args:
        endpoint: The endpoint string that may contain placeholders
        
    Returns:
        The processed endpoint with guidance if needed
    """
    if '<asset-id>' in endpoint or '<asset-uid>' in endpoint:
        print("\n" + "="*60)
        print("DYNAMIC ENDPOINT DETECTED")
        print("="*60)
        print("You've selected an endpoint with a dynamic placeholder.")
        
        if '<asset-id>' in endpoint:
            print("Please replace <asset-id> with an actual asset ID number.")
            print("\nExample:")
            print("  GET /catalog-server/api/assets/12345/metadata")
        elif '<asset-uid>' in endpoint:
            print("Please replace <asset-uid> with an actual asset UID.")
            print("\nExample:")
            print("  GET /catalog-server/api/assets/byUid/your.asset.uid/scores")
        
        print("\nTo get asset IDs or UIDs, you can:")
        print("  1. Use the asset-export command with a CSV file")
        print("  2. Use segments-export command in interactive mode")
        print("  3. Get asset details first with: GET /catalog-server/api/assets?uid=<uid>")
        print("  4. Use the discover endpoint: GET /catalog-server/api/assets/discover")
        print("="*60)
        return endpoint
    
    return endpoint


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
        command: Command string like "segments-export <csv_file> [--output-file <file>] [--quiet]"
        
    Returns:
        Tuple of (csv_file, output_file, quiet_mode)
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'segments-export':
        return None, None, False
    
    if len(parts) < 2:
        raise ValueError("CSV file path is required for segments-export command")
    
    csv_file = parts[1]
    output_file = None
    quiet_mode = False
    
    # Check for flags and options
    i = 2
    while i < len(parts):
        if parts[i] == '--output-file' and i + 1 < len(parts):
            output_file = parts[i + 1]
            parts.pop(i)  # Remove --output-file
            parts.pop(i)  # Remove the file path
        elif parts[i] == '--quiet':
            quiet_mode = True
            parts.remove('--quiet')
        else:
            i += 1
    
    # Generate default output file if not provided
    if not output_file:
        output_file = get_output_file_path(csv_file, f"{Path(csv_file).stem}_segments_output.csv")
    
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
        # Read source-env and target-env mappings from CSV file
        env_mappings = read_csv_uids(csv_file, logger)
        
        if not env_mappings:
            logger.warning("No environment mappings found in CSV file")
            return
        
        # Generate default output file if not provided
        if not output_file:
            output_file = get_output_file_path(csv_file, f"{Path(csv_file).stem}_segments_output.csv")
        
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
                if not quiet_mode:
                    print(f"\n[{i}/{len(env_mappings)}] Processing source-env: {source_env}")
                    print(f"Target-env: {target_env}")
                    print("-" * 60)
                
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


def setup_autocomplete():
    """Setup autocomplete for the interactive session."""
    # Available commands and their completions
    commands = [
        # 'GET',
        # 'PUT', 
        'segments-export',
        'segments-import',
        'asset-profile-export',
        'asset-profile-import',
        'asset-config-export',
        'set-output-dir',
        'guided-migration',
        'resume-migration',
        'delete-migration',
        'list-migrations',
        'help',
        'exit',
        'quit',
        'q'
    ]
    
    # Common endpoints for completion
    endpoints = [
        '/catalog-server/api/assets',
        '/catalog-server/api/assets?uid=',
        '/catalog-server/api/assets/',
        '/catalog-server/api/health',
        '/catalog-server/api/connections',
        '/catalog-server/api/assets/<asset-id>/metadata',
        '/catalog-server/api/assets/<asset-id>/rulesWithLatestExecution',
        '/catalog-server/api/assets/<asset-id>/scores',
        '/catalog-server/api/assets/discover',
        '/catalog-server/api/assets/byUid/<asset-uid>',
        '/catalog-server/api/assets/byUid/<asset-uid>/scores',
        '/catalog-server/api/profile/<asset-id>/config'
    ]
    
    # Flags for completion
    flags = [
        '--quiet',
        '--output-file',
        '--dry-run',
        '--verbose'
    ]
    
    def completer(text, state):
        """Completer function for readline."""
        options = []
        
        # Get the current line and cursor position
        line = readline.get_line_buffer()
        words = line.split()
        
        if not words:
            # If no words, suggest commands
            options = [cmd for cmd in commands if cmd.lower().startswith(text.lower())]
        elif len(words) == 1:
            # First word - suggest commands
            options = [cmd for cmd in commands if cmd.lower().startswith(text.lower())]
        elif len(words) == 2:
            if words[0].lower() in ['segments-export', 'segments-import', 'asset-profile-export', 'asset-profile-import', 'asset-config-export']:
                # Second word for segments-export/segments-import/asset-profile-export/asset-profile-import/asset-config-export - suggest CSV files
                # This is a simple suggestion - could be enhanced to scan directory
                options = ['data/samples_import_ready/segmented_spark_uids.csv', 'data/samples_import_ready/asset_uids.csv', 'data/samples_import_ready/asset-profiles-export.csv', 'data/samples/assets.csv', 'segments_output.csv']
            elif words[0].lower() == 'set-output-dir':
                # Second word for set-output-dir - suggest directory paths
                options = ['data/output', 'data/custom_output', 'output', 'exports', 'data/exports', 'data/samples_import_ready']
            elif words[0].lower() in ['guided-migration', 'resume-migration', 'delete-migration']:
                # Second word for migration commands - suggest migration names
                # This could be enhanced to scan actual migration files
                options = ['prod-to-dev', 'test-migration', 'my-migration', 'migration-1']
        elif len(words) >= 2:
            # Additional words - suggest flags
            if text.startswith('--'):
                options = [flag for flag in flags if flag.startswith(text)]
            elif words[0].upper() == 'PUT' and len(words) == 3:
                # Third word for PUT - suggest JSON payload start
                options = ['{"', '{']
            elif words[0].lower() in ['segments-export', 'segments-import', 'asset-profile-export', 'asset-profile-import', 'asset-config-export'] and len(words) >= 3:
                # For segments-export/segments-import/asset-profile-export/asset-profile-import/asset-config-export, suggest flags after CSV file
                if text.startswith('--'):
                    if words[0].lower() == 'segments-export':
                        # segments-export supports --output-file and --quiet
                        options = [flag for flag in flags if flag.startswith(text) and flag in ['--output-file', '--quiet']]
                    elif words[0].lower() == 'asset-profile-export':
                        # asset-profile-export supports --output-file, --quiet, and --verbose
                        options = [flag for flag in flags if flag.startswith(text) and flag in ['--output-file', '--quiet', '--verbose']]
                    elif words[0].lower() == 'asset-config-export':
                        # asset-config-export supports --output-file, --quiet, and --verbose
                        options = [flag for flag in flags if flag.startswith(text) and flag in ['--output-file', '--quiet', '--verbose']]
                    elif words[0].lower() == 'asset-profile-import':
                        # asset-profile-import supports --dry-run, --quiet, and --verbose
                        options = [flag for flag in flags if flag.startswith(text) and flag in ['--dry-run', '--quiet', '--verbose']]
                    else:
                        # segments-import supports --dry-run, --quiet, and --verbose
                        options = [flag for flag in flags if flag.startswith(text) and flag in ['--dry-run', '--quiet', '--verbose']]
                elif words[-2] == '--output-file' and len(words) >= 3:
                    # After --output-file, suggest output file names
                    if words[0].lower() == 'asset-profile-export':
                        options = ['data/output/output_import_ready/asset-profiles-export.csv', 'asset-profiles-export.csv', 'output.csv']
                    elif words[0].lower() == 'asset-config-export':
                        options = ['data/output/output_import_ready/asset-config-export.csv', 'asset-config-export.csv', 'output.csv']
                    else:
                        options = ['data/output/output_import_ready/segments_output.csv', 'my_segments.csv', 'output.csv']
        
        # Return the option at the requested state index
        if state < len(options):
            return options[state]
        else:
            return None
    
    # Set the completer function
    readline.set_completer(completer)
    
    # Enable tab completion with basic configuration
    try:
        # Basic tab completion setup
        readline.parse_and_bind('tab: complete')
    except Exception as e:
        # Fallback if readline configuration fails
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
        
        # Setup command history
        history_file = os.path.expanduser("~/.adoc_history")
        try:
            readline.read_history_file(history_file)
        except FileNotFoundError:
            pass  # History file doesn't exist yet
        
        # Set history file for future sessions
        readline.set_history_length(1000)  # Keep last 1000 commands
        
        # Setup autocomplete
        setup_autocomplete()
        
        while True:
            try:
                # Get user input
                command = input("\nADOC> ").strip()
                
                if not command:
                    continue
                
                if command.lower() in ['exit', 'quit', 'q']:
                    print("Goodbye!")
                    break
                
                # Check if it's a help command
                if command.lower() == 'help':
                    show_interactive_help()
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
                
                # Check if it's a set-output-dir command
                if command.lower().startswith('set-output-dir'):
                    directory = parse_set_output_dir_command(command)
                    if directory:
                        set_global_output_directory(directory, logger)
                    continue
                
                # Check if it's a guided-migration command
                if command.lower().startswith('guided-migration'):
                    migration_name = parse_guided_migration_command(command)
                    if migration_name:
                        execute_guided_migration(migration_name, client, logger)
                    continue
                
                # Check if it's a resume-migration command
                if command.lower().startswith('resume-migration'):
                    migration_name = parse_resume_migration_command(command)
                    if migration_name:
                        execute_resume_migration(migration_name, client, logger)
                    continue
                
                # Check if it's a delete-migration command
                if command.lower().startswith('delete-migration'):
                    migration_name = parse_delete_migration_command(command)
                    if migration_name:
                        execute_delete_migration(migration_name, logger)
                    continue
                
                # Check if it's a list-migrations command
                if parse_list_migrations_command(command):
                    execute_list_migrations(logger)
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
        command: Command string like "asset-profile-export <csv_file> [--output-file <file>] [--quiet] [--verbose]"
        
    Returns:
        Tuple of (csv_file, output_file, quiet_mode, verbose_mode)
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'asset-profile-export':
        return None, None, False, False
    
    if len(parts) < 2:
        raise ValueError("CSV file path is required for asset-profile-export command")
    
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
        output_file = get_output_file_path(csv_file, "asset-profiles-export.csv")
    
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
        # Read source-env and target-env mappings from CSV file
        env_mappings = read_csv_uids(csv_file, logger)
        
        if not env_mappings:
            logger.warning("No environment mappings found in CSV file")
            return
        
        # Generate default output file if not provided
        if not output_file:
            output_file = get_output_file_path(csv_file, "asset-profiles-export.csv")
        
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
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            
            # Write header
            writer.writerow(['target-env', 'profile_json'])
            
            for i, (source_env, target_env) in enumerate(env_mappings, 1):
                if not quiet_mode:
                    print(f"\n[{i}/{len(env_mappings)}] Processing source-env: {source_env}")
                    print(f"Target-env: {target_env}")
                    print("-" * 60)
                
                try:
                    # Step 1: Get asset details by source-env (UID)
                    if not quiet_mode:
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
                        if not quiet_mode:
                            print(f"❌ {error_msg}")
                        logger.error(error_msg)
                        failed += 1
                        continue
                    
                    data_array = asset_response['data']
                    if not data_array or len(data_array) == 0:
                        error_msg = f"Empty 'data' array in asset response for UID: {source_env}"
                        if not quiet_mode:
                            print(f"❌ {error_msg}")
                        logger.error(error_msg)
                        failed += 1
                        continue
                    
                    first_asset = data_array[0]
                    if 'id' not in first_asset:
                        error_msg = f"No 'id' field found in first asset for UID: {source_env}"
                        if not quiet_mode:
                            print(f"❌ {error_msg}")
                        logger.error(error_msg)
                        failed += 1
                        continue
                    
                    asset_id = first_asset['id']
                    if not quiet_mode:
                        print(f"Extracted asset ID: {asset_id}")
                    
                    # Step 3: Get profile configuration for the asset
                    if not quiet_mode:
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
                    
                    if not quiet_mode:
                        print(f"✅ Written to file: {target_env}")
                        if not verbose_mode:  # Only show response if not in verbose mode (to avoid duplication)
                            print("Profile Response:")
                            print(json.dumps(profile_response, indent=2, ensure_ascii=False))
                    
                    successful += 1
                    total_assets_processed += 1
                    
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
                elif header[0] != 'target-env' or header[1] != 'profile_json':
                    validation_errors.append(f"Invalid header: expected ['target-env', 'profile_json'], got {header}")
                
                # Validate each row
                for row_num, row in enumerate(reader, start=2):
                    row_count += 1
                    
                    # Check column count
                    if len(row) != 2:
                        validation_errors.append(f"Row {row_num}: Expected 2 columns, got {len(row)}")
                        continue
                    
                    target_env, profile_json_str = row
                    
                    # Check for empty values
                    if not target_env.strip():
                        validation_errors.append(f"Row {row_num}: Empty target-env value")
                    
                    if not profile_json_str.strip():
                        validation_errors.append(f"Row {row_num}: Empty profile_json value")
                        continue
                    
                    # Verify JSON is parsable
                    try:
                        profile_data = json.loads(profile_json_str)
                        
                        # Additional validation: check if it's a valid profile response
                        if not isinstance(profile_data, dict):
                            validation_errors.append(f"Row {row_num}: profile_json is not a valid JSON object")
                        elif not profile_data:  # Empty object
                            validation_errors.append(f"Row {row_num}: profile_json is empty")
                        
                    except json.JSONDecodeError as e:
                        validation_errors.append(f"Row {row_num}: Invalid JSON in profile_json - {e}")
                    except Exception as e:
                        validation_errors.append(f"Row {row_num}: Error parsing profile_json - {e}")
                
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
                        print(f"   Expected columns: target-env, profile_json")
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
        
    except Exception as e:
        error_msg = f"Error in asset-profile-export: {e}"
        if not quiet_mode:
            print(f"❌ {error_msg}")
        logger.error(error_msg)


def parse_asset_profile_import_command(command: str) -> tuple:
    """Parse an asset-profile-import command string into components.
    
    Args:
        command: Command string like "asset-profile-import <csv_file> [--dry-run] [--quiet] [--verbose]"
        
    Returns:
        Tuple of (csv_file, dry_run, quiet_mode, verbose_mode)
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'asset-profile-import':
        return None, False, True, False
    
    if len(parts) < 2:
        raise ValueError("CSV file path is required for asset-profile-import command")
    
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
        # Read target-env and profile_json from CSV file
        if not Path(csv_file).exists():
            error_msg = f"CSV file does not exist: {csv_file}"
            print(f"❌ {error_msg}")
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
        
        for i, (target_env, profile_json) in enumerate(import_mappings, 1):
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
                
                # Step 3: Parse profile JSON
                try:
                    profile_data = json.loads(profile_json)
                except json.JSONDecodeError as e:
                    error_msg = f"Invalid JSON in profile_json for UID {target_env}: {e}"
                    print(f"❌ [{i}/{len(import_mappings)}] {target_env}: {error_msg}")
                    logger.error(error_msg)
                    failed += 1
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
                        print(f"✅ [{i}/{len(import_mappings)}] {target_env}: Profile updated successfully")
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
                logger.info(f"Successfully processed target-env {target_env} (asset ID: {asset_id})")
                
            except Exception as e:
                error_msg = f"Failed to process UID {target_env}: {e}"
                print(f"❌ [{i}/{len(import_mappings)}] {target_env}: {error_msg}")
                logger.error(error_msg)
                failed += 1
        
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
        output_file = get_output_file_path(csv_file, "asset-config-export.csv")
    
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
            output_file = get_output_file_path(csv_file, "asset-config-export.csv")
        
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


def parse_guided_migration_command(command: str) -> str:
    """Parse a guided-migration command string into components.
    
    Args:
        command: Command string like "guided-migration <name>"
        
    Returns:
        str: Migration name or None if invalid
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'guided-migration':
        return None
    
    if len(parts) < 2:
        raise ValueError("Migration name is required for guided-migration command")
    
    # Join remaining parts in case name contains spaces
    name = ' '.join(parts[1:])
    return name


def parse_resume_migration_command(command: str) -> str:
    """Parse a resume-migration command string into components.
    
    Args:
        command: Command string like "resume-migration <name>"
        
    Returns:
        str: Migration name or None if invalid
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'resume-migration':
        return None
    
    if len(parts) < 2:
        raise ValueError("Migration name is required for resume-migration command")
    
    # Join remaining parts in case name contains spaces
    name = ' '.join(parts[1:])
    return name


def parse_delete_migration_command(command: str) -> str:
    """Parse a delete-migration command string into components.
    
    Args:
        command: Command string like "delete-migration <name>"
        
    Returns:
        str: Migration name or None if invalid
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'delete-migration':
        return None
    
    if len(parts) < 2:
        raise ValueError("Migration name is required for delete-migration command")
    
    # Join remaining parts in case name contains spaces
    name = ' '.join(parts[1:])
    return name


def parse_list_migrations_command(command: str) -> bool:
    """Parse a list-migrations command string.
    
    Args:
        command: Command string like "list-migrations"
        
    Returns:
        bool: True if command is valid, False otherwise
    """
    parts = command.strip().split()
    return parts and parts[0].lower() == 'list-migrations'


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog='adoc_export_import',
        description='ADOC Export Import - Professional tools for policy export processing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Available commands:
  formatter     Format policy export files by replacing substrings
  asset-export  Export asset details by reading UIDs from CSV file
  interactive   Interactive REST API client for making API calls

For help on a specific command:
  python -m adoc_export_import <command> --help
        """
    )
    
    subparsers = parser.add_subparsers(
        dest='command',
        help='Available commands',
        metavar='COMMAND'
    )
    
    # Add subcommands
    create_formatter_parser(subparsers)
    create_asset_export_parser(subparsers)
    create_interactive_parser(subparsers)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    if args.command == 'formatter':
        return run_formatter(args)
    elif args.command == 'asset-export':
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
    
    print("\n" + "="*80)
    print("ADOC INTERACTIVE MIGRATION TOOLKIT - COMMAND HELP")
    print("="*80)
    
    print(f"\n{BOLD}📊 SEGMENTS COMMANDS:{RESET}")
    print(f"  {BOLD}segments-export{RESET} <csv_file> [--output-file <file>] [--quiet]")
    print("    Description: Export segments from source environment to CSV file")
    print("    Arguments:")
    print("      csv_file: Path to CSV file with source-env and target-env mappings")
    print("      --output-file: Specify custom output file (optional)")
    print("      --quiet: Suppress console output, show only summary")
    print("    Examples:")
    print("      segments-export data/samples_import_ready/segmented_spark_uids.csv")
    print("      segments-export data/uids.csv --output-file my_segments.csv --quiet")
    print("    Behavior:")
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
    print("      segments-import data/samples_import_ready/segments_output.csv")
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
    print(f"  {BOLD}asset-profile-export{RESET} <csv_file> [--output-file <file>] [--quiet] [--verbose]")
    print("    Description: Export asset profiles from source environment to CSV file")
    print("    Arguments:")
    print("      csv_file: Path to CSV file with source-env and target-env mappings")
    print("      --output-file: Specify custom output file (optional)")
    print("      --quiet: Suppress console output, show only summary")
    print("      --verbose: Show detailed output including headers and responses")
    print("    Examples:")
    print("      asset-profile-export data/samples_import_ready/asset_uids.csv")
    print("      asset-profile-export uids.csv --output-file profiles.csv --verbose")
    
    print(f"\n  {BOLD}asset-profile-import{RESET} <csv_file> [--dry-run] [--quiet] [--verbose]")
    print("    Description: Import asset profiles to target environment from CSV file")
    print("    Arguments:")
    print("      csv_file: Path to CSV file with target-env and profile_json")
    print("      --dry-run: Preview changes without making API calls")
    print("      --quiet: Suppress console output (default)")
    print("      --verbose: Show detailed output including headers and responses")
    print("    Examples:")
    print("      asset-profile-import data/samples_import_ready/asset-profiles-export.csv")
    print("      asset-profile-import profiles.csv --dry-run --verbose")
    
    print(f"\n{BOLD}🔍 ASSET CONFIGURATION COMMANDS:{RESET}")
    print(f"  {BOLD}asset-config-export{RESET} <csv_file> [--output-file <file>] [--quiet] [--verbose]")
    print("    Description: Export asset configurations from source environment to CSV file")
    print("    Arguments:")
    print("      csv_file: Path to CSV file with UIDs in the first column")
    print("      --output-file: Specify custom output file (optional)")
    print("      --quiet: Suppress console output, show only summary (default)")
    print("      --verbose: Show detailed output including headers and responses")
    print("    Examples:")
    print("      asset-config-export data/samples_import_ready/asset_uids.csv")
    print("      asset-config-export uids.csv --output-file configs.csv --verbose")
    print("    Behavior:")
    print("      • Reads UIDs from the first column of the CSV file")
    print("      • Makes REST call to '/catalog-server/api/assets?uid=<uid>' to get asset ID")
    print("      • Uses asset ID to call '/catalog-server/api/assets/<id>/config'")
    print("      • Writes compressed JSON response to CSV with target-env UID")
    print("      • Shows status for each UID in quiet mode")
    print("      • Shows HTTP headers and response objects in verbose mode")
    print("      • Output format: target-env, config_json (compressed)")
    
    print(f"\n{BOLD}🛠️ UTILITY COMMANDS:{RESET}")
    print(f"  {BOLD}set-output-dir{RESET} <directory>")
    print("    Description: Set global output directory for all export commands")
    print("    Arguments:")
    print("      directory: Path to the output directory")
    print("    Examples:")
    print("      set-output-dir /path/to/my/output")
    print("      set-output-dir data/custom_output")
    
    print(f"\n{BOLD}🚀 GUIDED MIGRATION COMMANDS:{RESET}")
    print(f"  {BOLD}guided-migration{RESET} <name>")
    print("    Description: Start a new guided migration session")
    print("    Arguments:")
    print("      name: Unique name for the migration session")
    print("    Examples:")
    print("      guided-migration prod-to-dev")
    print("      guided-migration test-migration")
    print("    Features:")
    print("      • Step-by-step guidance through the complete migration process")
    print("      • State management - can pause and resume at any time")
    print("      • Validation of prerequisites at each step")
    print("      • Detailed help and instructions for each step")
    print("      • Automatic file path management")
    
    print(f"\n  {BOLD}resume-migration{RESET} <name>")
    print("    Description: Resume an existing guided migration session")
    print("    Arguments:")
    print("      name: Name of the existing migration session")
    print("    Examples:")
    print("      resume-migration prod-to-dev")
    print("    Features:")
    print("      • Continues from where you left off")
    print("      • Shows current progress and completed steps")
    print("      • Validates prerequisites before continuing")
    
    print(f"\n  {BOLD}delete-migration{RESET} <name>")
    print("    Description: Delete a migration state file")
    print("    Arguments:")
    print("      name: Name of the migration session to delete")
    print("    Examples:")
    print("      delete-migration prod-to-dev")
    print("    Features:")
    print("      • Confirms deletion to prevent accidental loss")
    print("      • Shows migration details before deletion")
    
    print(f"\n  {BOLD}list-migrations{RESET}")
    print("    Description: List all available migration sessions")
    print("    Examples:")
    print("      list-migrations")
    print("    Features:")
    print("      • Shows all migration names and their status")
    print("      • Displays creation date and current step")
    print("      • Shows completion progress")
    
    print(f"\n  {BOLD}help{RESET}")
    print("    Description: Show this help information")
    print("    Example: help")
    
    print(f"\n  {BOLD}exit, quit, q{RESET}")
    print("    Description: Exit the interactive client")
    print("    Examples: exit, quit, q")
    
    print(f"\n{BOLD}🔧 ENVIRONMENT BEHAVIOR:{RESET}")
    print("  • segments-export: Always exports from source environment")
    print("  • segments-import: Always imports to target environment")
    print("  • asset-profile-export: Always exports from source environment")
    print("  • asset-profile-import: Always imports to target environment")
    print("  • asset-config-export: Always exports from source environment")
    print(f"\n{BOLD}💡 TIPS:{RESET}")
    print("  • Use TAB key for command autocomplete")
    print("  • Use ↑/↓ arrow keys to navigate command history")
    print("  • Type part of an endpoint and press TAB to see suggestions")
    print("  • Use --dry-run to preview changes before making them")
    print("  • Use --verbose to see detailed API request/response information")
    print("  • Check log files for detailed error information")
    print("  • Set output directory once with set-output-dir to avoid specifying --output-file repeatedly")
    
    print(f"\n{BOLD}📁 FILE LOCATIONS:{RESET}")
    print("  • Input CSV files: data/samples_import_ready/")
    print("  • Output CSV files: *_import_ready/ directories (or custom output directory)")
    print("  • Log files: policy_export_formatter_*.log")
    
    print("="*80)


# Global output directory for all export commands
GLOBAL_OUTPUT_DIR = None


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
        
        logger.info(f"Global output directory set to: {GLOBAL_OUTPUT_DIR}")
        print(f"✅ Global output directory set to: {GLOBAL_OUTPUT_DIR}")
        return True
        
    except Exception as e:
        error_msg = f"Failed to set output directory '{directory}': {e}"
        print(f"❌ {error_msg}")
        logger.error(error_msg)
        return False


def get_output_file_path(csv_file: str, default_filename: str, custom_output_file: str = None) -> Path:
    """Get the output file path based on global output directory and custom settings.
    
    Args:
        csv_file: Path to the input CSV file
        default_filename: Default filename to use
        custom_output_file: Custom output file path (overrides global directory)
        
    Returns:
        Path: Output file path
    """
    if custom_output_file:
        # Use custom output file if specified
        return Path(custom_output_file)
    
    if GLOBAL_OUTPUT_DIR:
        # Use global output directory
        return GLOBAL_OUTPUT_DIR / default_filename
    
    # Fall back to original logic
    csv_path = Path(csv_file)
    if "_import_ready" in csv_path.parent.name:
        return csv_path.parent / default_filename
    else:
        import_ready_dir = csv_path.parent / f"{csv_path.parent.name}_import_ready"
        return import_ready_dir / default_filename


def execute_guided_migration(migration_name: str, client, logger: logging.Logger):
    """Execute a guided migration session.
    
    Args:
        migration_name: Name of the migration session
        client: API client instance
        logger: Logger instance
    """
    guided_migration = GuidedMigration(logger)
    
    # Check if migration already exists
    existing_state = guided_migration.load_state(migration_name)
    if existing_state:
        print(f"\n⚠️  Migration '{migration_name}' already exists!")
        print(f"Created: {existing_state.created_at}")
        print(f"Current step: {existing_state.current_step + 1}")
        print(f"Completed steps: {len(existing_state.completed_steps)}")
        
        response = input("\nDo you want to:\n1. Resume the existing migration\n2. Start a new migration (overwrites existing)\n3. Cancel\nEnter choice (1-3): ").strip()
        
        if response == '1':
            return execute_resume_migration(migration_name, client, logger)
        elif response == '2':
            # Delete existing state and start fresh
            guided_migration.delete_state(migration_name)
            print(f"Deleted existing migration '{migration_name}'")
        else:
            print("Cancelled.")
            return
    
    # Create new migration state
    state = MigrationState(migration_name)
    guided_migration.save_state(state)
    
    print(f"\n🚀 Starting guided migration: {migration_name}")
    print("="*80)
    
    # Start the migration loop
    execute_migration_loop(guided_migration, state, client, logger)


def execute_resume_migration(migration_name: str, client, logger: logging.Logger):
    """Resume an existing guided migration session.
    
    Args:
        migration_name: Name of the migration session
        client: API client instance
        logger: Logger instance
    """
    guided_migration = GuidedMigration(logger)
    
    # Load existing state
    state = guided_migration.load_state(migration_name)
    if not state:
        print(f"❌ Migration '{migration_name}' not found!")
        print("Available migrations:")
        migrations = guided_migration.list_migrations()
        if migrations:
            for migration in migrations:
                print(f"  - {migration}")
        else:
            print("  No migrations found")
        return
    
    print(f"\n🔄 Resuming migration: {migration_name}")
    print(f"Created: {state.created_at}")
    print(f"Current step: {state.current_step + 1}")
    print(f"Completed steps: {len(state.completed_steps)}")
    print("="*80)
    
    # Resume the migration loop
    execute_migration_loop(guided_migration, state, client, logger)


def execute_migration_loop(guided_migration: GuidedMigration, state: MigrationState, client, logger: logging.Logger):
    """Execute the main migration loop.
    
    Args:
        guided_migration: Guided migration instance
        state: Migration state
        client: API client instance
        logger: Logger instance
    """
    while state.current_step < len(guided_migration.STEPS):
        step_info = guided_migration.get_current_step_info(state)
        
        print(f"\n📋 Step {step_info['id']}: {step_info['title']}")
        print(f"Description: {step_info['description']}")
        print("-" * 60)
        
        # Validate prerequisites
        is_valid, errors = guided_migration.validate_step_prerequisites(step_info['name'], state)
        if not is_valid:
            print("❌ Prerequisites not met:")
            for error in errors:
                print(f"  - {error}")
            print("\nPlease fix the issues above and try again.")
            break
        
        # Show step-specific help
        help_text = guided_migration.get_step_help(step_info['name'])
        print("Help:")
        print(help_text)
        
        # Handle step-specific logic
        if step_info['name'] == 'setup':
            success = handle_setup_step(guided_migration, state, logger)
        elif step_info['name'] == 'export_policies':
            success = handle_export_policies_step(guided_migration, state, logger)
        elif step_info['name'] == 'process_formatter':
            success = handle_process_formatter_step(guided_migration, state, logger)
        elif step_info['name'] == 'export_profiles':
            success = handle_export_profiles_step(guided_migration, state, client, logger)
        elif step_info['name'] == 'import_profiles':
            success = handle_import_profiles_step(guided_migration, state, client, logger)
        elif step_info['name'] == 'export_configs':
            success = handle_export_configs_step(guided_migration, state, client, logger)
        elif step_info['name'] == 'import_configs':
            success = handle_import_configs_step(guided_migration, state, client, logger)
        elif step_info['name'] == 'handle_segments':
            success = handle_segments_step(guided_migration, state, client, logger)
        elif step_info['name'] == 'completion':
            success = handle_completion_step(guided_migration, state, logger)
        else:
            print(f"❌ Unknown step: {step_info['name']}")
            break
        
        if success:
            # Mark step as completed and move to next
            state.completed_steps.append(step_info['name'])
            state.current_step += 1
            guided_migration.save_state(state)
            print(f"✅ Step {step_info['id']} completed successfully!")
        else:
            print(f"❌ Step {step_info['id']} failed. You can resume later.")
            break
        
        # Ask if user wants to continue
        if state.current_step < len(guided_migration.STEPS):
            response = input("\nContinue to next step? (y/n/exit): ").strip().lower()
            if response in ['n', 'no', 'exit']:
                print("Migration paused. You can resume later with 'resume-migration' command.")
                break
    
    if state.current_step >= len(guided_migration.STEPS):
        print("\n🎉 Migration completed successfully!")
        print("You can delete the migration state with 'delete-migration' command.")


def handle_setup_step(guided_migration: GuidedMigration, state: MigrationState, logger: logging.Logger) -> bool:
    """Handle the setup step."""
    print("\n🔧 Migration Setup")
    print("Please provide the following information:")
    
    try:
        # Get source environment string
        source_string = input("Source environment string (e.g., PROD_DB): ").strip()
        if not source_string:
            print("❌ Source environment string is required")
            return False
        state.data['source_env_string'] = source_string
        
        # Get target environment string
        target_string = input("Target environment string (e.g., DEV_DB): ").strip()
        if not target_string:
            print("❌ Target environment string is required")
            return False
        state.data['target_env_string'] = target_string
        
        # Get input directory
        input_dir = input("Input directory path (containing ZIP files): ").strip()
        if not input_dir:
            print("❌ Input directory is required")
            return False
        
        input_path = Path(input_dir)
        if not input_path.exists():
            print(f"❌ Input directory does not exist: {input_dir}")
            return False
        
        state.data['input_directory'] = str(input_path.resolve())
        
        # Get output directory (optional)
        output_dir = input("Output directory path (optional, press Enter for default): ").strip()
        if output_dir:
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            state.data['output_directory'] = str(output_path.resolve())
        
        print("\n✅ Setup completed successfully!")
        print(f"Source: {source_string}")
        print(f"Target: {target_string}")
        print(f"Input: {state.data['input_directory']}")
        if 'output_directory' in state.data:
            print(f"Output: {state.data['output_directory']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Setup failed: {e}")
        return False


def handle_export_policies_step(guided_migration: GuidedMigration, state: MigrationState, logger: logging.Logger) -> bool:
    """Handle the export policies step."""
    print("\n📤 Export Policies from Source Environment")
    print("This is a manual step that must be completed using the Acceldata UI.")
    print("\nSteps:")
    print("1. Navigate to your source Acceldata environment")
    print("2. Go to the Policies section")
    print("3. Select the policies you want to migrate")
    print("4. Export them as ZIP files")
    print("5. Download the ZIP files to your local machine")
    print("6. Place them in the input directory configured in the setup step")
    
    input_dir = state.data.get('input_directory', '')
    if input_dir:
        print(f"\nInput directory: {input_dir}")
        zip_files = list(Path(input_dir).glob('*.zip'))
        if zip_files:
            print(f"Found {len(zip_files)} ZIP files:")
            for zip_file in zip_files:
                print(f"  - {zip_file.name}")
        else:
            print("No ZIP files found. Please export policies and place them in the input directory.")
    
    response = input("\nHave you completed the policy export? (y/n): ").strip().lower()
    return response in ['y', 'yes']


def handle_process_formatter_step(guided_migration: GuidedMigration, state: MigrationState, logger: logging.Logger) -> bool:
    """Handle the process formatter step."""
    print("\n🔄 Processing ZIP Files with Formatter")
    
    input_dir = state.data.get('input_directory')
    source_string = state.data.get('source_env_string')
    target_string = state.data.get('target_env_string')
    output_dir = state.data.get('output_directory')
    
    print(f"Input directory: {input_dir}")
    print(f"Source string: {source_string}")
    print(f"Target string: {target_string}")
    if output_dir:
        print(f"Output directory: {output_dir}")
    
    response = input("\nProceed with formatter processing? (y/n): ").strip().lower()
    if response not in ['y', 'yes']:
        return False
    
    try:
        # Execute the formatter step
        success, message = guided_migration.execute_step('process_formatter', state)
        if success:
            print(f"✅ {message}")
            return True
        else:
            print(f"❌ {message}")
            return False
    except Exception as e:
        print(f"❌ Formatter processing failed: {e}")
        return False


def handle_export_profiles_step(guided_migration: GuidedMigration, state: MigrationState, client, logger: logging.Logger) -> bool:
    """Handle the export profiles step."""
    print("\n📤 Export Asset Profiles")
    
    asset_uids_file = state.data.get('asset_uids_file')
    if not asset_uids_file:
        print("❌ Asset UIDs file not found. Please run the formatter step first.")
        return False
    
    print(f"Asset UIDs file: {asset_uids_file}")
    
    response = input("\nProceed with profile export? (y/n): ").strip().lower()
    if response not in ['y', 'yes']:
        return False
    
    try:
        # Execute the export profiles step
        success, message = guided_migration.execute_step('export_profiles', state, client)
        if success:
            print(f"✅ {message}")
            return True
        else:
            print(f"❌ {message}")
            return False
    except Exception as e:
        print(f"❌ Profile export failed: {e}")
        return False


def handle_import_profiles_step(guided_migration: GuidedMigration, state: MigrationState, client, logger: logging.Logger) -> bool:
    """Handle the import profiles step."""
    print("\n📥 Import Asset Profiles")
    
    profiles_file = state.data.get('profiles_export_file')
    if not profiles_file:
        print("❌ Asset profiles export file not found. Please run the export profiles step first.")
        return False
    
    print(f"Profiles file: {profiles_file}")
    
    dry_run = input("\nRun in dry-run mode first? (y/n): ").strip().lower() in ['y', 'yes']
    
    response = input(f"\nProceed with profile import{' (dry-run)' if dry_run else ''}? (y/n): ").strip().lower()
    if response not in ['y', 'yes']:
        return False
    
    try:
        # Execute the import profiles step
        success, message = guided_migration.execute_step('import_profiles', state, client)
        if success:
            print(f"✅ {message}")
            return True
        else:
            print(f"❌ {message}")
            return False
    except Exception as e:
        print(f"❌ Profile import failed: {e}")
        return False


def handle_export_configs_step(guided_migration: GuidedMigration, state: MigrationState, client, logger: logging.Logger) -> bool:
    """Handle the export configs step."""
    print("\n📤 Export Asset Configurations")
    
    asset_uids_file = state.data.get('asset_uids_file')
    if not asset_uids_file:
        print("❌ Asset UIDs file not found. Please run the formatter step first.")
        return False
    
    print(f"Asset UIDs file: {asset_uids_file}")
    
    response = input("\nProceed with configuration export? (y/n): ").strip().lower()
    if response not in ['y', 'yes']:
        return False
    
    try:
        # Execute the export configs step
        success, message = guided_migration.execute_step('export_configs', state, client)
        if success:
            print(f"✅ {message}")
            return True
        else:
            print(f"❌ {message}")
            return False
    except Exception as e:
        print(f"❌ Configuration export failed: {e}")
        return False


def handle_import_configs_step(guided_migration: GuidedMigration, state: MigrationState, client, logger: logging.Logger) -> bool:
    """Handle the import configs step."""
    print("\n📥 Import Asset Configurations")
    
    configs_file = state.data.get('configs_export_file')
    if not configs_file:
        print("❌ Asset configs export file not found. Please run the export configs step first.")
        return False
    
    print(f"Configs file: {configs_file}")
    print("Note: This command will be implemented in a future update.")
    
    response = input("\nProceed with configuration import? (y/n): ").strip().lower()
    if response not in ['y', 'yes']:
        return False
    
    try:
        # Execute the import configs step
        success, message = guided_migration.execute_step('import_configs', state, client)
        if success:
            print(f"✅ {message}")
            return True
        else:
            print(f"❌ {message}")
            return False
    except Exception as e:
        print(f"❌ Configuration import failed: {e}")
        return False


def handle_segments_step(guided_migration: GuidedMigration, state: MigrationState, client, logger: logging.Logger) -> bool:
    """Handle the segments step."""
    print("\n🔗 Handle Segmented Assets")
    
    segments_file = state.data.get('segmented_spark_uids_file')
    if not segments_file:
        print("❌ Segmented SPARK UIDs file not found. Please run the formatter step first.")
        return False
    
    print(f"Segments file: {segments_file}")
    
    response = input("\nProceed with segments handling? (y/n): ").strip().lower()
    if response not in ['y', 'yes']:
        return False
    
    try:
        # Execute the segments step
        success, message = guided_migration.execute_step('handle_segments', state, client)
        if success:
            print(f"✅ {message}")
            return True
        else:
            print(f"❌ {message}")
            return False
    except Exception as e:
        print(f"❌ Segments handling failed: {e}")
        return False


def handle_completion_step(guided_migration: GuidedMigration, state: MigrationState, logger: logging.Logger) -> bool:
    """Handle the completion step."""
    print("\n🎉 Migration Complete!")
    print("Congratulations! Your migration has been completed successfully.")
    
    print("\nFinal steps:")
    print("1. Verify that all assets have been migrated correctly")
    print("2. Test the migrated policies in the target environment")
    print("3. Clean up temporary files if needed")
    print("4. Delete migration state file if no longer needed")
    
    print(f"\nYou can delete the migration state with: delete-migration {state.name}")
    
    return True


def execute_delete_migration(migration_name: str, logger: logging.Logger):
    """Delete a migration state.
    
    Args:
        migration_name: Name of the migration session
        logger: Logger instance
    """
    guided_migration = GuidedMigration(logger)
    
    # Check if migration exists
    state = guided_migration.load_state(migration_name)
    if not state:
        print(f"❌ Migration '{migration_name}' not found!")
        return
    
    print(f"\n🗑️  Delete Migration: {migration_name}")
    print(f"Created: {state.created_at}")
    print(f"Current step: {state.current_step + 1}")
    print(f"Completed steps: {len(state.completed_steps)}")
    
    response = input("\nAre you sure you want to delete this migration? (y/n): ").strip().lower()
    if response not in ['y', 'yes']:
        print("Deletion cancelled.")
        return
    
    if guided_migration.delete_state(migration_name):
        print(f"✅ Migration '{migration_name}' deleted successfully!")
    else:
        print(f"❌ Failed to delete migration '{migration_name}'")


def execute_list_migrations(logger: logging.Logger):
    """List all available migrations.
    
    Args:
        logger: Logger instance
    """
    guided_migration = GuidedMigration(logger)
    
    migrations = guided_migration.list_migrations()
    
    print("\n📋 Available Migrations")
    print("="*40)
    
    if not migrations:
        print("No migrations found.")
        return
    
    for migration in migrations:
        state = guided_migration.load_state(migration)
        if state:
            step_info = guided_migration.get_current_step_info(state)
            print(f"Name: {migration}")
            print(f"Created: {state.created_at}")
            print(f"Current step: {step_info['title']}")
            print(f"Completed: {len(state.completed_steps)}/{len(guided_migration.STEPS)} steps")
            print("-" * 40)


if __name__ == "__main__":
    sys.exit(main()) 