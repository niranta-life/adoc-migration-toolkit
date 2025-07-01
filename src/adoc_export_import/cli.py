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
from .core import PolicyExportFormatter, setup_logging
from .api_client import create_api_client


def create_formatter_parser(subparsers):
    """Create the formatter subcommand parser."""
    formatter_parser = subparsers.add_parser(
        'formatter',
        help='Format policy export files by replacing substrings in JSON files and ZIP archives',
        description='Professional JSON String Replacer - Replace substrings in JSON files and ZIP archives',
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
        "--verbose", "-v", 
        action="store_true", 
        help="Enable verbose logging"
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
        "--verbose", "-v", 
        action="store_true", 
        help="Enable verbose logging"
    )
    
    return asset_export_parser


def create_rest_api_parser(subparsers):
    """Create the rest-api subcommand parser."""
    rest_api_parser = subparsers.add_parser(
        'rest-api',
        help='Interactive REST API client for making API calls',
        description='Interactive REST API Client - Make API calls with configurable endpoints and authentication',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m adoc_export_import rest-api --env-file=config.env
  python -m adoc_export_import rest-api --env-file=config.env --verbose

Interactive Commands:
  GET /catalog-server/api/assets?uid=123
  PUT /catalog-server/api/assets {"key": "value"}
  GET /catalog-server/api/assets?uid=123 --target-auth --target-tenant
  segments-export data/output/segmented_spark_uids.csv
  segments-export data/output/segmented_spark_uids.csv --target-auth --target-tenant
  exit

Features:
  - Interactive API client
  - Support for GET and PUT requests
  - Configurable source/target authentication and tenants
  - JSON payload support for PUT requests
  - Well-formatted JSON responses
        """
    )
    
    rest_api_parser.add_argument(
        "--env-file", 
        required=True,
        help="Path to environment file containing AD_HOST, AD_SOURCE_ACCESS_KEY, AD_SOURCE_SECRET_KEY, AD_SOURCE_TENANT"
    )
    rest_api_parser.add_argument(
        "--verbose", "-v", 
        action="store_true", 
        help="Enable verbose logging"
    )
    
    return rest_api_parser


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


def read_csv_uids(csv_file: str, logger: logging.Logger) -> list:
    """Read UIDs from the first column of CSV file.
    
    Args:
        csv_file: Path to the CSV file
        logger: Logger instance
        
    Returns:
        List of UIDs from the first column
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
                if row and len(row) > 0:
                    uid = row[0].strip()
                    if uid:  # Skip empty values
                        uids.append(uid)
                        logger.debug(f"Row {row_num}: Found UID: {uid}")
                    else:
                        logger.warning(f"Row {row_num}: Empty UID value")
                else:
                    logger.warning(f"Row {row_num}: Empty row")
        
        logger.info(f"Read {len(uids)} UIDs from CSV file: {csv_file}")
        return uids
        
    except Exception as e:
        logger.error(f"Error reading CSV file {csv_file}: {e}")
        raise


def run_formatter(args):
    """Run the formatter command."""
    try:
        # Setup logging
        logger = setup_logging(args.verbose)
        
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
        logger = setup_logging(args.verbose)
        
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
        
        for i, uid in enumerate(uids, 1):
            print(f"\n[{i}/{len(uids)}] Processing UID: {uid}")
            print("-" * 60)
            
            try:
                # Make API call
                response_data = client.get_asset_by_uid(uid)
                
                # Display formatted JSON response
                print(json.dumps(response_data, indent=2, ensure_ascii=False))
                successful += 1
                
            except Exception as e:
                logger.error(f"Failed to get asset details for UID {uid}: {e}")
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
        Tuple of (method, endpoint, json_payload, use_target_auth, use_target_tenant)
    """
    parts = command.strip().split()
    if not parts:
        return None, None, None, False, False
    
    method = parts[0].upper()
    if method not in ['GET', 'PUT']:
        raise ValueError(f"Unsupported HTTP method: {method}")
    
    if len(parts) < 2:
        raise ValueError("Endpoint is required")
    
    endpoint = parts[1]
    json_payload = None
    use_target_auth = False
    use_target_tenant = False
    
    # Check for flags
    if '--target-auth' in parts:
        use_target_auth = True
        parts.remove('--target-auth')
    
    if '--target-tenant' in parts:
        use_target_tenant = True
        parts.remove('--target-tenant')
    
    # For PUT requests, look for JSON payload
    if method == 'PUT' and len(parts) > 2:
        # Join remaining parts and try to parse as JSON
        json_str = ' '.join(parts[2:])
        try:
            json_payload = json.loads(json_str)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON payload: {e}")
    
    return method, endpoint, json_payload, use_target_auth, use_target_tenant


def parse_segments_export_command(command: str) -> tuple:
    """Parse a segments-export command string into components.
    
    Args:
        command: Command string like "segments-export <csv_file> [--target-auth] [--target-tenant]"
        
    Returns:
        Tuple of (csv_file, use_target_auth, use_target_tenant)
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'segments-export':
        return None, False, False
    
    if len(parts) < 2:
        raise ValueError("CSV file path is required for segments-export command")
    
    csv_file = parts[1]
    use_target_auth = False
    use_target_tenant = False
    
    # Check for flags
    if '--target-auth' in parts:
        use_target_auth = True
        parts.remove('--target-auth')
    
    if '--target-tenant' in parts:
        use_target_tenant = True
        parts.remove('--target-tenant')
    
    return csv_file, use_target_auth, use_target_tenant


def execute_segments_export(csv_file: str, client, use_target_auth: bool, use_target_tenant: bool, logger: logging.Logger):
    """Execute the segments-export command.
    
    Args:
        csv_file: Path to the CSV file containing UIDs
        client: API client instance
        use_target_auth: Whether to use target authentication
        use_target_tenant: Whether to use target tenant
        logger: Logger instance
    """
    try:
        # Read UIDs from CSV file
        uids = read_csv_uids(csv_file, logger)
        
        if not uids:
            logger.warning("No UIDs found in CSV file")
            return
        
        print(f"\nProcessing {len(uids)} UIDs from CSV file: {csv_file}")
        print("="*80)
        
        for i, uid in enumerate(uids, 1):
            print(f"\n[{i}/{len(uids)}] Processing UID: {uid}")
            print("-" * 60)
            
            try:
                # Step 1: Get asset details by UID
                print(f"Getting asset details for UID: {uid}")
                asset_response = client.make_api_call(
                    endpoint=f"/catalog-server/api/assets?uid={uid}",
                    method='GET',
                    use_target_auth=use_target_auth,
                    use_target_tenant=use_target_tenant
                )
                
                # Step 2: Extract the top-level "id" field
                if not asset_response or 'data' not in asset_response:
                    print(f"❌ No 'data' field found in asset response for UID: {uid}")
                    continue
                
                data_array = asset_response['data']
                if not data_array or len(data_array) == 0:
                    print(f"❌ Empty 'data' array in asset response for UID: {uid}")
                    continue
                
                first_asset = data_array[0]
                if 'id' not in first_asset:
                    print(f"❌ No 'id' field found in first asset for UID: {uid}")
                    continue
                
                asset_id = first_asset['id']
                print(f"Extracted asset ID: {asset_id}")
                
                # Step 3: Get segments for the asset
                print(f"Getting segments for asset ID: {asset_id}")
                segments_response = client.make_api_call(
                    endpoint=f"/catalog-server/api/assets/{asset_id}/segments",
                    method='GET',
                    use_target_auth=use_target_auth,
                    use_target_tenant=use_target_tenant
                )
                
                # Step 4: Display formatted segments JSON
                print("Segments Response:")
                print(json.dumps(segments_response, indent=2, ensure_ascii=False))
                
            except Exception as e:
                logger.error(f"Failed to process UID {uid}: {e}")
                print(f"❌ Error processing UID {uid}: {e}")
        
        print("\n" + "="*80)
        print("SEGMENTS EXPORT COMPLETED")
        print("="*80)
        
    except Exception as e:
        logger.error(f"Error in segments-export: {e}")
        print(f"❌ Error: {e}")


def setup_autocomplete():
    """Setup autocomplete for the interactive session."""
    # Available commands and their completions
    commands = [
        'GET',
        'PUT', 
        'segments-export',
        'exit',
        'quit',
        'q'
    ]
    
    # Common endpoints for completion
    endpoints = [
        '/catalog-server/api/assets',
        '/catalog-server/api/assets?uid=',
        '/catalog-server/api/assets/',
        '/catalog-server/api/health'
    ]
    
    # Flags for completion
    flags = [
        '--target-auth',
        '--target-tenant'
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
            if words[0].upper() in ['GET', 'PUT']:
                # Second word for GET/PUT - suggest endpoints
                options = [endpoint for endpoint in endpoints if endpoint.startswith(text)]
            elif words[0].lower() == 'segments-export':
                # Second word for segments-export - suggest CSV files
                # This is a simple suggestion - could be enhanced to scan directory
                options = ['data/samples_import_ready/segmented_spark_uids.csv', 'data/samples/assets.csv']
        elif len(words) >= 2:
            # Additional words - suggest flags
            if text.startswith('--'):
                options = [flag for flag in flags if flag.startswith(text)]
            elif words[0].upper() == 'PUT' and len(words) == 3:
                # Third word for PUT - suggest JSON payload start
                options = ['{"', '{']
        
        # Return the option at the requested state index
        if state < len(options):
            return options[state]
        else:
            return None
    
    # Set the completer function
    readline.set_completer(completer)
    
    # Enable tab completion
    readline.parse_and_bind('tab: complete')


def run_rest_api(args):
    """Run the interactive REST API client."""
    try:
        # Setup logging
        logger = setup_logging(args.verbose)
        
        # Validate arguments
        if not args.env_file or not args.env_file.strip():
            raise ValueError("Environment file path cannot be empty")
        
        env_path = Path(args.env_file)
        if not env_path.exists():
            raise FileNotFoundError(f"Environment file does not exist: {args.env_file}")
        
        if not env_path.is_file():
            raise ValueError(f"Environment path is not a file: {args.env_file}")
        
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
        
        print("\n" + "="*80)
        print("INTERACTIVE ADOC REST API CLIENT")
        print("="*80)
        print(f"Host: {client.host}")
        print(f"Source Tenant: {client.tenant}")
        if hasattr(client, 'target_tenant') and client.target_tenant:
            print(f"Target Tenant: {client.target_tenant}")
        print("\nCommands:")
        print("  GET <endpoint> [--target-auth] [--target-tenant]")
        print("  PUT <endpoint> <json_payload> [--target-auth] [--target-tenant]")
        print("  segments-export <csv_file> [--target-auth] [--target-tenant]")
        print("  exit")
        print("="*80)
        print("Use ↑/↓ arrow keys to navigate command history")
        print("Use TAB key for command autocomplete")
        print("="*80)
        
        while True:
            try:
                # Get user input
                command = input("\nADOC> ").strip()
                
                if not command:
                    continue
                
                if command.lower() in ['exit', 'quit', 'q']:
                    print("Goodbye!")
                    break
                
                # Check if it's a segments-export command
                if command.lower().startswith('segments-export'):
                    csv_file, use_target_auth, use_target_tenant = parse_segments_export_command(command)
                    if csv_file:
                        execute_segments_export(csv_file, client, use_target_auth, use_target_tenant, logger)
                    continue
                
                # Parse the command for GET/PUT requests
                method, endpoint, json_payload, use_target_auth, use_target_tenant = parse_api_command(command)
                
                if method is None:
                    continue
                
                # Make the API call
                print(f"\nMaking {method} request to: {endpoint}")
                if use_target_auth:
                    print("Using target authentication")
                if use_target_tenant:
                    print("Using target tenant")
                print("-" * 60)
                
                response_data = client.make_api_call(
                    endpoint=endpoint,
                    method=method,
                    json_payload=json_payload,
                    use_target_auth=use_target_auth,
                    use_target_tenant=use_target_tenant
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
  rest-api      Interactive REST API client for making API calls

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
    create_rest_api_parser(subparsers)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    if args.command == 'formatter':
        return run_formatter(args)
    elif args.command == 'asset-export':
        return run_asset_export(args)
    elif args.command == 'rest-api':
        return run_rest_api(args)
    else:
        print(f"Unknown command: {args.command}")
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main()) 