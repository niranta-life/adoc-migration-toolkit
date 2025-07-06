"""
CLI main execution functions.

This module contains the main CLI execution logic including the main function
and command execution functions.
"""

import argparse
import sys
import json
import logging
from pathlib import Path

from ..shared.logging import setup_logging
from ..shared.api_client import create_api_client
from .validators import validate_asset_export_arguments
from ..execution.utils import read_csv_uids


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


def run_interactive(args):
    """Run the interactive command."""
    from ..execution.interactive import run_interactive as run_interactive_impl
    return run_interactive_impl(args)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog='adoc-migration-toolkit',
        description='ADOC Migration Toolkit - Professional tool for migrating Acceldata policies and assets',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export assets from CSV file
  python -m adoc_migration_toolkit asset-export --csv-file=data/asset_uids.csv --env-file=config.env
  
  # Interactive mode
  python -m adoc_migration_toolkit interactive --env-file=config.env

For more information, visit: https://github.com/your-repo/adoc-migration-toolkit
        """
    )
    
    # Add global arguments
    parser.add_argument(
        '--version', '-V',
        action='version',
        version='adoc-migration-toolkit 1.0.0'
    )
    
    # Create subparsers for commands
    subparsers = parser.add_subparsers(
        dest='command',
        help='Available commands'
    )

    # Add command parsers
    from .parsers import create_interactive_parser

    create_interactive_parser(subparsers)
    
    # Parse arguments
    args = parser.parse_args()
    
    # Handle no command specified
    if not args.command:
        parser.print_help()
        return 1
    
    # Execute command
    if args.command == 'interactive':
        return run_interactive(args)
    else:
        print(f"Unknown command: {args.command}")
        return 1


if __name__ == '__main__':
    sys.exit(main()) 