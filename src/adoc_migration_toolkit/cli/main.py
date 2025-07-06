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