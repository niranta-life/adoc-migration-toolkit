"""
CLI argument parsers.

This module contains functions for creating command line argument parsers.
"""

import argparse

def create_interactive_parser(subparsers):
    """Create the interactive subcommand parser."""
    interactive_parser = subparsers.add_parser(
        'interactive',
        help='Interactive ADOC Migration Toolkit',
        description='ADOC Migration Toolkit for migration ADOC configurations from one environment to another.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m adoc_migration_toolkit interactive --env-file=config.env
  python -m adoc_migration_toolkit interactive --env-file=config.env --verbose

Interactive Commands:  
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
  
  # REST API Commands
  GET /catalog-server/api/assets?uid=123
  PUT /catalog-server/api/assets {"key": "value"}
  GET /catalog-server/api/assets?uid=123 --target-auth --target-tenant

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