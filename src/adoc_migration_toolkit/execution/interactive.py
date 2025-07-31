"""
Interactive mode execution functions.

This module contains execution functions for interactive mode operations
including help, autocomplete, and command history management.
"""

import sys
import json
import getpass
import signal
import logging
from datetime import datetime
from pathlib import Path

from .command_parsing import parse_run_profile_command
from .notification_operations import precheck_on_notifications
from .profile_operations import check_for_profiling_required_before_migration, trigger_profile_action
from .segment_operations import execute_segments_export, execute_segments_import
from ..shared.logging import setup_logging
from adoc_migration_toolkit.execution.output_management import load_global_output_directory
from ..shared.api_client import create_api_client
from .asset_operations import execute_asset_profile_export, execute_asset_profile_export_parallel, execute_asset_profile_import, execute_asset_config_export, execute_asset_config_export_parallel, execute_asset_list_export, execute_asset_list_export_parallel, execute_asset_tag_import, execute_asset_config_import
from .policy_operations import execute_policy_list_export, execute_policy_list_export_parallel, execute_policy_export, execute_policy_export_parallel, execute_policy_import
from .policy_operations import execute_rule_tag_export, execute_rule_tag_export_parallel
from .formatter import execute_formatter, parse_formatter_command
from adoc_migration_toolkit.shared import globals

# Import cross-platform readline wrapper
from ..shared.readline_wrapper import (
    set_history_file, set_completer, parse_and_bind, read_history_file,
    write_history_file, clear_history, add_history, get_current_history_length,
    get_history_item, get_line_buffer, input_with_history
)

import os


def log_session_event(logger, event_type: str, user_info: dict = None):
    """Log session start or exit events.
    
    Args:
        logger: Logger instance
        event_type: 'start' or 'exit'
        user_info: Dictionary containing user information
    """
    try:
        if not user_info:
            user_info = {}
        
        username = user_info.get('username', getpass.getuser())
        hostname = user_info.get('hostname', os.uname().nodename if hasattr(os, 'uname') else 'unknown')
        session_id = user_info.get('session_id', datetime.now().strftime('%Y%m%d_%H%M%S'))
        
        if event_type == 'start':
            logger.info(f"INTERACTIVE SESSION STARTED - User: {username}, Host: {hostname}, Session: {session_id}")
        elif event_type == 'exit':
            logger.info(f"INTERACTIVE SESSION EXITED - User: {username}, Host: {hostname}, Session: {session_id}")
    except Exception as e:
        # Don't let logging errors break the session
        try:
            logger.warning(f"Failed to log session event: {e}")
        except:
            pass


def get_user_session_info():
    """Get user and session information for logging.
    
    Returns:
        dict: User and session information
    """
    try:
        return {
            'username': getpass.getuser(),
            'hostname': os.uname().nodename if hasattr(os, 'uname') else 'unknown',
            'session_id': datetime.now().strftime('%Y%m%d_%H%M%S'),
            'pid': os.getpid()
        }
    except Exception:
        return {
            'username': 'unknown',
            'hostname': 'unknown',
            'session_id': datetime.now().strftime('%Y%m%d_%H%M%S'),
            'pid': os.getpid()
        }


def show_interactive_help():
    """Display concise help information for all available interactive commands."""
    # ANSI escape codes for formatting
    BOLD = '\033[1m'
    RESET = '\033[0m'
    
    # Get current output directory for dynamic paths
    current_output_dir = globals.GLOBAL_OUTPUT_DIR
    
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
    print("    Export segments from source environment to CSV file")
    print(f"  {BOLD}segments-import{RESET} <csv_file> [--dry-run] [--quiet] [--verbose]")
    print("    Import segments to target environment from CSV file")
    
    print(f"\n{BOLD}🔧 ASSET PROFILE COMMANDS:{RESET}")
    print(f"  {BOLD}asset-profile-export{RESET} [<csv_file>] [--output-file <file>] [--quiet] [--verbose] [--parallel] [--allowed-types <types>]")
    print("    Export asset profiles from source environment to CSV file")
    print(f"  {BOLD}asset-profile-import{RESET} [<csv_file>] [--dry-run] [--quiet] [--verbose] [--allowed-types <types>]")
    print("    Import asset profiles to target environment from CSV file")
    print(f"  {BOLD}profile-check{RESET} [<csv_file>] [--dry-run] [--quiet] [--verbose]")
    print("    Checks the assets configured with the provided policy type and not yet profiled.")
    print(f"  {BOLD}profile-run{RESET} [<csv_file>] [--run-profile] [--dry-run] [--quiet] [--verbose]")
    print("    Triggers the profiling[Changes the engine type to Pushdown] for the given assets supplied from CSV file.")
    
    print(f"\n{BOLD}🔍 ASSET CONFIGURATION COMMANDS:{RESET}")
    print(f"  {BOLD}asset-config-export{RESET} [<csv_file>] [--output-file <file>] [--quiet] [--verbose] [--parallel] [--allowed-types <types>]")
    print("    Export asset configurations from source environment to CSV file")
    print(f"  {BOLD}asset-config-import{RESET} [<csv_file>] [--dry-run] [--quiet] [--verbose] [--parallel] [--allowed-types <types>]")
    print("    Import asset configurations to target environment from CSV file")
    print(f"  {BOLD}asset-list-export{RESET} [--quiet] [--verbose] [--parallel] [--target] [--page-size <size>]")
    print("    Export all assets from source or target environment to CSV file")
    print(f"  {BOLD}asset-tag-import{RESET} [csv_file] [--quiet] [--verbose] [--parallel]")
    print("    Import tags for assets from CSV file")
    
    
    print(f"\n{BOLD}📋 POLICY COMMANDS:{RESET}")
    print(f"  {BOLD}policy-list-export{RESET} [--quiet] [--verbose] [--parallel] [--existing-target-assets]")
    print("    Export all policies from source environment to CSV file")
    print(f"  {BOLD}policy-export{RESET} [--type <export_type>] [--filter <filter_value>] [--quiet] [--verbose] [--batch-size <size>] [--parallel]")
    print("    Export policy definitions by different categories from source environment to ZIP files")
    print(f"  {BOLD}policy-import{RESET} <file_or_pattern> [--quiet] [--verbose]")
    print("    Import policy definitions from ZIP files to target environment")
    print(f"  {BOLD}rule-tag-export{RESET} [--quiet] [--verbose] [--parallel]")
    print("    Export rule tags for all policies from policies-all-export.csv")
    print(f"  {BOLD}policy-xfr{RESET} [--input <input_dir>] --string-transform \"A\":\"B\", \"C\":\"D\", \"E\":\"F\" [options]")
    print("    Format policy export files by replacing multiple substrings in JSON files and ZIP archives")
    print(f"  {BOLD}transform-and-merge{RESET} --string-transform \"A\":\"B\", \"C\":\"D\" [--quiet] [--verbose]")
    print("    Transform and merge asset CSV files from source and target environments")
    
    print(f"\n{BOLD}🔧 NOTIFICATION COMMANDS:{RESET}")
    print(f"  {BOLD}notifications-check{RESET} --source-context <id> --target-context <id> --assembly-ids <ids> [--quiet] [--verbose] [--parallel] [--page-size <size>]")
    print("    Compare notification groups between source and target environments and report differences.")

    print(f"\n{BOLD}🔧 VCS COMMANDS:{RESET}")
    print(f"  {BOLD}vcs-config{RESET} [--vcs-type <type>] [--remote-url <url>] [--username <user>] [--token <token>] [options]")
    print("    Configure enterprise VCS settings (Git/Mercurial/Subversion, HTTPS/SSH, proxy)")
    print(f"  {BOLD}vcs-init{RESET} [<base directory>]")
    print("    Initialize a VCS repository (Git or Mercurial) in the output directory or specified directory")
    print(f"  {BOLD}vcs-pull{RESET}")
    print("    Pull updates from the configured repository with authentication")
    print(f"  {BOLD}vcs-push{RESET}")
    print("    Push changes to the remote repository with authentication")
    
    print(f"\n{BOLD}🌐 REST API COMMANDS:{RESET}")
    print(f"  {BOLD}GET{RESET} <endpoint> [--target]")
    print("    Make GET request to API endpoint (use --target for target environment)")
    print(f"  {BOLD}PUT{RESET} <endpoint> <json_payload> [--target]")
    print("    Make PUT request to API endpoint with JSON payload (use --target for target environment)")
    print("    Examples:")
    print("      GET /catalog-server/api/assets?uid=123")
    print("      GET /catalog-server/api/assets?uid=snowflake_krish_test.DEMO_DB.CS_DEMO.Customer_Sample --target")
    print("      PUT /catalog-server/api/assets {\"key\": \"value\"}")
    
    print(f"\n{BOLD}🛠️ UTILITY COMMANDS:{RESET}")
    print(f"  {BOLD}set-output-dir{RESET} <directory>")
    print("    Set global output directory for all export commands")
    print(f"  {BOLD}set-log-level{RESET} <level>")
    print("    Change log level dynamically (ERROR, WARNING, INFO, DEBUG)")
    print(f"  {BOLD}set-http-config{RESET} [--timeout x] [--retry x] [--proxy url]")
    print("    Configure HTTP timeout, retry, and proxy settings")
    print(f"  {BOLD}show-config{RESET}")
    print("    Display current configuration (HTTP, logging, environment, output)")
    print(f"  {BOLD}help{RESET}")
    print("    Show this help information")
    print(f"  {BOLD}help <command>{RESET}")
    print("    Show detailed help for a specific command")
    print(f"  {BOLD}history{RESET}")
    print("    Show the last 25 commands with numbers")
    print(f"  {BOLD}exit, quit, q{RESET}")
    print("    Exit the interactive client")
    
    print(f"\n{BOLD}💡 TIPS:{RESET}")
    print("  • Use TAB key for command autocomplete")
    print("  • Use ↑/↓ arrow keys to navigate command history")
    print("  • Type 'help <command>' for detailed help on any command")
    print("  • Use --dry-run to preview changes before making them")
    print("  • Use --verbose to see detailed API request/response information")
    print("  • Set output directory once with set-output-dir to avoid specifying --output-file repeatedly")


def show_command_help(command_name: str):
    """Display detailed help information for a specific command."""
    # ANSI escape codes for formatting
    BOLD = '\033[1m'
    RESET = '\033[0m'
    
    # Get current output directory for dynamic paths
    current_output_dir = globals.GLOBAL_OUTPUT_DIR
    
    print(f"\n" + "="*80)
    print(f"ADOC INTERACTIVE MIGRATION TOOLKIT - DETAILED HELP FOR: {command_name.upper()}")
    print("="*80)
    
    # Show current output directory status
    if current_output_dir:
        print(f"\n📁 Current Output Directory: {current_output_dir}")
    else:
        print(f"\n📁 Current Output Directory: Not set (will use default: adoc-migration-toolkit-YYYYMMDDHHMM)")
    print("="*80)
    
    # Command-specific help content
    if command_name == 'segments-export':
        print(f"\n{BOLD}segments-export{RESET} [<csv_file>] [--output-file <file>] [--quiet]")
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
    
    elif command_name == 'segments-import':
        print(f"\n{BOLD}segments-import{RESET} <csv_file> [--dry-run] [--quiet] [--verbose]")
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
    
    elif command_name == 'asset-profile-export':
        print(f"\n{BOLD}asset-profile-export{RESET} [<csv_file>] [--output-file <file>] [--quiet] [--verbose] [--parallel]")
        print("    Description: Export asset profiles from source environment to CSV file")
        print("    Arguments:")
        print("      csv_file: Path to CSV file with source-env and target-env mappings (optional)")
        print("      --output-file: Specify custom output file (optional)")
        print("      --quiet: Suppress console output, show only summary (default)")
        print("      --verbose: Show detailed output including headers and responses")
        print("      --parallel: Use parallel processing for faster export (max 5 threads)")
        print("    Examples:")
        print("      asset-profile-export")
        print("      asset-profile-export <output-dir>/asset-export/asset_uids.csv")
        print("      asset-profile-export uids.csv --output-file profiles.csv --verbose")
        print("      asset-profile-export --parallel")
        print("    Behavior:")
        print("      • If no CSV file specified, uses default from output directory")
        print("      • Default input: <output-dir>/asset-export/asset_uids.csv")
        print("      • Default output: <output-dir>/asset-import/asset-profiles-import-ready.csv")
        print("      • Reads source-env and target-env mappings from CSV file")
        print("      • Makes API calls to get asset profiles from source environment")
        print("      • Writes profile JSON data to output CSV file")
        print("      • Shows minimal output by default, use --verbose for detailed information")
        print("      • Parallel mode: Uses up to 5 threads to process assets simultaneously")
        print("      • Parallel mode: Each thread has its own progress bar")
        print("      • Parallel mode: Significantly faster for large asset sets")
    
    elif command_name == 'asset-profile-import':
        print(f"\n{BOLD}asset-profile-import{RESET} [<csv_file>] [--dry-run] [--quiet] [--verbose]")
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
    
    elif command_name == 'asset-config-export':
        print(f"\n{BOLD}asset-config-export{RESET} [<csv_file>] [--output-file <file>] [--quiet] [--verbose] [--parallel]")
        print("    Description: Export asset configurations from source environment to CSV file")
        print("    Arguments:")
        print("      csv_file: Path to CSV file with 5 columns: source_id, source_uid, target_id, target_uid, tags (optional)")
        print("      --output-file: Specify custom output file (optional)")
        print("      --quiet: Suppress console output, show only summary")
        print("      --verbose: Show detailed output including headers and responses")
        print("      --parallel: Use parallel processing for faster export (max 5 threads, quiet mode default)")
        print("    Examples:")
        print("      asset-config-export")
        print("      asset-config-export <output-dir>/asset-import/asset-merged-all.csv")
        print("      asset-config-export uids.csv --output-file configs.csv --verbose")
        print("      asset-config-export --parallel")
        print("      asset-config-export --parallel --verbose")
        print("    Behavior:")
        print("      • Reads from asset-import/asset-merged-all.csv by default if no CSV file specified")
        print("      • Reads CSV with 5 columns: source_id, source_uid, target_id, target_uid, tags")
        print("      • Uses source_id to call '/catalog-server/api/assets/<source_id>/config'")
        print("      • Writes compressed JSON response to CSV with target_uid")
        print("      • Shows status for each asset in quiet mode")
        print("      • Shows HTTP headers and response objects in verbose mode")
        print("      • Output format: target_uid, config_json (compressed)")
        print("      • Parallel mode: Uses up to 5 threads, work divided equally between threads")
        print("      • Parallel mode: Quiet mode is default (shows tqdm progress bars)")
        print("      • Parallel mode: Use --verbose to see URL, headers, and response for each call")
        print("      • Thread names: Rocket, Lightning, Unicorn, Dragon, Shark (with green progress bars)")
        print("      • Default mode: Silent (no progress bars)")
    
    elif command_name == 'asset-config-import':
        print(f"\n{BOLD}asset-config-import{RESET} [<csv_file>] [--dry-run] [--quiet] [--verbose] [--parallel]")
        print("    Description: Import asset configurations to target environment from CSV file")
        print("    Arguments:")
        print("      csv_file: Path to CSV file with target_uid and config_json columns (optional)")
        print("      --dry-run: Preview requests and payloads without making API calls")
        print("      --quiet: Show progress bars (default for parallel mode)")
        print("      --verbose: Show detailed output including HTTP requests and responses")
        print("      --parallel: Use parallel processing for faster import (max 5 threads)")
        print("    Examples:")
        print("      asset-config-import")
        print("      asset-config-import /path/to/asset-config-import-ready.csv")
        print("      asset-config-import --dry-run --quiet --parallel")
        print("      asset-config-import --verbose")
        print("    Behavior:")
        print("      • Reads from asset-import/asset-config-import-ready.csv by default if no CSV file specified")
        print("      • Reads CSV with 2 columns: target_uid, config_json")
        print("      • Gets asset ID using GET /catalog-server/api/assets?uid=<target_uid>")
        print("      • Updates config using PUT /catalog-server/api/assets/<id>/config")
        print("      • Shows progress bar in quiet mode")
        print("      • Shows HTTP details in verbose mode")
        print("      • Parallel mode: Uses up to 5 threads, work divided equally between threads")
        print("      • Parallel mode: Quiet mode is default (shows tqdm progress bars)")
        print("      • Parallel mode: Use --verbose to see HTTP details for each call")
        print("      • Thread names: Rocket, Lightning, Unicorn, Dragon, Shark (with green progress bars)")
        print("      • Default mode: Silent (no progress bars)")
    
    elif command_name == 'asset-list-export':
        print(f"\n{BOLD}asset-list-export{RESET} [--quiet] [--verbose] [--parallel] [--target] [--page-size <size>]")
        print("    Description: Export all assets from source or target environment to CSV file")
        print("    Arguments:")
        print("      --quiet: Suppress console output, show only summary")
        print("      --verbose: Show detailed output including headers and responses")
        print("      --parallel: Use parallel processing for faster export (max 5 threads)")
        print("      --target: Use target environment instead of source environment")
        print("      --page-size: Number of assets per page (default: 500)")
        print("      --source_type_ids <list>     Comma-separated list of source type IDs (optional)")
        print("      --asset_type_ids <list>      Comma-separated list of asset type IDs (optional)")
        print("      --assembly_ids <list>        Comma-separated list of assembly IDs (optional)")
        print("    Examples:")
        print("      asset-list-export")
        print("      asset-list-export --quiet")
        print("      asset-list-export --verbose")
        print("      asset-list-export --parallel")
        print("      asset-list-export --parallel --source_type_ids=5 --asset_type_ids=2,23,53 --assembly_ids=100,101 ")
        print("      asset-list-export --target")
        print("      asset-list-export --target --verbose")
        print("      asset-list-export --page-size 1000")
        print("      asset-list-export --page-size 250 --parallel --source_type_ids=5 --asset_type_ids=2,23,53 --assembly_ids=100,101")
        print("    Behavior:")
        print("      • Uses '/catalog-server/api/assets/discover' endpoint with pagination")
        print("      • First call gets total count with size=0&page=0&profiled_assets=true&parents=true")
        print("      • Retrieves all pages with specified page size and profiled_assets=true&parents=true")
        print("      • Default page size: 500 assets per page")
        print("      • Source environment output: <output-dir>/asset-export/asset-all-source-export.csv")
        print("      • Target environment output: <output-dir>/asset-export/asset-all-target-export.csv")
        print("      • CSV columns: source_uid, source_id, target_uid, tags")
        print("      • Extracts asset.uid, asset.id, and asset.tags[].name from response")
        print("      • Concatenates tags with colon (:) separator in tags column")
        print("      • Sorts output by source_uid first, then by source_id")
        print("      • Shows page-by-page progress in quiet mode")
        print("      • Shows detailed request/response in verbose mode")
        print("      • Provides comprehensive statistics upon completion")
        print("      • Parallel mode: Divides pages among threads, combines results, deletes temp files")
        print("      • Target mode: Uses target access key, secret key, and tenant for authentication")
    
    elif command_name == 'asset-tag-import':
        print(f"\n{BOLD}asset-tag-import{RESET} [csv_file] [--quiet] [--verbose] [--parallel]")
        print("    Description: Import tags for assets from CSV file")
        print("    Arguments:")
        print("      csv_file: Path to CSV file (defaults to asset-merged-all.csv)")
        print("    Options:")
        print("      --quiet, -q: Suppress console output, show only summary")
        print("      --verbose, -v: Show detailed output including API calls")
        print("      --parallel, -p: Use parallel processing for faster import")
        print("    Examples:")
        print("      asset-tag-import")
        print("      asset-tag-import --quiet")
        print("      asset-tag-import --verbose")
        print("      asset-tag-import --parallel")
        print("      asset-tag-import /path/to/asset-data.csv --verbose --parallel")
        print("    Behavior:")
        print("      • Reads asset data from asset-merged-all.csv (or specified file)")
        print("      • Processes each asset to get asset ID from target_uid")
        print("      • Imports tags for each asset using POST /catalog-server/api/assets/<id>/tag")
        print("      • Tags are colon-separated in the CSV file (5th column)")
        print("      • Uses target environment authentication")
        print("      • Shows progress bar in quiet mode")
        print("      • Shows detailed API calls in verbose mode")
        print("      • Parallel mode: Uses up to 5 threads for faster processing")
        print("      • Provides comprehensive statistics upon completion")
    

    
    elif command_name == 'policy-list-export':
        print(f"\n{BOLD}policy-list-export{RESET} [--quiet] [--verbose] [--parallel] [--existing-target-assets]")
        print("    Description: Export all policies from source environment to CSV file")
        print("    Arguments:")
        print("      --quiet: Suppress console output, show only summary")
        print("      --verbose: Show detailed output including headers and responses")
        print("      --parallel: Use parallel processing for faster export (max 5 threads)")
        print("      --existing-target-assets: Only include policies for assets that exist in target environment")
        print("    Examples:")
        print("      policy-list-export")
        print("      policy-list-export --quiet")
        print("      policy-list-export --verbose")
        print("      policy-list-export --parallel")
        print("      policy-list-export --parallel --quiet")
        print("      policy-list-export --existing-target-assets")
        print("      policy-list-export --existing-target-assets --verbose")
        print("    Behavior:")
        print("      • Uses '/catalog-server/api/rules' endpoint with pagination")
        print("      • First call gets total count with page=0&size=0")
        print("      • Retrieves all pages with size=1000 (default)")
        print("      • Output file: <output-dir>/policy-export/policies-all-export.csv")
        print("      • CSV columns: id, type, engineType, tableAssetIds, assemblyIds, assemblyNames, sourceTypes")
        print("      • Sorts output by id")
        print("      • Shows page-by-page progress in quiet mode")
        print("      • Shows detailed request/response in verbose mode")
        print("      • Provides comprehensive statistics upon completion")
        print("      • --existing-target-assets: Reads asset-merged-all.csv from asset-import/ directory")
        print("      • --existing-target-assets: Only includes policies where tableAssetId matches source_id in merged file")
        print("      • --existing-target-assets: Optimized with in-memory asset ID lookup")
        print("      • Parallel mode: Uses up to 5 threads with minimum 10 policies per thread")
        print("      • Parallel mode: Each thread has its own progress bar")
        print("      • Parallel mode: Automatic retry (3 attempts) on failures")
        print("      • Parallel mode: Temporary files merged into final output")
    
    elif command_name == 'policy-export':
        print(f"\n{BOLD}policy-export{RESET} [--type <export_type>] [--filter <filter_value>] [--quiet] [--verbose] [--batch-size <size>] [--parallel]")
        print("    Description: Export policy definitions by different categories from source environment to ZIP files")
        print("    Arguments:")
        print("      --type: Export type (rule-types, engine-types, assemblies, source-types)")
        print("      --filter: Optional filter value within the export type")
        print("      --quiet: Suppress console output, show only summary")
        print("      --verbose: Show detailed output including headers and responses")
        print("      --batch-size: Number of policies to export in each batch (default: 50)")
        print("      --parallel: Use parallel processing for faster export (max 5 threads)")
        print("    Examples:")
        print("      policy-export")
        print("      policy-export --type rule-types")
        print("      policy-export --type engine-types --filter JDBC_URL")
        print("      policy-export --type assemblies --filter production-db")
        print("      policy-export --type source-types --filter PostgreSQL")
        print("      policy-export --type rule-types --batch-size 100 --quiet")
        print("      policy-export --type rule-types --parallel")
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
        print("      • Parallel mode: Uses up to 5 threads to process different policy types simultaneously")
        print("      • Parallel mode: Each thread has its own progress bar showing batch completion")
        print("      • Parallel mode: Significantly faster for large exports with multiple policy types")
    
    elif command_name == 'policy-import':
        print(f"\n{BOLD}policy-import{RESET} <file_or_pattern> [--quiet] [--verbose]")
        print("    Description: Import policy definitions from ZIP files to target environment")
        print("    Arguments:")
        print("      file_or_pattern: ZIP file path or glob pattern (e.g., *.zip)")
        print("      --quiet: Suppress console output, show only summary")
        print("      --verbose: Show detailed output including headers and responses")
        print("    Examples:")
        print("      policy-import *.zip")
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
    
    elif command_name == 'rule-tag-export':
        print(f"\n{BOLD}rule-tag-export{RESET} [--quiet] [--verbose] [--parallel]")
        print("    Description: Export rule tags for all policies from policies-all-export.csv")
        print("    Arguments:")
        print("      --quiet: Suppress console output, show only summary with progress bar")
        print("      --verbose: Show detailed output including headers and responses")
        print("      --parallel: Use parallel processing for faster export (max 5 threads)")
        print("    Examples:")
        print("      rule-tag-export")
        print("      rule-tag-export --quiet")
        print("      rule-tag-export --verbose")
        print("      rule-tag-export --parallel")
        print("    Behavior:")
        print("      • Automatically runs policy-list-export if policies-all-export.csv doesn't exist")
        print("      • Reads rule IDs from <output-dir>/policy-export/policies-all-export.csv (first column)")
        print("      • Makes API calls to '/catalog-server/api/rules/<id>/tags' for each rule")
        print("      • Extracts tag names from the response")
        print("      • Outputs to <output-dir>/policy-export/rule-tags-export.csv with rule ID and comma-separated tags")
        print("      • Shows progress bar in quiet mode")
        print("      • Shows detailed API calls in verbose mode")
        print("      • Provides comprehensive statistics upon completion")
        print("      • Parallel mode: Uses up to 5 threads to process rules simultaneously")
        print("      • Parallel mode: Each thread has its own progress bar")
        print("      • Parallel mode: Significantly faster for large rule sets")
    
    elif command_name == 'policy-xfr':
        print(f"\n{BOLD}policy-xfr{RESET} [--input <input_dir>] --string-transform \"A\":\"B\", \"C\":\"D\", \"E\":\"F\" [options]")
        print("    Description: Format policy export files by replacing multiple substrings in JSON files and ZIP archives")
        print("    Arguments:")
        print("      --string-transform: Multiple string transformations [REQUIRED]")
        print("                          Format: \"A\":\"B\", \"C\":\"D\", \"E\":\"F\"")
        print("    Options:")
        print("      --input: Input directory (auto-detected from policy-export if not specified)")
        print("      --output-dir: Output directory (defaults to organized subdirectories)")
        print("      --quiet: Suppress console output, show only summary")
        print("      --verbose: Show detailed output including processing details")
        print("    Examples:")
        print("      policy-xfr --string-transform \"PROD_DB\":\"DEV_DB\", \"PROD_URL\":\"DEV_URL\"")
        print("      policy-xfr --input data/samples --string-transform \"old\":\"new\", \"test\":\"prod\"")
        print("      policy-xfr --string-transform \"A\":\"B\", \"C\":\"D\", \"E\":\"F\" --verbose")
        print("    Legacy Support:")
        print("      policy-xfr --source-env-string \"PROD_DB\" --target-env-string \"DEV_DB\"")
        print("    Behavior:")
        print("      • Processes JSON files and ZIP archives in the input directory")
        print("      • Replaces all occurrences of multiple source strings with their target strings")
        print("      • Maintains file structure and count")
        print("      • Auto-detects input directory from <output-dir>/policy-export if not specified")
        print("      • Creates organized output directory structure")
        print("      • Extracts data quality policy assets to CSV files")
    
    elif command_name == 'transform-and-merge':
        print(f"\n{BOLD}transform-and-merge{RESET} --string-transform \"A\":\"B\", \"C\":\"D\" [--quiet] [--verbose]")
        print("    Description: Transform and merge asset CSV files from source and target environments")
        print("    Arguments:")
        print("      --string-transform: String transformations [REQUIRED]")
        print("                          Format: \"A\":\"B\", \"C\":\"D\"")
        print("    Options:")
        print("      --quiet: Suppress console output, show only summary")
        print("      --verbose: Show detailed output including transformation details")
        print("    Examples:")
        print("      transform-and-merge --string-transform \"PROD_DB\":\"DEV_DB\", \"PROD_URL\":\"DEV_URL\"")
        print("      transform-and-merge --string-transform \"old\":\"new\", \"test\":\"prod\" --verbose")
        print("    Behavior:")
        print("      • Reads asset-all-source-export.csv from asset-export/ directory")
        print("      • Reads asset-all-target-export.csv from asset-export/ directory")
        print("      • Applies exact string transformations to target_uid column in source file")
        print("      • Only replaces target_uid if it exactly matches a source string")
        print("      • Merges records based on transformed target_uid and target file's source_uid")
        print("      • Merge operation is like an INNER JOIN - only matched records included")
        print("      • Outputs merged file: asset-import/asset-merged-all.csv")
        print("      • Output columns: source_id, source_uid, target_id, target_uid, tags")
        print("      • Only includes records that successfully match between environments")
        print("      • Provides detailed statistics on transformations and matches")
        print("      • Generates <output-dir>/asset-export/asset_uids.csv and <output-dir>/policy-import/segmented_spark_uids.csv")
        print("      • Processes asset-all-export.csv -> asset-all-import-ready.csv")
        print("      • Processes asset-config-export.csv -> asset-config-import-ready.csv")
        print("      • Shows detailed processing statistics upon completion")
    
    elif command_name == 'vcs-config':
        print(f"\n{BOLD}vcs-config{RESET} [--vcs-type <type>] [--remote-url <url>] [--username <user>] [--token <token>] [options]")
        print("    Description: Configure enterprise VCS settings (Git/Mercurial/Subversion, HTTPS/SSH, proxy)")
        print("    Arguments:")
        print("      --vcs-type: VCS type (git, hg, svn)")
        print("      --remote-url: Remote repository URL")
        print("      --username: Username for HTTPS authentication")
        print("      --token: Token/password for HTTPS authentication")
        print("    Options:")
        print("      --ssh-key-path: Path to SSH private key")
        print("      --ssh-passphrase: SSH key passphrase")
        print("      --proxy-url: HTTP/HTTPS proxy URL")
        print("      --proxy-username: Proxy username")
        print("      --proxy-password: Proxy password")
        print("    Examples:")
        print("      vcs-config  # Interactive mode")
        print("      vcs-config --vcs-type git --remote-url https://github.com/user/repo.git")
        print("      vcs-config --vcs-type git --remote-url git@github.com:user/repo.git --ssh-key-path ~/.ssh/id_rsa")
        print("      vcs-config --vcs-type git --remote-url https://enterprise.gitlab.com/repo.git --username user --token <token>")
        print("    Behavior:")
        print("      • Interactive configuration mode when no arguments provided")
        print("      • Supports Git, Mercurial, and Subversion")
        print("      • HTTPS authentication with username/token")
        print("      • SSH authentication with key and passphrase")
        print("      • HTTP/HTTPS proxy support for enterprise networks")
        print("      • Secure credential storage in system keyring")
        print("      • Configuration stored in ~/.adoc_vcs_config.json")
        print("      • Validates URL format and authentication method")
        print("      • Shows configuration summary and next steps")
    
    elif command_name == 'vcs-init':
        print(f"\n{BOLD}vcs-init{RESET} [<base directory>]")
        print("    Description: Initialize a VCS repository (Git or Mercurial) in the output directory or specified directory.")
        print("    Arguments:")
        print("      base directory: Directory to initialize the repository in (optional, defaults to output directory)")
        print("    Behavior:")
        print("      • Initializes a Git or Mercurial repository in the target directory")
        print("      • Creates a .gitignore or .hgignore with patterns: *.zip, config.env, *.log, ~/.adoc_vcs_config.json")
        print("      • Uses the output directory if no directory is specified")
        print("      • Shows next steps for adding, committing, and pushing files")
    
    elif command_name == 'vcs-pull':
        print(f"\n{BOLD}vcs-pull{RESET}")
        print("    Description: Pull updates from the configured repository with authentication.")
        print("    Behavior:")
        print("      • Uses the output directory as the target for pulling files")
        print("      • Requires VCS configuration from 'vcs-config' command")
        print("      • Supports both Git and Mercurial repositories")
        print("      • Handles HTTPS authentication with username/token")
        print("      • Handles SSH authentication with key and passphrase")
        print("      • Supports HTTP/HTTPS proxy configuration")
        print("      • Automatically configures local repository settings")
        print("      • Shows detailed progress and change information")
    
    elif command_name == 'vcs-push':
        print(f"\n{BOLD}vcs-push{RESET}")
        print("    Description: Push changes to the remote repository with authentication.")
        print("    Behavior:")
        print("      • Uses the output directory as the source for pushing files")
        print("      • Requires VCS configuration from 'vcs-config' command")
        print("      • Requires a repository initialized with 'vcs-init' command")
        print("      • Supports both Git and Mercurial repositories")
        print("      • Handles HTTPS authentication with username/token")
        print("      • Handles SSH authentication with key and passphrase")
        print("      • Supports HTTP/HTTPS proxy configuration")
        print("      • Automatically commits uncommitted changes before pushing")
        print("      • Sets up remote tracking and upstream branches")
        print("      • Shows detailed progress and push results")
    
    elif command_name == 'set-output-dir':
        print(f"\n{BOLD}set-output-dir{RESET} <directory>")
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
    
    elif command_name == 'set-log-level':
        print(f"\n{BOLD}set-log-level{RESET} <level>")
        print("    Description: Change log level dynamically for all loggers in the application")
        print("    Arguments:")
        print("      level: Log level (ERROR, WARNING, INFO, DEBUG)")
        print("    Examples:")
        print("      set-log-level DEBUG")
        print("      set-log-level INFO")
        print("      set-log-level WARNING")
        print("      set-log-level ERROR")
        print("    Features:")
        print("      • Changes log level for all loggers immediately")
        print("      • Affects both file and console logging")
        print("      • Changes persist for the current session")
        print("      • Logs the level change for audit purposes")
        print("      • Validates log level before applying")
        print("      • ERROR: Only error messages")
        print("      • WARNING: Errors and warnings")
        print("      • INFO: Errors, warnings, and info messages")
        print("      • DEBUG: All messages including debug information")
    
    elif command_name == 'set-http-config':
        print(f"\n{BOLD}set-http-config{RESET} [--timeout x] [--retry x] [--proxy url]")
        print("    Description: Configure HTTP timeout, retry, and proxy settings for all API requests")
        print("    Arguments:")
        print("      --timeout x: Request timeout in seconds (integer)")
        print("      --retry x: Number of retry attempts (integer)")
        print("      --proxy url: Proxy URL (e.g., http://proxy.example.com:8080)")
        print("    Examples:")
        print("      set-http-config --timeout 30")
        print("      set-http-config --retry 5")
        print("      set-http-config --proxy http://proxy.company.com:8080")
        print("      set-http-config --timeout 20 --retry 3 --proxy http://proxy:8080")
        print("    Features:")
        print("      • Shows current HTTP configuration before changes")
        print("      • Applies changes immediately to global HTTP config")
        print("      • Affects all future API requests")
        print("      • Supports retry with exponential backoff")
        print("      • Retries on 429, 500, 502, 503, 504 status codes")
        print("      • Proxy support for HTTP and HTTPS requests")
        print("      • Changes persist for the current session")
        print("      • Shows new configuration after changes")
        print("      • Default timeout: 10 seconds")
        print("      • Default retry: 3 attempts")
        print("      • Default proxy: None")
    
    elif command_name == 'show-config':
        print(f"\n{BOLD}show-config{RESET}")
        print("    Description: Display current configuration for HTTP, logging, environment, and output settings")
        print("    Arguments:")
        print("      None (no arguments required)")
        print("    Example:")
        print("      show-config")
        print("    Features:")
        print("      • Shows HTTP configuration (timeout, retry, proxy)")
        print("      • Shows current log level")
        print("      • Shows environment configuration (host, tenants)")
        print("      • Shows access keys and secret keys (partially masked for security)")
        print("      • Shows output directory configuration")
        print("      • Displays host type (static or dynamic with tenant substitution)")
        print("      • Shows both source and target environment settings")
        print("      • Security: Only shows first 8 characters of sensitive keys")
        print("      • Clear section organization with emojis and formatting")
        print("      • Useful for troubleshooting and configuration verification")
    
    elif command_name == 'get':
        print(f"\n{BOLD}GET{RESET} <endpoint> [--target]")
        print("    Description: Make GET request to API endpoint")
        print("    Arguments:")
        print("      endpoint: API endpoint path (e.g., /catalog-server/api/assets?uid=123)")
        print("      --target: Use target environment authentication and tenant")
        print("    Examples:")
        print("      GET /catalog-server/api/assets?uid=123")
        print("      GET /catalog-server/api/assets?uid=snowflake_krish_test.DEMO_DB.CS_DEMO.Customer_Sample --target")
        print("      GET /catalog-server/api/policies")
        print("    Features:")
        print("      • Uses source environment authentication by default")
        print("      • Use --target flag to switch to target environment")
        print("      • Returns formatted JSON response")
        print("      • Supports query parameters in endpoint URL")
    
    elif command_name == 'put':
        print(f"\n{BOLD}PUT{RESET} <endpoint> <json_payload> [--target]")
        print("    Description: Make PUT request to API endpoint with JSON payload")
        print("    Arguments:")
        print("      endpoint: API endpoint path")
        print("      json_payload: JSON data to send (e.g., {\"key\": \"value\"})")
        print("      --target: Use target environment authentication and tenant")
        print("    Examples:")
        print("      PUT /catalog-server/api/assets {\"name\": \"test\", \"type\": \"database\"}")
        print("      PUT /catalog-server/api/policies {\"policy\": \"data\"} --target")
        print("    Features:")
        print("      • Uses source environment authentication by default")
        print("      • Use --target flag to switch to target environment")
        print("      • JSON payload must be valid JSON format")
        print("      • Returns formatted JSON response")
    
    elif command_name == 'help':
        print(f"\n{BOLD}help{RESET}")
        print("    Description: Show this help information")
        print("    Example: help")
    
    elif command_name == 'history':
        print(f"\n{BOLD}history{RESET}")
        print("    Description: Show the last 25 commands with numbers")
        print("    Example: history")
        print("    Features:")
        print("      • Displays the last 25 commands with numbered entries")
        print("      • Latest commands appear first (highest numbers)")
        print("      • Long commands are truncated for display")
        print("      • Enter a number to execute that command")
        print("      • Works alongside ↑/↓ arrow key navigation")
    
    elif command_name in ['exit', 'quit', 'q']:
        print(f"\n{BOLD}exit, quit, q{RESET}")
        print("    Description: Exit the interactive client")
        print("    Examples: exit, quit, q")

    elif command_name == 'profile-check':
        print(f"\n{BOLD}profile-check{RESET} --config <profile-assets.csv> [--quiet] [--verbose] [--parallel] [--run-profile]")
        print("    Description: Check which assets require profiling on the target environment and optionally trigger profiling actions.")
        print("    Arguments:")
        print("      --config <profile-assets.csv>: Path to the CSV file listing assets to check/profile (required)")
        print("      --quiet: Suppress console output, show only summary with progress bar")
        print("      --verbose: Show detailed output including API calls and responses")
        print("      --parallel: Use parallel processing for faster checks (max 5 threads)")
        print("      --run-profile: Automatically trigger profiling for assets that are not yet profiled")
        print("    Examples:")
        print("      profile-check --config profile-assets.csv")
        print("      profile-check --config profile-assets.csv --quiet")
        print("      profile-check --config profile-assets.csv --verbose")
        print("      profile-check --config profile-assets.csv --parallel")
        print("      profile-check --config profile-assets.csv --run-profile")
        print("    Behavior:")
        print("      • Reads asset IDs and UIDs from the specified CSV file")
        print("      • Checks if each asset is already profiled on the target environment")
        print("      • Outputs a CSV of assets that require profiling")
        print("      • Optionally triggers profiling for unprofiled assets if --run-profile is specified")
        print("      • Shows progress bar in quiet mode")
        print("      • Shows detailed API calls in verbose mode")
        print("      • Parallel mode: Uses up to 5 threads to process assets simultaneously")
        print("      • Parallel mode: Each thread has its own progress bar")
        print("      • Parallel mode: Significantly faster for large asset sets")
    elif command_name == 'profile-run':
        print(f"\n{BOLD}profile-run{RESET} --config <profile-assets.csv> [--quiet] [--verbose] [--parallel]")
        print("    Description: Trigger profiling for assets listed in the specified CSV file on the target environment.")
        print("    Arguments:")
        print("      --config <profile-assets.csv>: Path to the CSV file listing assets to profile (required)")
        print("      --quiet: Suppress console output, show only summary with progress bar")
        print("      --verbose: Show detailed output including API calls and responses")
        print("      --parallel: Use parallel processing for faster profiling (max 5 threads)")
        print("    Examples:")
        print("      profile-run --config profile-assets.csv")
        print("      profile-run --config profile-assets.csv --quiet")
        print("      profile-run --config profile-assets.csv --verbose")
        print("      profile-run --config profile-assets.csv --parallel")
        print("    Behavior:")
        print("      • Reads asset IDs and UIDs from the specified CSV file")
        print("      • Triggers profiling for each listed asset on the target environment")
        print("      • Monitors profiling status and reports completion or errors")
        print("      • Shows progress bar in quiet mode")
        print("      • Shows detailed API calls in verbose mode")
        print("      • Parallel mode: Uses up to 5 threads to process assets simultaneously")
        print("      • Parallel mode: Each thread has its own progress bar")
        print("      • Parallel mode: Significantly faster for large asset sets")
    elif command_name == 'notifications-check':
        print(f"\n{BOLD}notifications-check{RESET} --source-context <id> --target-context <id> --assembly-ids <ids> [--quiet] [--verbose] [--parallel] [--page-size <size>]")
        print("    Description: Compare notification groups between source and target environments and report differences.")
        print("    Arguments:")
        print("      --source-context <id>: Source context ID (required)")
        print("      --target-context <id>: Target context ID (required)")
        print("      --assembly-ids <ids>: Comma-separated list of assembly IDs to check (required)")
        print("      --quiet: Suppress console output, show only summary with progress bar")
        print("      --verbose: Show detailed output including API calls and responses")
        print("      --parallel: Use parallel processing for faster comparison (max 5 threads)")
        print("      --page-size <size>: Number of notification groups per page (default: 100)")
        print("    Examples:")
        print("      notifications-check --source-context 1 --target-context 2 --assembly-ids 100,101")
        print("      notifications-check --source-context 1 --target-context 2 --assembly-ids 100,101 --quiet")
        print("      notifications-check --source-context 1 --target-context 2 --assembly-ids 100,101 --verbose")
        print("      notifications-check --source-context 1 --target-context 2 --assembly-ids 100,101 --parallel")
        print("      notifications-check --source-context 1 --target-context 2 --assembly-ids 100,101 --page-size 200")
        print("    Behavior:")
        print("      • Fetches notification groups from both source and target environments")
        print("      • Compares notification group definitions by name and type")
        print("      • Reports missing, extra, or mismatched notification groups")
        print("      • Outputs a CSV report of comparison results")
        print("      • Shows progress bar in quiet mode")
        print("      • Shows detailed API calls in verbose mode")
        print("      • Parallel mode: Uses up to 5 threads to process groups simultaneously")
        print("      • Parallel mode: Each thread has its own progress bar")
        print("      • Parallel mode: Significantly faster for large notification sets")
    else:
        print(f"\n❌ Unknown command: {command_name}")
        print("💡 Use 'help' to see all available commands")
        print("💡 Use 'help <command>' for detailed help on a specific command")
        return
    
    print("\n" + "="*80)


def setup_autocomplete():
    """Setup command autocomplete functionality."""
    # Define all available commands and their completions
    commands = [
        'segments-export', 'segments-import',
        'asset-profile-export', 'asset-profile-import',
        'asset-config-export', 'asset-config-import', 'asset-list-export', 'asset-tag-import',
        'policy-list-export', 'policy-export', 'policy-import', 'policy-xfr', 'rule-tag-export',
        'vcs-config', 'vcs-init', 'vcs-pull', 'vcs-push',
        'GET', 'PUT',  # REST API commands
        'set-output-dir', 'set-log-level', 'set-http-config', 'show-config', 'help', 'history', 'exit', 'quit', 'q'
    ]
    
    # Define command-specific completions
    command_completions = {
        'help': commands,  # help can be followed by any command
        'asset-config-export': ['--output-file', '--quiet', '--verbose', '--parallel'],
        'asset-config-import': ['--dry-run', '--quiet', '--verbose', '--parallel'],
                    'asset-list-export': ['--quiet', '--verbose', '--parallel', '--target', '--page-size'],
        'asset-profile-export': ['--output-file', '--quiet', '--verbose', '--parallel'],
        'asset-profile-import': ['--dry-run', '--quiet', '--verbose'],
        'asset-tag-import': ['--quiet', '--verbose', '--parallel'],
    
        'policy-export': ['--type', '--filter', '--quiet', '--verbose', '--batch-size', '--parallel'],
        'policy-import': ['--quiet', '--verbose'],
        'policy-list-export': ['--quiet', '--verbose', '--parallel', '--existing-target-assets'],
        'policy-xfr': ['--input', '--source-env-string', '--target-env-string', '--quiet', '--verbose'],
        'transform-and-merge': ['--string-transform', '--quiet', '--verbose'],
        'rule-tag-export': ['--quiet', '--verbose', '--parallel'],
        'segments-export': ['--output-file', '--quiet'],
        'segments-import': ['--dry-run', '--quiet', '--verbose'],
        'vcs-config': ['--vcs-type', '--remote-url', '--username', '--token', '--ssh-key-path', '--ssh-passphrase', '--proxy-url', '--proxy-username', '--proxy-password'],
        'vcs-init': [],
        'vcs-pull': [],
        'vcs-push': [],
        'GET': ['--target'],  # REST API commands
        'PUT': ['--target'],  # REST API commands
        'set-output-dir': [],
        'set-log-level': ['ERROR', 'WARNING', 'INFO', 'DEBUG'],
        'set-http-config': ['--timeout', '--retry', '--proxy'],
        'show-config': []
    }
    
    # Define option values for specific options
    option_values = {
        '--type': ['rule-types', 'engine-types', 'assemblies', 'source-types'],
        '--vcs-type': ['git', 'hg', 'svn'],
        '--log-level': ['ERROR', 'WARNING', 'INFO', 'DEBUG']
    }
    
    def completer(text, state):
        """Custom completer function for readline."""
        try:
            # Get the current line
            line = get_line_buffer()
            words = line.split()
            
            # If we're at the beginning or after a space, complete commands
            if not words or (len(words) == 1 and not line.endswith(' ')):
                matches = [cmd for cmd in commands if cmd.startswith(text)]
                if state < len(matches):
                    return matches[state]
                return None
            
            # Get the command (first word)
            command = words[0].lower()
            
            # If this is a help command with one argument, complete command names
            if command == 'help' and len(words) == 2:
                matches = [cmd for cmd in commands if cmd.startswith(text)]
                if state < len(matches):
                    return matches[state]
                return None
            
            # Get completions for this command
            completions = command_completions.get(command, [])
            
            # Filter completions that start with the current text
            matches = [comp for comp in completions if comp.startswith(text)]
            
            # If we have matches, return them
            if state < len(matches):
                return matches[state]
            
            # If no matches for options, check if we need to complete option values
            if not matches and text.startswith('--'):
                # Check if we're completing a value for a specific option
                for option, values in option_values.items():
                    if option.startswith(text):
                        if state < len([v for v in values if v.startswith(text.replace(option, ''))]):
                            return option
                        return None
            
            return None
            
        except Exception:
            return None
    
    # Set the completer
    set_completer(completer)
    
    # Enable tab completion
    parse_and_bind('tab: complete')


def get_user_input(prompt: str) -> str:
    """Get user input with improved cursor handling."""
    try:
        if hasattr(sys.stdin, 'flush'):
            sys.stdin.flush()
        command = input_with_history(prompt).strip()
        return command
    except (EOFError, KeyboardInterrupt):
        raise
    except Exception as e:
        print(f"\nInput error: {e}")
        return ""


def cleanup_command_history():
    """Clean up command history to prevent cursor position issues."""
    try:
        # Clear the current line buffer to reset cursor position
        clear_history()
        
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
            read_history_file(history_file)
            
    except Exception:
        # If cleanup fails, just continue
        pass


def show_command_history():
    """Display the last 25 commands from history with numbers."""
    try:
        # Clean current session history first
        clean_current_session_history()
        
        # Get current history length
        history_length = get_current_history_length()
        
        if history_length == 0:
            print("\n📋 No command history available.")
            return
        
        # Get the last 25 commands (or all if less than 25)
        start_index = max(0, history_length - 25)
        commands = []
        
        for i in range(start_index, history_length):
            try:
                command = get_history_item(i + 1)  # readline uses 1-based indexing
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
        history_length = get_current_history_length()
        
        if history_length == 0:
            return
        
        # Create a new clean history
        clean_history = []
        
        for i in range(history_length):
            try:
                command = get_history_item(i + 1)  # readline uses 1-based indexing
                if command and command.strip():
                    # Only keep commands that are not utility commands
                    if command.strip().lower() not in ['exit', 'quit', 'q', 'history', 'help']:
                        clean_history.append(command.strip())
            except Exception:
                continue
        
        # Clear current history and reload clean version
        clear_history()
        
        # Add back only the clean commands
        for command in clean_history:
            try:
                add_history(command)
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
        history_length = get_current_history_length()
        
        if history_length == 0:
            return None
        
        # Get the last 25 commands (or all if less than 25)
        start_index = max(0, history_length - 25)
        commands = []
        
        for i in range(start_index, history_length):
            try:
                command = get_history_item(i + 1)  # readline uses 1-based indexing
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


def run_interactive(args):
    """Run the interactive REST API client."""
    try:
        # Create API client first to get configuration
        client = create_api_client(env_file=args.env_file)
        
        # Setup logging with log file path from client configuration
        log_file_path = client.get_log_file_path()
        logger = setup_logging(args.verbose, args.log_level, log_file_path)
        
        # Get user session info for logging
        user_info = get_user_session_info()
        
        # Log session start
        log_session_event(logger, 'start', user_info)
        
        # Validate arguments
        from ..cli.validators import validate_rest_api_arguments
        validate_rest_api_arguments(args)
        
        # Test connection
        if not client.test_connection():
            logger.error("Failed to connect to API")
            log_session_event(logger, 'exit', user_info)
            return 1
        
        # Update client with logger
        client.logger = logger
        
        # Load global output directory from configuration
        globals.GLOBAL_OUTPUT_DIR = load_global_output_directory()
        
        # Display current output directory status
        print("\n" + "="*80)
        print("\033[1m\033[36mADOC INTERACTIVE MIGRATION TOOLKIT\033[0m")
        print("="*80)
        if globals.GLOBAL_OUTPUT_DIR:
            print(f"📁 Output Directory: {globals.GLOBAL_OUTPUT_DIR}")
            print(f"📁 Current Directory: {os.getcwd()}")
            print(f"📋 Config File: {args.env_file}")
            print(f"🌍 Source Tenant: {client.tenant}")
            print(f"🌍 Target Tenant: {client.target_tenant}")
            print(f"🌍 Source Environment: {client.host}")
            print(f"🌍 Target Environment: {client.target_host}")

        else:
            print(f"📁 Output Directory: Not set (will use default timestamped directories)")
            print(f"💡 Use 'set-output-dir <directory>' to set a persistent output directory")
        print("="*80)
        
        # Setup command history
        history_file = os.path.expanduser("~/.adoc_migration_toolkit_history")
        try:
            read_history_file(history_file)
        except FileNotFoundError:
            pass  # History file doesn't exist yet
        
        # Set history file for future sessions
        set_history_file(history_file)
        
        # Configure readline for better cursor handling
        try:
            # Set input mode for better cursor behavior
            parse_and_bind('set input-meta on')
            parse_and_bind('set output-meta on')
            parse_and_bind('set convert-meta off')
            parse_and_bind('set horizontal-scroll-mode on')
            parse_and_bind('set completion-query-items 0')
            parse_and_bind('set page-completions off')
            parse_and_bind('set skip-completed-text on')
            parse_and_bind('set completion-ignore-case on')
            parse_and_bind('set show-all-if-ambiguous on')
            parse_and_bind('set show-all-if-unmodified on')
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
                    log_session_event(logger, 'exit', user_info)
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
                    'asset-config-export', 'asset-list-export', 'asset-tag-import',
                    'policy-list-export', 'policy-export', 'policy-import', 'policy-xfr', 'rule-tag-export',
                    'vcs-config',
                    'vcs-init',
                    'vcs-pull',
                    'vcs-push',
                    'GET', 'PUT',  # REST API commands
                    'set-output-dir', 'set-log-level',
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
                        add_history(command)
                    except Exception:
                        pass  # Ignore history errors
                
                # Check if it's a help command
                if command.lower().startswith('help'):
                    parts = command.split()
                    if len(parts) == 1:
                        show_interactive_help()
                    elif len(parts) == 2:
                        show_command_help(parts[1])
                    else:
                        print("❌ Usage: help [<command>]")
                        print("💡 Use 'help' to see all commands")
                        print("💡 Use 'help <command>' for detailed help on a specific command")
                    continue
                
                # Check if it's a history command
                if command.lower() == 'history':
                    show_command_history()
                    continue
                
                # Check if it's a segments-export command
                if command.lower().startswith('segments-export'):
                    from .command_parsing import parse_segments_export_command
                    csv_file, output_file, quiet_mode = parse_segments_export_command(command)
                    if csv_file:
                        execute_segments_export(csv_file, client, logger, output_file, quiet_mode)
                    continue
                
                # Check if it's a segments-import command
                if command.lower().startswith('segments-import'):
                    from .command_parsing import parse_segments_import_command
                    csv_file, dry_run, quiet_mode, verbose_mode = parse_segments_import_command(command)
                    if csv_file:
                        execute_segments_import(csv_file, client, logger, dry_run, quiet_mode, verbose_mode)
                    continue
                
                # Check if it's an asset-profile-export command
                if command.lower().startswith('asset-profile-export'):
                    from .command_parsing import parse_asset_profile_export_command
                    csv_file, output_file, quiet_mode, verbose_mode, parallel_mode, allowed_types = parse_asset_profile_export_command(command)
                    if csv_file:
                        if parallel_mode:
                            execute_asset_profile_export_parallel(csv_file, client, logger, output_file, quiet_mode, verbose_mode, allowed_types)
                        else:
                            execute_asset_profile_export(csv_file, client, logger, output_file, quiet_mode, verbose_mode, allowed_types)
                    continue
                
                # Check if it's an asset-profile-import command
                if command.lower().startswith('asset-profile-import'):
                    from .command_parsing import parse_asset_profile_import_command
                    csv_file, dry_run, quiet_mode, verbose_mode, max_threads = parse_asset_profile_import_command(command)
                    if csv_file:
                        # If execute_asset_profile_import supports max_threads, pass it; otherwise, ignore
                        try:
                            execute_asset_profile_import(csv_file, client, logger, dry_run, quiet_mode, verbose_mode, max_threads=max_threads)
                        except TypeError:
                            execute_asset_profile_import(csv_file, client, logger, dry_run, quiet_mode, verbose_mode)
                    continue
                
                # Check if it's an asset-list-export command (check this first to avoid conflicts)
                if command.lower().startswith('asset-list-export'):
                    from .command_parsing import parse_asset_list_export_command
                    quiet_mode, verbose_mode, parallel_mode, use_target, page_size, source_type_ids, asset_type_ids, assembly_ids, max_threads = parse_asset_list_export_command(command)
                    if parallel_mode:
                        execute_asset_list_export_parallel(client, logger, source_type_ids, asset_type_ids, assembly_ids, quiet_mode, verbose_mode, use_target, page_size, max_threads)
                    else:
                        execute_asset_list_export(client, logger, source_type_ids, asset_type_ids, assembly_ids, quiet_mode, verbose_mode, use_target, page_size)
                    continue
            
                
                # Check if it's an asset-config-export command
                if command.lower().startswith('asset-config-export'):
                    from .command_parsing import parse_asset_config_export_command
                    csv_file, output_file, quiet_mode, verbose_mode, parallel_mode, max_threads, allowed_types = parse_asset_config_export_command(command)
                    if csv_file:
                        if parallel_mode:
                            execute_asset_config_export_parallel(csv_file, client, logger, output_file, quiet_mode, verbose_mode, max_threads, allowed_types)
                        else:
                            execute_asset_config_export(csv_file, client, logger, output_file, quiet_mode, verbose_mode)
                    continue
                
                # Check if it's an asset-config-import command
                if command.lower().startswith('asset-config-import'):
                    from .command_parsing import parse_asset_config_import_command
                    csv_file, dry_run, quiet_mode, verbose_mode, parallel_mode, max_threads = parse_asset_config_import_command(command)
                    
                    # Use default CSV file if not specified
                    if not csv_file:
                        if globals.GLOBAL_OUTPUT_DIR:
                            csv_file = str(globals.GLOBAL_OUTPUT_DIR / "asset-import" / "asset-config-import-ready.csv")
                        else:
                            # Try to find the latest toolkit directory
                            current_dir = Path.cwd()
                            toolkit_dirs = [d for d in current_dir.iterdir() if d.is_dir() and d.name.startswith("adoc-migration-toolkit-")]
                            if toolkit_dirs:
                                toolkit_dirs.sort(key=lambda x: x.stat().st_ctime, reverse=True)
                                latest_toolkit_dir = toolkit_dirs[0]
                                csv_file = str(latest_toolkit_dir / "asset-import" / "asset-config-import-ready.csv")
                            else:
                                csv_file = "asset-config-import-ready.csv"
                    
                    execute_asset_config_import(csv_file, client, logger, quiet_mode, verbose_mode, parallel_mode, dry_run, max_threads)
                    continue
                
                # Check if it's an asset-tag-import command
                if command.lower().startswith('asset-tag-import'):
                    from .command_parsing import parse_asset_tag_import_command
                    csv_file, quiet_mode, verbose_mode, parallel_mode = parse_asset_tag_import_command(command)
                    
                    # Use default CSV file if not specified
                    if not csv_file:
                        if globals.GLOBAL_OUTPUT_DIR:
                            csv_file = str(globals.GLOBAL_OUTPUT_DIR / "asset-import" / "asset-merged-all.csv")
                        else:
                            # Try to find the latest toolkit directory
                            current_dir = Path.cwd()
                            toolkit_dirs = [d for d in current_dir.iterdir() if d.is_dir() and d.name.startswith("adoc-migration-toolkit-")]
                            if toolkit_dirs:
                                toolkit_dirs.sort(key=lambda x: x.stat().st_ctime, reverse=True)
                                latest_toolkit_dir = toolkit_dirs[0]
                                csv_file = str(latest_toolkit_dir / "asset-import" / "asset-merged-all.csv")
                            else:
                                csv_file = "asset-merged-all.csv"
                    
                    execute_asset_tag_import(csv_file, client, logger, quiet_mode, verbose_mode, parallel_mode)
                    continue
                

                
                # Check if it's a policy-list-export command
                if command.lower().startswith('policy-list-export'):
                    from .command_parsing import parse_policy_list_export_command
                    quiet_mode, verbose_mode, parallel_mode, existing_target_assets_mode = parse_policy_list_export_command(command)
                    if parallel_mode:
                        execute_policy_list_export_parallel(client, logger, quiet_mode, verbose_mode, existing_target_assets_mode)
                    else:
                        execute_policy_list_export(client, logger, quiet_mode, verbose_mode, existing_target_assets_mode)
                    continue
                
                # Check if it's a policy-export command
                if command.lower().startswith('policy-export'):
                    from .command_parsing import parse_policy_export_command
                    quiet_mode, verbose_mode, batch_size, export_type, filter_value, parallel_mode = parse_policy_export_command(command)
                    if parallel_mode:
                        execute_policy_export_parallel(client, logger, quiet_mode, verbose_mode, batch_size, export_type, filter_value)
                    else:
                        execute_policy_export(client, logger, quiet_mode, verbose_mode, batch_size, export_type, filter_value)
                    continue
                
                # Check if it's a policy-import command
                if command.lower().startswith('policy-import'):
                    from .command_parsing import parse_policy_import_command
                    file_pattern, quiet_mode, verbose_mode = parse_policy_import_command(command)
                    if file_pattern:
                        execute_policy_import(client, logger, file_pattern, quiet_mode, verbose_mode)
                    continue
                
                # Check if it's a rule-tag-export command
                if command.lower().startswith('rule-tag-export'):
                    from .command_parsing import parse_rule_tag_export_command
                    quiet_mode, verbose_mode, parallel_mode = parse_rule_tag_export_command(command)
                    if parallel_mode:
                        execute_rule_tag_export_parallel(client, logger, quiet_mode, verbose_mode)
                    else:
                        execute_rule_tag_export(client, logger, quiet_mode, verbose_mode)
                    continue
                
                # Check if it's a policy-xfr command
                if command.lower().startswith('policy-xfr'):
                    input_dir, string_transforms, output_dir, quiet_mode, verbose_mode = parse_formatter_command(command)
                    if string_transforms:
                        execute_formatter(input_dir, string_transforms, output_dir, quiet_mode, verbose_mode, logger)
                    continue
                
                # Check if it's a profile-check command
                if command.lower().startswith('profile-check'):
                    from .command_parsing import parse_profile_command
                    policy_types, parallel_mode, run_profile, verbose_mode, quiet_mode = parse_profile_command(command)
                    if parallel_mode:
                        check_for_profiling_required_before_migration(client, logger, policy_types, run_profile, quiet_mode, verbose_mode)
                    else:
                        check_for_profiling_required_before_migration(client, logger, policy_types, run_profile, quiet_mode, verbose_mode)
                    continue

                # Check if it's a profile-run command
                if command.lower().startswith('profile-run'):
                    from .command_parsing import parse_run_profile_command
                    profile_assets_config_csv_path, parallel_mode, verbose_mode, quiet_mode = parse_run_profile_command(command)
                    if parallel_mode:
                        trigger_profile_action(client, logger, profile_assets_config_csv_path, quiet_mode, verbose_mode)
                    else:
                        trigger_profile_action(client, logger, profile_assets_config_csv_path, quiet_mode, verbose_mode)
                    continue

                # Check if it's an asset-list-export command (check this first to avoid conflicts)
                if command.lower().startswith('notifications-check'):
                    from .command_parsing import parse_notifications_check_command
                    quiet_mode, verbose_mode, parallel_mode, page_size, source_context_id, target_context_id, assembly_ids = parse_notifications_check_command(command)
                    if parallel_mode:
                        precheck_on_notifications(client, logger, source_context_id, target_context_id, assembly_ids, quiet_mode, verbose_mode)
                    else:
                        precheck_on_notifications(client, logger, source_context_id, target_context_id, assembly_ids, quiet_mode, verbose_mode)
                    continue

                # Check if it's a transform-and-merge command
                if command.lower().startswith('transform-and-merge'):
                    from .command_parsing import parse_transform_and_merge_command
                    from .asset_operations import execute_transform_and_merge
                    try:
                        string_transforms, quiet_mode, verbose_mode = parse_transform_and_merge_command(command)
                        if string_transforms:
                            execute_transform_and_merge(string_transforms, quiet_mode, verbose_mode, logger)
                    except ValueError as e:
                        print(f"❌ Error: {e}")
                        print("💡 Usage: transform-and-merge --string-transform \"A\":\"B\", \"C\":\"D\" [--quiet] [--verbose]")
                    continue
                
                # Check if it's a set-output-dir command
                if command.lower().startswith('set-output-dir'):
                    from .output_management import parse_set_output_dir_command, set_global_output_directory
                    directory = parse_set_output_dir_command(command)
                    if directory:
                        set_global_output_directory(directory, logger)
                    continue
                
                # Check if it's a set-log-level command
                if command.lower().startswith('set-log-level'):
                    from .command_parsing import parse_set_log_level_command
                    from ..shared.logging import change_log_level
                    import logging
                    new_level = parse_set_log_level_command(command)
                    if new_level:
                        current_level = logging.getLevelName(logging.getLogger().getEffectiveLevel())
                        print(f"Current log level: {current_level}")
                        if change_log_level(new_level):
                            updated_level = logging.getLevelName(logging.getLogger().getEffectiveLevel())
                            print(f"✅ Log level changed to: {updated_level}")
                        else:
                            print(f"❌ Failed to change log level to: {new_level}")
                    continue

                # Check if it's a set-http-config command
                if command.lower().startswith('set-http-config'):
                    import logging
                    from .command_parsing import parse_set_http_config_command
                    from adoc_migration_toolkit.shared import globals as shared_globals
                    
                    config = parse_set_http_config_command(command)
                    if config is not None:
                        # Show current config
                        current = shared_globals.HTTP_CONFIG.copy()
                        print("Current HTTP config:")
                        print(f"  Timeout: {current['timeout']}s")
                        print(f"  Retry:   {current['retry']}")
                        print(f"  Proxy:   {current['proxy']}")
                        # Apply changes
                        changed = False
                        for k in ['timeout', 'retry', 'proxy']:
                            if config[k] is not None:
                                shared_globals.HTTP_CONFIG[k] = config[k]
                                changed = True
                        if changed:
                            print("\n✅ HTTP config updated.")
                        else:
                            print("\n(No changes made)")
                        # Show new config
                        new = shared_globals.HTTP_CONFIG.copy()
                        print("New HTTP config:")
                        print(f"  Timeout: {new['timeout']}s")
                        print(f"  Retry:   {new['retry']}")
                        print(f"  Proxy:   {new['proxy']}")
                    continue

                # Check if it's a show-config command
                if command.lower().startswith('show-config'):
                    import logging
                    from .command_parsing import parse_show_config_command
                    from adoc_migration_toolkit.shared import globals as shared_globals
                    
                    if parse_show_config_command(command):
                        print("\n" + "="*60)
                        print("🔧 CURRENT CONFIGURATION")
                        print("="*60)
                        
                        # HTTP Configuration
                        print(f"\n🌐 HTTP CONFIGURATION:")
                        http_config = shared_globals.HTTP_CONFIG.copy()
                        print(f"  Timeout: {http_config['timeout']} seconds")
                        print(f"  Retry:   {http_config['retry']} attempts")
                        print(f"  Proxy:   {http_config['proxy'] or 'None'}")
                        
                        # Log Level
                        print(f"\n📝 LOGGING CONFIGURATION:")
                        current_level = logging.getLevelName(logging.getLogger().getEffectiveLevel())
                        print(f"  Log Level: {current_level}")
                        
                        # Environment Configuration
                        print(f"\n🏢 ENVIRONMENT CONFIGURATION:")
                        
                        # Host configuration
                        host = os.getenv('AD_HOST')
                        if host:
                            print(f"  Host Template: {host}")
                            if "${tenant}" in host:
                                print(f"  Host Type: Dynamic (uses tenant substitution)")
                            else:
                                print(f"  Host Type: Static")
                        else:
                            print(f"  Host: Not set in environment")
                        
                        # Source tenant and credentials
                        source_tenant = os.getenv('AD_SOURCE_TENANT')
                        source_access_key = os.getenv('AD_SOURCE_ACCESS_KEY')
                        source_secret_key = os.getenv('AD_SOURCE_SECRET_KEY')
                        
                        if source_tenant:
                            print(f"  Source Tenant: {source_tenant}")
                        else:
                            print(f"  Source Tenant: Not set")
                            
                        if source_access_key:
                            masked_key = source_access_key[:8] + "..." if len(source_access_key) > 8 else source_access_key
                            print(f"  Source Access Key: {masked_key}")
                        else:
                            print(f"  Source Access Key: Not set")
                            
                        if source_secret_key:
                            masked_secret = source_secret_key[:8] + "..." if len(source_secret_key) > 8 else source_secret_key
                            print(f"  Source Secret Key: {masked_secret}")
                        else:
                            print(f"  Source Secret Key: Not set")
                        
                        # Target tenant and credentials
                        target_tenant = os.getenv('AD_TARGET_TENANT')
                        target_access_key = os.getenv('AD_TARGET_ACCESS_KEY')
                        target_secret_key = os.getenv('AD_TARGET_SECRET_KEY')
                        
                        if target_tenant:
                            print(f"  Target Tenant: {target_tenant}")
                        else:
                            print(f"  Target Tenant: Not set")
                            
                        if target_access_key:
                            masked_target_key = target_access_key[:8] + "..." if len(target_access_key) > 8 else target_access_key
                            print(f"  Target Access Key: {masked_target_key}")
                        else:
                            print(f"  Target Access Key: Not set")
                            
                        if target_secret_key:
                            masked_target_secret = target_secret_key[:8] + "..." if len(target_secret_key) > 8 else target_secret_key
                            print(f"  Target Secret Key: {masked_target_secret}")
                        else:
                            print(f"  Target Secret Key: Not set")
                        
                        # Output directory
                        print(f"\n📁 OUTPUT CONFIGURATION:")
                        if shared_globals.GLOBAL_OUTPUT_DIR:
                            print(f"  Output Directory: {shared_globals.GLOBAL_OUTPUT_DIR}")
                        else:
                            print(f"  Output Directory: Not set (will use default)")
                        
                        print("\n" + "="*60)
                    continue

                # Check if it's a vcs-config command
                if command.lower().startswith('vcs-config'):
                    from ..vcs.operations import execute_vcs_config
                    execute_vcs_config(command)
                    continue
                
                # Check if it's a vcs-init command
                if command.lower().startswith('vcs-init'):
                    from ..vcs.operations import execute_vcs_init
                    from .command_parsing import parse_vcs_init_command
                    base_dir = parse_vcs_init_command(command)
                    # Use global output dir if not specified
                    output_dir = str(globals.GLOBAL_OUTPUT_DIR) if getattr(globals, 'GLOBAL_OUTPUT_DIR', None) else None
                    execute_vcs_init(command, output_dir=output_dir)
                    continue
                
                # Check if it's a vcs-pull command
                if command.lower().startswith('vcs-pull'):
                    from ..vcs.operations import execute_vcs_pull
                    from .command_parsing import parse_vcs_pull_command
                    if parse_vcs_pull_command(command):
                        # Use global output dir
                        output_dir = str(globals.GLOBAL_OUTPUT_DIR) if getattr(globals, 'GLOBAL_OUTPUT_DIR', None) else None
                        execute_vcs_pull(command, output_dir=output_dir)
                    continue
                
                # Check if it's a vcs-push command
                if command.lower().startswith('vcs-push'):
                    from ..vcs.operations import execute_vcs_push
                    from .command_parsing import parse_vcs_push_command
                    if parse_vcs_push_command(command):
                        # Use global output dir
                        output_dir = str(globals.GLOBAL_OUTPUT_DIR) if getattr(globals, 'GLOBAL_OUTPUT_DIR', None) else None
                        execute_vcs_push(command, output_dir=output_dir)
                    continue
                
                # Parse the command for GET/PUT requests
                from .command_parsing import parse_api_command
                method, endpoint, json_payload, use_target_auth, use_target_tenant = parse_api_command(command)
                
                if method is None:
                    continue
                
                # Handle dynamic endpoints with placeholders
                if '<asset-id>' in endpoint or '<asset-uid>' in endpoint:
                    from .command_parsing import handle_dynamic_endpoints
                    endpoint = handle_dynamic_endpoints(endpoint)
                    print(f"\nCurrent endpoint: {endpoint}")
                    print("Please modify the command to replace <asset-id> or <asset-uid> with actual values, then press Enter to continue...")
                    continue
                
                # Make the API call
                auth_info = " (target environment)" if use_target_auth else " (source environment)"
                print(f"\nMaking {method} request to: {endpoint}{auth_info}")
                print("-" * 60)
                
                # Add debug information for target environment
                if use_target_auth:
                    print(f"🔍 Using target authentication:")
                    print(f"   Target Access Key: {client.target_access_key[:8]}..." if client.target_access_key else "   Target Access Key: Not configured")
                    print(f"   Target Secret Key: {client.target_secret_key[:8]}..." if client.target_secret_key else "   Target Secret Key: Not configured")
                    print(f"   Target Tenant: {client.target_tenant}" if client.target_tenant else "   Target Tenant: Not configured")
                    if client.host_template:
                        target_host = client.host_template.replace("${tenant}", client.target_tenant or "UNKNOWN")
                        print(f"   Target Host: {target_host}")
                
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
                log_session_event(logger, 'exit', user_info)
                break
            except (ValueError, FileNotFoundError, PermissionError) as e:
                print(f"❌ Error: {e}")
            except Exception as e:
                print(f"❌ Unexpected error: {e}")
                logger.error(f"Unexpected error in interactive mode: {e}")
        
        # Save command history
        try:
            write_history_file(history_file)
        except Exception as e:
            logger.warning(f"Could not save command history: {e}")
        
        # Close client
        client.close()
        log_session_event(logger, 'exit', user_info)
        return 0
        
    except (ValueError, FileNotFoundError, PermissionError) as e:
        print(f"❌ Configuration error: {e}")
        try:
            log_session_event(logger, 'exit', user_info)
        except:
            pass
        return 1
    except KeyboardInterrupt:
        print("\n⚠️  Client interrupted by user.")
        try:
            log_session_event(logger, 'exit', user_info)
        except:
            pass
        return 1
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        try:
            log_session_event(logger, 'exit', user_info)
        except:
            pass
        return 1 