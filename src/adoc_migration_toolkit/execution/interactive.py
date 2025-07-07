"""
Interactive mode execution functions.

This module contains execution functions for interactive mode operations
including help, autocomplete, and command history management.
"""

import os
import sys
import readline
import json
from pathlib import Path
from .segment_operations import execute_segments_export, execute_segments_import
from ..shared.logging import setup_logging
from adoc_migration_toolkit.execution.output_management import load_global_output_directory
from ..shared.api_client import create_api_client
from .asset_operations import execute_asset_profile_export, execute_asset_profile_export_parallel, execute_asset_profile_import, execute_asset_config_export, execute_asset_list_export
from .policy_operations import execute_policy_list_export, execute_policy_list_export_parallel, execute_policy_export, execute_policy_export_parallel, execute_policy_import
from .policy_operations import execute_rule_tag_export, execute_rule_tag_export_parallel
from .formatter import execute_formatter, parse_formatter_command
from adoc_migration_toolkit.shared import globals

def show_interactive_help():
    """Display help information for all available interactive commands."""
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
    print(f"  {BOLD}asset-profile-export{RESET} [<csv_file>] [--output-file <file>] [--quiet] [--verbose] [--parallel]")
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
    
    print(f"\n{BOLD}📋 POLICY COMMANDS:{RESET}")
    print(f"  {BOLD}policy-list-export{RESET} [--quiet] [--verbose] [--parallel]")
    print("    Description: Export all policies from source environment to CSV file")
    print("    Arguments:")
    print("      --quiet: Suppress console output, show only summary")
    print("      --verbose: Show detailed output including headers and responses")
    print("      --parallel: Use parallel processing for faster export (max 5 threads)")
    print("    Examples:")
    print("      policy-list-export")
    print("      policy-list-export --quiet")
    print("      policy-list-export --verbose")
    print("      policy-list-export --parallel")
    print("      policy-list-export --parallel --quiet")
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
    print("      • Parallel mode: Uses up to 5 threads with minimum 10 policies per thread")
    print("      • Parallel mode: Each thread has its own progress bar")
    print("      • Parallel mode: Automatic retry (3 attempts) on failures")
    print("      • Parallel mode: Temporary files merged into final output")
    
    print(f"\n  {BOLD}policy-export{RESET} [--type <export_type>] [--filter <filter_value>] [--quiet] [--verbose] [--batch-size <size>] [--parallel]")
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
    
    print(f"\n  {BOLD}policy-import{RESET} <file_or_pattern> [--quiet] [--verbose]")
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
    
    print(f"\n  {BOLD}rule-tag-export{RESET} [--quiet] [--verbose] [--parallel]")
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
    
    print(f"\n{BOLD}🔗 LINEAGE COMMANDS:{RESET}")
    print(f"  {BOLD}create-lineage{RESET} <csv_file> [--dry-run] [--quiet] [--verbose]")
    print("    Description: Create lineage relationships between assets from CSV file")
    print("    Arguments:")
    print("      csv_file: Path to CSV file containing lineage data")
    print("      --dry-run: Validate CSV and assets without creating lineage")
    print("      --quiet: Suppress console output, show only summary")
    print("      --verbose: Show detailed output including API calls")
    print("    Examples:")
    print("      create-lineage lineage_data.csv")
    print("      create-lineage lineage.csv --dry-run --verbose")
    print("      create-lineage data/lineage.csv --quiet")
    print("    CSV Format:")
    print("      Required columns: Source Asset ID, Target Asset ID, Relationship Type")
    print("      Optional columns: Group ID, Step Order, Source Column, Target Column, Transformation, Notes")
    print("      Relationship Type: 'upstream' (source → target) or 'downstream' (target ← source)")
    print("    Behavior:")
    print("      • Parses CSV file with lineage relationship data")
    print("      • Validates that all referenced assets exist in the environment")
    print("      • Groups lineage by target asset and relationship type")
    print("      • Creates lineage using POST /torch-pipeline/api/assets/:assetId/lineage")
    print("      • Supports complex lineage with grouping and multi-step operations")
    print("      • Generates process names and descriptions from transformation data")
    print("      • Provides comprehensive validation and error reporting")
    print("      • Dry-run mode validates without making API calls")
    
    print(f"\n{BOLD}🔧 VCS COMMANDS:{RESET}")
    print(f"  {BOLD}vcs-config{RESET} [--vcs-type <type>] [--remote-url <url>] [--username <user>] [--token <token>] [options]")
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
    print(f"  {BOLD}vcs-init{RESET} [<base directory>]")
    print("    Description: Initialize a VCS repository (Git or Mercurial) in the output directory or specified directory.")
    print("    Arguments:")
    print("      base directory: Directory to initialize the repository in (optional, defaults to output directory)")
    print("    Behavior:")
    print("      • Initializes a Git or Mercurial repository in the target directory")
    print("      • Creates a .gitignore or .hgignore with patterns: *.zip, config.env, *.log, ~/.adoc_vcs_config.json")
    print("      • Uses the output directory if no directory is specified")
    print("      • Shows next steps for adding, committing, and pushing files")
    
    print(f"\n  {BOLD}vcs-pull{RESET}")
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
    
    print(f"\n  {BOLD}vcs-push{RESET}")
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


def setup_autocomplete():
    """Setup command autocomplete functionality."""
    # This function will be moved from cli.py
    pass


def get_user_input(prompt: str) -> str:
    """Get user input with improved cursor handling."""
    try:
        if hasattr(sys.stdin, 'flush'):
            sys.stdin.flush()
        command = input(prompt).strip()
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


def run_interactive(args):
    """Run the interactive REST API client."""
    try:
        # Setup logging
        logger = setup_logging(args.verbose, args.log_level)
        
        # Validate arguments
        from ..cli.validators import validate_rest_api_arguments
        validate_rest_api_arguments(args)
        
        # Create API client
        client = create_api_client(env_file=args.env_file, logger=logger)
        
        # Test connection
        if not client.test_connection():
            logger.error("Failed to connect to API")
            return 1
        
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
                    'create-lineage',
                    'vcs-config',
                    'vcs-init',
                    'vcs-pull',
                    'vcs-push',
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
                    csv_file, output_file, quiet_mode, verbose_mode, parallel_mode = parse_asset_profile_export_command(command)
                    if csv_file:
                        if parallel_mode:
                            execute_asset_profile_export_parallel(csv_file, client, logger, output_file, quiet_mode, verbose_mode)
                        else:
                            execute_asset_profile_export(csv_file, client, logger, output_file, quiet_mode, verbose_mode)
                    continue
                
                # Check if it's an asset-profile-import command
                if command.lower().startswith('asset-profile-import'):
                    from .command_parsing import parse_asset_profile_import_command
                    csv_file, dry_run, quiet_mode, verbose_mode = parse_asset_profile_import_command(command)
                    if csv_file:
                        execute_asset_profile_import(csv_file, client, logger, dry_run, quiet_mode, verbose_mode)
                    continue
                
                # Check if it's an asset-config-export command
                if command.lower().startswith('asset-config-export'):
                    from .command_parsing import parse_asset_config_export_command
                    csv_file, output_file, quiet_mode, verbose_mode = parse_asset_config_export_command(command)
                    if csv_file:
                        execute_asset_config_export(csv_file, client, logger, output_file, quiet_mode, verbose_mode)
                    continue
                
                # Check if it's an asset-list-export command
                if command.lower().startswith('asset-list-export'):
                    from .command_parsing import parse_asset_list_export_command
                    quiet_mode, verbose_mode = parse_asset_list_export_command(command)
                    execute_asset_list_export(client, logger, quiet_mode, verbose_mode)
                    continue
                
                # Check if it's a policy-list-export command
                if command.lower().startswith('policy-list-export'):
                    from .command_parsing import parse_policy_list_export_command
                    quiet_mode, verbose_mode, parallel_mode = parse_policy_list_export_command(command)
                    if parallel_mode:
                        execute_policy_list_export_parallel(client, logger, quiet_mode, verbose_mode)
                    else:
                        execute_policy_list_export(client, logger, quiet_mode, verbose_mode)
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
                    input_dir, source_string, target_string, output_dir, quiet_mode, verbose_mode = parse_formatter_command(command)
                    if source_string and target_string:
                        execute_formatter(input_dir, source_string, target_string, output_dir, quiet_mode, verbose_mode, logger)
                    continue
                
                # Check if it's a set-output-dir command
                if command.lower().startswith('set-output-dir'):
                    from .output_management import parse_set_output_dir_command, set_global_output_directory
                    directory = parse_set_output_dir_command(command)
                    if directory:
                        set_global_output_directory(directory, logger)
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
                
                # Check if it's a create-lineage command
                if command.lower().startswith('create-lineage'):
                    from .command_parsing import parse_create_lineage_command
                    from .lineage_operations import execute_create_lineage
                    csv_file, dry_run, quiet_mode, verbose_mode = parse_create_lineage_command(command)
                    if csv_file:
                        execute_create_lineage(csv_file, client, logger, dry_run, quiet_mode, verbose_mode)
                    continue
                
                # Parse the command for GET/PUT requests
                from .command_parsing import parse_api_command
                method, endpoint, json_payload = parse_api_command(command)
                
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