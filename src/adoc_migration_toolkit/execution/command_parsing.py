import json
import os
from pathlib import Path
from adoc_migration_toolkit.shared import globals
from ..shared.file_utils import get_output_file_path

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
        if globals.GLOBAL_OUTPUT_DIR:
            csv_file = str(globals.GLOBAL_OUTPUT_DIR / "policy-export" / "segmented_spark_uids.csv")
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
        if globals.GLOBAL_OUTPUT_DIR:
            csv_file = str(globals.GLOBAL_OUTPUT_DIR / "asset-export" / "asset_uids.csv")
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
        if globals.GLOBAL_OUTPUT_DIR:
            csv_file = str(globals.GLOBAL_OUTPUT_DIR / "asset-import" / "asset-profiles-import-ready.csv")
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
            print("  file_or_pattern: File path or glob pattern (e.g., *.json, data/*.zip)")
            print("\nOptions:")
            print("  --quiet, -q        Quiet mode (minimal output)")
            print("  --verbose, -v      Verbose mode (detailed output)")
            print("  --help, -h         Show this help message")
            print("\nExamples:")
            print("  policy-import data/policy-export/*.json")
            print("  policy-import policy-export.zip --quiet")
            print("  policy-import *.json --verbose")
            print("\nFeatures:")
            print("  - Supports file paths and glob patterns")
            print("  - Processes JSON files and ZIP archives")
            print("  - Validates file format before import")
            print("  - Shows progress and detailed statistics")
            print("  - Comprehensive error handling and logging")
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