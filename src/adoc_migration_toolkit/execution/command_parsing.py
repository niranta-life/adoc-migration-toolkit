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
        command: Command string like "asset-profile-export [<csv_file>] [--output-file <file>] [--quiet] [--verbose] [--parallel]"
        
    Returns:
        Tuple of (csv_file, output_file, quiet_mode, verbose_mode, parallel_mode)
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'asset-profile-export':
        return None, None, False, False, False
    
    csv_file = None
    output_file = None
    quiet_mode = False  # Default to showing progress bar and status
    verbose_mode = False
    parallel_mode = False
    
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
        elif parts[i] == '--parallel':
            parallel_mode = True
            parts.remove('--parallel')
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
    
    return csv_file, output_file, quiet_mode, verbose_mode, parallel_mode

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
        command: Command string like "asset-config-export [<csv_file>] [--output-file <file>] [--quiet] [--verbose] [--parallel]"
        
    Returns:
        Tuple of (csv_file, output_file, quiet_mode, verbose_mode, parallel_mode)
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'asset-config-export':
        return None, None, False, False, False
    
    csv_file = None
    output_file = None
    quiet_mode = False
    verbose_mode = False
    parallel_mode = False
    
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
        elif parts[i] == '--parallel':
            parallel_mode = True
            parts.remove('--parallel')
        elif i == 1 and not parts[i].startswith('--'):
            # This is the CSV file argument (first non-flag argument)
            csv_file = parts[i]
            parts.remove(parts[i])
        else:
            i += 1
    
    # Set default CSV file if not provided
    if not csv_file:
        from ..shared import globals
        if globals.GLOBAL_OUTPUT_DIR:
            csv_file = str(globals.GLOBAL_OUTPUT_DIR / "asset-export" / "asset-all-export.csv")
        else:
            csv_file = "asset-export/asset-all-export.csv"
        
        # Check if the default file exists
        import os
        if not os.path.exists(csv_file):
            error_msg = f"Default CSV file not found: {csv_file}"
            if globals.GLOBAL_OUTPUT_DIR:
                error_msg += f"\n💡 Please run 'asset-list-export' first to generate the asset-all-export.csv file"
                error_msg += f"\n   Expected location: {globals.GLOBAL_OUTPUT_DIR}/asset-export/asset-all-export.csv"
            else:
                error_msg += f"\n💡 Please run 'asset-list-export' first to generate the asset-all-export.csv file"
                error_msg += f"\n   Expected location: asset-export/asset-all-export.csv"
            raise FileNotFoundError(error_msg)
    
    # Generate default output file if not provided
    if not output_file:
        output_file = get_output_file_path(csv_file, "asset-config-export.csv", category="asset-export")
    
    # For parallel mode, default to quiet mode unless verbose is explicitly specified
    if parallel_mode and not verbose_mode and not quiet_mode:
        quiet_mode = True
    
    return csv_file, output_file, quiet_mode, verbose_mode, parallel_mode

def parse_asset_list_export_command(command: str) -> tuple:
    """Parse an asset-list-export command string into components.
    
    Args:
        command: Command string like "asset-list-export [--quiet] [--verbose] [--parallel]"
        
    Returns:
        Tuple of (quiet_mode, verbose_mode, parallel_mode)
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'asset-list-export':
        return False, False, False
    
    quiet_mode = False
    verbose_mode = False
    parallel_mode = False
    
    # Check for flags
    if '--quiet' in parts:
        quiet_mode = True
        verbose_mode = False  # Quiet overrides verbose
        parts.remove('--quiet')
    
    if '--verbose' in parts:
        verbose_mode = True
        quiet_mode = False  # Verbose overrides quiet
        parts.remove('--verbose')
    
    if '--parallel' in parts:
        parallel_mode = True
        parts.remove('--parallel')
    
    return quiet_mode, verbose_mode, parallel_mode

def parse_policy_list_export_command(command: str) -> tuple:
    """Parse a policy-list-export command string into components.
    
    Args:
        command: Command string like "policy-list-export [--quiet] [--verbose] [--parallel]"
        
    Returns:
        Tuple of (quiet_mode, verbose_mode, parallel_mode)
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'policy-list-export':
        return False, False, False
    
    quiet_mode = False
    verbose_mode = False
    parallel_mode = False
    
    # Check for flags
    if '--quiet' in parts:
        quiet_mode = True
        verbose_mode = False  # Quiet overrides verbose
        parts.remove('--quiet')
    
    if '--verbose' in parts:
        verbose_mode = True
        quiet_mode = False  # Verbose overrides quiet
        parts.remove('--verbose')
    
    if '--parallel' in parts:
        parallel_mode = True
        parts.remove('--parallel')
    
    return quiet_mode, verbose_mode, parallel_mode

def parse_policy_export_command(command: str) -> tuple:
    """Parse a policy-export command string into components.
    
    Args:
        command: Command string like "policy-export [--type <export_type>] [--filter <filter_value>] [--quiet] [--verbose] [--batch-size <size>] [--parallel]"
        
    Returns:
        Tuple of (quiet_mode, verbose_mode, batch_size, export_type, filter_value, parallel_mode)
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'policy-export':
        return False, False, 50, None, None, False
    
    quiet_mode = False
    verbose_mode = False
    batch_size = 50  # Default batch size
    export_type = None
    filter_value = None
    parallel_mode = False
    
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
        elif parts[i] == '--parallel':
            parallel_mode = True
            parts.remove('--parallel')
        else:
            i += 1
    
    return quiet_mode, verbose_mode, batch_size, export_type, filter_value, parallel_mode

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
        tuple: (quiet_mode, verbose_mode, parallel_mode)
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'rule-tag-export':
        return False, False, False
    
    quiet_mode = False
    verbose_mode = False
    parallel_mode = False
    
    # Check for flags
    for i in range(1, len(parts)):
        if parts[i] == '--quiet' or parts[i] == '-q':
            quiet_mode = True
        elif parts[i] == '--verbose' or parts[i] == '-v':
            verbose_mode = True
        elif parts[i] == '--parallel':
            parallel_mode = True
        elif parts[i] == '--help' or parts[i] == '-h':
            print("\n" + "="*60)
            print("RULE-TAG-EXPORT COMMAND HELP")
            print("="*60)
            print("Usage: rule-tag-export [options]")
            print("\nOptions:")
            print("  --quiet, -q        Quiet mode (minimal output with progress bar)")
            print("  --verbose, -v      Verbose mode (detailed output)")
            print("  --parallel         Use parallel processing for faster export (max 5 threads)")
            print("  --help, -h         Show this help message")
            print("\nExamples:")
            print("  rule-tag-export")
            print("  rule-tag-export --quiet")
            print("  rule-tag-export --verbose")
            print("  rule-tag-export --parallel")
            print("\nFeatures:")
            print("  - Exports rule tags for all policies from policies-all-export.csv")
            print("  - Automatically runs policy-list-export if policies-all-export.csv doesn't exist")
            print("  - Makes API calls to /catalog-server/api/rules/<id>/tags for each rule")
            print("  - Outputs to rule-tags-export.csv with rule ID and comma-separated tags")
            print("  - Shows progress bar in quiet mode")
            print("  - Shows detailed API calls in verbose mode")
            print("  - Provides comprehensive statistics upon completion")
            print("  - Parallel mode: Uses up to 5 threads to process rules simultaneously")
            print("  - Parallel mode: Each thread has its own progress bar")
            print("  - Parallel mode: Significantly faster for large rule sets")
            print("="*60)
            return False, False, False
    
    return quiet_mode, verbose_mode, parallel_mode

def parse_vcs_config_command(command: str) -> tuple:
    """Parse a vcs-config command string into components.
    
    Args:
        command: Command string like "vcs-config [--vcs-type git] [--remote-url url] [--username user] [--token token]"
        
    Returns:
        Tuple of (vcs_type, remote_url, username, token, ssh_key_path, ssh_passphrase, proxy_url, proxy_username, proxy_password)
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'vcs-config':
        return None, None, None, None, None, None, None, None, None
    
    vcs_type = None
    remote_url = None
    username = None
    token = None
    ssh_key_path = None
    ssh_passphrase = None
    proxy_url = None
    proxy_username = None
    proxy_password = None
    
    # Parse arguments
    i = 1
    while i < len(parts):
        if parts[i] == '--vcs-type' and i + 1 < len(parts):
            vcs_type = parts[i + 1]
            parts.pop(i)  # Remove --vcs-type
            parts.pop(i)  # Remove the value
        elif parts[i] == '--remote-url' and i + 1 < len(parts):
            remote_url = parts[i + 1]
            parts.pop(i)  # Remove --remote-url
            parts.pop(i)  # Remove the value
        elif parts[i] == '--username' and i + 1 < len(parts):
            username = parts[i + 1]
            parts.pop(i)  # Remove --username
            parts.pop(i)  # Remove the value
        elif parts[i] == '--token' and i + 1 < len(parts):
            token = parts[i + 1]
            parts.pop(i)  # Remove --token
            parts.pop(i)  # Remove the value
        elif parts[i] == '--ssh-key-path' and i + 1 < len(parts):
            ssh_key_path = parts[i + 1]
            parts.pop(i)  # Remove --ssh-key-path
            parts.pop(i)  # Remove the value
        elif parts[i] == '--ssh-passphrase' and i + 1 < len(parts):
            ssh_passphrase = parts[i + 1]
            parts.pop(i)  # Remove --ssh-passphrase
            parts.pop(i)  # Remove the value
        elif parts[i] == '--proxy-url' and i + 1 < len(parts):
            proxy_url = parts[i + 1]
            parts.pop(i)  # Remove --proxy-url
            parts.pop(i)  # Remove the value
        elif parts[i] == '--proxy-username' and i + 1 < len(parts):
            proxy_username = parts[i + 1]
            parts.pop(i)  # Remove --proxy-username
            parts.pop(i)  # Remove the value
        elif parts[i] == '--proxy-password' and i + 1 < len(parts):
            proxy_password = parts[i + 1]
            parts.pop(i)  # Remove --proxy-password
            parts.pop(i)  # Remove the value
        elif parts[i] == '--help' or parts[i] == '-h':
            print("\n" + "="*60)
            print("VCS-CONFIG COMMAND HELP")
            print("="*60)
            print("Usage: vcs-config [options]")
            print("\nOptions:")
            print("  --vcs-type <type>        VCS type (git, hg, svn)")
            print("  --remote-url <url>       Remote repository URL")
            print("  --username <user>        Username for HTTPS authentication")
            print("  --token <token>          Token/password for HTTPS authentication")
            print("  --ssh-key-path <path>    Path to SSH private key")
            print("  --ssh-passphrase <pass>  SSH key passphrase")
            print("  --proxy-url <url>        HTTP/HTTPS proxy URL")
            print("  --proxy-username <user>  Proxy username")
            print("  --proxy-password <pass>  Proxy password")
            print("  --help, -h               Show this help message")
            print("\nExamples:")
            print("  vcs-config  # Interactive mode")
            print("  vcs-config --vcs-type git --remote-url https://github.com/user/repo.git")
            print("  vcs-config --vcs-type git --remote-url git@github.com:user/repo.git --ssh-key-path ~/.ssh/id_rsa")
            print("  vcs-config --vcs-type git --remote-url https://enterprise.gitlab.com/repo.git --username user --token <token>")
            print("\nFeatures:")
            print("  - Interactive configuration mode")
            print("  - Supports Git, Mercurial, and Subversion")
            print("  - HTTPS authentication with username/token")
            print("  - SSH authentication with key and passphrase")
            print("  - HTTP/HTTPS proxy support")
            print("  - Secure credential storage in system keyring")
            print("  - Configuration stored in ~/.adoc_vcs_config.json")
            print("="*60)
            return None, None, None, None, None, None, None, None, None
        else:
            i += 1
    
    return vcs_type, remote_url, username, token, ssh_key_path, ssh_passphrase, proxy_url, proxy_username, proxy_password

def parse_vcs_init_command(command: str) -> str:
    """Parse a vcs-init command string to extract the base directory (if any).
    Args:
        command: Command string like 'vcs-init [<base directory>]'
    Returns:
        base_dir: The base directory argument, or None if not provided
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'vcs-init':
        return None
    if len(parts) > 1:
        return parts[1]
    return None

def parse_vcs_pull_command(command: str) -> bool:
    """Parse a vcs-pull command string.
    Args:
        command: Command string like 'vcs-pull'
    Returns:
        True if it's a vcs-pull command, False otherwise
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'vcs-pull':
        return False
    return True

def parse_vcs_push_command(command: str) -> bool:
    """Parse a vcs-push command string.
    Args:
        command: Command string like 'vcs-push'
    Returns:
        True if it's a vcs-push command, False otherwise
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'vcs-push':
        return False
    return True 

def parse_asset_tag_import_command(command: str) -> tuple:
    """Parse an asset-tag-import command string into components.
    
    Args:
        command: Command string like "asset-tag-import [csv_file] [--quiet] [--verbose] [--parallel]"
        
    Returns:
        Tuple of (csv_file, quiet_mode, verbose_mode, parallel_mode)
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'asset-tag-import':
        return None, False, False, False
    
    csv_file = None
    quiet_mode = False
    verbose_mode = False
    parallel_mode = False
    
    # Check for flags and options
    i = 1
    while i < len(parts):
        arg = parts[i]
        if arg == '--quiet' or arg == '-q':
            quiet_mode = True
            i += 1
        elif arg == '--verbose' or arg == '-v':
            verbose_mode = True
            i += 1
        elif arg == '--parallel' or arg == '-p':
            parallel_mode = True
            i += 1
        elif arg == '--help' or arg == '-h':
            print("\n" + "="*60)
            print("ASSET-TAG-IMPORT COMMAND HELP")
            print("="*60)
            print("Usage: asset-tag-import [csv_file] [options]")
            print("\nArguments:")
            print("  csv_file: Path to CSV file (defaults to asset-all-import-ready.csv)")
            print("\nOptions:")
            print("  --quiet, -q: Suppress console output, show only summary")
            print("  --verbose, -v: Show detailed output including API calls")
            print("  --parallel, -p: Use parallel processing for faster import")
            print("  --help, -h: Show this help message")
            print("\nExamples:")
            print("  asset-tag-import")
            print("  asset-tag-import --quiet")
            print("  asset-tag-import --verbose")
            print("  asset-tag-import --parallel")
            print("  asset-tag-import /path/to/asset-data.csv --verbose --parallel")
            print("="*60)
            return None, False, False, False
        else:
            # This should be the CSV file path
            if csv_file is None:
                csv_file = arg
            else:
                print(f"❌ Unknown argument: {arg}")
                print("💡 Use 'asset-tag-import --help' for usage information")
                return None, False, False, False
            i += 1
    
    return csv_file, quiet_mode, verbose_mode, parallel_mode 

def parse_asset_config_import_command(command: str) -> tuple:
    """Parse an asset-config-import command string into components.
    
    Args:
        command: Command string like "asset-config-import [<csv_file>] [--quiet] [--verbose] [--parallel]"
        
    Returns:
        Tuple of (csv_file, quiet_mode, verbose_mode, parallel_mode)
    """
    parts = command.strip().split()
    if not parts or parts[0].lower() != 'asset-config-import':
        return None, False, False, False
    
    csv_file = None
    quiet_mode = False
    verbose_mode = False
    parallel_mode = False
    
    # Check for flags and options
    i = 1
    while i < len(parts):
        arg = parts[i]
        if arg == '--quiet' or arg == '-q':
            quiet_mode = True
            i += 1
        elif arg == '--verbose' or arg == '-v':
            verbose_mode = True
            i += 1
        elif arg == '--parallel':
            parallel_mode = True
            i += 1
        elif arg == '--help' or arg == '-h':
            print("\n" + "="*60)
            print("ASSET-CONFIG-IMPORT COMMAND HELP")
            print("="*60)
            print("Usage: asset-config-import [<csv_file>] [--quiet] [--verbose] [--parallel]")
            print("\nArguments:")
            print("  csv_file: Path to CSV file with target_uid and config_json columns (optional)")
            print("\nOptions:")
            print("  --quiet, -q                   Quiet mode (shows progress bars)")
            print("  --verbose, -v                 Verbose mode (shows HTTP details)")
            print("  --parallel                    Use parallel processing (max 5 threads)")
            print("  --help, -h                    Show this help message")
            print("\nExamples:")
            print("  asset-config-import")
            print("  asset-config-import /path/to/asset-config-import-ready.csv")
            print("  asset-config-import --quiet --parallel")
            print("  asset-config-import --verbose")
            print("="*60)
            return None, False, False, False
        else:
            # This should be the CSV file path
            if csv_file is None:
                csv_file = arg
                i += 1
            else:
                print(f"❌ Unknown argument: {arg}")
                print("💡 Use 'asset-config-import --help' for usage information")
                return None, False, False, False
    
    return csv_file, quiet_mode, verbose_mode, parallel_mode 