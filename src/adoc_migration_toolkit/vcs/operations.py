"""
VCS Operations module.

This module contains the execution functions for VCS-related interactive commands.
"""

import getpass
import re
import time
from typing import Optional, Tuple
from .config import VCSConfig, VCSConfigManager
import os
import subprocess
from pathlib import Path
from adoc_migration_toolkit.shared import globals


def execute_vcs_config(command: str) -> bool:
    """Execute vcs-config command.
    
    Args:
        command: Command string like "vcs-config [--vcs-type git] [--remote-url url] [--username user] [--token token]"
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Parse command arguments
        vcs_type, remote_url, username, token, ssh_key_path, ssh_passphrase, proxy_url, proxy_username, proxy_password = parse_vcs_config_command(command)
        
        # Create config manager
        config_manager = VCSConfigManager()
        
        # Interactive mode if no arguments provided
        if not vcs_type and not remote_url:
            return execute_vcs_config_interactive(config_manager)
        
        # Non-interactive mode with provided arguments
        config = VCSConfig(
            vcs_type=vcs_type,
            remote_url=remote_url,
            username=username,
            token=token,
            ssh_key_path=ssh_key_path,
            ssh_passphrase=ssh_passphrase,
            proxy_url=proxy_url,
            proxy_username=proxy_username,
            proxy_password=proxy_password
        )
        
        return save_vcs_config(config_manager, config)
        
    except Exception as e:
        print(f"❌ Error executing vcs-config command: {e}")
        return False


def parse_vcs_config_command(command: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Parse vcs-config command string into components.
    
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
        else:
            i += 1
    
    return vcs_type, remote_url, username, token, ssh_key_path, ssh_passphrase, proxy_url, proxy_username, proxy_password


def execute_vcs_config_interactive(config_manager: VCSConfigManager) -> bool:
    """Execute vcs-config in interactive mode.
    
    Args:
        config_manager: VCS configuration manager
        
    Returns:
        True if successful, False otherwise
    """
    print("\n🔧 VCS Configuration Setup")
    print("=" * 50)
    
    # Load existing config if available
    existing_config = config_manager.load_config()
    if existing_config:
        print(f"📋 Current configuration found:")
        print(f"   VCS Type: {existing_config.vcs_type}")
        print(f"   Remote URL: {existing_config.remote_url}")
        print(f"   Username: {existing_config.username or 'Not set'}")
        print(f"   SSH Key: {existing_config.ssh_key_path or 'Not set'}")
        print(f"   Proxy: {existing_config.proxy_url or 'Not set'}")
        
        overwrite = input("\n❓ Overwrite existing configuration? (y/N): ").strip().lower()
        if overwrite not in ['y', 'yes']:
            print("❌ Configuration not updated.")
            return True
    
    # Collect configuration interactively
    config = collect_vcs_config_interactive(existing_config)
    
    if not config:
        print("❌ Configuration cancelled.")
        return False
    
    return save_vcs_config(config_manager, config)


def collect_vcs_config_interactive(existing_config: Optional[VCSConfig] = None) -> Optional[VCSConfig]:
    """Collect VCS configuration interactively.
    
    Args:
        existing_config: Existing configuration to use as defaults
        
    Returns:
        VCSConfig object if successful, None if cancelled
    """
    try:
        # VCS Type
        vcs_type = input(f"VCS type (git, hg, svn) [{existing_config.vcs_type if existing_config else 'git'}]: ").strip()
        if not vcs_type:
            vcs_type = existing_config.vcs_type if existing_config else 'git'
        
        if vcs_type.lower() not in ['git', 'hg', 'svn']:
            print("❌ Invalid VCS type. Must be git, hg, or svn.")
            return None
        
        # Remote URL
        remote_url = input(f"Remote repository URL [{existing_config.remote_url if existing_config else ''}]: ").strip()
        if not remote_url:
            remote_url = existing_config.remote_url if existing_config else ''
        
        if not remote_url:
            print("❌ Remote URL is required.")
            return None
        
        # Validate URL format
        if not is_valid_remote_url(remote_url):
            print("❌ Invalid remote URL format.")
            return None
        
        # Allow user to choose authentication method
        detected_auth = determine_auth_method(remote_url)
        print(f"\n🔐 Detected authentication method: {detected_auth.upper()}")
        
        auth_choice = input(f"Authentication method (https/ssh) [{detected_auth}]: ").strip().lower()
        if not auth_choice:
            auth_method = detected_auth
        elif auth_choice in ['https', 'ssh']:
            auth_method = auth_choice
        else:
            print("❌ Invalid authentication method. Must be 'https' or 'ssh'.")
            return None
        
        username = None
        token = None
        ssh_key_path = None
        ssh_passphrase = None
        
        print(f"\n🔐 Using {auth_method.upper()} authentication")
        
        if auth_method == 'https':
            # HTTPS Authentication
            username = input(f"Username [{existing_config.username if existing_config else ''}]: ").strip()
            if not username:
                username = existing_config.username if existing_config else ''
            
            if username:
                token = getpass.getpass("Token (hidden): ")
                if not token and existing_config and existing_config.token:
                    token = existing_config.token
            else:
                print("⚠️  No username provided. HTTPS authentication may fail.")
        
        elif auth_method == 'ssh':
            # SSH Authentication
            print("SSH authentication requires a private key file.")
            ssh_key_path = input(f"SSH key path [{existing_config.ssh_key_path if existing_config else '~/.ssh/id_rsa'}]: ").strip()
            if not ssh_key_path:
                ssh_key_path = existing_config.ssh_key_path if existing_config else '~/.ssh/id_rsa'
            
            # Always ask for SSH key path, even if empty
            if not ssh_key_path:
                ssh_key_path = input("SSH key path (required for SSH authentication): ").strip()
                if not ssh_key_path:
                    print("❌ SSH key path is required for SSH authentication.")
                    return None
            
            # Validate SSH key exists
            expanded_ssh_key_path = os.path.expanduser(ssh_key_path)
            if not os.path.exists(expanded_ssh_key_path):
                print(f"⚠️  Warning: SSH key not found at {expanded_ssh_key_path}")
                continue_anyway = input("Continue anyway? (y/N): ").strip().lower()
                if continue_anyway not in ['y', 'yes']:
                    return None
            
            ssh_passphrase = getpass.getpass("SSH passphrase (optional, hidden): ")
            if not ssh_passphrase and existing_config and existing_config.ssh_passphrase:
                ssh_passphrase = existing_config.ssh_passphrase
        
        # Proxy Configuration
        proxy_url = input(f"HTTP/HTTPS proxy (optional) [{existing_config.proxy_url if existing_config else ''}]: ").strip()
        if not proxy_url:
            proxy_url = existing_config.proxy_url if existing_config else ''
        
        proxy_username = None
        proxy_password = None
        
        if proxy_url:
            proxy_username = input(f"Proxy username (optional) [{existing_config.proxy_username if existing_config else ''}]: ").strip()
            if not proxy_username:
                proxy_username = existing_config.proxy_username if existing_config else ''
            
            if proxy_username:
                proxy_password = getpass.getpass("Proxy password (optional, hidden): ")
                if not proxy_password and existing_config and existing_config.proxy_password:
                    proxy_password = existing_config.proxy_password
        
        # Create configuration object
        config = VCSConfig(
            vcs_type=vcs_type.lower(),
            remote_url=remote_url,
            username=username,
            token=token,
            ssh_key_path=ssh_key_path,
            ssh_passphrase=ssh_passphrase,
            proxy_url=proxy_url,
            proxy_username=proxy_username,
            proxy_password=proxy_password
        )
        
        return config
        
    except KeyboardInterrupt:
        print("\n❌ Configuration cancelled by user.")
        return None
    except Exception as e:
        print(f"❌ Error collecting configuration: {e}")
        return None


def check_vcs_client_installed(vcs_type: str) -> bool:
    """Check if the required VCS client is installed.
    
    Args:
        vcs_type: VCS type (git, hg, svn)
        
    Returns:
        True if installed, False otherwise
    """
    import subprocess
    
    try:
        if vcs_type == 'git':
            result = subprocess.run(['git', '--version'], 
                                  capture_output=True, text=True, check=True)
            version = result.stdout.strip()
            print(f"✅ Git client found: {version}")
            return True
        elif vcs_type == 'hg':
            result = subprocess.run(['hg', '--version'], 
                                  capture_output=True, text=True, check=True)
            # Mercurial version output is multi-line, get first line
            version = result.stdout.split('\n')[0].strip()
            print(f"✅ Mercurial client found: {version}")
            return True
        elif vcs_type == 'svn':
            result = subprocess.run(['svn', '--version'], 
                                  capture_output=True, text=True, check=True)
            # Subversion version output is multi-line, get first line
            version = result.stdout.split('\n')[0].strip()
            print(f"✅ Subversion client found: {version}")
            return True
        else:
            print(f"❌ Unknown VCS type: {vcs_type}")
            return False
    except subprocess.CalledProcessError:
        return False
    except FileNotFoundError:
        return False


def save_vcs_config(config_manager: VCSConfigManager, config: VCSConfig) -> bool:
    """Save VCS configuration and display summary.
    
    Args:
        config_manager: VCS configuration manager
        config: VCS configuration object
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Check if VCS client is installed
        print(f"\n🔍 Checking for {config.vcs_type} client installation...")
        if not check_vcs_client_installed(config.vcs_type):
            print(f"❌ {config.vcs_type.upper()} client is not installed or not available in PATH.")
            print(f"\n💡 To use {config.vcs_type} commands, you need to install the client first:")
            
            if config.vcs_type == 'git':
                print("   • macOS: brew install git")
                print("   • Ubuntu/Debian: sudo apt-get install git")
                print("   • CentOS/RHEL: sudo yum install git")
                print("   • Windows: Download from https://git-scm.com/")
                print("   • Or use your system's package manager")
            elif config.vcs_type == 'hg':
                print("   • macOS: brew install mercurial")
                print("   • Ubuntu/Debian: sudo apt-get install mercurial")
                print("   • CentOS/RHEL: sudo yum install mercurial")
                print("   • Windows: Download from https://www.mercurial-scm.org/")
                print("   • Or use your system's package manager")
            elif config.vcs_type == 'svn':
                print("   • macOS: brew install subversion")
                print("   • Ubuntu/Debian: sudo apt-get install subversion")
                print("   • CentOS/RHEL: sudo yum install subversion")
                print("   • Windows: Download from https://subversion.apache.org/")
                print("   • Or use your system's package manager")
            
            print(f"\n⚠️  Configuration will be saved, but {config.vcs_type} commands will not work until the client is installed.")
            
            # Ask user if they want to continue
            response = input(f"\n❓ Continue saving configuration anyway? (y/N): ").strip().lower()
            if response not in ['y', 'yes']:
                print("❌ Configuration cancelled.")
                return False
        
        # Save configuration
        if config_manager.save_config(config):
            print("\n✅ VCS configuration saved successfully!")
            print("\n📋 Configuration Summary:")
            print(f"   VCS Type: {config.vcs_type}")
            print(f"   Remote URL: {config.remote_url}")
            print(f"   Username: {config.username or 'Not set'}")
            print(f"   SSH Key: {config.ssh_key_path or 'Not set'}")
            print(f"   Proxy: {config.proxy_url or 'Not set'}")
            print(f"   Config File: {config_manager.config_file}")
            print(f"   Credentials: Stored securely in system keyring")
            
            # Show next steps
            print("\n💡 Next Steps:")
            if config.vcs_type == 'git':
                print("   • Use 'vcs-init' to initialize a repository")
                print("   • Use 'vcs-pull' to pull updates")
                print("   • Use 'vcs-push' to push changes")
            elif config.vcs_type == 'hg':
                print("   • Use 'vcs-init' to initialize a repository")
                print("   • Use 'vcs-pull' to pull updates")
                print("   • Use 'vcs-push' to push changes")
            elif config.vcs_type == 'svn':
                print("   • Use 'vcs-init' to initialize a repository")
                print("   • Use 'vcs-pull' to pull updates")
                print("   • Use 'vcs-push' to push changes")
            
            return True
        else:
            print("❌ Failed to save VCS configuration.")
            return False
            
    except Exception as e:
        print(f"❌ Error saving VCS configuration: {e}")
        return False


def is_valid_remote_url(url: str) -> bool:
    """Validate remote URL format.
    
    Args:
        url: Remote URL to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not url or not url.strip():
        return False
    
    # Basic URL validation patterns
    https_pattern = r'^https?://[^\s/$.?#].[^\s]*$'
    ssh_pattern = r'^(ssh://)?[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(:[0-9]+)?(/[^\s]*)?$'
    git_pattern = r'^git@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}:[^\s]*$'
    user_ssh_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}:[^\s]*$'
    
    return bool(re.match(https_pattern, url) or re.match(ssh_pattern, url) or re.match(git_pattern, url) or re.match(user_ssh_pattern, url))


def determine_auth_method(url: str) -> str:
    """Determine authentication method based on URL.
    
    Args:
        url: Remote URL
        
    Returns:
        'https' or 'ssh'
    """
    if url.startswith(('http://', 'https://')):
        return 'https'
    elif url.startswith(('ssh://', 'git@')):
        return 'ssh'
    else:
        # Default to HTTPS for unknown formats
        return 'https'


def execute_vcs_init(command: str, output_dir: str = None) -> bool:
    """Execute vcs-init command to initialize a VCS repository in the output directory.
    Args:
        command: Command string like 'vcs-init [<base directory>]'
        output_dir: The default output directory to use if not specified in command
    Returns:
        True if successful, False otherwise
    """
    # Parse command for base directory
    parts = command.strip().split()
    base_dir = None
    if len(parts) > 1:
        base_dir = parts[1]
    elif output_dir:
        base_dir = output_dir
    elif getattr(globals, 'GLOBAL_OUTPUT_DIR', None):
        base_dir = str(globals.GLOBAL_OUTPUT_DIR)
    else:
        base_dir = os.getcwd()

    base_dir = os.path.abspath(os.path.expanduser(base_dir))
    print(f"\n🔧 Initializing VCS repository in: {base_dir}")

    # Choose VCS type (default: git)
    vcs_type = 'git'
    if os.path.exists(os.path.join(base_dir, '.hg')):
        vcs_type = 'hg'
    # Optionally, prompt user for VCS type if both are present

    # Initialize repository
    try:
        if vcs_type == 'git':
            if not os.path.exists(os.path.join(base_dir, '.git')):
                # Suppress Git warnings about default branch name
                result = subprocess.run(['git', 'init'], cwd=base_dir, check=True, 
                                      stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
                print("✅ Initialized Git repository.")
            ignore_file = os.path.join(base_dir, '.gitignore')
        elif vcs_type == 'hg':
            if not os.path.exists(os.path.join(base_dir, '.hg')):
                subprocess.run(['hg', 'init'], cwd=base_dir, check=True, 
                             capture_output=True, text=True)
                print("✅ Initialized Mercurial repository.")
            ignore_file = os.path.join(base_dir, '.hgignore')
        else:
            print(f"❌ Unsupported VCS type: {vcs_type}")
            return False
    except Exception as e:
        print(f"❌ Error initializing repository: {e}")
        return False

    # Write ignore file
    ignore_patterns = [
        '*.zip',
        'config.env',
        '*.log',
        '.adoc_backup_*/',
        str(Path.home() / '.adoc_vcs_config.json'),
    ]
    try:
        with open(ignore_file, 'w') as f:
            if vcs_type == 'git':
                for pattern in ignore_patterns:
                    f.write(pattern + '\n')
            elif vcs_type == 'hg':
                f.write('syntax: glob\n')
                for pattern in ignore_patterns:
                    f.write(pattern + '\n')
        print(f"✅ Created {os.path.basename(ignore_file)} with patterns: {', '.join(ignore_patterns)}")
    except Exception as e:
        print(f"❌ Error writing ignore file: {e}")
        return False

    print("\n💡 Next steps:")
    print(f"   • Use 'vcs-config' to configure remote repository settings")
    print(f"   • Use 'vcs-add' to stage files for commit")
    print(f"   • Use 'vcs-commit' to commit changes")
    print(f"   • Use 'vcs-push' to push to remote repository")
    return True


def execute_vcs_pull(command: str, output_dir: str = None) -> bool:
    """Execute vcs-pull command to pull updates from the configured repository.
    Args:
        command: Command string like 'vcs-pull'
        output_dir: The output directory to use for pulling updates
    Returns:
        True if successful, False otherwise
    """
    import subprocess
    from pathlib import Path
    from adoc_migration_toolkit.shared import globals
    
    # Determine the target directory
    if output_dir:
        target_dir = output_dir
    elif getattr(globals, 'GLOBAL_OUTPUT_DIR', None):
        target_dir = str(globals.GLOBAL_OUTPUT_DIR)
    else:
        print("❌ No output directory set. Use 'set-output-dir <directory>' first.")
        return False
    
    target_dir = os.path.abspath(os.path.expanduser(target_dir))
    print(f"\n📥 Pulling updates from repository to: {target_dir}")
    
    # Load VCS configuration
    config_manager = VCSConfigManager()
    config = config_manager.load_config()
    
    if not config:
        print("❌ No VCS configuration found.")
        print("\n💡 To use vcs-pull, you need to configure your repository settings first:")
        print("   • Run 'vcs-config' to set up your repository configuration")
        print("   • This will configure authentication, remote URL, and other settings")
        print("   • Example: vcs-config --vcs-type git --remote-url https://github.com/user/repo.git")
        print("   • Or run 'vcs-config' for interactive configuration")
        return False
    
    print(f"📋 Using VCS configuration:")
    print(f"   VCS Type: {config.vcs_type}")
    print(f"   Remote URL: {config.remote_url}")
    
    # Check if directory exists and is a repository
    if not os.path.exists(target_dir):
        print(f"❌ Target directory does not exist: {target_dir}")
        return False
    
    # Determine if it's a Git or Mercurial repository
    vcs_type = None
    if os.path.exists(os.path.join(target_dir, '.git')):
        vcs_type = 'git'
    elif os.path.exists(os.path.join(target_dir, '.hg')):
        vcs_type = 'hg'
    else:
        print(f"❌ No VCS repository found in {target_dir}")
        print("   Use 'vcs-init' to initialize a repository first.")
        return False
    
    # Set up environment variables for authentication
    env = os.environ.copy()
    
    if vcs_type == 'git':
        # Configure Git authentication
        if config.username and config.token:
            # HTTPS authentication
            remote_url = config.remote_url
            if remote_url.startswith('https://'):
                # Insert credentials into URL
                from urllib.parse import urlparse
                parsed = urlparse(remote_url)
                auth_url = f"https://{config.username}:{config.token}@{parsed.netloc}{parsed.path}"
                remote_url = auth_url
            
            # Set Git configuration
            try:
                subprocess.run(['git', 'config', '--local', 'user.name', config.username], 
                             cwd=target_dir, check=True, capture_output=True)
                subprocess.run(['git', 'config', '--local', 'user.email', f"{config.username}@adoc-migration-toolkit"], 
                             cwd=target_dir, check=True, capture_output=True)
            except subprocess.CalledProcessError:
                pass  # Ignore if already configured
        
        elif config.ssh_key_path:
            # SSH authentication
            ssh_key_path = os.path.expanduser(config.ssh_key_path)
            if not os.path.exists(ssh_key_path):
                print(f"❌ SSH key not found: {ssh_key_path}")
                return False
            
            # Set up SSH agent or GIT_SSH_COMMAND
            if config.ssh_passphrase:
                # Use ssh-agent with passphrase
                env['GIT_SSH_COMMAND'] = f'ssh -i {ssh_key_path}'
            else:
                env['GIT_SSH_COMMAND'] = f'ssh -i {ssh_key_path} -o IdentitiesOnly=yes'
        
        # Set proxy if configured
        if config.proxy_url:
            env['HTTP_PROXY'] = config.proxy_url
            env['HTTPS_PROXY'] = config.proxy_url
            if config.proxy_username and config.proxy_password:
                env['HTTP_PROXY_AUTH'] = f"{config.proxy_username}:{config.proxy_password}"
        
        # Set up remote and tracking information
        try:
            # Check if remote 'origin' exists
            result = subprocess.run(['git', 'remote', 'get-url', 'origin'], 
                                  cwd=target_dir, capture_output=True, text=True)
            
            if result.returncode != 0:
                # No remote exists, add it
                print("🔧 Setting up remote 'origin'...")
                subprocess.run(['git', 'remote', 'add', 'origin', config.remote_url], 
                             cwd=target_dir, check=True, capture_output=True)
            else:
                # Remote exists, update it if different
                current_url = result.stdout.strip()
                if current_url != config.remote_url:
                    print("🔧 Updating remote 'origin' URL...")
                    subprocess.run(['git', 'remote', 'set-url', 'origin', config.remote_url], 
                                 cwd=target_dir, check=True, capture_output=True)
            
            # Get current branch name
            branch_result = subprocess.run(['git', 'branch', '--show-current'], 
                                         cwd=target_dir, capture_output=True, text=True, check=True)
            current_branch = branch_result.stdout.strip()
            
            # Set up tracking for current branch
            try:
                subprocess.run(['git', 'branch', '--set-upstream-to', f'origin/{current_branch}', current_branch], 
                             cwd=target_dir, check=True, capture_output=True)
            except subprocess.CalledProcessError:
                # If the remote branch doesn't exist, try to fetch first
                subprocess.run(['git', 'fetch', 'origin'], cwd=target_dir, check=True, capture_output=True)
                
                # Check if the remote branch exists
                branch_check = subprocess.run(['git', 'ls-remote', '--heads', 'origin', current_branch], 
                                            cwd=target_dir, capture_output=True, text=True)
                
                if branch_check.stdout.strip():
                    # Remote branch exists, try to set up tracking again
                    try:
                        subprocess.run(['git', 'branch', '--set-upstream-to', f'origin/{current_branch}', current_branch], 
                                     cwd=target_dir, check=True, capture_output=True)
                    except subprocess.CalledProcessError:
                        pass  # Continue without tracking
                else:
                    # Remote branch doesn't exist - this is normal for new repositories
                    pass
                
                # Try to pull without tracking setup
                try:
                    subprocess.run(['git', 'pull', 'origin', current_branch], 
                                 cwd=target_dir, check=True, capture_output=True)
                    print("✅ Successfully pulled updates from repository")
                    return True
                except subprocess.CalledProcessError as pull_error:
                    error_msg = pull_error.stderr.decode() if pull_error.stderr else str(pull_error)
                    
                    # Check if this is an untracked files conflict
                    if "untracked working tree files would be overwritten" in error_msg:
                        print("⚠️  Untracked files conflict detected. Resolving automatically...")
                        
                        # Get the list of conflicting files
                        import re
                        conflict_files = re.findall(r'error: The following untracked working tree files would be overwritten by merge:\n\t(.+?)\n', error_msg)
                        
                        if conflict_files:
                            # Create backup directory for conflicting files
                            backup_dir = os.path.join(target_dir, '.adoc_backup_' + str(int(time.time())))
                            os.makedirs(backup_dir, exist_ok=True)
                            
                            # Backup and remove conflicting files
                            for file_path in conflict_files:
                                full_path = os.path.join(target_dir, file_path)
                                if os.path.exists(full_path):
                                    backup_path = os.path.join(backup_dir, file_path)
                                    os.makedirs(os.path.dirname(backup_path), exist_ok=True)
                                    import shutil
                                    shutil.copy2(full_path, backup_path)
                                    os.remove(full_path)
                            
                            print(f"💾 Backed up {len(conflict_files)} file(s) to: {os.path.basename(backup_dir)}")
                            
                            # Now try to pull again
                            try:
                                subprocess.run(['git', 'pull', 'origin', current_branch], 
                                             cwd=target_dir, check=True, capture_output=True)
                                print("✅ Successfully pulled updates from repository")
                                return True
                            except subprocess.CalledProcessError as retry_error:
                                print(f"❌ Error on retry: {retry_error}")
                                return False
                    
                    # If pull fails, try a simple fetch and merge
                    try:
                        print("🔄 Trying fetch and merge approach...")
                        subprocess.run(['git', 'fetch', 'origin'], cwd=target_dir, check=True, capture_output=True)
                        subprocess.run(['git', 'merge', f'origin/{current_branch}'], 
                                     cwd=target_dir, check=True, capture_output=True)
                        print("✅ Successfully merged updates from repository")
                        return True
                    except subprocess.CalledProcessError as merge_error:
                        error_msg = merge_error.stderr.decode() if merge_error.stderr else str(merge_error)
                        
                        # Check if merge also has untracked files conflict
                        if "untracked working tree file" in error_msg:
                            print("⚠️  Merge also has untracked files conflict.")
                            print("💡 Manual resolution required:")
                            print("   1. Check which files are conflicting: 'git status'")
                            print("   2. Move or remove conflicting files")
                            print("   3. Run 'vcs-pull' again")
                        else:
                            print(f"❌ Error with fetch and merge: {merge_error}")
                        
                        return False
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Error setting up remote tracking: {e}")
            return False
        
        # Perform Git pull
        try:
            print("🔄 Pulling updates from remote repository...")
            result = subprocess.run(['git', 'pull'], cwd=target_dir, env=env, 
                                  capture_output=True, text=True, check=True)
            print("✅ Successfully pulled updates from repository")
            if result.stdout.strip():
                print(f"📝 Changes: {result.stdout.strip()}")
            return True
            
        except subprocess.CalledProcessError as e:
            error_msg = e.stderr if e.stderr else str(e)
            
            # Check if this is a divergent branches error
            if "divergent branches" in error_msg or "Need to specify how to reconcile" in error_msg:
                print("⚠️  Divergent branches detected. Configuring pull strategy...")
                
                # Configure pull strategy to use merge (safer option)
                try:
                    subprocess.run(['git', 'config', 'pull.rebase', 'false'], 
                                 cwd=target_dir, check=True, capture_output=True)
                    print("🔧 Configured pull strategy: merge")
                    
                    # Try pull again with merge strategy
                    print("🔄 Retrying pull with merge strategy...")
                    result = subprocess.run(['git', 'pull'], cwd=target_dir, env=env, 
                                          capture_output=True, text=True, check=True)
                    print("✅ Successfully pulled updates from repository")
                    if result.stdout.strip():
                        print(f"📝 Changes: {result.stdout.strip()}")
                    return True
                    
                except subprocess.CalledProcessError as merge_error:
                    print(f"❌ Error with merge strategy: {merge_error}")
                    if merge_error.stderr:
                        print(f"Error details: {merge_error.stderr}")
                    
                    # If merge fails, try rebase strategy
                    try:
                        print("🔄 Trying rebase strategy...")
                        subprocess.run(['git', 'config', 'pull.rebase', 'true'], 
                                     cwd=target_dir, check=True, capture_output=True)
                        result = subprocess.run(['git', 'pull'], cwd=target_dir, env=env, 
                                              capture_output=True, text=True, check=True)
                        print("✅ Successfully pulled updates from repository")
                        if result.stdout.strip():
                            print(f"📝 Changes: {result.stdout.strip()}")
                        return True
                        
                    except subprocess.CalledProcessError as rebase_error:
                        print(f"❌ Error with rebase strategy: {rebase_error}")
                        if rebase_error.stderr:
                            print(f"Error details: {rebase_error.stderr}")
                        
                        print("\n💡 Manual resolution required:")
                        print("   The repository has divergent branches that need manual resolution.")
                        print("   You can resolve this by:")
                        print("   1. Running 'git status' to see the current state")
                        print("   2. Choosing a strategy: 'git pull --rebase' or 'git pull --no-rebase'")
                        print("   3. Resolving any conflicts manually")
                        return False
            
            # Handle other pull errors
            print(f"❌ Error pulling from repository: {e}")
            if e.stderr:
                print(f"Error details: {e.stderr}")
            return False
    
    elif vcs_type == 'hg':
        # Configure Mercurial authentication
        hgrc_path = os.path.join(target_dir, '.hg', 'hgrc')
        
        # Create or update hgrc file with authentication
        hgrc_content = []
        if os.path.exists(hgrc_path):
            with open(hgrc_path, 'r') as f:
                hgrc_content = f.readlines()
        
        # Add or update [paths] section
        paths_section = False
        for i, line in enumerate(hgrc_content):
            if line.strip() == '[paths]':
                paths_section = True
                # Update default path
                for j in range(i + 1, len(hgrc_content)):
                    if hgrc_content[j].startswith('default'):
                        hgrc_content[j] = f'default = {config.remote_url}\n'
                        break
                else:
                    hgrc_content.insert(i + 1, f'default = {config.remote_url}\n')
                break
        else:
            hgrc_content.extend(['\n', '[paths]\n', f'default = {config.remote_url}\n'])
        
        # Add authentication if using HTTPS
        if config.username and config.token and config.remote_url.startswith('https://'):
            auth_section = False
            for i, line in enumerate(hgrc_content):
                if line.strip() == '[auth]':
                    auth_section = True
                    # Update or add default auth
                    for j in range(i + 1, len(hgrc_content)):
                        if hgrc_content[j].startswith('default.username'):
                            hgrc_content[j] = f'default.username = {config.username}\n'
                            break
                        elif hgrc_content[j].startswith('['):
                            hgrc_content.insert(j, f'default.username = {config.username}\n')
                            break
                    else:
                        hgrc_content.append(f'default.username = {config.username}\n')
                    break
            else:
                hgrc_content.extend(['\n', '[auth]\n', f'default.username = {config.username}\n'])
        
        # Write updated hgrc
        os.makedirs(os.path.dirname(hgrc_path), exist_ok=True)
        with open(hgrc_path, 'w') as f:
            f.writelines(hgrc_content)
        
        # Set proxy if configured
        if config.proxy_url:
            env['HTTP_PROXY'] = config.proxy_url
            env['HTTPS_PROXY'] = config.proxy_url
        
        # Perform Mercurial pull
        try:
            print("🔄 Pulling updates from remote repository...")
            result = subprocess.run(['hg', 'pull'], cwd=target_dir, env=env, 
                                  capture_output=True, text=True, check=True)
            print("✅ Successfully pulled updates from repository")
            if result.stdout.strip():
                print(f"📝 Changes: {result.stdout.strip()}")
            
            # Update working directory
            update_result = subprocess.run(['hg', 'update'], cwd=target_dir, env=env, 
                                         capture_output=True, text=True, check=True)
            if update_result.stdout.strip():
                print(f"📝 Updated: {update_result.stdout.strip()}")
            
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Error pulling from repository: {e}")
            if e.stderr:
                print(f"Error details: {e.stderr}")
            return False
    
    else:
        print(f"❌ Unsupported VCS type: {vcs_type}")
        return False


def execute_vcs_push(command: str, output_dir: str = None) -> bool:
    """Execute vcs-push command to push changes to the configured repository.
    Args:
        command: Command string like 'vcs-push'
        output_dir: The output directory to use for pushing changes
    Returns:
        True if successful, False otherwise
    """
    import subprocess
    from pathlib import Path
    from adoc_migration_toolkit.shared import globals
    
    # Determine the target directory
    if output_dir:
        target_dir = output_dir
    elif getattr(globals, 'GLOBAL_OUTPUT_DIR', None):
        target_dir = str(globals.GLOBAL_OUTPUT_DIR)
    else:
        print("❌ No output directory set.")
        print("\n💡 To use vcs-push, you need to set an output directory first:")
        print("   • Run 'set-output-dir <directory>' to set the output directory")
        print("   • Example: set-output-dir /path/to/your/repo")
        return False
    
    target_dir = os.path.abspath(os.path.expanduser(target_dir))
    print(f"\n📤 Pushing changes from repository: {target_dir}")
    
    # Check if directory exists
    if not os.path.exists(target_dir):
        print(f"❌ Target directory does not exist: {target_dir}")
        print("\n💡 To use vcs-push, you need to initialize a repository first:")
        print("   • Run 'vcs-init' to initialize a repository in the output directory")
        print("   • Or run 'vcs-init <directory>' to initialize in a specific directory")
        return False
    
    # Check if it's a VCS repository
    vcs_type = None
    if os.path.exists(os.path.join(target_dir, '.git')):
        vcs_type = 'git'
    elif os.path.exists(os.path.join(target_dir, '.hg')):
        vcs_type = 'hg'
    else:
        print(f"❌ No VCS repository found in {target_dir}")
        print("\n💡 To use vcs-push, you need to initialize a repository first:")
        print("   • Run 'vcs-init' to initialize a repository in the output directory")
        print("   • Or run 'vcs-init <directory>' to initialize in a specific directory")
        return False
    
    # Load VCS configuration
    config_manager = VCSConfigManager()
    config = config_manager.load_config()
    
    if not config:
        print("❌ No VCS configuration found.")
        print("\n💡 To use vcs-push, you need to configure your repository settings first:")
        print("   • Run 'vcs-config' to set up your repository configuration")
        print("   • This will configure authentication, remote URL, and other settings")
        print("   • Example: vcs-config --vcs-type git --remote-url https://github.com/user/repo.git")
        print("   • Or run 'vcs-config' for interactive configuration")
        return False
    
    print(f"📋 Using VCS configuration:")
    print(f"   VCS Type: {config.vcs_type}")
    print(f"   Remote URL: {config.remote_url}")
    
    # Set up environment variables for authentication
    env = os.environ.copy()
    
    if vcs_type == 'git':
        # Configure Git authentication
        if config.username and config.token:
            # HTTPS authentication
            remote_url = config.remote_url
            if remote_url.startswith('https://'):
                # Insert credentials into URL
                from urllib.parse import urlparse
                parsed = urlparse(remote_url)
                auth_url = f"https://{config.username}:{config.token}@{parsed.netloc}{parsed.path}"
                remote_url = auth_url
            
            # Set Git configuration
            try:
                subprocess.run(['git', 'config', '--local', 'user.name', config.username], 
                             cwd=target_dir, check=True, capture_output=True)
                subprocess.run(['git', 'config', '--local', 'user.email', f"{config.username}@adoc-migration-toolkit"], 
                             cwd=target_dir, check=True, capture_output=True)
            except subprocess.CalledProcessError:
                pass  # Ignore if already configured
        
        elif config.ssh_key_path:
            # SSH authentication
            ssh_key_path = os.path.expanduser(config.ssh_key_path)
            if not os.path.exists(ssh_key_path):
                print(f"❌ SSH key not found: {ssh_key_path}")
                return False
            
            # Set up SSH agent or GIT_SSH_COMMAND
            if config.ssh_passphrase:
                # Use ssh-agent with passphrase
                env['GIT_SSH_COMMAND'] = f'ssh -i {ssh_key_path}'
            else:
                env['GIT_SSH_COMMAND'] = f'ssh -i {ssh_key_path} -o IdentitiesOnly=yes'
        
        # Set proxy if configured
        if config.proxy_url:
            env['HTTP_PROXY'] = config.proxy_url
            env['HTTPS_PROXY'] = config.proxy_url
            if config.proxy_username and config.proxy_password:
                env['HTTP_PROXY_AUTH'] = f"{config.proxy_username}:{config.proxy_password}"
        
        # Set up remote if not already configured
        try:
            # Check if remote 'origin' exists
            result = subprocess.run(['git', 'remote', 'get-url', 'origin'], 
                                  cwd=target_dir, capture_output=True, text=True)
            
            if result.returncode != 0:
                # No remote exists, add it
                print("🔧 Setting up remote 'origin'...")
                subprocess.run(['git', 'remote', 'add', 'origin', config.remote_url], 
                             cwd=target_dir, check=True, capture_output=True)
            else:
                # Remote exists, update it if different
                current_url = result.stdout.strip()
                if current_url != config.remote_url:
                    print("🔧 Updating remote 'origin' URL...")
                    subprocess.run(['git', 'remote', 'set-url', 'origin', config.remote_url], 
                                 cwd=target_dir, check=True, capture_output=True)
            
            # Get current branch name
            branch_result = subprocess.run(['git', 'branch', '--show-current'], 
                                         cwd=target_dir, capture_output=True, text=True, check=True)
            current_branch = branch_result.stdout.strip()
            
            # Find and expand ZIP files before checking for changes
            print("🔍 Looking for ZIP files to expand...")
            zip_files = []
            for root, dirs, files in os.walk(target_dir):
                for file in files:
                    if file.endswith('.zip'):
                        zip_files.append(os.path.join(root, file))
            
            if zip_files:
                print(f"📦 Found {len(zip_files)} ZIP file(s) to expand:")
                for zip_file in zip_files:
                    print(f"   • {os.path.relpath(zip_file, target_dir)}")
                
                print("\n📂 Expanding ZIP files...")
                for zip_file in zip_files:
                    try:
                        # Get the directory name (ZIP filename without .zip extension)
                        zip_name = os.path.splitext(os.path.basename(zip_file))[0]
                        
                        # Clean up directory name: remove date part and keep only base-name-range
                        # Example: "snowflake-07-06-2025-19-00-0-49" → "snowflake-0-49"
                        # Example: "snowflake-07-06-2025-19-00-100-149" → "snowflake-100-149"
                        import re
                        
                        # Pattern to match: base-name-date-range or base-name-range
                        # This handles both formats: "snowflake-07-06-2025-19-00-0-49" and "snowflake-0-49"
                        dir_pattern = r'^(.+?)(?:-\d{2}-\d{2}-\d{4}-\d{2}-\d{2})?(-\d+-\d+)$'
                        dir_match = re.match(dir_pattern, zip_name)
                        
                        if dir_match:
                            base_name = dir_match.group(1)
                            range_part = dir_match.group(2)
                            clean_zip_name = base_name + range_part
                        else:
                            # If pattern doesn't match, use original name
                            clean_zip_name = zip_name
                        
                        zip_dir = os.path.join(os.path.dirname(zip_file), clean_zip_name)
                        
                        # Create the target directory if it doesn't exist
                        os.makedirs(zip_dir, exist_ok=True)
                        
                        # Extract ZIP file to the directory
                        import zipfile
                        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                            zip_ref.extractall(zip_dir)
                        
                        print(f"   ✅ Expanded {os.path.basename(zip_file)} → {clean_zip_name}/")
                        
                        # Track original file names for metadata
                        original_zip_name = os.path.basename(zip_file)
                        file_renames = {}  # Track original JSON names to new names
                        dir_renames = {}   # Track original directory names to new names
                        
                        # Clean up directory and file names within the extracted content
                        print(f"   🧹 Cleaning up directory and file names in {clean_zip_name}/...")
                        
                        # First, clean up directory names (remove -import-ready suffix)
                        for root, dirs, files in os.walk(zip_dir, topdown=False):
                            for dir_name in dirs:
                                if dir_name.endswith('-import-ready'):
                                    old_dir_path = os.path.join(root, dir_name)
                                    new_dir_name = dir_name[:-13]  # Remove '-import-ready'
                                    new_dir_path = os.path.join(root, new_dir_name)
                                    
                                    try:
                                        os.rename(old_dir_path, new_dir_path)
                                        # Track the rename for metadata
                                        dir_renames[dir_name] = new_dir_name
                                        print(f"      📁 Renamed directory: {dir_name} → {new_dir_name}")
                                    except Exception as e:
                                        print(f"      ⚠️  Could not rename directory {dir_name}: {e}")
                        
                        # Then clean up file names (remove UUIDs)
                        for root, dirs, files in os.walk(zip_dir):
                            for file in files:
                                if file.endswith('.json'):
                                    # Remove UUID from filename
                                    # Example: "asset_udf_variables-0c8c971e-a741-4672-a33c-0c0723134d83.json" → "asset_udf_variables.json"
                                    file_pattern = r'^(.+?)-[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\.json$'
                                    file_match = re.match(file_pattern, file)
                                    
                                    if file_match:
                                        base_filename = file_match.group(1)
                                        new_filename = base_filename + '.json'
                                        old_filepath = os.path.join(root, file)
                                        new_filepath = os.path.join(root, new_filename)
                                        
                                        # Track the rename for metadata
                                        file_renames[file] = new_filename
                                        
                                        # Rename the file
                                        os.rename(old_filepath, new_filepath)
                                        print(f"      📝 Renamed: {file} → {new_filename}")
                        
                        # Create .adoc-migration-meta file in the extracted directory
                        meta_file_path = os.path.join(zip_dir, '.adoc-migration-meta')
                        meta_content = {
                            'original_zip_file': original_zip_name,
                            'extraction_timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                            'file_renames': file_renames,
                            'directory_renames': dir_renames,
                            'original_directory_name': zip_name,
                            'clean_directory_name': clean_zip_name
                        }
                        
                        import json
                        with open(meta_file_path, 'w') as meta_file:
                            json.dump(meta_content, meta_file, indent=2)
                        
                        print(f"   📋 Created metadata file: {clean_zip_name}/.adoc-migration-meta")
                        
                        # Remove the original ZIP file after successful extraction
                        os.remove(zip_file)
                        print(f"   🗑️  Removed original ZIP file: {os.path.basename(zip_file)}")
                        
                    except Exception as e:
                        print(f"   ❌ Error expanding {os.path.basename(zip_file)}: {e}")
                        continue
                
                print("✅ ZIP file expansion completed")
            else:
                print("📦 No ZIP files found to expand")
            
            # Check if there are changes to commit
            status_result = subprocess.run(['git', 'status', '--porcelain'], 
                                         cwd=target_dir, capture_output=True, text=True, check=True)
            
            if status_result.stdout.strip():
                print("📝 Found uncommitted changes. Committing changes...")
                # Add all changes
                subprocess.run(['git', 'add', '.'], cwd=target_dir, check=True, capture_output=True)
                
                # Commit with a default message
                commit_message = f"ADOC Migration Toolkit - Auto commit {subprocess.run(['date'], capture_output=True, text=True).stdout.strip()}"
                subprocess.run(['git', 'commit', '-m', commit_message], 
                             cwd=target_dir, check=True, capture_output=True)
                print("✅ Changes committed successfully")
            
            # Set up tracking for current branch if not already set
            tracking_result = subprocess.run(['git', 'branch', '--show-current', '--track'], 
                                           cwd=target_dir, capture_output=True, text=True)
            
            if 'origin/' not in tracking_result.stdout:
                print(f"🔧 Setting up tracking for branch '{current_branch}'...")
                try:
                    subprocess.run(['git', 'branch', '--set-upstream-to', f'origin/{current_branch}', current_branch], 
                                 cwd=target_dir, check=True, capture_output=True)
                except subprocess.CalledProcessError:
                    # If the remote branch doesn't exist, we'll push with -u flag
                    pass
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Error setting up repository: {e}")
            return False
        
        # Perform Git push
        try:
            print("🔄 Pushing changes to remote repository...")
            
            # Try to push with tracking first
            try:
                result = subprocess.run(['git', 'push'], cwd=target_dir, env=env, 
                                      capture_output=True, text=True, check=True)
            except subprocess.CalledProcessError as push_error:
                error_msg = push_error.stderr if push_error.stderr else str(push_error)
                
                # Check if this is a "fetch first" error
                if "fetch first" in error_msg or "Updates were rejected" in error_msg:
                    print("⚠️  Remote has changes that need to be pulled first...")
                    
                    # Pull the remote changes first
                    try:
                        print("📥 Pulling remote changes...")
                        pull_result = subprocess.run(['git', 'pull'], cwd=target_dir, env=env, 
                                                   capture_output=True, text=True, check=True)
                        print("✅ Successfully pulled remote changes")
                        if pull_result.stdout.strip():
                            print(f"📝 Pull result: {pull_result.stdout.strip()}")
                        
                        # Now try to push again
                        print("🔄 Pushing changes after pulling updates...")
                        result = subprocess.run(['git', 'push'], cwd=target_dir, env=env, 
                                              capture_output=True, text=True, check=True)
                        
                    except subprocess.CalledProcessError as pull_error:
                        print(f"❌ Error pulling remote changes: {pull_error}")
                        if pull_error.stderr:
                            print(f"Pull error details: {pull_error.stderr}")
                        
                        # If pull fails, try with -u flag to set upstream
                        print("🔄 Trying to set upstream and push...")
                        result = subprocess.run(['git', 'push', '-u', 'origin', current_branch], 
                                              cwd=target_dir, env=env, capture_output=True, text=True, check=True)
                else:
                    # If that fails, try pushing with -u flag to set upstream
                    print("🔄 Setting upstream and pushing...")
                    result = subprocess.run(['git', 'push', '-u', 'origin', current_branch], 
                                          cwd=target_dir, env=env, capture_output=True, text=True, check=True)
            
            print("✅ Successfully pushed changes to repository")
            if result.stdout.strip():
                print(f"📝 Push result: {result.stdout.strip()}")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Error pushing to repository: {e}")
            if e.stderr:
                print(f"Error details: {e.stderr}")
            
            # Provide helpful guidance for common push issues
            if "fetch first" in (e.stderr or ""):
                print("\n💡 The remote repository has changes that aren't in your local repository.")
                print("   You can resolve this by:")
                print("   1. Running 'vcs-pull' to get the latest changes")
                print("   2. Resolving any conflicts if they occur")
                print("   3. Running 'vcs-push' again")
            
            return False
    
    elif vcs_type == 'hg':
        # Configure Mercurial authentication
        hgrc_path = os.path.join(target_dir, '.hg', 'hgrc')
        
        # Create or update hgrc file with authentication
        hgrc_content = []
        if os.path.exists(hgrc_path):
            with open(hgrc_path, 'r') as f:
                hgrc_content = f.readlines()
        
        # Add or update [paths] section
        paths_section = False
        for i, line in enumerate(hgrc_content):
            if line.strip() == '[paths]':
                paths_section = True
                # Update default path
                for j in range(i + 1, len(hgrc_content)):
                    if hgrc_content[j].startswith('default'):
                        hgrc_content[j] = f'default = {config.remote_url}\n'
                        break
                else:
                    hgrc_content.insert(i + 1, f'default = {config.remote_url}\n')
                break
        else:
            hgrc_content.extend(['\n', '[paths]\n', f'default = {config.remote_url}\n'])
        
        # Add authentication if using HTTPS
        if config.username and config.token and config.remote_url.startswith('https://'):
            auth_section = False
            for i, line in enumerate(hgrc_content):
                if line.strip() == '[auth]':
                    auth_section = True
                    # Update or add default auth
                    for j in range(i + 1, len(hgrc_content)):
                        if hgrc_content[j].startswith('default.username'):
                            hgrc_content[j] = f'default.username = {config.username}\n'
                            break
                        elif hgrc_content[j].startswith('['):
                            hgrc_content.insert(j, f'default.username = {config.username}\n')
                            break
                    else:
                        hgrc_content.append(f'default.username = {config.username}\n')
                    break
            else:
                hgrc_content.extend(['\n', '[auth]\n', f'default.username = {config.username}\n'])
        
        # Write updated hgrc
        os.makedirs(os.path.dirname(hgrc_path), exist_ok=True)
        with open(hgrc_path, 'w') as f:
            f.writelines(hgrc_content)
        
        # Set proxy if configured
        if config.proxy_url:
            env['HTTP_PROXY'] = config.proxy_url
            env['HTTPS_PROXY'] = config.proxy_url
        
        # Find and expand ZIP files before checking for changes
        print("🔍 Looking for ZIP files to expand...")
        zip_files = []
        for root, dirs, files in os.walk(target_dir):
            for file in files:
                if file.endswith('.zip'):
                    zip_files.append(os.path.join(root, file))
        
        if zip_files:
            print(f"📦 Found {len(zip_files)} ZIP file(s) to expand:")
            for zip_file in zip_files:
                print(f"   • {os.path.relpath(zip_file, target_dir)}")
            
            print("\n📂 Expanding ZIP files...")
            for zip_file in zip_files:
                try:
                    # Get the directory name (ZIP filename without .zip extension)
                    zip_name = os.path.splitext(os.path.basename(zip_file))[0]
                    
                    # Clean up directory name: remove date part and keep only base-name-range
                    # Example: "snowflake-07-06-2025-19-00-0-49" → "snowflake-0-49"
                    # Example: "snowflake-07-06-2025-19-00-100-149" → "snowflake-100-149"
                    import re
                    
                    # Pattern to match: base-name-date-range or base-name-range
                    # This handles both formats: "snowflake-07-06-2025-19-00-0-49" and "snowflake-0-49"
                    dir_pattern = r'^(.+?)(?:-\d{2}-\d{2}-\d{4}-\d{2}-\d{2})?(-\d+-\d+)$'
                    dir_match = re.match(dir_pattern, zip_name)
                    
                    if dir_match:
                        base_name = dir_match.group(1)
                        range_part = dir_match.group(2)
                        clean_zip_name = base_name + range_part
                    else:
                        # If pattern doesn't match, use original name
                        clean_zip_name = zip_name
                    
                    zip_dir = os.path.join(os.path.dirname(zip_file), clean_zip_name)
                    
                    # Create the target directory if it doesn't exist
                    os.makedirs(zip_dir, exist_ok=True)
                    
                    # Extract ZIP file to the directory
                    import zipfile
                    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                        zip_ref.extractall(zip_dir)
                    
                    print(f"   ✅ Expanded {os.path.basename(zip_file)} → {clean_zip_name}/")
                    
                    # Clean up file names within the directory
                    print(f"   🧹 Cleaning up file names in {clean_zip_name}/...")
                    for root, dirs, files in os.walk(zip_dir):
                        for file in files:
                            if file.endswith('.json'):
                                # Remove UUID from filename
                                # Example: "asset_udf_variables-0c8c971e-a741-4672-a33c-0c0723134d83.json" → "asset_udf_variables.json"
                                file_pattern = r'^(.+?)-[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\.json$'
                                file_match = re.match(file_pattern, file)
                                
                                if file_match:
                                    base_filename = file_match.group(1)
                                    new_filename = base_filename + '.json'
                                    old_filepath = os.path.join(root, file)
                                    new_filepath = os.path.join(root, new_filename)
                                    
                                    # Rename the file
                                    os.rename(old_filepath, new_filepath)
                                    print(f"      📝 Renamed: {file} → {new_filename}")
                    
                    # Remove the original ZIP file after successful extraction
                    os.remove(zip_file)
                    print(f"   🗑️  Removed original ZIP file: {os.path.basename(zip_file)}")
                    
                except Exception as e:
                    print(f"   ❌ Error expanding {os.path.basename(zip_file)}: {e}")
                    continue
            
            print("✅ ZIP file expansion completed")
        else:
            print("📦 No ZIP files found to expand")
        
        # Check if there are changes to commit
        status_result = subprocess.run(['hg', 'status'], cwd=target_dir, 
                                     capture_output=True, text=True, check=True)
        
        if status_result.stdout.strip():
            print("📝 Found uncommitted changes. Committing changes...")
            # Add all changes
            subprocess.run(['hg', 'add'], cwd=target_dir, check=True, capture_output=True)
            
            # Commit with a default message
            commit_message = f"ADOC Migration Toolkit - Auto commit"
            subprocess.run(['hg', 'commit', '-m', commit_message], 
                         cwd=target_dir, check=True, capture_output=True)
            print("✅ Changes committed successfully")
        
        # Perform Mercurial push
        try:
            print("🔄 Pushing changes to remote repository...")
            result = subprocess.run(['hg', 'push'], cwd=target_dir, env=env, 
                                  capture_output=True, text=True, check=True)
            print("✅ Successfully pushed changes to repository")
            if result.stdout.strip():
                print(f"📝 Push result: {result.stdout.strip()}")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Error pushing to repository: {e}")
            if e.stderr:
                print(f"Error details: {e.stderr}")
            return False
    
    else:
        print(f"❌ Unsupported VCS type: {vcs_type}")
        return False 