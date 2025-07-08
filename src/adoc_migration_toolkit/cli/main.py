"""
CLI main execution functions.

This module contains the main CLI execution logic including the main function
and command execution functions.
"""

import sys
import click


def run_integrity_check():
    """Run integrity verification."""
    try:
        from ..integrity import verify_integrity
        success = verify_integrity()
        if not success:
            click.echo("❌ Integrity verification failed!", err=True)
            sys.exit(1)
        click.echo("✅ Integrity verification passed!")
        return True
    except ImportError as e:
        click.echo(f"⚠️  Integrity module not available: {e}", err=True)
        return True  # Don't fail if integrity module is missing


def run_interactive(env_file, log_level, verbose):
    """Run the interactive command."""
    # Defer import to avoid dependency issues during CLI setup
    try:
        from ..execution.interactive import run_interactive as run_interactive_impl
        
        # Create a simple args object for backward compatibility
        class Args:
            def __init__(self, env_file, log_level, verbose):
                self.env_file = env_file
                self.log_level = log_level
                self.verbose = verbose
        
        args = Args(env_file, log_level, verbose)
        return run_interactive_impl(args)
    except ImportError as e:
        click.echo(f"❌ Error: Could not import execution module: {e}", err=True)
        click.echo("Please ensure all dependencies are installed: pip install -e .", err=True)
        return 1


@click.group()
@click.version_option(version='1.0.0', prog_name='adoc-migration-toolkit')
def cli():
    """
    ADOC Migration Toolkit - Professional tool for migrating Acceldata policies and assets.
    
    This toolkit provides comprehensive tools for migrating ADOC configurations
    from one environment to another, including interactive mode for guided workflows.
    
    Examples:
        
    \b
    # Interactive mode
    adoc-migration-toolkit interactive --env-file=config.env

    For more information, visit: https://github.com/your-repo/adoc-migration-toolkit
    """
    pass


@cli.command()
@click.option(
    '--env-file',
    required=True,
    type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=str),
    help='Path to environment file containing AD_HOST, AD_SOURCE_ACCESS_KEY, AD_SOURCE_SECRET_KEY, AD_SOURCE_TENANT'
)
@click.option(
    '--log-level', '-l',
    type=click.Choice(['ERROR', 'WARNING', 'INFO', 'DEBUG'], case_sensitive=False),
    default='ERROR',
    help='Set logging level (default: ERROR)'
)
@click.option(
    '--verbose', '-v',
    is_flag=True,
    help='Enable verbose logging (overrides --log-level)'
)
def interactive(env_file, log_level, verbose):
    """
    Interactive ADOC Migration Toolkit.
    
    ADOC Migration Toolkit for migration ADOC configurations from one environment another.
    
    Examples:
    
    \b
    python -m adoc_migration_toolkit interactive --env-file=config.env
    python -m adoc_migration_toolkit interactive --env-file=config.env --verbose

    Interactive Commands:  
    
    \b
    # Segments Commands
    segments-export [csv_file] [--output-file file] [--quiet]
    segments-import [csv_file] [--dry-run] [--quiet] [--verbose]
    
    \b
    # Asset Profile Commands
    asset-profile-export [csv_file] [--output-file file] [--quiet] [--verbose]
    asset-profile-import [csv_file] [--dry-run] [--quiet] [--verbose]
    
    \b
    # Asset Configuration Commands
    asset-config-export <csv_file> [--output-file file] [--quiet] [--verbose]
    asset-list-export [--quiet] [--verbose]
    
    \b
    # Policy Commands
    policy-list-export [--quiet] [--verbose]
    policy-export [--type export_type] [--filter filter_value] [--quiet] [--verbose] [--batch-size size]
    policy-import <file_or_pattern> [--quiet] [--verbose]
    policy-xfr [--input input_dir] --source-env-string source --target-env-string target [--quiet] [--verbose]
    
    \b
    # REST API Commands
    GET /catalog-server/api/assets?uid=123
    PUT /catalog-server/api/assets {"key": "value"}
    GET /catalog-server/api/assets?uid=123 --target-auth --target-tenant

    \b
    # Utility Commands
    set-output-dir <directory>
    
    \b
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
    return run_interactive(env_file, log_level, verbose)


def main():
    """Main CLI entry point."""
    return cli()


if __name__ == '__main__':
    sys.exit(main()) 