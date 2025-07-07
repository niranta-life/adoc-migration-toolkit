"""
CLI command definitions using Click.

This module contains Click command definitions for the ADOC Migration Toolkit CLI.
"""

import click


def create_interactive_command():
    """Create the interactive command using Click."""
    
    @click.command()
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
        from ..execution.interactive import run_interactive as run_interactive_impl
        
        # Create a simple args object for backward compatibility
        class Args:
            def __init__(self, env_file, log_level, verbose):
                self.env_file = env_file
                self.log_level = log_level
                self.verbose = verbose
        
        args = Args(env_file, log_level, verbose)
        return run_interactive_impl(args)
    
    return interactive


# Legacy function for backward compatibility
def create_interactive_parser(subparsers):
    """Legacy function for backward compatibility - now returns a Click command."""
    return create_interactive_command() 