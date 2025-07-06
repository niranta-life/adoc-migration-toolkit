"""
Command Line Interface for the adoc migration toolkit.

This module contains CLI parsing, validation, and main execution logic.
"""

from .parsers import (
    create_interactive_parser
)

from .validators import (
    validate_formatter_arguments,
    validate_asset_export_arguments,
    validate_rest_api_arguments
)

from .main import (
    main,
    run_asset_export,
    run_interactive
)

__all__ = [
    # Parsers
    'create_interactive_parser',
    
    # Validators
    'validate_formatter_arguments',
    'validate_asset_export_arguments',
    'validate_rest_api_arguments',
    
    # Main execution
    'main',
    'run_asset_export',
    'run_interactive'
] 