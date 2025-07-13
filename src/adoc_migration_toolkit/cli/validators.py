"""
CLI argument validators.

This module contains functions for validating command line arguments,
both for Click-based CLI and legacy argparse-based validation.
"""

from pathlib import Path

import click


def validate_formatter_arguments(args):
    """Validate formatter command line arguments."""
    if not args.source_env_string or not args.source_env_string.strip():
        raise ValueError("Source environment string cannot be empty")

    if args.target_env_string is None:
        raise ValueError("Target environment string cannot be None")

    # Input directory validation is now handled in run_formatter since it's optional
    # and can auto-detect the policy-export directory


def validate_asset_export_arguments(args):
    """Validate asset-export command line arguments."""
    if not args.csv_file or not args.csv_file.strip():
        raise ValueError("CSV file path cannot be empty")

    if not args.env_file or not args.env_file.strip():
        raise ValueError("Environment file path cannot be empty")

    # Check if CSV file exists
    csv_path = Path(args.csv_file)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file does not exist: {args.csv_file}")

    if not csv_path.is_file():
        raise ValueError(f"CSV path is not a file: {args.csv_file}")

    # Check if environment file exists
    env_path = Path(args.env_file)
    if not env_path.exists():
        raise FileNotFoundError(f"Environment file does not exist: {args.env_file}")

    if not env_path.is_file():
        raise ValueError(f"Environment path is not a file: {args.env_file}")


def validate_rest_api_arguments(args):
    """Validate rest-api command line arguments."""
    if not args.env_file or not args.env_file.strip():
        raise ValueError("Environment file path cannot be empty")

    env_path = Path(args.env_file)
    if not env_path.exists():
        raise FileNotFoundError(f"Environment file does not exist: {args.env_file}")

    if not env_path.is_file():
        raise ValueError(f"Environment path is not a file: {args.env_file}")


# Click-specific validation functions
def validate_env_file(ctx, param, value):
    """Click validator for environment file."""
    if value is None:
        return value

    env_path = Path(value)
    if not env_path.exists():
        raise click.BadParameter(f"Environment file does not exist: {value}")

    if not env_path.is_file():
        raise click.BadParameter(f"Environment path is not a file: {value}")

    return value


def validate_csv_file(ctx, param, value):
    """Click validator for CSV file."""
    if value is None:
        return value

    csv_path = Path(value)
    if not csv_path.exists():
        raise click.BadParameter(f"CSV file does not exist: {value}")

    if not csv_path.is_file():
        raise click.BadParameter(f"CSV path is not a file: {value}")

    return value


def validate_non_empty_string(ctx, param, value):
    """Click validator for non-empty string."""
    if value is None:
        return value

    if not value.strip():
        raise click.BadParameter(f"{param.name} cannot be empty")

    return value


def validate_log_level(ctx, param, value):
    """Click validator for log level."""
    if value is None:
        return value

    valid_levels = ["ERROR", "WARNING", "INFO", "DEBUG"]
    if value.upper() not in valid_levels:
        raise click.BadParameter(f"Log level must be one of: {', '.join(valid_levels)}")

    return value.upper()
