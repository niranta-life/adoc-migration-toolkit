# Asset Config Import Command

The `asset-config-import` command imports asset configurations to the target environment from processed CSV files.

## Overview

This command imports asset configurations that have been prepared by the `asset-config-export` and `policy-xfr` commands. It processes the `asset-config-import-ready.csv` file and applies the configurations to the target environment.

## Usage

```bash
asset-config-import [<csv_file>] [--dry-run] [--quiet] [--verbose] [--parallel] [--allowed-types <types>]
```

## Parameters

### Optional Parameters

- `<csv_file>`: Path to the CSV file containing asset configurations (defaults to `asset-import/asset-config-import-ready.csv`)
- `--dry-run`: Preview the import operation without making actual changes
- `--quiet`: Suppress detailed output (only show errors and warnings)
- `--verbose`: Show detailed processing information including API requests/responses
- `--parallel`: Use parallel processing for faster import operations
- `--allowed-types`: Comma-separated list of asset types to import (e.g., `"database,table,view"`)

## Examples

### Basic Import

```bash
asset-config-import
```

### Import with Custom File

```bash
asset-config-import /path/to/asset-config-import-ready.csv
```

### Dry Run Mode

```bash
asset-config-import --dry-run --quiet --parallel
```

### Verbose Import

```bash
asset-config-import --verbose
```

### Import Specific Asset Types

```bash
asset-config-import --allowed-types "database,table" --verbose
```

## Input Files

The command expects the following input file:

- `asset-import/asset-config-import-ready.csv` - Processed asset configurations (default)
- Or a custom CSV file specified as a parameter

## File Structure

The CSV file should contain the following columns:
- Asset UID
- Asset configuration data
- Environment-specific settings
- Metadata for import processing

## Workflow Integration

This command is typically used in the following workflow:

1. **Export Configurations**: Use `asset-config-export` to export asset configurations from source
2. **Transform Data**: Use `policy-xfr` to process and transform the exported data
3. **Import Configurations**: Use this command to import the processed configurations

## Error Handling

The command includes comprehensive error handling:

- **Missing Input File**: Provides clear guidance on expected file location
- **Invalid Data**: Validates CSV structure and data format
- **API Errors**: Handles authentication and permission issues
- **Network Issues**: Retries failed operations with exponential backoff

## Safety Features

- **Dry Run Mode**: Use `--dry-run` to preview changes without making them
- **Validation**: Validates asset configurations before import
- **Rollback**: Provides guidance for rolling back changes if needed
- **Logging**: Comprehensive logging for audit trails

## Performance

- **Parallel Processing**: Use `--parallel` for faster import of large datasets
- **Batch Operations**: Processes assets in optimized batches
- **Memory Management**: Efficient memory usage for large files
- **Progress Tracking**: Real-time progress indicators for long operations

## Tips

- **Always Test**: Use `--dry-run` first to verify the import will work as expected
- **Backup**: Ensure you have backups of the target environment before importing
- **Monitor**: Use `--verbose` mode to monitor the import process
- **Validate**: Review the output files after import to ensure success

## Related Commands

- [asset-config-export](asset-config-export.md) - Export asset configurations from source environment
- [policy-xfr](policy-xfr.md) - Transform asset configuration files
- [asset-profile-import](asset-profile-import.md) - Import asset profiles
- [asset-tag-import](asset-tag-import.md) - Import asset tags 