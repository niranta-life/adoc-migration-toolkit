# Asset Tag Import Command

The `asset-tag-import` command imports asset tags to the target environment from CSV files containing tag data.

## Overview

This command imports asset tags that have been exported from the source environment. It processes CSV files containing asset UIDs and their associated tags, then applies these tags to the corresponding assets in the target environment.

## Usage

```bash
asset-tag-import [csv_file] [--quiet] [--verbose] [--parallel]
```

## Parameters

### Optional Parameters

- `csv_file`: Path to the CSV file containing asset tag data (defaults to `asset-export/asset_uids.csv`)
- `--quiet`: Suppress detailed output (only show errors and warnings)
- `--verbose`: Show detailed processing information including API requests/responses
- `--parallel`: Use parallel processing for faster import operations

## Examples

### Basic Import

```bash
asset-tag-import
```

### Import with Custom File

```bash
asset-tag-import /path/to/asset-data.csv --verbose --parallel
```

### Quiet Mode

```bash
asset-tag-import --quiet
```

### Verbose Import

```bash
asset-tag-import --verbose
```

### Parallel Processing

```bash
asset-tag-import --parallel
```

## Input Files

The command expects the following input file:

- `asset-export/asset_uids.csv` - Asset data with tags (default)
- Or a custom CSV file specified as a parameter

## File Structure

The CSV file should contain the following columns:
- Asset UID
- Asset tags (comma-separated or in separate columns)
- Additional metadata as needed

## Tag Format

Tags can be specified in various formats:
- **Comma-separated**: `"tag1,tag2,tag3"`
- **Semicolon-separated**: `"tag1;tag2;tag3"`
- **Pipe-separated**: `"tag1|tag2|tag3"`
- **Individual columns**: Each tag in its own column

## Workflow Integration

This command is typically used in the following workflow:

1. **Export Assets**: Use `asset-list-export` to export assets with their tags
2. **Process Data**: Optionally transform the data using other commands
3. **Import Tags**: Use this command to import the tags to the target environment

## Error Handling

The command includes comprehensive error handling:

- **Missing Input File**: Provides clear guidance on expected file location
- **Invalid Tag Format**: Validates tag structure and format
- **Asset Not Found**: Handles cases where assets don't exist in target environment
- **API Errors**: Handles authentication and permission issues
- **Network Issues**: Retries failed operations with exponential backoff

## Performance

- **Parallel Processing**: Use `--parallel` for faster import of large datasets
- **Batch Operations**: Processes assets in optimized batches
- **Memory Management**: Efficient memory usage for large files
- **Progress Tracking**: Real-time progress indicators for long operations

## Safety Features

- **Validation**: Validates asset UIDs before attempting to apply tags
- **Error Recovery**: Continues processing even if individual tag applications fail
- **Logging**: Comprehensive logging for audit trails
- **Rollback**: Provides guidance for rolling back changes if needed

## Tips

- **Verify Assets**: Ensure target assets exist before importing tags
- **Tag Format**: Use consistent tag formats across environments
- **Monitor**: Use `--verbose` mode to monitor the import process
- **Test**: Test with a small subset of assets first
- **Backup**: Ensure you have backups of the target environment before importing

## Common Use Cases

### Environment Migration
Import tags from production to development environment:
```bash
asset-tag-import production-tags.csv --verbose
```

### Tag Standardization
Apply standardized tags across environments:
```bash
asset-tag-import standard-tags.csv --parallel
```

### Selective Tag Import
Import specific tag categories:
```bash
asset-tag-import security-tags.csv --quiet
```

## Related Commands

- [asset-list-export](asset-list-export.md) - Export assets with tags from source environment
- [asset-config-import](asset-config-import.md) - Import asset configurations
- [asset-profile-import](asset-profile-import.md) - Import asset profiles
- [segments-import](segments-import.md) - Import segments 