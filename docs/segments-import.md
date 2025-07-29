# segments-import

Import segments to target environment from CSV file.

## Synopsis

```bash
segments-import <csv_file> [--dry-run] [--quiet] [--verbose]
```

## Description

The `segments-import` command imports segment configurations from a CSV file to the target Acceldata environment. This command reads the CSV file generated from the `segments-export` command and applies the segment configurations to the target environment.

## Arguments

- `csv_file` (required): Path to CSV file with target-env and segments_json
- `--dry-run`: Preview changes without making API calls
- `--quiet`: Suppress console output (default)
- `--verbose`: Show detailed output including headers

## Examples

```bash
# Import segments from default output file
segments-import <output-dir>/policy-import/segments_output.csv

# Import with dry run to preview changes
segments-import segments.csv --dry-run --verbose

# Import with detailed output
segments-import segments.csv --verbose
```

## Behavior

### Input File Format
The CSV file should contain:
- `target-env`: Target environment UID
- `segments_json`: JSON configuration for segments

### Processing Logic
- Reads the CSV file generated from `segments-export` command
- Targets UIDs for which segments are present and engine is SPARK
- Imports segments configuration to target environment
- Creates new segments (removes existing IDs)
- Supports both SPARK and JDBC_SQL engine types
- Validates CSV format and JSON content
- Processes only assets that have valid segments configuration

### Engine Type Support
- **SPARK**: Primary use case - segments are required for proper configuration
- **JDBC_SQL**: Supported but segments are typically available in standard import

## Use Cases

1. **Environment Migration**: Import segments from production to development environments
2. **Configuration Synchronization**: Sync segment configurations across environments
3. **Disaster Recovery**: Restore segment configurations from backup

## Related Commands

- [segments-export](segments-export.md) - Export segments from source environment
- [asset-config-import](asset-config-import.md) - Import asset configurations
- [policy-import](policy-import.md) - Import policy definitions

## Tips

- Always use `--dry-run` first to preview the changes
- Use `--verbose` to see detailed API calls and responses
- The command only processes assets with valid segment configurations
- For Spark assets, this step is essential after asset import
- Invalid JSON in segments_json will cause the asset to be skipped

## Error Handling

- Invalid CSV format will cause the command to fail
- Invalid JSON in segments_json will skip that asset
- Network errors are retried automatically
- Authentication errors will fail immediately

## Output

### Quiet Mode (Default)
- Shows progress summary
- Displays final statistics

### Verbose Mode
- Shows detailed API request/response information
- Displays headers and response objects
- Provides comprehensive processing details

### Dry Run Mode
- Shows what would be imported without making actual API calls
- Displays preview of changes
- Useful for validation before actual import 