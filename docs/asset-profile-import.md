# asset-profile-import

Import asset profiles to target environment from CSV file.

## Synopsis

```bash
asset-profile-import [<csv_file>] [--dry-run] [--quiet] [--verbose]
```

## Description

The `asset-profile-import` command imports asset profiles from a CSV file to the target Acceldata environment. This command reads the CSV file generated from the `asset-profile-export` command and applies the profile configurations to the target environment.

## Arguments

- `csv_file` (optional): Path to CSV file with target-env and profile_json
- `--dry-run`: Preview changes without making API calls
- `--quiet`: Suppress console output (default)
- `--verbose`: Show detailed output including headers and responses

## Examples

```bash
# Import asset profiles using default input file
asset-profile-import

# Import from specific CSV file
asset-profile-import <output-dir>/asset-import/asset-profiles-import-ready.csv

# Import with dry run to preview changes
asset-profile-import profiles.csv --dry-run --verbose

# Import with detailed output
asset-profile-import profiles.csv --verbose
```

## Behavior

### Default File Locations
- **Input**: `<output-dir>/asset-import/asset-profiles-import-ready.csv`

### Processing Logic
- Reads target-env and profile_json from CSV file
- Makes API calls to update asset profiles in target environment
- Supports dry-run mode for previewing changes
- Validates JSON content before processing

### Input File Format
The CSV file should contain:
- `target-env`: Target environment UID
- `profile_json`: JSON profile data for the asset

## Use Cases

1. **Asset Migration**: Import asset profiles for migration between environments
2. **Profile Restoration**: Restore asset profiles from backup
3. **Environment Synchronization**: Sync asset profiles across environments
4. **Configuration Updates**: Update asset profile configurations

## Related Commands

- [asset-profile-export](asset-profile-export.md) - Export asset profiles from source environment
- [asset-config-import](asset-config-import.md) - Import asset configurations
- [asset-tag-import](asset-tag-import.md) - Import asset tags

## Tips

- Always use `--dry-run` first to preview the changes
- Use `--verbose` to see detailed API calls and responses
- The command validates JSON content before processing
- Invalid JSON in profile_json will cause the asset to be skipped
- This step is typically performed after asset creation in the target environment

## Error Handling

- Invalid CSV format will cause the command to fail
- Invalid JSON in profile_json will skip that asset
- Network errors are retried automatically
- Authentication errors will fail immediately
- Missing target assets will cause the asset to be skipped

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

## Processing Flow

1. **Read CSV File**: Parse the input CSV file
2. **Validate JSON**: Check that profile_json contains valid JSON
3. **Get Asset ID**: Retrieve asset ID from target environment using target-env
4. **Update Profile**: Make API call to update asset profile
5. **Report Results**: Show success/failure for each asset

## Success Criteria

- Asset exists in target environment
- Profile JSON is valid
- API call succeeds
- Asset profile is updated successfully 