# asset-profile-export

Export asset profiles from source environment to CSV file.

## Synopsis

```bash
asset-profile-export [<csv_file>] [--output-file <file>] [--quiet] [--verbose] [--parallel]
```

## Description

The `asset-profile-export` command exports asset profiles from the source Acceldata environment to a CSV file. This command reads source and target environment mappings from a CSV file and retrieves the profile JSON data for each asset from the source environment.

## Arguments

- `csv_file` (optional): Path to CSV file with source-env and target-env mappings
- `--output-file <file>` (optional): Specify custom output file path
- `--quiet`: Suppress console output, show only summary (default)
- `--verbose`: Show detailed output including headers and responses
- `--parallel`: Use parallel processing for faster export (max 5 threads)

## Examples

```bash
# Export asset profiles using default input file
asset-profile-export

# Export from specific CSV file
asset-profile-export <output-dir>/asset-export/asset_uids.csv

# Export with custom output file and verbose mode
asset-profile-export uids.csv --output-file profiles.csv --verbose

# Export using parallel processing
asset-profile-export --parallel
```

## Behavior

### Default File Locations
- **Input**: `<output-dir>/asset-export/asset_uids.csv`
- **Output**: `<output-dir>/asset-import/asset-profiles-import-ready.csv`

### Processing Logic
- Reads source-env and target-env mappings from CSV file
- Makes API calls to get asset profiles from source environment
- Writes profile JSON data to output CSV file
- Shows minimal output by default, use `--verbose` for detailed information

### Parallel Processing
- Uses up to 5 threads to process assets simultaneously
- Each thread has its own progress bar
- Significantly faster for large asset sets
- Work is divided equally between threads

## Use Cases

1. **Asset Migration**: Export asset profiles for migration between environments
2. **Profile Backup**: Create backups of asset profile configurations
3. **Environment Synchronization**: Sync asset profiles across environments
4. **Configuration Analysis**: Analyze asset profile configurations

## Related Commands

- [asset-profile-import](asset-profile-import.md) - Import asset profiles to target environment
- [asset-config-export](asset-config-export.md) - Export asset configurations
- [asset-list-export](asset-list-export.md) - Export all assets

## Tips

- Use `--parallel` for faster processing of large asset sets
- Use `--verbose` to see detailed API request/response information
- The output file is used as input for the `asset-profile-import` command
- Parallel mode is especially effective for assets with complex profiles

## Error Handling

- Invalid asset UIDs are logged as warnings and skipped
- Network errors are retried automatically with exponential backoff
- Authentication errors will fail immediately
- Missing input files will cause the command to fail

## Output

### Quiet Mode (Default)
- Shows minimal output with progress summary
- Displays final statistics

### Verbose Mode
- Shows detailed API request/response information
- Displays headers and response objects
- Provides comprehensive processing details

### Parallel Mode
- Shows progress bars for each thread
- Thread names: Rocket, Lightning, Unicorn, Dragon, Shark
- Green progress bars for visual distinction
- Combined statistics at completion

## File Format

### Input CSV Format
- `source-env`: Source environment UID
- `target-env`: Target environment UID

### Output CSV Format
- `target-env`: Target environment UID
- `profile_json`: JSON profile data for the asset 