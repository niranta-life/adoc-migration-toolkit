# asset-config-export

Export asset configurations from source environment to CSV file.

## Synopsis

```bash
asset-config-export [<csv_file>] [--output-file <file>] [--quiet] [--verbose] [--parallel]
```

## Description

The `asset-config-export` command exports asset configurations from the source Acceldata environment to a CSV file. This command reads asset mappings from a CSV file and retrieves the configuration JSON data for each asset from the source environment.

## Arguments

- `csv_file` (optional): Path to CSV file with 5 columns: source_id, source_uid, target_id, target_uid, tags
- `--output-file <file>` (optional): Specify custom output file path
- `--quiet`: Suppress console output, show only summary
- `--verbose`: Show detailed output including headers and responses
- `--parallel`: Use parallel processing for faster export (max 5 threads, quiet mode default)

## Examples

```bash
# Export asset configurations using default input file
asset-config-export

# Export from specific CSV file
asset-config-export <output-dir>/asset-import/asset-merged-all.csv

# Export with custom output file and verbose mode
asset-config-export uids.csv --output-file configs.csv --verbose

# Export using parallel processing
asset-config-export --parallel

# Export using parallel processing with verbose output
asset-config-export --parallel --verbose
```

## Behavior

### Default File Locations
- **Input**: `<output-dir>/asset-import/asset-merged-all.csv`
- **Output**: `<output-dir>/asset-import/asset-config-import-ready.csv`

### Processing Logic
- Reads CSV with 5 columns: source_id, source_uid, target_id, target_uid, tags
- Uses source_id to call `/catalog-server/api/assets/<source_id>/config`
- Writes compressed JSON response to CSV with target_uid
- Shows status for each asset in quiet mode
- Shows HTTP headers and response objects in verbose mode

### Output Format
- `target_uid`: Target environment UID
- `config_json`: Compressed JSON configuration

### Parallel Processing
- Uses up to 5 threads, work divided equally between threads
- Quiet mode is default (shows tqdm progress bars)
- Use `--verbose` to see URL, headers, and response for each call
- Thread names: Rocket, Lightning, Unicorn, Dragon, Shark (with green progress bars)
- Default mode: Silent (no progress bars)

## Use Cases

1. **Asset Migration**: Export asset configurations for migration between environments
2. **Configuration Backup**: Create backups of asset configurations
3. **Environment Synchronization**: Sync asset configurations across environments
4. **Configuration Analysis**: Analyze asset configuration settings

## Related Commands

- [asset-config-import](asset-config-import.md) - Import asset configurations to target environment
- [asset-profile-export](asset-profile-export.md) - Export asset profiles
- [asset-list-export](asset-list-export.md) - Export all assets

## Tips

- Use `--parallel` for faster processing of large asset sets
- Use `--verbose` to see detailed API request/response information
- The output file is used as input for the `asset-config-import` command
- Parallel mode is especially effective for assets with complex configurations
- The configuration JSON is compressed to reduce file size

## Error Handling

- Invalid asset IDs are logged as warnings and skipped
- Network errors are retried automatically with exponential backoff
- Authentication errors will fail immediately
- Missing input files will cause the command to fail

## Output

### Quiet Mode (Default)
- Shows status for each asset
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
- `source_id`: Asset ID in source environment
- `source_uid`: Asset UID in source environment
- `target_id`: Asset ID in target environment
- `target_uid`: Asset UID in target environment
- `tags`: Asset tags (colon-separated)

### Output CSV Format
- `target_uid`: Target environment UID
- `config_json`: Compressed JSON configuration data 