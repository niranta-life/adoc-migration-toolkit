# profile-run

Trigger profiling for assets listed in the specified CSV file on the target environment.

## Synopsis

```bash
profile-run --config <profile-assets.csv> [--quiet] [--verbose] [--parallel]
```

## Description

The `profile-run` command triggers profiling for assets listed in the specified CSV file on the target environment. This command changes the engine type to Pushdown for the given assets, which is essential for proper asset configuration and performance.

## Arguments

- `--config <profile-assets.csv>` (required): Path to the CSV file listing assets to profile
- `--quiet`: Suppress console output, show only summary with progress bar
- `--verbose`: Show detailed output including API calls and responses
- `--parallel`: Use parallel processing for faster profiling (max 5 threads)

## Examples

```bash
# Trigger profiling for assets
profile-run --config profile-assets.csv

# Trigger with quiet mode
profile-run --config profile-assets.csv --quiet

# Trigger with detailed output
profile-run --config profile-assets.csv --verbose

# Trigger using parallel processing
profile-run --config profile-assets.csv --parallel
```

## Behavior

### Processing Logic
- Reads asset IDs and UIDs from the specified CSV file
- Triggers profiling for each listed asset on the target environment
- Monitors profiling status and reports completion or errors
- Shows progress bar in quiet mode
- Shows detailed API calls in verbose mode

### Parallel Processing
- Uses up to 5 threads to process assets simultaneously
- Each thread has its own progress bar
- Significantly faster for large asset sets
- Work is divided equally between threads

### Engine Type Change
- Changes the engine type to Pushdown for profiled assets
- This is required for proper asset configuration
- Improves performance and functionality

## Use Cases

1. **Asset Configuration**: Trigger profiling for newly imported assets
2. **Performance Optimization**: Enable Pushdown engine for better performance
3. **Migration Completion**: Finalize asset configuration after migration
4. **Bulk Operations**: Profile multiple assets simultaneously

## Related Commands

- [profile-check](profile-check.md) - Check assets that require profiling
- [asset-profile-import](asset-profile-import.md) - Import asset profiles
- [asset-config-import](asset-config-import.md) - Import asset configurations

## Tips

- Use `--parallel` for faster processing of large asset sets
- Use `--verbose` to see detailed API calls and responses
- This command is typically run after asset import and profile import
- The engine type change to Pushdown is essential for proper functionality
- Monitor the progress to ensure all assets are profiled successfully

## Error Handling

- Invalid asset UIDs are logged as warnings and skipped
- Network errors are retried automatically with exponential backoff
- Authentication errors will fail immediately
- Missing assets in target environment will be reported
- Profiling failures are logged with details

## Output

### Quiet Mode (Default)
- Shows progress bar
- Displays summary statistics

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
- `asset_id`: Asset ID in target environment
- `asset_uid`: Asset UID in target environment

### Output
- Console output showing profiling status
- Statistics on successful vs failed profiling
- Engine type change confirmation

## Success Criteria

- Asset exists in target environment
- Profiling is triggered successfully
- Engine type is changed to Pushdown
- Asset is properly configured for use 