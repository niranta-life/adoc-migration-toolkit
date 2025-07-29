# profile-check

Check which assets require profiling on the target environment and optionally trigger profiling actions.

## Synopsis

```bash
profile-check --config <profile-assets.csv> [--quiet] [--verbose] [--parallel] [--run-profile]
```

## Description

The `profile-check` command checks which assets require profiling on the target environment and optionally triggers profiling for assets that are not yet profiled. This is useful for ensuring that assets are properly configured before migration or for monitoring profiling status.

## Arguments

- `--config <profile-assets.csv>` (required): Path to the CSV file listing assets to check/profile
- `--quiet`: Suppress console output, show only summary with progress bar
- `--verbose`: Show detailed output including API calls and responses
- `--parallel`: Use parallel processing for faster checks (max 5 threads)
- `--run-profile`: Automatically trigger profiling for assets that are not yet profiled

## Examples

```bash
# Check profiling status for assets
profile-check --config profile-assets.csv

# Check with quiet mode
profile-check --config profile-assets.csv --quiet

# Check with detailed output
profile-check --config profile-assets.csv --verbose

# Check using parallel processing
profile-check --config profile-assets.csv --parallel

# Check and trigger profiling for unprofiled assets
profile-check --config profile-assets.csv --run-profile
```

## Behavior

### Processing Logic
- Reads asset IDs and UIDs from the specified CSV file
- Checks if each asset is already profiled on the target environment
- Outputs a CSV of assets that require profiling
- Optionally triggers profiling for unprofiled assets if `--run-profile` is specified
- Shows progress bar in quiet mode
- Shows detailed API calls in verbose mode

### Parallel Processing
- Uses up to 5 threads to process assets simultaneously
- Each thread has its own progress bar
- Significantly faster for large asset sets
- Work is divided equally between threads

## Use Cases

1. **Pre-Migration Check**: Verify which assets need profiling before migration
2. **Post-Migration Verification**: Check profiling status after asset import
3. **Monitoring**: Regularly check profiling status of assets
4. **Automated Profiling**: Trigger profiling for assets that need it

## Related Commands

- [profile-run](profile-run.md) - Trigger profiling for assets
- [asset-profile-import](asset-profile-import.md) - Import asset profiles
- [asset-config-import](asset-config-import.md) - Import asset configurations

## Tips

- Use `--parallel` for faster processing of large asset sets
- Use `--verbose` to see detailed API calls and responses
- The `--run-profile` option is useful for automated workflows
- This command is typically run after asset import to ensure proper configuration

## Error Handling

- Invalid asset UIDs are logged as warnings and skipped
- Network errors are retried automatically with exponential backoff
- Authentication errors will fail immediately
- Missing assets in target environment will be reported

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
- Optional CSV file with assets requiring profiling
- Statistics on profiled vs unprofiled assets 