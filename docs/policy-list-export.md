# policy-list-export

Export all policies from source environment to CSV file.

## Synopsis

```bash
policy-list-export [--quiet] [--verbose] [--parallel] [--existing-target-assets]
```

## Description

The `policy-list-export` command exports all policies from the source Acceldata environment to a CSV file. This command uses the `/catalog-server/api/rules` endpoint with pagination to retrieve all policies and their metadata.

## Arguments

- `--quiet`: Suppress console output, show only summary
- `--verbose`: Show detailed output including headers and responses
- `--parallel`: Use parallel processing for faster export (max 5 threads)
- `--existing-target-assets`: Only include policies for assets that exist in target environment

## Examples

```bash
# Export all policies
policy-list-export

# Export with quiet mode
policy-list-export --quiet

# Export with detailed output
policy-list-export --verbose

# Export using parallel processing
policy-list-export --parallel

# Export using parallel processing with quiet mode
policy-list-export --parallel --quiet

# Export only policies for existing target assets
policy-list-export --existing-target-assets

# Export with existing target assets and verbose output
policy-list-export --existing-target-assets --verbose
```

## Behavior

### Processing Logic
- Uses `/catalog-server/api/rules` endpoint with pagination
- First call gets total count with `page=0&size=0`
- Retrieves all pages with `size=1000` (default)
- Output file: `<output-dir>/policy-export/policies-all-export.csv`
- CSV columns: id, type, engineType, tableAssetIds, assemblyIds, assemblyNames, sourceTypes
- Sorts output by id
- Shows page-by-page progress in quiet mode
- Shows detailed request/response in verbose mode

### Existing Target Assets Filter
- Reads `asset-merged-all.csv` from `asset-import/` directory
- Only includes policies where `tableAssetId` matches `source_id` in merged file
- Optimized with in-memory asset ID lookup
- Useful for ensuring only relevant policies are exported

### Parallel Processing
- Uses up to 5 threads with minimum 10 policies per thread
- Each thread has its own progress bar
- Automatic retry (3 attempts) on failures
- Temporary files merged into final output

## Use Cases

1. **Policy Migration**: Export all policies for migration between environments
2. **Policy Analysis**: Analyze policy types and configurations
3. **Environment Synchronization**: Sync policies across environments
4. **Selective Export**: Export only policies for existing target assets

## Related Commands

- [policy-export](policy-export.md) - Export policy definitions by categories
- [policy-import](policy-import.md) - Import policy definitions
- [rule-tag-export](rule-tag-export.md) - Export rule tags for policies

## Tips

- Use `--parallel` for faster processing of large policy sets
- Use `--verbose` to see detailed API request/response information
- The `--existing-target-assets` option is useful for targeted migrations
- This command is typically the first step in policy migration workflows
- The output file is used as input for other policy commands

## Error Handling

- Network errors are retried automatically with exponential backoff
- Authentication errors will fail immediately
- Invalid policy data is logged as warnings
- Pagination errors are handled gracefully

## Output

### Quiet Mode (Default)
- Shows page-by-page progress
- Displays final statistics

### Verbose Mode
- Shows detailed API request/response information
- Displays headers and response objects
- Provides comprehensive processing details

### Parallel Mode
- Shows progress bars for each thread
- Automatic retry on failures
- Combined statistics at completion

## File Format

### Output CSV Format
- `id`: Policy ID
- `type`: Policy type
- `engineType`: Engine type (SPARK, JDBC_SQL, etc.)
- `tableAssetIds`: Associated asset IDs
- `assemblyIds`: Associated assembly IDs
- `assemblyNames`: Associated assembly names
- `sourceTypes`: Associated source types

## Statistics

The command provides comprehensive statistics including:
- Total policies exported
- Policies by type
- Policies by engine type
- Processing time
- Success/failure counts 