# asset-list-export

Export all assets from source or target environment to CSV file.

## Synopsis

```bash
asset-list-export [--quiet] [--verbose] [--parallel] [--target] [--page-size <size>] [--source_type_ids <list>] [--asset_type_ids <list>] [--assembly_ids <list>]
```

## Description

The `asset-list-export` command exports all assets from either the source or target Acceldata environment to a CSV file. This command uses the `/catalog-server/api/assets/discover` endpoint with pagination to retrieve all assets and their metadata.

## Arguments

- `--quiet`: Suppress console output, show only summary
- `--verbose`: Show detailed output including headers and responses
- `--parallel`: Use parallel processing for faster export (max 5 threads)
- `--target`: Use target environment instead of source environment
- `--page-size <size>`: Number of assets per page (default: 500)
- `--source_type_ids <list>`: Comma-separated list of source type IDs (optional)
- `--asset_type_ids <list>`: Comma-separated list of asset type IDs (optional)
- `--assembly_ids <list>`: Comma-separated list of assembly IDs (optional)

## Examples

```bash
# Export all assets from source environment
asset-list-export

# Export with quiet mode
asset-list-export --quiet

# Export with detailed output
asset-list-export --verbose

# Export using parallel processing
asset-list-export --parallel

# Export from target environment
asset-list-export --target

# Export with custom page size
asset-list-export --page-size 1000

# Export with filters
asset-list-export --parallel --source_type_ids=5 --asset_type_ids=2,23,53 --assembly_ids=100,101

# Export with page size and filters
asset-list-export --page-size 250 --parallel --source_type_ids=5 --asset_type_ids=2,23,53 --assembly_ids=100,101
```

## Behavior

### Processing Logic
- Uses `/catalog-server/api/assets/discover` endpoint with pagination
- First call gets total count with `size=0&page=0&profiled_assets=true&parents=true`
- Retrieves all pages with specified page size and `profiled_assets=true&parents=true`
- Default page size: 500 assets per page
- Source environment output: `<output-dir>/asset-export/asset-all-source-export.csv`
- Target environment output: `<output-dir>/asset-export/asset-all-target-export.csv`

### Output Format
- CSV columns: source_uid, source_id, target_uid, tags
- Extracts asset.uid, asset.id, and asset.tags[].name from response
- Concatenates tags with colon (:) separator in tags column
- Sorts output by source_uid first, then by source_id

### Filtering Options
- **source_type_ids**: Filter by source type IDs
- **asset_type_ids**: Filter by asset type IDs
- **assembly_ids**: Filter by assembly IDs
- Filters are applied at the API level for efficiency

### Parallel Processing
- Uses up to 5 threads to process pages simultaneously
- Each thread has its own progress bar
- Significantly faster for large asset sets
- Work is divided equally between threads

## Use Cases

1. **Asset Discovery**: Discover all assets in an environment
2. **Environment Comparison**: Compare assets between source and target
3. **Asset Analysis**: Analyze asset types and distributions
4. **Migration Planning**: Plan asset migration strategies
5. **Filtered Export**: Export specific asset subsets

## Related Commands

- [asset-profile-export](asset-profile-export.md) - Export asset profiles
- [asset-config-export](asset-config-export.md) - Export asset configurations
- [transform-and-merge](transform-and-merge.md) - Transform and merge asset files

## Tips

- Use `--parallel` for faster processing of large asset sets
- Use `--verbose` to see detailed API request/response information
- Use `--target` to export from target environment
- Use filters to reduce export size and processing time
- The output file is used as input for other asset commands

## Error Handling

- Network errors are retried automatically with exponential backoff
- Authentication errors will fail immediately
- Invalid filter values are logged as warnings
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
- Thread names: Rocket, Lightning, Unicorn, Dragon, Shark
- Green progress bars for visual distinction
- Combined statistics at completion

## File Format

### Output CSV Format
- `source_uid`: Asset UID in source environment
- `source_id`: Asset ID in source environment
- `target_uid`: Asset UID in target environment (if applicable)
- `tags`: Asset tags (colon-separated)

## Statistics

The command provides comprehensive statistics including:
- Total assets exported
- Assets by type
- Assets by assembly
- Processing time
- Success/failure counts
- Filter application results

## Environment Selection

### Source Environment (Default)
- Uses source access key, secret key, and tenant
- Output file: `asset-all-source-export.csv`
- Suitable for initial asset discovery

### Target Environment (`--target`)
- Uses target access key, secret key, and tenant
- Output file: `asset-all-target-export.csv`
- Suitable for target environment analysis 