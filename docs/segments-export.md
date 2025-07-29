# segments-export

Export segments from source environment to CSV file.

## Synopsis

```bash
segments-export [<csv_file>] [--output-file <file>] [--quiet]
```

## Description

The `segments-export` command exports segment configurations from the source Acceldata environment to a CSV file. This is particularly important for Spark assets that have segmented configurations, as these are not directly imported with standard import capabilities.

## Arguments

- `csv_file` (optional): Path to CSV file with source-env and target-env mappings
- `--output-file <file>` (optional): Specify custom output file path
- `--quiet`: Suppress console output, show only summary

## Examples

```bash
# Export segments using default input file
segments-export

# Export segments from specific CSV file
segments-export <output-dir>/policy-export/segmented_spark_uids.csv

# Export with custom output file and quiet mode
segments-export data/uids.csv --output-file my_segments.csv --quiet
```

## Behavior

### Default File Locations
- **Input**: `<output-dir>/policy-export/segmented_spark_uids.csv`
- **Output**: `<output-dir>/policy-import/segments_output.csv`

### Processing Logic
- Exports segments configuration for assets with `isSegmented=true`
- **For engineType=SPARK**: Required because segmented Spark configurations are not directly imported with standard import capability
- **For engineType=JDBC_SQL**: Already available in standard import, so no additional configuration needed
- Only processes assets that have segments defined
- Skips assets without segments (logged as info)

### Output Format
The output CSV file contains:
- `target-env`: Target environment UID
- `segments_json`: JSON configuration for segments

## Use Cases

1. **Spark Asset Migration**: Essential for migrating Spark assets with segmented configurations
2. **Environment Synchronization**: Export segments from production to development environments
3. **Backup and Recovery**: Create backups of segment configurations

## Related Commands

- [segments-import](segments-import.md) - Import segments to target environment
- [asset-config-export](asset-config-export.md) - Export asset configurations
- [policy-export](policy-export.md) - Export policy definitions

## Tips

- Use `--quiet` for batch processing or when you only need the summary
- The command automatically skips assets without segments, so it's safe to run on all assets
- For Spark assets, this step is mandatory before import operations
- The output file is used as input for the `segments-import` command

## Error Handling

- If the input CSV file doesn't exist, the command will fail with a clear error message
- Invalid asset UIDs are logged as warnings and skipped
- Network errors are retried automatically with exponential backoff 