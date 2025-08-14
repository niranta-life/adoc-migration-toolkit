# Transform and Merge Command

The `transform-and-merge` command processes and merges asset CSV files from source and target environments, applying string transformations to prepare data for migration.

## Overview

This command is essential for preparing asset data for migration by:
- Transforming environment-specific strings in asset configurations
- Merging source and target asset data into a unified format
- Creating processed files ready for import operations

## Usage

```bash
transform-and-merge --string-transform "A":"B", "C":"D" [--quiet] [--verbose]
```

## Parameters

### Required Parameters

- `--string-transform`: String transformation mappings in format `"old":"new", "old2":"new2"`
  - Multiple transformations can be specified
  - Each transformation should be in quotes and separated by commas
  - Example: `"PROD_DB":"DEV_DB", "PROD_URL":"DEV_URL"`

### Optional Parameters

- `--quiet`: Suppress detailed output (only show errors and warnings)
- `--verbose`: Show detailed processing information including API requests/responses

## Examples

### Basic Transformation

```bash
transform-and-merge --string-transform "PROD_DB":"DEV_DB"
```

### Multiple Transformations

```bash
transform-and-merge --string-transform "PROD_DB":"DEV_DB", "PROD_URL":"DEV_URL", "PROD_API":"DEV_API"
```

### With Verbose Output

```bash
transform-and-merge --string-transform "old":"new", "test":"prod" --verbose
```

### Quiet Mode

```bash
transform-and-merge --string-transform "A":"B", "C":"D" --quiet
```

## Input Files

The command expects the following input files to be present:

- `asset-export/asset_uids.csv` - Source environment asset data
- `asset-export/asset_uids_target.csv` - Target environment asset data

## Output Files

The command generates the following output files:

- `asset-merged-all.csv` - Merged and transformed asset data ready for import
- `asset-import/asset-config-import-ready.csv` - Processed asset configurations

## Workflow Integration

This command is typically used in the following workflow:

1. **Export Assets**: Use `asset-list-export` to export assets from both environments
2. **Transform and Merge**: Use this command to process the exported data
3. **Import Assets**: Use `asset-config-import` to import the processed data

## Error Handling

The command includes comprehensive error handling:

- **Missing Input Files**: Provides clear guidance on required files
- **Invalid Transformations**: Validates transformation syntax
- **Processing Errors**: Detailed error messages with recovery suggestions

## Tips

- **Environment Strings**: Focus on transforming environment-specific identifiers like database names, URLs, and API endpoints
- **Testing**: Use `--verbose` mode to see exactly what transformations are being applied
- **Validation**: Review the output files before proceeding with import operations
- **Backup**: Always backup original files before running transformations

## Related Commands

- [asset-list-export](asset-list-export.md) - Export assets from source/target environments
- [asset-config-import](asset-config-import.md) - Import processed asset configurations
- [policy-xfr](policy-xfr.md) - Transform policy files with string replacements 