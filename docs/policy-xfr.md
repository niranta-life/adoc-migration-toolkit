# Policy XFR Command

The `policy-xfr` command formats policy export files by replacing multiple substrings in JSON files and ZIP archives. This is essential for preparing policy files for migration between environments.

## Overview

This command processes policy export files (ZIP archives and JSON files) by replacing environment-specific strings with target environment equivalents. It's commonly used to transform production environment references to development or staging environment references.

## Usage

```bash
policy-xfr [--input <input_dir>] --string-transform "A":"B", "C":"D", "E":"F" [options]
```

## Parameters

### Required Parameters

- `--string-transform`: String transformation mappings in format `"old":"new", "old2":"new2"`
  - Multiple transformations can be specified
  - Each transformation should be in quotes and separated by commas
  - Example: `"PROD_DB":"DEV_DB", "PROD_URL":"DEV_URL"`

### Optional Parameters

- `--input <input_dir>`: Input directory containing policy files (auto-detected from policy-export if not specified)
- `--quiet`: Suppress detailed output (only show errors and warnings)
- `--verbose`: Show detailed processing information including file operations

## Examples

### Basic Transformation

```bash
policy-xfr --string-transform "PROD_DB":"DEV_DB"
```

### Multiple Transformations

```bash
policy-xfr --string-transform "PROD_DB":"DEV_DB", "PROD_URL":"DEV_URL", "PROD_API":"DEV_API"
```

### With Custom Input Directory

```bash
policy-xfr --input data/samples --string-transform "old":"new", "test":"prod"
```

### Verbose Mode

```bash
policy-xfr --string-transform "A":"B", "C":"D", "E":"F" --verbose
```

### Environment String Replacement

```bash
policy-xfr --source-env-string "PROD_DB" --target-env-string "DEV_DB"
```

## Input Files

The command processes files from the following locations:

- **Auto-detected**: `<output-dir>/policy-export/` (default)
- **Custom directory**: Specified with `--input` parameter
- **File types**: ZIP archives and JSON files

## Output Files

The command creates processed files in the same directory structure:

- **ZIP files**: Processed with string replacements
- **JSON files**: Processed with string replacements
- **Metadata**: Preserves original file structure

## String Transformations

### Common Transformation Patterns

- **Database Names**: `"PROD_DB":"DEV_DB"`
- **URLs**: `"https://prod.company.com":"https://dev.company.com"`
- **API Endpoints**: `"prod-api":"dev-api"`
- **Environment Variables**: `"PROD_ENV":"DEV_ENV"`

### Transformation Rules

- **Case-sensitive**: Transformations are case-sensitive
- **Exact matches**: Only exact string matches are replaced
- **Preserves structure**: File structure and formatting are preserved
- **Multiple passes**: All transformations are applied to each file

## Workflow Integration

This command is typically used in the following workflow:

1. **Export Policies**: Use `policy-export` to create ZIP files
2. **Transform Files**: Use this command to replace environment-specific strings
3. **Import Policies**: Use `policy-import` to import the transformed files

## Error Handling

The command includes comprehensive error handling:

- **Missing Input Directory**: Provides clear guidance on expected file locations
- **Invalid Transformations**: Validates transformation syntax
- **File System Errors**: Handles disk space and permission issues
- **Processing Errors**: Detailed error messages with recovery suggestions

## Safety Features

- **Backup**: Original files are preserved
- **Validation**: Validates file integrity after processing
- **Rollback**: Provides guidance for rolling back changes
- **Logging**: Comprehensive logging for audit trails

## Performance

- **Batch Processing**: Processes multiple files efficiently
- **Memory Management**: Efficient memory usage for large ZIP files
- **Progress Tracking**: Real-time progress indicators for long operations
- **Error Recovery**: Continues processing even if individual files fail

## Tips

- **Test First**: Test with a small subset of files before processing all
- **Backup**: Always backup original files before transformation
- **Validation**: Review transformed files before import
- **Environment Mapping**: Create a comprehensive mapping of environment-specific strings
- **Incremental**: Process files in smaller batches for large datasets

## Common Use Cases

### Production to Development Migration
Replace production environment references:
```bash
policy-xfr --string-transform "PROD_DB":"DEV_DB", "PROD_URL":"DEV_URL"
```

### Environment Standardization
Standardize environment references:
```bash
policy-xfr --string-transform "old-env":"new-env", "legacy":"modern"
```

### Multi-Environment Setup
Prepare files for multiple environments:
```bash
policy-xfr --string-transform "env1":"env2", "config1":"config2"
```

## Related Commands

- [policy-export](policy-export.md) - Export policies by category to ZIP files
- [policy-import](policy-import.md) - Import policy ZIP files to target environment
- [transform-and-merge](transform-and-merge.md) - Transform and merge asset CSV files
- [asset-config-import](asset-config-import.md) - Import asset configurations 