# Policy Import Command

The `policy-import` command imports policy definitions from ZIP files to the target environment.

## Overview

This command imports policy definitions that have been exported by the `policy-export` command. It processes ZIP files containing policy definitions and applies them to the target environment. The command supports various file patterns and can handle multiple ZIP files simultaneously.

## Usage

```bash
policy-import <file_or_pattern> [--quiet] [--verbose]
```

## Parameters

### Required Parameters

- `<file_or_pattern>`: File or pattern to import
  - Specific file: `policy-export.zip`
  - Pattern: `*.zip`, `data-quality-*.zip`
  - Directory: `data/policy-export/*.json`

### Optional Parameters

- `--quiet`: Suppress detailed output (only show errors and warnings)
- `--verbose`: Show detailed processing information including API requests/responses

## Examples

### Import All ZIP Files

```bash
policy-import *.zip
```

### Import Specific File

```bash
policy-import /path/to/specific-file.zip
```

### Import with Verbose Output

```bash
policy-import *.zip --verbose
```

### Import from Directory

```bash
policy-import data/policy-export/*.json
```

### Import with Pattern

```bash
policy-import data-quality-*.zip --verbose
```

## File Types

The command supports importing the following file types:

- **ZIP Files**: Policy definitions exported by `policy-export`
- **JSON Files**: Individual policy definition files
- **Patterns**: Wildcard patterns for multiple files

## File Locations

The command looks for files in the following locations:

1. **Specified Path**: If a full path is provided
2. **Policy Import Directory**: `<output-dir>/policy-import/` (default)
3. **Current Directory**: If no specific path is provided

## Workflow Integration

This command is typically used in the following workflow:

1. **Export Policies**: Use `policy-list-export` to export all policies
2. **Export by Category**: Use `policy-export` to create ZIP files by category
3. **Import Policies**: Use this command to import the ZIP files to target environment

## Error Handling

The command includes comprehensive error handling:

- **Missing Files**: Provides clear guidance on expected file locations
- **Invalid File Format**: Validates ZIP and JSON file structures
- **API Errors**: Handles authentication and permission issues
- **Network Issues**: Retries failed operations with exponential backoff
- **Import Conflicts**: Handles duplicate policy definitions gracefully

## Safety Features

- **Validation**: Validates policy definitions before import
- **Conflict Resolution**: Handles duplicate policies with appropriate strategies
- **Rollback**: Provides guidance for rolling back changes if needed
- **Logging**: Comprehensive logging for audit trails

## Performance

- **Batch Processing**: Processes multiple files efficiently
- **Memory Management**: Efficient memory usage for large ZIP files
- **Progress Tracking**: Real-time progress indicators for long operations
- **Error Recovery**: Continues processing even if individual files fail

## Tips

- **Test First**: Test with a small subset of policies before full import
- **Backup**: Ensure you have backups of the target environment before importing
- **Monitor**: Use `--verbose` mode to monitor the import process
- **Validate**: Review the imported policies after completion
- **Patterns**: Use file patterns to import specific categories of policies

## Common Use Cases

### Complete Policy Migration
Import all policy categories:
```bash
policy-import *.zip --verbose
```

### Selective Import
Import specific policy categories:
```bash
policy-import data-quality-*.zip --verbose
```

### Individual File Import
Import a specific policy file:
```bash
policy-import /path/to/policy.zip
```

### Directory Import
Import all JSON files from a directory:
```bash
policy-import data/policy-export/*.json
```

## File Structure

ZIP files should contain:
- Policy definition JSON files
- Metadata files (if applicable)
- Supporting configuration files

## Related Commands

- [policy-list-export](policy-list-export.md) - Export all policies to CSV file
- [policy-export](policy-export.md) - Export policies by category to ZIP files
- [policy-xfr](policy-xfr.md) - Transform policy files with string replacements
- [rule-tag-export](rule-tag-export.md) - Export rule tags for policies 