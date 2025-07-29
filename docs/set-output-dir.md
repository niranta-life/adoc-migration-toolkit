# set-output-dir

Set global output directory for all export commands.

## Synopsis

```bash
set-output-dir <directory>
```

## Description

The `set-output-dir` command sets the global output directory for all export commands in the ADOC Migration Toolkit. This directory is used as the base path for all exported files and is persisted across multiple interactive sessions.

## Arguments

- `directory` (required): Path to the output directory

## Examples

```bash
# Set output directory to a custom path
set-output-dir /path/to/my/output

# Set output directory to a relative path
set-output-dir data/custom_output

# Set output directory to current directory
set-output-dir .
```

## Behavior

### Features
- Sets the output directory for all export commands
- Creates the directory if it doesn't exist
- Validates write permissions
- Saves configuration to `~/.adoc_migration_config.json`
- Persists across multiple interactive sessions
- Can be changed anytime with another `set-output-dir` command

### Directory Structure
When set, the output directory will contain:
```
<output-dir>/
├── asset-export/          # Asset export files
├── asset-import/          # Asset import files
├── policy-export/         # Policy export files
├── policy-import/         # Policy import files
└── logs/                 # Log files
```

## Use Cases

1. **Organized Workflows**: Set a dedicated directory for migration projects
2. **Multiple Projects**: Use different directories for different environments
3. **Team Collaboration**: Share output directories across team members
4. **Backup Management**: Use specific directories for backup operations

## Related Commands

- [show-config](show-config.md) - Display current configuration
- [asset-list-export](asset-list-export.md) - Export all assets
- [policy-list-export](policy-list-export.md) - Export all policies

## Tips

- Set the output directory early in your workflow
- Use descriptive directory names for different projects
- Ensure the directory has sufficient disk space
- Use absolute paths for better reliability
- The directory will be created automatically if it doesn't exist

## Error Handling

- Invalid paths will cause the command to fail
- Insufficient permissions will be reported
- Disk space issues will be detected
- Configuration file write errors will be reported

## Output

The command provides feedback including:
- Directory creation confirmation
- Permission validation
- Configuration save confirmation
- Current output directory display

## Configuration Persistence

The output directory setting is stored in:
- File: `~/.adoc_migration_config.json`
- Format: JSON configuration
- Persists across sessions
- Can be manually edited if needed

## Default Behavior

If no output directory is set:
- Default pattern: `adoc-migration-toolkit-YYYYMMDDHHMM`
- Created in current working directory
- Timestamp-based naming for uniqueness 