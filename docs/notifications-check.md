# notifications-check

Compare notification groups between source and target environments and report differences.

## Synopsis

```bash
notifications-check --source-context <id> --target-context <id> --assembly-ids <ids> [--quiet] [--verbose] [--parallel] [--page-size <size>]
```

## Description

The `notifications-check` command compares notification groups between source and target Acceldata environments and reports differences. This command is useful for ensuring that notification configurations are properly synchronized between environments.

## Arguments

- `--source-context <id>` (required): Source context ID
- `--target-context <id>` (required): Target context ID
- `--assembly-ids <ids>` (required): Comma-separated list of assembly IDs to check
- `--quiet`: Suppress console output, show only summary with progress bar
- `--verbose`: Show detailed output including API calls and responses
- `--parallel`: Use parallel processing for faster comparison (max 5 threads)
- `--page-size <size>`: Number of notification groups per page (default: 100)

## Examples

```bash
# Check notification groups for specific assemblies
notifications-check --source-context 1 --target-context 2 --assembly-ids 100,101

# Check with quiet mode
notifications-check --source-context 1 --target-context 2 --assembly-ids 100,101 --quiet

# Check with detailed output
notifications-check --source-context 1 --target-context 2 --assembly-ids 100,101 --verbose

# Check using parallel processing
notifications-check --source-context 1 --target-context 2 --assembly-ids 100,101 --parallel

# Check with custom page size
notifications-check --source-context 1 --target-context 2 --assembly-ids 100,101 --page-size 200
```

## Behavior

### Processing Logic
- Fetches notification groups from both source and target environments
- Compares notification group definitions by name and type
- Reports missing, extra, or mismatched notification groups
- Outputs a CSV report of comparison results
- Shows progress bar in quiet mode
- Shows detailed API calls in verbose mode

### Comparison Criteria
- **Name Matching**: Compares notification group names
- **Type Matching**: Compares notification group types
- **Configuration**: Compares notification configurations
- **Assembly Association**: Verifies assembly associations

### Parallel Processing
- Uses up to 5 threads to process groups simultaneously
- Each thread has its own progress bar
- Significantly faster for large notification sets
- Work is divided equally between threads

## Use Cases

1. **Migration Verification**: Verify notification groups after migration
2. **Environment Synchronization**: Ensure notifications are synced between environments
3. **Configuration Audit**: Audit notification configurations
4. **Troubleshooting**: Identify notification-related issues

## Related Commands

- [asset-list-export](asset-list-export.md) - Export all assets
- [policy-list-export](policy-list-export.md) - Export all policies
- [show-config](show-config.md) - Display current configuration

## Tips

- Use `--parallel` for faster processing of large notification sets
- Use `--verbose` to see detailed API calls and responses
- Ensure both contexts exist in their respective environments
- Use this command after major migrations to verify notification integrity

## Error Handling

- Invalid context IDs are logged as errors
- Network errors are retried automatically with exponential backoff
- Authentication errors will fail immediately
- Missing assemblies are reported in the output

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

### Output CSV Format
- `assembly_id`: Assembly ID
- `notification_group_name`: Name of the notification group
- `source_status`: Status in source environment (present/missing)
- `target_status`: Status in target environment (present/missing)
- `comparison_result`: Result of comparison (match/mismatch/missing)

## Statistics

The command provides comprehensive statistics including:
- Total notification groups compared
- Matched notification groups
- Mismatched notification groups
- Missing notification groups
- Processing time
- Success/failure counts

## Context and Assembly Management

### Context IDs
- **Source Context**: Context ID in source environment
- **Target Context**: Context ID in target environment
- Both contexts must exist in their respective environments

### Assembly IDs
- Comma-separated list of assembly IDs to check
- Only notification groups associated with these assemblies are compared
- Use specific assembly IDs for targeted comparison
- Use multiple assembly IDs for broader comparison

## Best Practices

1. **Verify Contexts**: Ensure both contexts exist before running
2. **Use Specific Assemblies**: Focus on relevant assemblies
3. **Review Results**: Carefully review comparison results
4. **Follow Up**: Address any mismatches or missing notifications 