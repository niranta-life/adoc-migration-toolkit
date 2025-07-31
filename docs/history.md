# history

Show the last 25 commands with numbers.

## Synopsis

```bash
history
```

## Description

The `history` command displays the last 25 commands executed in the current interactive session, along with their command numbers. This allows you to review recent commands and re-execute them by entering their number.

## Arguments

- None (no arguments required)

## Examples

```bash
# Show command history
history
```

## Behavior

### Display Format
- Shows the last 25 commands with numbered entries
- Latest commands appear first (highest numbers)
- Long commands are truncated for display
- Commands are numbered sequentially

### Command Numbers
- Each command has a unique number
- Numbers are sequential and increment with each command
- You can re-execute commands by entering their number
- Numbers persist for the current session

### Navigation
- Works alongside ↑/↓ arrow key navigation
- Provides alternative to arrow key navigation
- Useful for accessing commands from earlier in session
- Numbers are stable during the session

## Use Cases

1. **Command Review**: Review recent commands and their sequence
2. **Command Re-execution**: Re-run previous commands by number
3. **Workflow Tracking**: Track the sequence of operations performed
4. **Troubleshooting**: Review commands that led to issues

## Related Commands

- [help](help.md) - Show help information
- [exit](exit.md) - Exit the interactive client
- [show-config](show-config.md) - Display current configuration

## Tips

- Use command numbers to quickly re-execute commands
- Long commands are truncated but still functional
- History is session-specific and doesn't persist across sessions
- Use arrow keys for immediate command access

## Output Format

### Sample Output
```
Command History (last 25):
25: asset-list-export --parallel
24: set-output-dir /path/to/output
23: policy-list-export --quiet
22: asset-profile-export --verbose
21: help asset-config-export
20: show-config
19: GET /catalog-server/api/assets?uid=123
18: segments-export --quiet
17: policy-export --type rule-types
16: asset-config-export --parallel
15: set-log-level DEBUG
14: help
13: asset-profile-import --dry-run
12: policy-import *.zip
11: vcs-config --vcs-type git
10: notifications-check --source-context 1 --target-context 2
9: profile-check --config assets.csv
8: asset-tag-import --verbose
7: transform-and-merge --string-transform "PROD":"DEV"
6: rule-tag-export --parallel
5: policy-xfr --string-transform "old":"new"
4: asset-config-import --dry-run
3: segments-import segments.csv
2: set-http-config --timeout 30
1: asset-profile-export --parallel --verbose
```

### Command Re-execution
To re-execute a command, simply enter its number:
```
Enter command number: 25
```

## Navigation Features

### Arrow Key Navigation
- **↑**: Navigate to previous command
- **↓**: Navigate to next command
- **Ctrl+R**: Reverse search through history
- **Tab**: Command completion

### Number-based Navigation
- Enter command number to re-execute
- Numbers are stable during session
- Useful for accessing older commands
- Alternative to arrow key navigation

## History Management

### Session History
- Limited to current session
- Automatically managed
- Commands are numbered sequentially
- Long commands are truncated for display

### History File
- Command history is saved to `~/.adoc_history`
- Persists across sessions
- Can be accessed in future sessions
- File size is managed automatically

## Best Practices

1. **Review History**: Use `history` to review recent operations
2. **Command Numbers**: Use numbers for quick command re-execution
3. **Session Management**: History is session-specific
4. **Navigation**: Combine with arrow keys for efficient navigation

## Error Handling

- Invalid command numbers are ignored
- History display errors are handled gracefully
- Missing history file is handled automatically
- Long commands are safely truncated

## Session Persistence

### History File
- Location: `~/.adoc_history`
- Format: Plain text with timestamps
- Persistence: Survives across sessions
- Size: Limited to prevent excessive growth

### Session Recovery
- History is automatically loaded on session start
- Previous session commands are available
- Numbers are reset for new session
- Old history is preserved in file 