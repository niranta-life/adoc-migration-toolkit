# exit

Exit the interactive client.

## Synopsis

```bash
exit
quit
q
```

## Description

The `exit` command (along with `quit` and `q`) exits the ADOC Migration Toolkit interactive client. This command gracefully terminates the interactive session, saves command history, and returns control to the shell.

## Arguments

- None (no arguments required)

## Examples

```bash
# Exit the interactive client
exit

# Alternative exit commands
quit
q
```

## Behavior

### Session Cleanup
- Saves command history to file
- Logs session exit event
- Cleans up temporary resources
- Closes API connections gracefully

### History Preservation
- Command history is automatically saved
- History persists across sessions
- Available in next interactive session
- Can be accessed with `history` command

### Logging
- Logs session exit event with timestamp
- Records session duration
- Captures user and host information
- Maintains audit trail

## Use Cases

1. **Session Completion**: Exit after completing migration tasks
2. **Configuration Changes**: Exit to apply configuration changes
3. **Troubleshooting**: Exit to restart with different settings
4. **Workflow Management**: Exit to switch between different projects

## Related Commands

- [help](help.md) - Show help information
- [history](history.md) - Show command history
- [show-config](show-config.md) - Display current configuration

## Tips

- Use `exit` to gracefully end your session
- Command history is automatically saved
- You can resume work in a new session
- All configuration changes are preserved

## Output

### Normal Exit
```
✅ Session completed successfully
📝 Command history saved
👋 Goodbye!
```

### Exit with Warnings
```
⚠️  Active operations detected
📝 Command history saved
👋 Goodbye!
```

## Session Management

### History File
- Location: `~/.adoc_history`
- Format: Plain text with timestamps
- Persistence: Survives across sessions
- Size: Limited to prevent excessive growth

### Session Logging
- Event: Session exit
- Timestamp: Current date and time
- Duration: Session length
- User: Current user information
- Host: System hostname

## Alternative Exit Methods

### Keyboard Shortcuts
- `Ctrl+C`: Interrupt current operation and exit
- `Ctrl+D`: End of input (EOF) - exits gracefully

### Programmatic Exit
- `sys.exit()`: Force exit (not recommended)
- `os._exit()`: Immediate exit (not recommended)

## Best Practices

1. **Graceful Exit**: Always use `exit`, `quit`, or `q`
2. **Save Work**: Ensure important operations are completed
3. **Check Status**: Verify no active operations before exiting
4. **Review History**: Use `history` to review session commands

## Error Handling

- Active operations are detected and reported
- Unfinished tasks are logged as warnings
- Network connections are closed properly
- Temporary files are cleaned up

## Session Recovery

### Restarting
- Start new session with `adoc-migration-toolkit interactive`
- Command history is automatically loaded
- Configuration settings are preserved
- Output directory settings are maintained

### History Access
- Use `history` command to see previous commands
- Use command numbers to re-execute commands
- History is searchable and navigable 