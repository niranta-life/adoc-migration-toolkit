# Set Log Level Command

The `set-log-level` command changes the logging level dynamically during interactive sessions.

## Overview

This command allows you to adjust the logging verbosity in real-time without restarting the interactive session. It's useful for debugging, monitoring operations, and controlling output detail levels.

## Usage

```bash
set-log-level <level>
```

## Parameters

### Required Parameters

- `<level>`: Log level to set
  - `ERROR`: Only error messages
  - `WARNING`: Error and warning messages
  - `INFO`: Error, warning, and info messages (default)
  - `DEBUG`: All messages including debug information

## Examples

### Set Debug Level

```bash
set-log-level DEBUG
```

### Set Info Level

```bash
set-log-level INFO
```

### Set Warning Level

```bash
set-log-level WARNING
```

### Set Error Level

```bash
set-log-level ERROR
```

## Log Levels

### ERROR Level
- **Purpose**: Minimal logging for production use
- **Output**: Only critical error messages
- **Use Case**: When you want minimal output

### WARNING Level
- **Purpose**: Error and warning messages
- **Output**: Errors and warnings, no info messages
- **Use Case**: When you want to see issues but not routine info

### INFO Level
- **Purpose**: Standard logging level (default)
- **Output**: Errors, warnings, and informational messages
- **Use Case**: Normal operation and monitoring

### DEBUG Level
- **Purpose**: Maximum verbosity for debugging
- **Output**: All messages including detailed debug information
- **Use Case**: Troubleshooting and detailed analysis

## Dynamic Behavior

The log level change takes effect immediately for:
- **New Commands**: All subsequent commands use the new level
- **API Calls**: HTTP requests and responses
- **File Operations**: File read/write operations
- **Error Messages**: Error handling and reporting

## Use Cases

### Debugging Operations
When troubleshooting issues:
```bash
set-log-level DEBUG
# Run problematic command
asset-config-export --verbose
```

### Production Monitoring
For minimal output in production:
```bash
set-log-level ERROR
# Run commands with minimal logging
```

### Development Workflow
For detailed development work:
```bash
set-log-level DEBUG
# Work with detailed logging
```

### Performance Testing
For performance-focused operations:
```bash
set-log-level WARNING
# Run performance tests with reduced logging
```

## Tips

- **Start High**: Use DEBUG level when troubleshooting
- **Production**: Use ERROR or WARNING level in production
- **Development**: Use INFO or DEBUG level during development
- **Temporary**: You can change levels temporarily for specific operations
- **Reset**: Use INFO to return to default level

## Related Commands

- [set-http-config](set-http-config.md) - Configure HTTP settings
- [show-config](show-config.md) - Display current configuration
- [help](help.md) - Show command help 