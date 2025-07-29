# help

Show help information for commands.

## Synopsis

```bash
help [<command>]
```

## Description

The `help` command displays help information for the ADOC Migration Toolkit. When used without arguments, it shows a comprehensive overview of all available commands. When used with a specific command name, it displays detailed help for that command.

## Arguments

- `command` (optional): Name of the command for detailed help

## Examples

```bash
# Show general help information
help

# Show detailed help for a specific command
help asset-profile-export

# Show help for another command
help policy-list-export
```

## Behavior

### General Help (no arguments)
When used without arguments, the `help` command displays:

#### Command Categories
- **📊 Segments Commands**: Export and import segment configurations
- **🔧 Asset Profile Commands**: Manage asset profiles and configurations
- **🔍 Asset Configuration Commands**: Handle asset configurations and lists
- **📋 Policy Commands**: Export and import policy definitions
- **🔧 Notification Commands**: Compare notification groups
- **🔧 VCS Commands**: Version control system operations
- **🌐 REST API Commands**: Direct API access
- **🛠️ Utility Commands**: Configuration and session management

#### Current Status
- **Output Directory**: Shows current output directory setting
- **Configuration Status**: Displays configuration information
- **Usage Tips**: Provides helpful tips for using the toolkit

### Command-Specific Help
When used with a command name, displays:

#### Detailed Information
- **Synopsis**: Command syntax and usage
- **Description**: What the command does
- **Arguments**: All available arguments and options
- **Examples**: Practical usage examples
- **Behavior**: How the command works
- **Use Cases**: When to use the command
- **Related Commands**: Related commands to consider
- **Tips**: Best practices and tips
- **Error Handling**: How errors are handled
- **Output**: What output to expect

## Use Cases

1. **Getting Started**: Learn about available commands
2. **Command Reference**: Look up specific command details
3. **Troubleshooting**: Understand command behavior and options
4. **Learning**: Discover new commands and features

## Related Commands

- [show-config](show-config.md) - Display current configuration
- [history](history.md) - Show command history
- [exit](exit.md) - Exit the interactive client

## Tips

- Use `help` without arguments to see all available commands
- Use `help <command>` for detailed information about specific commands
- The help system is context-aware and shows relevant information
- Help information is always up-to-date with the current version

## Output Format

### General Help Output
```
ADOC INTERACTIVE MIGRATION TOOLKIT - COMMAND HELP
==================================================

📁 Current Output Directory: /path/to/output
💡 Use 'set-output-dir <directory>' to change the output directory

📊 SEGMENTS COMMANDS:
  segments-export [<csv_file>] [--output-file <file>] [--quiet]
    Export segments from source environment to CSV file
  segments-import <csv_file> [--dry-run] [--quiet] [--verbose]
    Import segments to target environment from CSV file

[... more command categories ...]

💡 TIPS:
  • Use TAB key for command autocomplete
  • Use ↑/↓ arrow keys to navigate command history
  • Type 'help <command>' for detailed help on any command
  • Use --dry-run to preview changes before making them
  • Use --verbose to see detailed API request/response information
  • Set output directory once with set-output-dir to avoid specifying --output-file repeatedly
```

### Command-Specific Help Output
```
ADOC INTERACTIVE MIGRATION TOOLKIT - DETAILED HELP FOR: ASSET-PROFILE-EXPORT
============================================================================

📁 Current Output Directory: /path/to/output

asset-profile-export [<csv_file>] [--output-file <file>] [--quiet] [--verbose] [--parallel]
    Description: Export asset profiles from source environment to CSV file
    Arguments:
      csv_file: Path to CSV file with source-env and target-env mappings (optional)
      --output-file: Specify custom output file (optional)
      --quiet: Suppress console output, show only summary (default)
      --verbose: Show detailed output including headers and responses
      --parallel: Use parallel processing for faster export (max 5 threads)
    Examples:
      asset-profile-export
      asset-profile-export <output-dir>/asset-export/asset_uids.csv
      asset-profile-export uids.csv --output-file profiles.csv --verbose
      asset-profile-export --parallel
    Behavior:
      • If no CSV file specified, uses default from output directory
      • Default input: <output-dir>/asset-export/asset_uids.csv
      • Default output: <output-dir>/asset-import/asset-profiles-import-ready.csv
      • Reads source-env and target-env mappings from CSV file
      • Makes API calls to get asset profiles from source environment
      • Writes profile JSON data to output CSV file
      • Shows minimal output by default, use --verbose for detailed information
      • Parallel mode: Uses up to 5 threads to process assets simultaneously
      • Parallel mode: Each thread has its own progress bar
      • Parallel mode: Significantly faster for large asset sets

[... more detailed information ...]
```

## Navigation

- **Tab Completion**: Use TAB to autocomplete command names
- **Arrow Keys**: Use ↑/↓ to navigate command history
- **Command Numbers**: Use history numbers to re-execute commands
- **Exit**: Use `exit`, `quit`, or `q` to exit the interactive client 