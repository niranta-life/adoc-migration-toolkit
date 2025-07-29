# ADOC Migration Toolkit - User Guides

Welcome to the ADOC Migration Toolkit documentation. This guide provides detailed information about each command available in the interactive mode of the toolkit.

## Overview

The ADOC Migration Toolkit is a professional tool for migrating Acceldata policies and assets between environments. It provides comprehensive tools for migrating ADOC configurations from one environment to another, including interactive mode for guided workflows.

## Getting Started

1. **Installation**: Follow the installation instructions in the main [README.md](../README.md)
2. **Configuration**: Set up your environment configuration file
3. **Interactive Mode**: Start the interactive session with `adoc-migration-toolkit interactive --env-file=config.env`

## Command Categories

### 📊 Segments Commands
- [segments-export](segments-export.md) - Export segments from source environment
- [segments-import](segments-import.md) - Import segments to target environment

### 🔧 Asset Profile Commands
- [asset-profile-export](asset-profile-export.md) - Export asset profiles from source environment
- [asset-profile-import](asset-profile-import.md) - Import asset profiles to target environment
- [profile-check](profile-check.md) - Check assets that require profiling
- [profile-run](profile-run.md) - Trigger profiling for assets

### 🔍 Asset Configuration Commands
- [asset-config-export](asset-config-export.md) - Export asset configurations from source environment
- [asset-config-import](asset-config-import.md) - Import asset configurations to target environment
- [asset-list-export](asset-list-export.md) - Export all assets from source or target environment
- [asset-tag-import](asset-tag-import.md) - Import tags for assets from CSV file

### 📋 Policy Commands
- [policy-list-export](policy-list-export.md) - Export all policies from source environment
- [policy-export](policy-export.md) - Export policy definitions by categories
- [policy-import](policy-import.md) - Import policy definitions from ZIP files
- [rule-tag-export](rule-tag-export.md) - Export rule tags for all policies
- [policy-xfr](policy-xfr.md) - Format policy export files with string transformations
- [transform-and-merge](transform-and-merge.md) - Transform and merge asset CSV files

### 🔧 Notification Commands
- [notifications-check](notifications-check.md) - Compare notification groups between environments

### 🔧 VCS Commands
- [vcs-config](vcs-config.md) - Configure enterprise VCS settings
- [vcs-init](vcs-init.md) - Initialize a VCS repository
- [vcs-pull](vcs-pull.md) - Pull updates from configured repository
- [vcs-push](vcs-push.md) - Push changes to remote repository

### 🌐 REST API Commands
- [GET](api-get.md) - Make GET requests to API endpoints
- [PUT](api-put.md) - Make PUT requests to API endpoints

### 🛠️ Utility Commands
- [set-output-dir](set-output-dir.md) - Set global output directory
- [set-log-level](set-log-level.md) - Change log level dynamically
- [set-http-config](set-http-config.md) - Configure HTTP settings
- [show-config](show-config.md) - Display current configuration
- [help](help.md) - Show help information
- [history](history.md) - Show command history
- [exit](exit.md) - Exit the interactive client

## Quick Reference

### Basic Workflow
1. **Export**: Use export commands to extract data from source environment
2. **Transform**: Use transformation commands to adapt data for target environment
3. **Import**: Use import commands to load data into target environment

### Common Options
- `--quiet`: Suppress console output, show only summary
- `--verbose`: Show detailed output including API calls and responses
- `--parallel`: Use parallel processing for faster operations
- `--dry-run`: Preview changes without making API calls

### File Structure
The toolkit uses a structured output directory:
```
<output-dir>/
├── asset-export/          # Asset export files
├── asset-import/          # Asset import files
├── policy-export/         # Policy export files
├── policy-import/         # Policy import files
└── logs/                 # Log files
```

## Tips and Best Practices

1. **Start with a dry run**: Use `--dry-run` to preview changes before making them
2. **Use parallel processing**: Add `--parallel` for faster operations on large datasets
3. **Set output directory**: Use `set-output-dir` to avoid specifying `--output-file` repeatedly
4. **Check configuration**: Use `show-config` to verify your environment settings
5. **Use command history**: Use `history` to see and reuse previous commands

## Troubleshooting

- **Authentication issues**: Check your access keys and secret keys in the config file
- **Network issues**: Use `set-http-config` to adjust timeout and retry settings
- **File not found**: Ensure the output directory is set correctly with `set-output-dir`
- **API errors**: Use `--verbose` to see detailed API request/response information

## Support

For additional help:
- Use `help` in the interactive mode for general help
- Use `help <command>` for detailed help on specific commands
- Check the main [README.md](../README.md) for installation and setup instructions 