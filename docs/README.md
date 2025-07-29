# ADOC Migration Toolkit - User Guides

Welcome to the comprehensive documentation for the ADOC Migration Toolkit. This guide provides detailed information about each command available in the interactive mode of the toolkit, designed for professional migration workflows between Acceldata environments.

## Overview

The ADOC Migration Toolkit is a professional-grade tool for migrating Acceldata policies and assets between environments. It provides comprehensive tools for migrating ADOC configurations from one environment to another, including interactive mode for guided workflows and automated migration processes.

## Quick Start

### Prerequisites

- Python 3.9 or higher
- Access to Acceldata environments (source and target)
- Proper authentication credentials

### Installation

1. **Clone the repository** (if not already done):
   ```bash
   git clone <repository-url>
   cd adoc-migration-toolkit
   ```

2. **Install in development mode**:
   ```bash
   pip install -e .
   ```

3. **Make scripts executable**:
   ```bash
   chmod +x bin/adoc-migration-toolkit
   chmod +x bin/sjson
   ```

### Starting the Migration Toolkit

#### Using the Binary Script (Recommended)

The easiest way to start the ADOC Migration Toolkit is using the provided binary script:

```bash
# Start interactive mode with default configuration
./bin/adoc-migration-toolkit interactive --env-file=config/config.env

# Start with custom configuration file
./bin/adoc-migration-toolkit interactive --env-file=/path/to/your/config.env

# Start with verbose logging
./bin/adoc-migration-toolkit interactive --env-file=config/config.env --verbose

# Start with custom log level
./bin/adoc-migration-toolkit interactive --env-file=config/config.env --log-level=DEBUG
```

#### Using Python Module

Alternatively, you can start the toolkit using the Python module:

```bash
# Start interactive mode
python -m adoc_migration_toolkit interactive --env-file=config/config.env

# Start with custom options
python -m adoc_migration_toolkit interactive --env-file=config/config.env --verbose
```

### Configuration Setup

1. **Copy the example configuration**:
   ```bash
   cp config/config.env.example config/config.env
   ```

2. **Edit the configuration file** with your environment details:
   ```bash
   # Acceldata Environment Configuration
   AD_HOST=https://${tenant}.your-acceldata-host.com
   
   # Source Environment
   AD_SOURCE_ACCESS_KEY=your_source_access_key
   AD_SOURCE_SECRET_KEY=your_source_secret_key
   AD_SOURCE_TENANT=your_source_tenant
   
   # Target Environment
   AD_TARGET_ACCESS_KEY=your_target_access_key
   AD_TARGET_SECRET_KEY=your_target_secret_key
   AD_TARGET_TENANT=your_target_tenant
   ```

3. **Verify your configuration**:
   ```bash
   ./bin/adoc-migration-toolkit --check-env
   ```

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
- [asset-list-export](asset-list-export.md) - Export all assets from source or target environment

### 📋 Policy Commands
- [policy-list-export](policy-list-export.md) - Export all policies from source environment

### 🔧 Notification Commands
- [notifications-check](notifications-check.md) - Compare notification groups between environments

### 🌐 REST API Commands
- [GET](api-get.md) - Make GET requests to API endpoints
- [PUT](api-put.md) - Make PUT requests to API endpoints

### 🛠️ Utility Commands
- [set-output-dir](set-output-dir.md) - Set global output directory
- [show-config](show-config.md) - Display current configuration
- [help](help.md) - Show help information
- [history](history.md) - Show command history
- [exit](exit.md) - Exit the interactive client

## Professional Workflow

### 1. Environment Setup
```bash
# Start the toolkit
./bin/adoc-migration-toolkit interactive --env-file=config/config.env

# Verify configuration
show-config

# Set output directory
set-output-dir /path/to/migration/project
```

### 2. Asset Discovery and Export
```bash
# Export all assets from source environment
asset-list-export --parallel

# Export asset profiles
asset-profile-export --parallel

# Export asset configurations
asset-config-export --parallel
```

### 3. Policy Export
```bash
# Export all policies
policy-list-export --parallel

# Export policy definitions by type
policy-export --type rule-types --parallel
```

### 4. Data Transformation
```bash
# Transform asset mappings
transform-and-merge --string-transform "PROD_DB":"DEV_DB"

# Format policy exports
policy-xfr --string-transform "PROD_URL":"DEV_URL"
```

### 5. Import to Target Environment
```bash
# Import asset profiles
asset-profile-import --dry-run --verbose

# Import asset configurations
asset-config-import --dry-run --verbose

# Import policies
policy-import *.zip --verbose
```

### 6. Verification and Profiling
```bash
# Check profiling status
profile-check --config assets.csv --parallel

# Trigger profiling
profile-run --config assets.csv --parallel

# Verify notifications
notifications-check --source-context 1 --target-context 2 --assembly-ids 100,101
```

## Quick Reference

### Basic Workflow
1. **Export**: Use export commands to extract data from source environment
2. **Transform**: Use transformation commands to adapt data for target environment
3. **Import**: Use import commands to load data into target environment
4. **Verify**: Use verification commands to ensure successful migration

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

## Professional Tips and Best Practices

### 1. **Start with a Dry Run**
Always use `--dry-run` to preview changes before making them:
```bash
asset-profile-import --dry-run --verbose
```

### 2. **Use Parallel Processing**
Add `--parallel` for faster operations on large datasets:
```bash
asset-list-export --parallel
```

### 3. **Set Output Directory Once**
Use `set-output-dir` to avoid specifying `--output-file` repeatedly:
```bash
set-output-dir /path/to/migration/project
```

### 4. **Check Configuration**
Use `show-config` to verify your environment settings:
```bash
show-config
```

### 5. **Use Command History**
Use `history` to see and reuse previous commands:
```bash
history
```

### 6. **Monitor Progress**
Use `--verbose` to see detailed API request/response information:
```bash
policy-list-export --verbose
```

## Troubleshooting

### Authentication Issues
- Check your access keys and secret keys in the config file
- Verify tenant names are correct
- Ensure network connectivity to Acceldata environments

### Network Issues
- Use `set-http-config` to adjust timeout and retry settings
- Check proxy configuration if behind corporate firewall
- Verify SSL certificate validity

### File Not Found
- Ensure the output directory is set correctly with `set-output-dir`
- Check file permissions on output directory
- Verify CSV files exist before import operations

### API Errors
- Use `--verbose` to see detailed API request/response information
- Check API endpoint availability
- Verify authentication credentials

## Support and Resources

### Interactive Help
- Use `help` in the interactive mode for general help
- Use `help <command>` for detailed help on specific commands

### Documentation
- Check the main [README.md](../README.md) for installation and setup instructions
- Review command-specific guides for detailed information
- Follow cross-references to discover related functionality

### Best Practices
- Always test with `--dry-run` before production operations
- Use parallel processing for large datasets
- Set output directory early in your workflow
- Monitor progress with verbose logging
- Keep command history for audit trails

## Advanced Features

### Parallel Processing
Most commands support parallel processing for improved performance:
```bash
# Export assets with parallel processing
asset-list-export --parallel --page-size 1000

# Import configurations with parallel processing
asset-config-import --parallel --verbose
```

### API Access
Direct API access for custom operations:
```bash
# Query assets
GET /catalog-server/api/assets?uid=123

# Update configurations
PUT /catalog-server/api/assets/456 {"key": "value"}
```

### Configuration Management
Dynamic configuration updates:
```bash
# Set log level
set-log-level DEBUG

# Configure HTTP settings
set-http-config --timeout 30 --retry 5

# View current configuration
show-config
```

This comprehensive documentation provides everything needed to effectively use the ADOC Migration Toolkit for professional migration workflows. 