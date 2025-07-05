# ADOC Migration Toolkit Guide

The ADOC Migration Toolkit simplifies moving ADOC configurations between environments. 
It lets you export policies from one environment, adjust them for a new environment, 
and import them smoothly. The toolkit also handles segment management and asset-level 
configuration migration. In the future, it will support pipelines and other ADOC features 
to improve migration between environments. Its interactive interfaces make migration 
workflows clear and efficient, with a guided migration feature that allows you to 
start multiple migrations, pause, and resume them independently.

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Configuration](#configuration)
- [Command Line Interface](#command-line-interface)
- [Interactive Mode](#interactive-mode)
- [Guided Migration](#guided-migration)
- [Asset Management](#asset-management)
- [Policy Management](#policy-management)
- [Segments Management](#segments-management)
- [Utility Commands](#utility-commands)
- [File Structure](#file-structure)
- [Examples](#examples)
- [Troubleshooting](#troubleshooting)

## Overview

The ADOC Migration Toolkit is designed to facilitate the migration of policies, 
assets, and configurations between Acceldata environments. It provides:

- **Interactive CLI**: Full-featured interactive mode with autocomplete and command history
- **Guided Migration**: Step-by-step migration workflows with state management
- **Asset Management**: Export and import asset profiles and configurations
- **Policy Management**: Comprehensive policy export/import capabilities
- **Segments Management**: Specialized handling for segmented Spark assets
- **File Processing**: JSON and ZIP file processing with string replacement

## Installation

### Prerequisites

- Python 3.9 or higher
- Access to Acceldata environments (source and target)

### Install from Source

```bash
# Clone the repository
git clone <repository-url>
cd adoc-export-import

# Install in development mode
pip install -e .

# Make scripts executable
chmod +x bin/adoc-migration-toolkit
chmod +x bin/sjson
```

### Using the Migration Toolkit Script

```bash
# Use the provided script for easy access
./bin/adoc-migration-toolkit

# With custom config
./bin/adoc-migration-toolkit -c /path/to/config.env

# Check environment configuration
./bin/adoc-migration-toolkit --check-env
```

## Configuration

### Environment Setup

1. Copy the example configuration:
   ```bash
   cp config/config.env.example config/config.env
   ```

2. Edit `config/config.env` with your environment details:
   ```bash
   # Acceldata Environment Configuration
   AD_HOST=https://your-acceldata-host.com
   
   # Source Environment
   AD_SOURCE_ACCESS_KEY=your_source_access_key
   AD_SOURCE_SECRET_KEY=your_source_secret_key
   AD_SOURCE_TENANT=your_source_tenant
   
   # Target Environment
   AD_TARGET_ACCESS_KEY=your_target_access_key
   AD_TARGET_SECRET_KEY=your_target_secret_key
   AD_TARGET_TENANT=your_target_tenant
   ```

### Configuration File Details

The `config.env` file contains all the necessary credentials and connection information for both source and target Acceldata environments. This file is critical for authentication and API access.

#### Required Environment Variables

**`AD_HOST`**
- **Purpose**: The base URL of your Acceldata environment
- **Format**: Full HTTPS URL (e.g., `https://your-instance.acceldata.app`)
- **Required**: Yes
- **Example**: `https://se-demo.acceldata.app`
- **Notes**: 
  - Must include the protocol (https://)
  - Should not include trailing slashes
  - Must be accessible from your local machine
  - Used for both source and target environments

**Source Environment Variables**

**`AD_SOURCE_ACCESS_KEY`**
- **Purpose**: Access key for authenticating with the source environment
- **Format**: String (typically alphanumeric)
- **Required**: Yes
- **Example**: `ak_1234567890abcdef`
- **Notes**:
  - Used for all export operations (policies, assets, profiles, configurations)
  - Must have read permissions on the source environment
  - Should be kept secure and not shared

**`AD_SOURCE_SECRET_KEY`**
- **Purpose**: Secret key for authenticating with the source environment
- **Format**: String (typically alphanumeric)
- **Required**: Yes
- **Example**: `sk_abcdef1234567890`
- **Notes**:
  - Used in combination with access key for authentication
  - Must have read permissions on the source environment
  - Highly sensitive - should be kept secure
  - Never commit to version control

**`AD_SOURCE_TENANT`**
- **Purpose**: Tenant identifier for the source environment
- **Format**: String (typically lowercase with hyphens)
- **Required**: Yes
- **Example**: `my-company-dev`
- **Notes**:
  - Identifies the specific tenant in multi-tenant environments
  - Used for API endpoint routing
  - Must match the tenant configuration in Acceldata

**Target Environment Variables**

**`AD_TARGET_ACCESS_KEY`**
- **Purpose**: Access key for authenticating with the target environment
- **Format**: String (typically alphanumeric)
- **Required**: Yes (for import operations)
- **Example**: `ak_0987654321fedcba`
- **Notes**:
  - Used for all import operations (policies, assets, profiles, configurations)
  - Must have write permissions on the target environment
  - Should be kept secure and not shared

**`AD_TARGET_SECRET_KEY`**
- **Purpose**: Secret key for authenticating with the target environment
- **Format**: String (typically alphanumeric)
- **Required**: Yes (for import operations)
- **Example**: `sk_fedcba0987654321`
- **Notes**:
  - Used in combination with access key for authentication
  - Must have write permissions on the target environment
  - Highly sensitive - should be kept secure
  - Never commit to version control

**`AD_TARGET_TENANT`**
- **Purpose**: Tenant identifier for the target environment
- **Format**: String (typically lowercase with hyphens)
- **Required**: Yes (for import operations)
- **Example**: `my-company-uat`
- **Notes**:
  - Identifies the specific tenant in multi-tenant environments
  - Used for API endpoint routing
  - Must match the tenant configuration in Acceldata

#### Configuration Scenarios

**Single Environment (Export Only)**
If you only need to export data from a source environment:
```bash
# Required for export operations
AD_HOST=https://source.acceldata.app
AD_SOURCE_ACCESS_KEY=your_source_access_key
AD_SOURCE_SECRET_KEY=your_source_secret_key
AD_SOURCE_TENANT=your_source_tenant

# Optional - can be empty or omitted
AD_TARGET_ACCESS_KEY=
AD_TARGET_SECRET_KEY=
AD_TARGET_TENANT=
```

**Dual Environment (Full Migration)**
For complete migration workflows:
```bash
# Source environment (for export)
AD_HOST=https://source.acceldata.app
AD_SOURCE_ACCESS_KEY=your_source_access_key
AD_SOURCE_SECRET_KEY=your_source_secret_key
AD_SOURCE_TENANT=your_source_tenant

# Target environment (for import)
AD_TARGET_ACCESS_KEY=your_target_access_key
AD_TARGET_SECRET_KEY=your_target_secret_key
AD_TARGET_TENANT=your_target_tenant
```

**Different Hosts**
If source and target are on different hosts:
```bash
# Source environment
AD_HOST=https://source.acceldata.app
AD_SOURCE_ACCESS_KEY=your_source_access_key
AD_SOURCE_SECRET_KEY=your_source_secret_key
AD_SOURCE_TENANT=your_source_tenant

# Target environment (different host)
AD_TARGET_HOST=https://target.acceldata.app  # Note: This would require code modification
AD_TARGET_ACCESS_KEY=your_target_access_key
AD_TARGET_SECRET_KEY=your_target_secret_key
AD_TARGET_TENANT=your_target_tenant
```

#### Security Best Practices

1. **File Permissions**
   ```bash
   # Set restrictive permissions on config file
   chmod 600 config/config.env
   ```

2. **Environment Variable Usage**
   ```bash
   # Instead of hardcoding, use environment variables
   export AD_SOURCE_ACCESS_KEY="your_key"
   export AD_SOURCE_SECRET_KEY="your_secret"
   ```

3. **Version Control**
   - Never commit `config.env` to version control
   - Use `config.env.example` as a template
   - Add `config.env` to `.gitignore`

4. **Key Rotation**
   - Regularly rotate access and secret keys
   - Use temporary keys for testing
   - Monitor key usage and permissions

#### Troubleshooting Configuration Issues

**Common Issues:**

1. **Authentication Errors**
   - Verify access and secret keys are correct
   - Check tenant names match exactly
   - Ensure keys have appropriate permissions

2. **Connection Errors**
   - Verify `AD_HOST` is accessible from your machine
   - Check network connectivity and firewalls
   - Ensure HTTPS is properly configured

3. **Permission Errors**
   - Source keys need read permissions
   - Target keys need write permissions
   - Verify tenant access and permissions

4. **Validation Errors**
   ```bash
   # Use the validation command
   ./bin/adoc-migration-toolkit --check-env
   ```

#### Configuration Validation

The toolkit provides built-in configuration validation:

```bash
# Validate configuration without starting the tool
./bin/adoc-migration-toolkit --check-env

# Expected output for valid configuration:
# [SUCCESS] Configuration file validated: config/config.env
# [SUCCESS] Python module 'adoc_export_import' found.
# [SUCCESS] Environment configuration is valid.
```

**Validation Checks:**
- File existence and readability
- Required environment variables present
- Variable format validation
- Basic connectivity testing
- Authentication verification

### Configuration Validation

```bash
# Validate configuration without starting the tool
./bin/adoc-migration-toolkit --check-env
```

## Command Line Interface

### Main Commands

```bash
# Asset Export - Export asset details from CSV UIDs
python -m adoc_export_import asset-export --csv-file data/uids.csv --env-file config.env

# Interactive Mode - Full-featured interactive client
python -m adoc_export_import interactive --env-file config.env
```

## Interactive Mode

Start the interactive client:

```bash
python -m adoc_export_import interactive --env-file config.env
```

### Interactive Features

The interactive mode provides a powerful command-line interface with advanced features for managing complex migration workflows:

- **Command Autocomplete**: Use TAB key for command and path completion - suggests available commands, file paths, and API endpoints
- **Command History**: Use ↑/↓ arrow keys to navigate through previous commands - persists across sessions
- **Session Management**: Commands and settings persist across multiple interactive sessions
- **Output Directory Management**: Set global output directory for all operations to avoid repeated path specifications
- **Real-time API Testing**: Test API endpoints directly with authentication switching
- **Batch Operations**: Execute complex workflows with multiple commands in sequence
- **Error Recovery**: Comprehensive error handling with detailed logging and recovery options

### REST API Commands

The interactive mode allows direct REST API calls to the Acceldata environment with full authentication control and response formatting:

```bash
# Basic API calls
GET /catalog-server/api/assets?uid=123
PUT /catalog-server/api/assets {"key": "value"}

# With authentication targeting
GET /catalog-server/api/assets?uid=123 --target-auth --target-tenant
```

**Purpose:**
- Test API endpoints directly without external tools
- Debug authentication and permission issues
- Validate data structures and responses
- Perform ad-hoc data operations

**Features:**
- **Authentication Switching**: Use `--target-auth` and `--target-tenant` to switch between source and target environments
- **JSON Payload Support**: PUT requests accept JSON payloads for data modification
- **Formatted Responses**: All responses are automatically formatted for readability
- **Error Handling**: Comprehensive error reporting with HTTP status codes
- **Session Persistence**: Authentication tokens are cached for efficient API calls

## Asset Management

### Asset Profile Commands

Asset profiles contain metadata and configuration settings that define how assets behave in the Acceldata environment. These commands handle the export and import of asset profile configurations.

```bash
# Export asset profiles from source environment
asset-profile-export [csv_file] [--output-file file] [--quiet] [--verbose]

# Import asset profiles to target environment
asset-profile-import [csv_file] [--dry-run] [--quiet] [--verbose]
```

**Purpose:**
- **Export**: Extract asset profile configurations from source environment for migration
- **Import**: Apply asset profile configurations to target environment
- **Validation**: Ensure profile configurations are compatible and valid
- **Backup**: Create backups of asset configurations before migration

**What Asset Profiles Include:**
- Asset metadata and properties
- Configuration settings and parameters
- Connection information and credentials
- Performance tuning parameters
- Custom attributes and tags

**Examples:**
```bash
# Export with default files
asset-profile-export

# Export with custom files
asset-profile-export data/uids.csv --output-file profiles.csv --verbose

# Import with dry-run
asset-profile-import data/profiles.csv --dry-run --verbose
```

**Important Notes:**
- Export always uses source environment authentication
- Import always uses target environment authentication
- Use `--dry-run` to preview changes before applying
- Profile configurations may contain environment-specific settings that need validation

### Asset Configuration Commands

Asset configurations contain the detailed technical settings and parameters that define how assets connect to and interact with data sources. These commands handle the extraction and management of detailed asset configurations.

```bash
# Export asset configurations from source
asset-config-export <csv_file> [--output-file file] [--quiet] [--verbose]

# Export all assets from source environment
asset-list-export [--quiet] [--verbose]
```

**Purpose:**
- **Configuration Export**: Extract detailed technical configurations for migration
- **Asset Discovery**: Identify all assets in the source environment
- **Configuration Analysis**: Analyze asset configurations for compatibility
- **Migration Planning**: Prepare detailed asset inventories for migration

**What Asset Configurations Include:**
- Connection strings and parameters
- Authentication credentials and methods
- Data source configurations
- Query and transformation settings
- Performance and tuning parameters
- Custom configuration properties

**Examples:**
```bash
# Export configurations
asset-config-export data/asset_uids.csv --verbose

# Export all assets
asset-list-export --quiet
```

**Technical Details:**
- Uses `/catalog-server/api/assets/discover` endpoint with pagination
- Retrieves all pages with configurable batch sizes
- Sorts output by UID and ID for consistency
- Provides comprehensive statistics upon completion
- Handles large asset inventories efficiently

## Policy Management

### Policy Export Commands

Policy management is a core component of Acceldata environments. These commands handle the comprehensive export and categorization of policies for migration and analysis purposes.

```bash
# Export all policies from source environment
policy-list-export [--quiet] [--verbose]

# Export policy definitions by categories
policy-export [--type export_type] [--filter filter_value] [--quiet] [--verbose] [--batch-size size]
```

**Purpose:**
- **Policy Inventory**: Create comprehensive lists of all policies
- **Categorized Export**: Export policies by specific criteria for targeted migration
- **Batch Processing**: Handle large policy sets efficiently
- **Migration Preparation**: Prepare policies for environment transfer

**Export Types and Use Cases:**
- `rule-types`: Export by rule type (e.g., data quality, governance) - Useful for migrating specific policy categories
- `engine-types`: Export by engine type (e.g., JDBC_URL, SPARK) - Essential for engine-specific migrations
- `assemblies`: Export by assembly (e.g., production-db, test-env) - Critical for environment-specific migrations
- `source-types`: Export by source type (e.g., PostgreSQL, MySQL) - Important for database-specific migrations

**Examples:**
```bash
# Export all policies
policy-list-export --quiet

# Export by rule types
policy-export --type rule-types --batch-size 100

# Export specific engine type
policy-export --type engine-types --filter JDBC_URL

# Export specific assembly
policy-export --type assemblies --filter production-db
```

**Technical Details:**
- Uses `/catalog-server/api/rules` endpoint with pagination
- Supports configurable batch sizes for large policy sets
- Generates ZIP files with policy definitions
- Creates timestamped files for version control
- Provides detailed progress reporting and statistics

### Policy Import Commands

Policy import is the final step in the migration process, where processed policy definitions are applied to the target environment. This command handles the bulk import of policy configurations with comprehensive validation and conflict resolution.

```bash
# Import policy definitions from ZIP files
policy-import <file_or_pattern> [--quiet] [--verbose]
```

**Purpose:**
- **Bulk Import**: Import multiple policy definitions efficiently
- **Conflict Resolution**: Handle existing policies and assemblies gracefully
- **Validation**: Ensure imported policies are compatible with target environment
- **Audit Trail**: Track imported policies and their UUIDs for verification

**Import Process:**
- Uploads ZIP files to `/catalog-server/api/rules/import/policy-definitions/upload-config`
- Uses target environment authentication
- Validates file integrity and content
- Reports conflicts and resolution status
- Provides comprehensive import statistics

**Examples:**
```bash
# Import all ZIP files
policy-import *.zip

# Import specific pattern
policy-import data-quality-*.zip --verbose

# Import specific file
policy-import /path/to/policy.zip
```

**Important Considerations:**
- Always uses target environment authentication
- Supports glob patterns for batch processing
- Validates file existence and readability
- Reports detailed conflicts (assemblies, policies, SQL views, visual views)
- Tracks UUIDs of successfully imported policies
- Provides rollback information for troubleshooting

### Policy Transformer

The policy transformer  is a critical component that processes policy export ZIP files to prepare them for import into target environments. It handles environment string translation, asset extraction, and file organization.

```bash
# Format policy export files with string replacement
policy-xfr [--input input_dir] --source-env-string <source> --target-env-string <target> [options]
```

**Purpose:**
- **Environment Translation**: Replace source environment strings with target environment strings
- **Asset Extraction**: Identify and extract asset information for specialized handling
- **File Organization**: Create structured output directories for import workflows
- **Validation**: Ensure processed files are ready for target environment import

**Processing Pipeline:**
1. **File Discovery**: Scans input directory for ZIP and JSON files
2. **Content Extraction**: Extracts and processes JSON content from ZIP archives
3. **String Replacement**: Performs environment string translation throughout all files
4. **Asset Analysis**: Identifies and categorizes assets for specialized handling
5. **Output Generation**: Creates import-ready files and asset management CSVs

**Examples:**
```bash
# Basic string replacement
policy-xfr --source-env-string "PROD_DB" --target-env-string "DEV_DB"

# With custom input directory
policy-xfr --input data/samples --source-env-string "old" --target-env-string "new" --verbose
```

**Generated Outputs:**
- `*_import_ready/` directories with translated ZIP files
- `segmented_spark_uids.csv` - UIDs of segmented SPARK assets requiring special handling
- `asset_uids.csv` - All asset UIDs for profile and configuration management
- Processing statistics and validation reports

**Critical Requirements:**
- Environment strings must match exactly (case-sensitive)
- Source environment strings must exist in the policy files
- Target environment strings should be valid for the target environment
- Input directory should contain valid policy export ZIP files

## Segments Management

### Segments Commands

Segments are specialized configurations for data partitioning and processing optimization in Acceldata. These commands handle the export and import of segment configurations, which are critical for SPARK assets but handled differently for JDBC_SQL assets.

```bash
# Export segments from source environment
segments-export [csv_file] [--output-file file] [--quiet]

# Import segments to target environment
segments-import [csv_file] [--dry-run] [--quiet] [--verbose]
```

**Purpose:**
- **SPARK Asset Support**: Export and import segment configurations for SPARK assets
- **Performance Optimization**: Maintain data partitioning and processing configurations
- **Migration Completeness**: Ensure all asset configurations are properly migrated
- **Validation**: Verify segment configurations are compatible with target environment

**Why Segments Matter:**
- **SPARK Assets**: Segmented Spark configurations are NOT included in standard policy imports and require separate handling
- **JDBC_SQL Assets**: Segment configurations are already available in standard import capabilities
- **Performance Impact**: Segments affect data processing performance and resource utilization
- **Data Partitioning**: Segments define how data is partitioned for parallel processing

**Examples:**
```bash
# Export segments
segments-export

# Import segments with dry-run
segments-import data/segments.csv --dry-run --verbose
```

**Behavior and Requirements:**
- **Export**: Always exports from source environment using source authentication
- **Import**: Always imports to target environment using target authentication
- **SPARK Assets**: Required for segmented Spark configurations - must be imported separately
- **JDBC_SQL Assets**: Already available in standard import - no additional configuration needed
- **Validation**: Only processes assets that have valid segments configuration
- **Error Handling**: Skips assets without segments (logged as info)

**Technical Details:**
- Reads UIDs from CSV files generated by the formatter
- Makes API calls to extract segment configurations
- Creates new segments in target environment (removes existing IDs)
- Supports both SPARK and JDBC_SQL engine types
- Validates CSV format and JSON content before processing

## Utility Commands

### Output Directory Management

The output directory management system provides centralized control over where all export and import files are stored, eliminating the need to specify file paths repeatedly across multiple commands.

```bash
# Set global output directory for all export commands
set-output-dir <directory>
```

**Purpose:**
- **Centralized Configuration**: Set a single output directory for all operations
- **Path Simplification**: Eliminate repeated `--output-file` specifications
- **Organization**: Maintain consistent file organization across migration workflows
- **Persistence**: Settings persist across multiple interactive sessions

**Benefits:**
- **Consistency**: All commands use the same output structure
- **Efficiency**: No need to specify file paths for each command
- **Organization**: Automatic creation of categorized subdirectories
- **Persistence**: Settings saved to `~/.adoc_migration_config.json`

**Examples:**
```bash
set-output-dir /path/to/my/output
set-output-dir data/custom_output
```

**Directory Structure Created:**
```
<output-directory>/
├── asset-export/          # Asset export files
├── asset-import/          # Asset import files
├── policy-export/         # Policy export files
└── policy-import/         # Policy import files
```

**Features:**
- **Automatic Creation**: Creates directory if it doesn't exist
- **Permission Validation**: Checks write permissions before setting
- **Path Validation**: Ensures directory path is valid and accessible
- **Configuration Persistence**: Settings saved across sessions
- **Override Capability**: Can be changed anytime with another command

### Session Management

Session management commands provide essential tools for navigating and controlling the interactive environment, including help, history, and session control.

```bash
# Show command help
help

# Show command history
history

# Exit interactive client
exit, quit, q
```

**Help Command (`help`):**
- **Purpose**: Display comprehensive help information for all available commands
- **Content**: Detailed command descriptions, examples, and usage patterns
- **Format**: Organized by command categories with clear explanations
- **Accessibility**: Available anytime during interactive sessions

**History Command (`history`):**
- **Purpose**: Display the last 25 commands with numbered entries
- **Features**: 
  - Shows command numbers for easy reference
  - Latest commands appear first (highest numbers)
  - Long commands are truncated for display
  - Enter a number to execute that command
  - Works alongside ↑/↓ arrow key navigation
- **Persistence**: Command history persists across sessions

**Exit Commands (`exit`, `quit`, `q`):**
- **Purpose**: Safely exit the interactive client
- **Behavior**: 
  - Saves current session state
  - Cleans up resources
  - Preserves command history
  - Maintains output directory settings
- **Multiple Options**: Three different commands for the same action

**Session Features:**
- **State Persistence**: Settings and history maintained across sessions
- **Graceful Exit**: Clean shutdown with state preservation
- **Resource Management**: Automatic cleanup of temporary resources
- **Configuration Retention**: Output directory and other settings preserved

### JSON Processing

The JSON processing utility provides a simple way to format and view JSON files in a human-readable format, which is essential for analyzing policy configurations and API responses.

```bash
# Pretty print JSON files
./bin/sjson <file.json>
```

**Purpose:**
- **JSON Formatting**: Convert compact JSON to human-readable format
- **Configuration Analysis**: Easily view and analyze policy configurations
- **Debug Support**: Format API responses for troubleshooting
- **Documentation**: Create readable versions of configuration files

**Use Cases:**
- **Policy Analysis**: Format policy export files for review
- **API Debugging**: Format API responses for troubleshooting
- **Configuration Review**: Read and analyze asset configurations
- **Migration Planning**: Review configuration details before migration

**Features:**
- **Simple Interface**: Single command with file path argument
- **Standard Formatting**: Uses Python's json.tool for consistent formatting
- **Error Handling**: Graceful handling of invalid JSON files
- **Cross-Platform**: Works on Unix, macOS, and Windows systems

**Example Usage:**
```bash
# Format a policy configuration file
./bin/sjson policy-config.json

# Format an API response file
./bin/sjson api-response.json

# Pipe JSON content directly
echo '{"key":"value"}' | ./bin/sjson
```

**Technical Details:**
- Uses Python's built-in `json.tool` module
- Maintains JSON validity and structure
- Handles large JSON files efficiently
- Provides consistent indentation and formatting

## File Structure

### Default Output Directory Structure

```
adoc-migration-toolkit-YYYYMMDDHHMM/
├── asset-export/
│   ├── asset_uids.csv
│   └── asset-all-export.csv
├── asset-import/
│   ├── asset-profiles-import-ready.csv
│   └── asset-configs-import-ready.csv
├── policy-export/
│   ├── policies-all-export.csv
│   ├── segmented_spark_uids.csv
│   └── *.zip (policy definition files)
└── policy-import/
    └── segments_output.csv
```

### Configuration Files

- `config/config.env`: Environment configuration
- `~/.adoc_migration_config.json`: Persistent settings
- `~/.adoc-migrations/`: Migration state files

## Examples

### Complete Migration Workflow

```bash
# 1. Start interactive client
python -m adoc_export_import interactive --env-file config.env

# 2. Set output directory
set-output-dir /path/to/migration

# 3. Export all policies
policy-list-export --quiet

# 4. Export policies by categories
policy-export --type rule-types
policy-export --type engine-types --filter JDBC_URL

# 5. Process with formatter
policy-xfr --source-env-string "PROD" --target-env-string "DEV"

# 6. Export asset profiles
asset-profile-export

# 7. Import asset profiles
asset-profile-import --dry-run --verbose

# 8. Handle segments
segments-export
segments-import --dry-run --verbose

# 9. Import policies
policy-import *.zip --verbose
asset-profile-import
asset-config-import
segments-import 
```

### Guided Migration Example

```bash
# Start guided migration
guided-migration dev-to-uat
guided-migration uat-to-prod

# Follow the step-by-step prompts
# Migration can be paused and resumed at any time

# Resume later
resume-migration dev-to-uat
```

## Troubleshooting

### Common Issues

1. **Configuration Errors**
   ```bash
   # Validate configuration
   ./bin/adoc-migration-toolkit --check-env
   ```

2. **Permission Issues**
   ```bash
   # Make scripts executable
   chmod +x bin/*
   ```

3. **Python Module Not Found**
   ```bash
   # Install in development mode
   pip install -e .
   ```

4. **File Not Found Errors**
   - Ensure CSV files exist and are readable
   - Check file paths are correct
   - Verify output directory permissions

### Logging

- **Log Files**: `adoc-migration-toolkit-YYYYMMDD.log`
- **Log Levels**: ERROR, WARNING, INFO, DEBUG
- **Verbose Mode**: Use `--verbose` flag for detailed output

### Environment Behavior

- **Export Commands**: Always use source environment authentication
- **Import Commands**: Always use target environment authentication
- **Segments**: Special handling for SPARK vs JDBC_SQL engine types

### Tips

- Use TAB key for command autocomplete
- Use ↑/↓ arrow keys for command history
- Set output directory once to avoid repeated `--output-file` flags
- Use `--dry-run` to preview changes before making them
- Use `--verbose` for detailed API request/response information
- Check log files for detailed error information

## Support

For issues and questions:
- Check the log files for detailed error information
- Use `--verbose` flag for detailed output
- Validate configuration with `--check-env`
- Review the interactive help with `help` command

## Interactive Interface Example

Here's what the interactive interface looks like when you start the toolkit:

```bash
(adoc-export-import) nitinmotgi@MBA-K609Q0JDGC adoc-export-import % bin/adoc-migration-toolkit 

================================================================================
ADOC INTERACTIVE MIGRATION TOOLKIT
================================================================================
📁 Output Directory: /Users/nitinmotgi/Work/adoc-export-import/data/se-demo
📁 Current Directory: /Users/nitinmotgi/Work/adoc-export-import
📋 Config File: config/config.env
🌍 Source Environment: https://se-demo.acceldata.app
🌍 Source Tenant: se-demo
================================================================================
✅ Tab completion configured successfully

ADOC > help

================================================================================
ADOC INTERACTIVE MIGRATION TOOLKIT - COMMAND HELP
================================================================================

📁 Current Output Directory: /Users/nitinmotgi/Work/adoc-export-import/data/se-demo
💡 Use 'set-output-dir <directory>' to change the output directory
================================================================================

📊 SEGMENTS COMMANDS:
  segments-export [<csv_file>] [--output-file <file>] [--quiet]
    Description: Export segments from source environment to CSV file
    Arguments:
      csv_file: Path to CSV file with source-env and target-env mappings (optional)
      --output-file: Specify custom output file (optional)
      --quiet: Suppress console output, show only summary
    Examples:
      segments-export
      segments-export /Users/nitinmotgi/Work/adoc-export-import/data/se-demo/policy-export/segmented_spark_uids.csv
      segments-export data/uids.csv --output-file my_segments.csv --quiet
    Behavior:
      • If no CSV file specified, uses default from output directory
      • Default input: /Users/nitinmotgi/Work/adoc-export-import/data/se-demo/policy-export/segmented_spark_uids.csv
      • Default output: /Users/nitinmotgi/Work/adoc-export-import/data/se-demo/policy-import/segments_output.csv
      • Exports segments configuration for assets with isSegmented=true
      • For engineType=SPARK: Required because segmented Spark configurations
        are not directly imported with standard import capability
      • For engineType=JDBC_SQL: Already available in standard import,
        so no additional configuration needed
      • Only processes assets that have segments defined
      • Skips assets without segments (logged as info)

  segments-import <csv_file> [--dry-run] [--quiet] [--verbose]
    Description: Import segments to target environment from CSV file
    Arguments:
      csv_file: Path to CSV file with target-env and segments_json
      --dry-run: Preview changes without making API calls
      --quiet: Suppress console output (default)
      --verbose: Show detailed output including headers
    Examples:
      segments-import /Users/nitinmotgi/Work/adoc-export-import/data/se-demo/policy-import/segments_output.csv
      segments-import segments.csv --dry-run --verbose
    Behavior:
      • Reads the CSV file generated from segments-export command
      • Targets UIDs for which segments are present and engine is SPARK
      • Imports segments configuration to target environment
      • Creates new segments (removes existing IDs)
      • Supports both SPARK and JDBC_SQL engine types
      • Validates CSV format and JSON content
      • Processes only assets that have valid segments configuration

🔧 ASSET PROFILE COMMANDS:
  asset-profile-export [<csv_file>] [--output-file <file>] [--quiet] [--verbose]
    Description: Export asset profiles from source environment to CSV file
    Arguments:
      csv_file: Path to CSV file with source-env and target-env mappings (optional)
      --output-file: Specify custom output file (optional)
      --quiet: Suppress console output, show only summary (default)
      --verbose: Show detailed output including headers and responses
    Examples:
      asset-profile-export
      asset-profile-export /Users/nitinmotgi/Work/adoc-export-import/data/se-demo/asset-export/asset_uids.csv
      asset-profile-export uids.csv --output-file profiles.csv --verbose
    Behavior:
      • If no CSV file specified, uses default from output directory
      • Default input: /Users/nitinmotgi/Work/adoc-export-import/data/se-demo/asset-export/asset_uids.csv
      • Default output: /Users/nitinmotgi/Work/adoc-export-import/data/se-demo/asset-import/asset-profiles-import-ready.csv
      • Reads source-env and target-env mappings from CSV file
      • Makes API calls to get asset profiles from source environment
      • Writes profile JSON data to output CSV file
      • Shows minimal output by default, use --verbose for detailed information

  asset-profile-import [<csv_file>] [--dry-run] [--quiet] [--verbose]
    Description: Import asset profiles to target environment from CSV file
    Arguments:
      csv_file: Path to CSV file with target-env and profile_json (optional)
      --dry-run: Preview changes without making API calls
      --quiet: Suppress console output (default)
      --verbose: Show detailed output including headers and responses
    Examples:
      asset-profile-import
      asset-profile-import /Users/nitinmotgi/Work/adoc-export-import/data/se-demo/asset-import/asset-profiles-import-ready.csv
      asset-profile-import profiles.csv --dry-run --verbose
    Behavior:
      • If no CSV file specified, uses default from output directory
      • Default input: /Users/nitinmotgi/Work/adoc-export-import/data/se-demo/asset-import/asset-profiles-import-ready.csv
      • Reads target-env and profile_json from CSV file
      • Makes API calls to update asset profiles in target environment
      • Supports dry-run mode for previewing changes

🔍 ASSET CONFIGURATION COMMANDS:
  asset-config-export <csv_file> [--output-file <file>] [--quiet] [--verbose]
    Description: Export asset configurations from source environment to CSV file
    Arguments:
      csv_file: Path to CSV file with UIDs in the first column
      --output-file: Specify custom output file (optional)
      --quiet: Suppress console output, show only summary (default)
      --verbose: Show detailed output including headers and responses
    Examples:
      asset-config-export /Users/nitinmotgi/Work/adoc-export-import/data/se-demo/asset-export/asset_uids.csv
      asset-config-export uids.csv --output-file configs.csv --verbose
    Behavior:
      • Reads UIDs from the first column of the CSV file
      • Makes REST call to '/catalog-server/api/assets?uid=<uid>' to get asset ID
      • Uses asset ID to call '/catalog-server/api/assets/<id>/config'
      • Writes compressed JSON response to CSV with target-env UID
      • Shows status for each UID in quiet mode
      • Shows HTTP headers and response objects in verbose mode
      • Output format: target-env, config_json (compressed)

  asset-list-export [--quiet] [--verbose]
    Description: Export all assets from source environment to CSV file
    Arguments:
      --quiet: Suppress console output, show only summary
      --verbose: Show detailed output including headers and responses
    Examples:
      asset-list-export
      asset-list-export --quiet
      asset-list-export --verbose
    Behavior:
      • Uses '/catalog-server/api/assets/discover' endpoint with pagination
      • First call gets total count with size=0&page=0
      • Retrieves all pages with size=500 (default)
      • Output file: /Users/nitinmotgi/Work/adoc-export-import/data/se-demo/asset-export/asset-all-export.csv
      • CSV columns: uid, id
      • Sorts output by uid first, then by id
      • Shows page-by-page progress in quiet mode
      • Shows detailed request/response in verbose mode
      • Provides comprehensive statistics upon completion

  policy-list-export [--quiet] [--verbose]
    Description: Export all policies from source environment to CSV file
    Arguments:
      --quiet: Suppress console output, show only summary
      --verbose: Show detailed output including headers and responses
    Examples:
      policy-list-export
      policy-list-export --quiet
      policy-list-export --verbose
    Behavior:
      • Uses '/catalog-server/api/rules' endpoint with pagination
      • First call gets total count with page=0&size=0
      • Retrieves all pages with size=1000 (default)
      • Output file: /Users/nitinmotgi/Work/adoc-export-import/data/se-demo/policy-export/policies-all-export.csv
      • CSV columns: id, type, engineType
      • Sorts output by id
      • Shows page-by-page progress in quiet mode
      • Shows detailed request/response in verbose mode
      • Provides comprehensive statistics upon completion

  policy-export [--type <export_type>] [--filter <filter_value>] [--quiet] [--verbose] [--batch-size <size>]
    Description: Export policy definitions by different categories from source environment to ZIP files
    Arguments:
      --type: Export type (rule-types, engine-types, assemblies, source-types)
      --filter: Optional filter value within the export type
      --quiet: Suppress console output, show only summary
      --verbose: Show detailed output including headers and responses
      --batch-size: Number of policies to export in each batch (default: 50)
    Examples:
      policy-export
      policy-export --type rule-types
      policy-export --type engine-types --filter JDBC_URL
      policy-export --type assemblies --filter production-db
      policy-export --type source-types --filter PostgreSQL
      policy-export --type rule-types --batch-size 100 --quiet
    Behavior:
      • Reads policies from /Users/nitinmotgi/Work/adoc-export-import/data/se-demo/policy-export/policies-all-export.csv (generated by policy-list-export)
      • Groups policies by the specified export type
      • Optionally filters to a specific value within that type
      • Exports each group in batches using '/catalog-server/api/rules/export/policy-definitions'
      • Output files: <export_type>[-<filter>]-<timestamp>-<range>.zip in /Users/nitinmotgi/Work/adoc-export-import/data/se-demo/policy-export
      • Default batch size: 50 policies per ZIP file
      • Filename examples:
        - rule_types-07-04-2025-17-21-0-99.zip
        - engine_types_jdbc_url-07-04-2025-17-21-0-99.zip
        - assemblies_production_db-07-04-2025-17-21-0-99.zip
      • Shows batch-by-batch progress in quiet mode
      • Shows detailed request/response in verbose mode
      • Provides comprehensive statistics upon completion

  policy-import <file_or_pattern> [--quiet] [--verbose]
    Description: Import policy definitions from ZIP files to target environment
    Arguments:
      file_or_pattern: ZIP file path or glob pattern (e.g., *.zip)
      --quiet: Suppress console output, show only summary
      --verbose: Show detailed output including headers and responses
    Examples:
      policy-import *.zip
      policy-import data-quality-*.zip
      policy-import /path/to/specific-file.zip
      policy-import *.zip --verbose
    Behavior:
      • Uploads ZIP files to '/catalog-server/api/rules/import/policy-definitions/upload-config'
      • Uses target environment authentication (target access key, secret key, and tenant)
      • By default, looks for files in output-dir/policy-import directory
      • Supports absolute paths to override default directory
      • Supports glob patterns for multiple files
      • Validates that files exist and are readable
      • Aggregates statistics across all imported files
      • Shows detailed import results and conflicts
      • Provides comprehensive summary with aggregated statistics
      • Tracks UUIDs of imported policy definitions
      • Reports conflicts (assemblies, policies, SQL views, visual views)

  policy-xfr [--input <input_dir>] --source-env-string <source> --target-env-string <target> [options]
    Description: Format policy export files by replacing substrings in JSON files and ZIP archives
    Arguments:
      --source-env-string: Substring to search for (source environment) [REQUIRED]
      --target-env-string: Substring to replace with (target environment) [REQUIRED]
    Options:
      --input: Input directory (auto-detected from policy-export if not specified)
      --output-dir: Output directory (defaults to organized subdirectories)
      --quiet: Suppress console output, show only summary
      --verbose: Show detailed output including processing details
    Examples:
      policy-xfr --source-env-string "PROD_DB" --target-env-string "DEV_DB"
      policy-xfr --input data/samples --source-env-string "old" --target-env-string "new"
      policy-xfr --source-env-string "PROD_DB" --target-env-string "DEV_DB" --verbose
    Behavior:
      • Processes JSON files and ZIP archives in the input directory
      • Replaces all occurrences of source string with target string
      • Maintains file structure and count
      • Auto-detects input directory from /Users/nitinmotgi/Work/adoc-export-import/data/se-demo/policy-export if not specified
      • Creates organized output directory structure
      • Extracts data quality policy assets to CSV files
      • Generates /Users/nitinmotgi/Work/adoc-export-import/data/se-demo/asset-export/asset_uids.csv and /Users/nitinmotgi/Work/adoc-export-import/data/se-demo/policy-import/segmented_spark_uids.csv
      • Shows detailed processing statistics upon completion

🛠️ UTILITY COMMANDS:
  set-output-dir <directory>
    Description: Set global output directory for all export commands
    Arguments:
      directory: Path to the output directory
    Examples:
      set-output-dir /path/to/my/output
      set-output-dir data/custom_output
    Features:
      • Sets the output directory for all export commands
      • Creates the directory if it doesn't exist
      • Validates write permissions
      • Saves configuration to ~/.adoc_migration_config.json
      • Persists across multiple interactive sessions
      • Can be changed anytime with another set-output-dir command

🚀 GUIDED MIGRATION COMMANDS:
  guided-migration <name>
    Description: Start a new guided migration session
    Arguments:
      name: Unique name for the migration session
    Examples:
      guided-migration prod-to-dev
      guided-migration test-migration
    Features:
      • Step-by-step guidance through the complete migration process
      • State management - can pause and resume at any time
      • Validation of prerequisites at each step
      • Detailed help and instructions for each step
      • Automatic file path management

  resume-migration <name>
    Description: Resume an existing guided migration session
    Arguments:
      name: Name of the existing migration session
    Examples:
      resume-migration prod-to-dev
    Features:
      • Continues from where you left off
      • Shows current progress and completed steps
      • Validates prerequisites before continuing

  delete-migration <name>
    Description: Delete a migration state file
    Arguments:
      name: Name of the migration session to delete
    Examples:
      delete-migration prod-to-dev
    Features:
      • Confirms deletion to prevent accidental loss
      • Shows migration details before deletion

  list-migrations
    Description: List all available migration sessions
    Examples:
      list-migrations
    Features:
      • Shows all migration names and their status
      • Displays creation date and current step
      • Shows completion progress

  help
    Description: Show this help information
    Example: help

  history
    Description: Show the last 25 commands with numbers
    Example: history
    Features:
      • Displays the last 25 commands with numbered entries
      • Latest commands appear first (highest numbers)
      • Long commands are truncated for display
      • Enter a number to execute that command
      • Works alongside ↑/↓ arrow key navigation

  exit, quit, q
    Description: Exit the interactive client
    Examples: exit, quit, q

🔧 ENVIRONMENT BEHAVIOR:
  • segments-export: Always exports from source environment
  • segments-import: Always imports to target environment
  • asset-profile-export: Always exports from source environment
  • asset-profile-import: Always imports to target environment
  • asset-config-export: Always exports from source environment
  • asset-list-export: Always exports from source environment
  • policy-list-export: Always exports from source environment
  • policy-export: Always exports from source environment
  • policy-import: Always imports to target environment

💡 TIPS:
  • Use TAB key for command autocomplete
  • Use ↑/↓ arrow keys to navigate command history
  • Type part of an endpoint and press TAB to see suggestions
  • Use --dry-run to preview changes before making them
  • Use --verbose to see detailed API request/response information
  • Check log files for detailed error information
  • Set output directory once with set-output-dir to avoid specifying --output-file repeatedly

📁 FILE LOCATIONS:
  • Input CSV files: /Users/nitinmotgi/Work/adoc-export-import/data/se-demo/asset-export/ and /Users/nitinmotgi/Work/adoc-export-import/data/se-demo/policy-import/
  • Output CSV files: /Users/nitinmotgi/Work/adoc-export-import/data/se-demo/ (organized by category)
  • Log files: adoc-migration-toolkit-YYYYMMDD.log
```

This example shows:
- **Startup Information**: Environment details, output directory, and configuration status
- **Interactive Prompt**: The `ADOC >` prompt ready for commands
- **Comprehensive Help**: Detailed command documentation with examples and behavior descriptions
- **Command Categories**: Organized sections for different types of operations
- **Real File Paths**: Actual paths from a working environment
- **User-Friendly Interface**: Clear formatting with emojis and structured information 