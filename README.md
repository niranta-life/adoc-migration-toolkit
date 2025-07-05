# ADOC Export Import Tool

A comprehensive tool for migrating Acceldata policies and configurations between environments. This tool processes policy export ZIP files, extracts asset configurations, and provides interactive commands for managing asset profiles and configurations.

## Overview

The ADOC Export Import Tool automates the complex process of migrating policies and asset configurations from one Acceldata environment to another. It handles the translation of environment-specific strings, extracts specialized configurations for segmented assets, and provides interactive commands for profile and configuration management.

## Prerequisites

- Python 3.8 or higher
- uv (install from https://github.com/astral-sh/uv)
- Access to source and target Acceldata environments
- Policy export ZIP files from the source environment

## Installation

1. **Clone the repository:**
```bash
git clone <repository-url>
cd adoc-export-import
```

2. **Install dependencies:**
```bash
uv sync
```

3. **Activate the virtual environment:**
```bash
source .venv/bin/activate  # On Unix/macOS
# or
.venv\Scripts\activate     # On Windows
```

4. **Configure environment variables:**
Create a `config.env` file with your environment credentials:
```bash
# Host URL for the Acceldata environment
AD_HOST=https://your-acceldata-instance.com

# Source environment credentials
AD_SOURCE_ACCESS_KEY=your_source_access_key
AD_SOURCE_SECRET_KEY=your_source_secret_key
AD_SOURCE_TENANT=your_source_tenant

# Target environment credentials (for import operations)
AD_TARGET_ACCESS_KEY=your_target_access_key
AD_TARGET_SECRET_KEY=your_target_secret_key
AD_TARGET_TENANT=your_target_tenant
```

## Complete Migration Workflow

### Step 1: Export Policies from Source Environment

**Action Required:** Use the Acceldata UI to export policies from your source environment.

1. Navigate to the Acceldata UI in your source environment
2. Go to the Policies section
3. Select the policies you want to migrate
4. Export them as ZIP files
5. Download the ZIP files to your local machine

**Output:** Policy export ZIP files containing JSON configurations

### Step 2: Process ZIP Files with Formatter

**Purpose:** Translate environment strings and extract asset information

```bash
python -m adoc_export_import formatter \
  --input data/policy_exports \
  --source-env-string "PROD_DB" \
  --target-env-string "DEV_DB" \
  --verbose
```

**What this does:**
- Reads policy export ZIP files from the input directory
- Extracts JSON files while maintaining structure
- Replaces source environment strings with target environment strings
- Generates import-ready ZIP files
- Creates specialized CSV files for asset management

**Output:**
- `*_import_ready/` directories with translated ZIP files
- `segmented_spark_uids.csv` - UIDs of segmented SPARK assets
- `asset_uids.csv` - All asset UIDs for profile/configuration management

**Important Notes:**
- Environment strings must match exactly (case-sensitive)
- Example: If your source uses "PROD_DB" and target uses "DEV_DB", specify exactly these strings
- The formatter identifies assets that need special handling during import

### Step 3: Start Interactive Mode

**Purpose:** Access interactive commands for asset management

```bash
python -m adoc_export_import interactive --env-file config.env
```

This opens an interactive session where you can run specialized commands.

### Step 4: Export Asset Profiles

**Purpose:** Extract profile configurations from source environment

```bash
ADOC> asset-profile-export
ADOC> asset-profile-export --verbose
```

**What this does:**
- Reads UIDs from the `asset_uids.csv` file generated in Step 2
- Makes API calls to get profile configurations for each asset
- Exports profile configurations to CSV format

**Output:** `asset-profiles-import-ready.csv` file with profile configurations

### Step 5: Import Asset Profiles

**Purpose:** Import profile configurations to target environment

```bash
ADOC> asset-profile-import --dry-run --verbose
```

**What this does:**
- Reads profile configurations from the CSV file generated in Step 4
- Imports profile configurations to the target environment
- Supports dry-run mode to preview changes before applying

**Options:**
- `--dry-run`: Preview changes without applying them
- `--verbose`: Show detailed API calls and responses
- `--quiet`: Minimal output (default)

### Step 6: Export Asset Configurations

**Purpose:** Extract detailed asset configurations from source environment

```bash
ADOC> asset-config-export data/samples_import_ready/asset_uids.csv --verbose
```

**What this does:**
- Reads UIDs from the `asset_uids.csv` file
- Makes API calls to get asset details and extract asset IDs
- Fetches detailed configurations using asset IDs
- Exports compressed JSON configurations to CSV

**Output:** `asset-config-export.csv` file with detailed configurations

### Step 7: Import Asset Configurations

**Purpose:** Import detailed configurations to target environment

```bash
ADOC> asset-config-import data/samples_import_ready/asset-config-export.csv --dry-run --verbose
```

**Note:** This command will be implemented in a future update.

### Step 8: Handle Segmented Assets (if applicable)

**Purpose:** Import segment configurations for SPARK assets

```bash
ADOC> segments-export
ADOC> segments-export --verbose
ADOC> segments-import segments_output.csv --dry-run --verbose
```

**What this does:**
- Reads UIDs of segmented SPARK assets from the CSV generated in Step 2
- Exports segment configurations that aren't included in standard imports

**Output:** `segments_output.csv` file with segment configurations

```bash
ADOC> segments-import data/samples_import_ready/segments_output.csv --dry-run --verbose
```

**What this does:**
- Imports segment configurations to target environment
- Required for SPARK assets with segmentation
- Optional for JDBC_SQL assets (already handled by standard import)

## Command Reference

### Formatter Command

```bash
python -m adoc_export_import formatter \
  --input <input_directory> \
  --source-env-string <source_string> \
  --target-env-string <target_string> \
  [--output-dir <output_directory>] \
  [--verbose] \
  [--log-level <level>]
```

**Arguments:**
- `--input`: Directory containing policy export ZIP files
- `--source-env-string`: Exact string to replace (e.g., "PROD_DB")
- `--target-env-string`: String to replace with (e.g., "DEV_DB")
- `--output-dir`: Custom output directory (optional)
- `--verbose`: Enable detailed logging
- `--log-level`: Set logging level (ERROR, WARNING, INFO, DEBUG)

### Interactive Mode

```bash
python -m adoc_export_import interactive --env-file config.env [--verbose]
```

**Available Commands:**
- `segments-export [<csv_file>] [--output-file <file>] [--quiet]`
- `segments-import <csv_file> [--dry-run] [--quiet] [--verbose]`
- `asset-profile-export [<csv_file>] [--output-file <file>] [--quiet] [--verbose]`
- `asset-profile-import [<csv_file>] [--dry-run] [--quiet] [--verbose]`
- `asset-config-export <csv_file> [--output-file <file>] [--quiet] [--verbose]`
- `set-output-dir <directory>`: Set global output directory
- `help`: Show detailed help
- `exit`: Exit interactive mode

## File Structure

```
project/
├── data/
│   ├── policy_exports/           # Input: Policy export ZIP files
│   │   ├── policy_export.zip
│   │   └── metadata.json
│   ├── samples_import_ready/     # Output: Processed files
│   │   ├── [translated ZIP files]
│   │   ├── segmented_spark_uids.csv
│   │   ├── asset_uids.csv
│   │   ├── asset-profiles-import-ready.csv
│   │   ├── asset-config-export.csv
│   │   └── segments_output.csv
│   ├── asset-export/
│   │   ├── asset_uids.csv
│   │   ├── asset-all-export.csv
│   │   └── asset-config-export.csv
│   ├── asset-import/
│   │   ├── asset-profiles-import-ready.csv
│   │   └── segments_output.csv
│   ├── policy-export/
│   │   ├── policies-all-export.csv
│   │   ├── segmented_spark_uids.csv
│   │   └── *.zip files
│   └── policy-import/
│       └── processed files
├── config.env                    # Environment configuration
└── README.md
```

## Common Use Cases

### Development Environment Migration

```bash
# 1. Process policy exports
python -m adoc_export_import formatter \
  --input data/policy_exports \
  --source-env-string "PROD_DB" \
  --target-env-string "DEV_DB" \
  --verbose

# 2. Start interactive session
python -m adoc_export_import interactive --env-file config.env

# 3. Export and import profiles
ADOC> asset-profile-export
ADOC> asset-profile-export --verbose
ADOC> asset-profile-import --dry-run --verbose

# 4. Export and import configurations
ADOC> asset-config-export data/samples_import_ready/asset_uids.csv --verbose
# asset-config-import command will be available in future update

# 5. Handle segmented assets (if any)
ADOC> segments-export
ADOC> segments-export --verbose
ADOC> segments-import segments_output.csv --dry-run --verbose
```

### Testing Environment Migration

```bash
# Similar to development, but with different environment strings
python -m adoc_export_import formatter \
  --input data/policy_exports \
  --source-env-string "PROD_DB" \
  --target-env-string "TEST_DB" \
  --verbose
```

## Troubleshooting

### Common Issues

1. **Environment String Not Found**
   - Ensure exact string matching (case-sensitive)
   - Check the actual strings in your policy files
   - Use `--verbose` to see detailed processing

2. **API Connection Errors**
   - Verify `config.env` file has correct credentials
   - Check network connectivity to Acceldata instance
   - Ensure access keys have appropriate permissions

3. **CSV File Not Found**
   - Verify file paths are correct
   - Check that formatter has generated the required CSV files
   - Use absolute paths if needed

4. **Permission Errors**
   - Ensure write permissions for output directories
   - Check file permissions for input files

### Logging

The tool provides comprehensive logging:
- Console output with `--verbose` flag
- Log files: `adoc-migration-toolkit-YYYYMMDD.log` (rotates daily)
- Interactive mode history: `~/.adoc_migration_toolkit_history`

### Getting Help

- Use `help` command in interactive mode for detailed command information
- Check log files for detailed error information
- Use `--dry-run` flag to preview changes before applying them

## Best Practices

1. **Always use dry-run first**: Test your commands with `--dry-run` before applying changes
2. **Backup your data**: Keep backups of original policy exports
3. **Verify environment strings**: Double-check source and target environment strings
4. **Test in non-production**: Always test the migration process in a test environment first
5. **Monitor logs**: Use verbose mode and check log files for detailed information
6. **Validate outputs**: Verify generated CSV files before proceeding to import steps

## Support

For issues and questions:
- Check the troubleshooting section above
- Review log files for detailed error information
- Use the GitHub issue tracker for bug reports and feature requests

## Formatter Technical Details

The formatter is the core component of the ADOC Export Import Tool, responsible for processing policy export ZIP files and preparing them for import into target environments. This section provides detailed technical information about how the formatter works.

### Architecture Overview

The formatter operates on a multi-stage pipeline:

1. **File Discovery**: Scans input directory for ZIP and JSON files
2. **Content Extraction**: Extracts and processes JSON content from ZIP archives
3. **String Replacement**: Performs environment string translation
4. **Asset Analysis**: Identifies and categorizes assets for specialized handling
5. **Output Generation**: Creates import-ready files and asset management CSVs

### Processing Pipeline

#### Stage 1: File Discovery and Validation
```
Input Directory Scan
├── ZIP Files (*.zip)
│   ├── Validate ZIP structure
│   ├── Check for JSON content
│   └── Extract file metadata
└── JSON Files (*.json)
    ├── Validate JSON syntax
    ├── Check for policy content
    └── Prepare for processing
```

#### Stage 2: Content Extraction and Processing
```
ZIP Archive Processing
├── Extract all files maintaining structure
├── Identify JSON files within archives
├── Parse JSON content recursively
└── Build content tree for analysis

JSON File Processing
├── Parse JSON structure
├── Traverse all nested objects and arrays
├── Identify string fields for replacement
└── Maintain data type integrity
```

#### Stage 3: Environment String Replacement
```
String Replacement Engine
├── Exact String Matching
│   ├── Case-sensitive comparison
│   ├── Whole string replacement
│   └── Preserve surrounding context
├── Recursive Processing
│   ├── Object properties
│   ├── Array elements
│   └── Nested structures
└── Validation
    ├── JSON syntax preservation
    ├── Data type consistency
    └── Replacement verification
```

#### Stage 4: Asset Analysis and Categorization
```
Asset Identification
├── Scan processed JSON for asset definitions
├── Extract asset metadata
│   ├── UID (Unique Identifier)
│   ├── Engine Type (SPARK, JDBC_SQL, etc.)
│   ├── Segmentation Status
│   └── Configuration Type
└── Categorize assets
    ├── Segmented SPARK assets
    ├── Segmented JDBC_SQL assets
    ├── Non-segmented assets
    └── Special configuration assets
```

#### Stage 5: Output Generation
```
File Output Generation
├── Import-Ready ZIP Files
│   ├── Maintain original structure
│   ├── Include translated JSON
│   ├── Preserve file metadata
│   └── Create _import_ready directories
├── Asset Management CSVs
│   ├── segmented_spark_uids.csv
│   ├── asset_uids.csv
│   └── Validation and formatting
└── Processing Statistics
    ├── File counts and types
    ├── Replacement statistics
    ├── Asset categorization
    └── Error reporting
```

### String Replacement Algorithm

The formatter uses a sophisticated string replacement algorithm that ensures data integrity:

```python
def replace_strings_recursive(obj, search_string, replace_string):
    """
    Recursively replace strings in JSON objects while preserving structure.
    
    Args:
        obj: JSON object, array, or primitive value
        search_string: String to search for (exact match)
        replace_string: String to replace with
        
    Returns:
        Modified object with strings replaced
    """
    if isinstance(obj, dict):
        return {key: replace_strings_recursive(value, search_string, replace_string) 
                for key, value in obj.items()}
    elif isinstance(obj, list):
        return [replace_strings_recursive(item, search_string, replace_string) 
                for item in obj]
    elif isinstance(obj, str):
        return obj.replace(search_string, replace_string)
    else:
        return obj  # Preserve non-string types (numbers, booleans, null)
```

### Asset Detection and Classification

The formatter identifies assets using multiple criteria:

#### Asset Detection Criteria
```json
{
  "asset": {
    "uid": "string",           // Required: Unique asset identifier
    "engineType": "SPARK|JDBC_SQL|...",  // Engine classification
    "isSegmented": true|false, // Segmentation status
    "configType": "string",    // Configuration type
    "metadata": {...}          // Additional metadata
  }
}
```

#### Classification Logic
```python
def classify_asset(asset_data):
    """
    Classify asset based on engine type and segmentation status.
    """
    engine_type = asset_data.get('engineType', '')
    is_segmented = asset_data.get('isSegmented', False)
    
    if is_segmented and engine_type == 'SPARK':
        return 'segmented_spark'
    elif is_segmented and engine_type == 'JDBC_SQL':
        return 'segmented_jdbc'
    else:
        return 'standard'
```

### File Structure Preservation

The formatter maintains the original file structure while processing:

```
Original ZIP Structure
├── policy_export.zip
│   ├── assets/
│   │   ├── asset1.json
│   │   ├── asset2.json
│   │   └── metadata.json
│   ├── policies/
│   │   ├── policy1.json
│   │   └── policy2.json
│   └── config/
│       └── settings.json

Processed Output Structure
├── policy_export_import_ready/
│   ├── assets/
│   │   ├── asset1.json (translated)
│   │   ├── asset2.json (translated)
│   │   └── metadata.json (translated)
│   ├── policies/
│   │   ├── policy1.json (translated)
│   │   └── policy2.json (translated)
│   ├── config/
│   │   └── settings.json (translated)
│   ├── segmented_spark_uids.csv
│   └── asset_uids.csv
```

### Error Handling and Validation

The formatter implements comprehensive error handling:

#### Error Categories
1. **File System Errors**
   - Invalid file paths
   - Permission issues
   - Disk space problems

2. **ZIP Processing Errors**
   - Corrupted ZIP files
   - Invalid ZIP structure
   - Extraction failures

3. **JSON Processing Errors**
   - Invalid JSON syntax
   - Encoding issues
   - Memory constraints

4. **String Replacement Errors**
   - Invalid search/replace strings
   - Data type corruption
   - Replacement failures

#### Validation Checks
```python
def validate_replacement(original, modified, search_string, replace_string):
    """
    Validate that string replacement was successful and safe.
    """
    # Check JSON syntax
    if not is_valid_json(modified):
        raise JSONSyntaxError("Replacement corrupted JSON structure")
    
    # Verify replacements occurred
    if search_string in str(original) and search_string in str(modified):
        raise ReplacementError("Not all instances were replaced")
    
    # Check data type preservation
    if type(original) != type(modified):
        raise DataTypeError("Replacement changed data types")
    
    return True
```

### Performance Optimizations

The formatter includes several performance optimizations:

#### Memory Management
- **Streaming Processing**: Large ZIP files are processed in chunks
- **Lazy Loading**: JSON content is parsed only when needed
- **Memory Pooling**: Reuses objects to reduce garbage collection

#### Parallel Processing
- **File-Level Parallelism**: Multiple files processed concurrently
- **Content-Level Parallelism**: Large JSON objects processed in parallel
- **I/O Optimization**: Asynchronous file operations

#### Caching
- **String Cache**: Caches frequently used search/replace operations
- **Pattern Cache**: Caches compiled regex patterns
- **Metadata Cache**: Caches file metadata for repeated access

### Statistics and Reporting

The formatter provides detailed statistics for monitoring and debugging:

#### Processing Statistics
```json
{
  "total_files": 150,
  "json_files": 120,
  "zip_files": 30,
  "files_investigated": 145,
  "changes_made": 89,
  "successful": 142,
  "failed": 3,
  "total_policies_processed": 89,
  "segmented_spark_policies": 12,
  "segmented_jdbc_policies": 8,
  "non_segmented_policies": 69,
  "extracted_assets": 156,
  "all_assets": 156,
  "errors": ["error1", "error2", "error3"]
}
```

#### Asset Statistics
```json
{
  "asset_categories": {
    "segmented_spark": 12,
    "segmented_jdbc": 8,
    "standard": 136
  },
  "engine_types": {
    "SPARK": 45,
    "JDBC_SQL": 67,
    "PYTHON": 23,
    "OTHER": 21
  },
  "segmentation_status": {
    "segmented": 20,
    "non_segmented": 136
  }
}
```

### Configuration Options

The formatter supports various configuration options:

#### Command Line Options
```bash
python -m adoc_export_import formatter \
  --input <directory>           # Input directory path
  --source-env-string <string>  # Source environment string
  --target-env-string <string>  # Target environment string
  --output-dir <directory>      # Custom output directory
  --verbose                     # Enable verbose logging
  --log-level <level>          # Set logging level
```

#### Advanced Configuration
```python
# Programmatic configuration
formatter = PolicyExportFormatter(
    input_dir="data/policy_exports",
    search_string="PROD_DB",
    replace_string="DEV_DB",
    output_dir="data/import_ready",
    logger=custom_logger,
    max_workers=4,              # Parallel processing workers
    chunk_size=8192,            # File processing chunk size
    validate_json=True,         # JSON validation
    preserve_structure=True,    # Maintain file structure
    create_backups=True         # Create backup files
)
```

### Integration Points

The formatter integrates with other components:

#### CSV Generation
- **segmented_spark_uids.csv**: Used by segments-export command
- **asset_uids.csv**: Used by asset-profile-export and asset-config-export commands

#### API Integration
- Generated CSV files feed into interactive mode commands
- Asset UIDs enable API-based configuration management
- Segmentation data supports specialized import workflows

#### Logging Integration
- Comprehensive logging for debugging and monitoring
- Error tracking for failed operations
- Performance metrics for optimization

This technical foundation ensures the formatter can handle complex policy migrations while maintaining data integrity and providing detailed feedback for troubleshooting and optimization. 