# ADOC Export Import

A professional tool for replacing substrings in JSON files and ZIP archives, and exporting asset details via API calls. This tool is designed to process policy export files with comprehensive error handling and logging.

## Features

- **JSON Processing**: Recursively processes JSON files and replaces substrings throughout the data structure
- **ZIP Archive Support**: Handles ZIP files containing JSON data, maintaining file structure and count
- **Asset Export**: Reads UIDs from CSV files and exports asset details via REST API calls
- **Interactive REST API Client**: Interactive command-line tool for making API calls with configurable authentication
- **Comprehensive Logging**: Professional logging with both file and console output
- **Error Handling**: Robust error handling with detailed error reporting
- **Batch Processing**: Processes entire directories of files efficiently
- **Statistics**: Provides detailed statistics about processing results

## Installation

This project uses [uv](https://github.com/astral-sh/uv) for dependency management and packaging.

### Prerequisites

- Python 3.8 or higher
- uv (install from https://github.com/astral-sh/uv)

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd adoc-export-import
```

2. Install dependencies and setup the environment:
```bash
uv sync
```

3. Activate the virtual environment:
```bash
source .venv/bin/activate  # On Unix/macOS
# or
.venv\Scripts\activate     # On Windows
```

## Usage

### Command Line Interface

The tool provides three main commands:

#### Formatter Command

Processes JSON files and ZIP archives by replacing substrings:

```bash
# Basic usage
python -m adoc_export_import formatter --input <input_directory> --source-env-string <search_string> --target-env-string <replace_string>

# With custom output directory
python -m adoc_export_import formatter --input <input_directory> --source-env-string <search_string> --target-env-string <replace_string> --output-dir <output_directory>

# With verbose logging
python -m adoc_export_import formatter --input <input_directory> --source-env-string <search_string> --target-env-string <replace_string> --verbose
```

#### Asset Export Command

Exports asset details by reading UIDs from CSV files and making API calls:

```bash
# Basic usage
python -m adoc_export_import asset-export --csv-file <csv_file> --env-file <env_file>

# With verbose logging
python -m adoc_export_import asset-export --csv-file <csv_file> --env-file <env_file> --verbose
```

#### REST API Command

Interactive REST API client for making API calls:

```bash
# Basic usage
python -m adoc_export_import rest-api --env-file <env_file>

# With verbose logging
python -m adoc_export_import rest-api --env-file <env_file> --verbose
```

Interactive Commands:
```
ADOC> GET /catalog-server/api/assets?uid=123
ADOC> PUT /catalog-server/api/assets {"name": "test", "value": 123}
ADOC> GET /catalog-server/api/assets?uid=123 --target-auth --target-tenant
ADOC> exit
```

**Features:**
- Command history with up/down arrow key navigation
- History persists between sessions (stored in `~/.adoc_history`)
- Supports up to 1000 commands in history

### Examples

#### Formatter Examples

```bash
# Replace database connection strings
python -m adoc_export_import formatter --input data/samples --source-env-string "PROD_DB" --target-env-string "DEV_DB"

# Replace with custom output directory
python -m adoc_export_import formatter --input data/samples --source-env-string "old" --target-env-string "new" --output-dir data/output

# Verbose processing
python -m adoc_export_import formatter --input data/samples --source-env-string "COMM_APAC_ETL_PROD_DB" --target-env-string "NEW_DB_NAME" --verbose
```

#### Asset Export Examples

```bash
# Export assets from CSV file
python -m adoc_export_import asset-export --csv-file data/output/extracted_assets.csv --env-file config.env

# Export with verbose logging
python -m adoc_export_import asset-export --csv-file data/output/extracted_assets.csv --env-file config.env --verbose
```

### Environment Configuration

For the asset-export command, create a `config.env` file with the following variables:

```bash
# Host URL for the Acceldata environment
AD_HOST=https://se-demo.acceldata.app

# Access keys and secret keys for authentication
AD_SOURCE_ACCESS_KEY=your_source_access_key_here
AD_SOURCE_SECRET_KEY=your_source_secret_key_here
AD_TARGET_ACCESS_KEY=your_target_access_key_here
AD_TARGET_SECRET_KEY=your_target_secret_key_here

# Tenant names
AD_SOURCE_TENANT=your_source_tenant_here
AD_TARGET_TENANT=your_target_tenant_here
```

### Programmatic Usage

You can also use the tool programmatically:

```python
from adoc_export_import import PolicyExportFormatter, create_api_client
import logging

# Setup logging
logger = logging.getLogger(__name__)

# Create formatter instance
formatter = PolicyExportFormatter(
    input_dir="data/samples",
    search_string="PROD_DB",
    replace_string="DEV_DB",
    output_dir="data/output",
    logger=logger
)

# Process the directory
stats = formatter.process_directory()
print(f"Processed {stats['total_files']} files with {stats['changes_made']} changes")

# Create API client for asset export
client = create_api_client(env_file="config.env", logger=logger)

# Get asset details
asset_data = client.get_asset_by_uid("your_uid_here")
print(asset_data)
```

## Project Structure

```
adoc-export-import/
├── src/
│   └── adoc_export_import/
│       ├── __init__.py
│       ├── core.py              # Main functionality (policy_export_formatter.py)
│       ├── cli.py               # Command line interface
│       └── api_client.py        # API client for asset export
├── data/
│   ├── samples/                 # Sample input files
│   ├── samples_import_ready/    # Sample output files
│   └── output/                  # Output directory
├── tests/                       # Test files
├── docs/                        # Documentation
├── config.env.example           # Example environment configuration
├── pyproject.toml              # Project configuration
└── README.md                   # This file
```

## Development

### Running Tests

```bash
uv run pytest
```

### Code Formatting

```bash
uv run black src/ tests/
uv run isort src/ tests/
```

### Type Checking

```bash
uv run mypy src/
```

### Linting

```bash
uv run flake8 src/ tests/
```

## Configuration

The project uses `pyproject.toml` for configuration, including:

- **Build System**: hatchling
- **Code Formatting**: black with 88 character line length
- **Import Sorting**: isort with black profile
- **Type Checking**: mypy with strict settings
- **Testing**: pytest with coverage reporting

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## Support

For issues and questions, please use the GitHub issue tracker. 