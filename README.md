# ADOC Export Import

A professional tool for replacing substrings in JSON files and ZIP archives. This tool is designed to process policy export files with comprehensive error handling and logging.

## Features

- **JSON Processing**: Recursively processes JSON files and replaces substrings throughout the data structure
- **ZIP Archive Support**: Handles ZIP files containing JSON data, maintaining file structure and count
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

The tool can be used as a command-line application:

```bash
# Basic usage
python -m adoc_export_import <input_directory> <search_string> <replace_string>

# With custom output directory
python -m adoc_export_import <input_directory> <search_string> <replace_string> --output-dir <output_directory>

# With verbose logging
python -m adoc_export_import <input_directory> <search_string> <replace_string> --verbose
```

### Examples

```bash
# Replace database connection strings
python -m adoc_export_import data/samples "PROD_DB" "DEV_DB"

# Replace with custom output directory
python -m adoc_export_import data/samples "old" "new" --output-dir data/output

# Verbose processing
python -m adoc_import_export data/samples "COMM_APAC_ETL_PROD_DB" "NEW_DB_NAME" --verbose
```

### Programmatic Usage

You can also use the tool programmatically:

```python
from adoc_export_import import JSONStringReplacer
import logging

# Setup logging
logger = logging.getLogger(__name__)

# Create replacer instance
replacer = JSONStringReplacer(
    input_dir="data/samples",
    search_string="PROD_DB",
    replace_string="DEV_DB",
    output_dir="data/output",
    logger=logger
)

# Process the directory
stats = replacer.process_directory()
print(f"Processed {stats['total_files']} files with {stats['changes_made']} changes")
```

## Project Structure

```
adoc-export-import/
├── src/
│   └── adoc_export_import/
│       ├── __init__.py
│       ├── core.py              # Main functionality (policy_export_formatter.py)
│       └── cli.py               # Command line interface
├── data/
│   ├── samples/                 # Sample input files
│   ├── samples_import_ready/    # Sample output files
│   └── output/                  # Output directory
├── tests/                       # Test files
├── docs/                        # Documentation
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