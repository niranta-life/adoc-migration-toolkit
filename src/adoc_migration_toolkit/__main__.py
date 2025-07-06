"""
Entry point for the adoc-migration-toolkit package.

This module is called when the package is run as a script:
    python -m adoc_migration_toolkit
"""

import sys
from .cli import main

if __name__ == '__main__':
    sys.exit(main()) 