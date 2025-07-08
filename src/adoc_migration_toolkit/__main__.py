# Multiprocessing freeze support for PyInstaller
import multiprocessing

if multiprocessing.freeze_support():
    pass

"""
Entry point for the adoc-migration-toolkit package.

This module is called when the package is run as a script:
    python -m adoc_migration_toolkit
"""

import sys
from adoc_migration_toolkit.cli import main

if __name__ == '__main__':
    sys.exit(main()) 