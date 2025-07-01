"""
Entry point for running the package as a module.

This allows the package to be executed with:
    python -m adoc_export_import <args>
"""

from .cli import main

if __name__ == "__main__":
    main() 