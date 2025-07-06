"""
Core functionality for the adoc migration toolkit.

This module contains core classes and utilities including the PolicyTransformer
and logging setup.
"""

from .logging import setup_logging
from .transformer import PolicyTranformer
from .utils import validate_arguments

__all__ = [
    'setup_logging',
    'PolicyTranformer', 
    'validate_arguments'
] 