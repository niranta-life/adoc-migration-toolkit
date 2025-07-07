#!/usr/bin/env python3
"""
Simple test script to verify the Click CLI works correctly.
"""

import sys
import os

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

try:
    # Import just the CLI module directly
    from adoc_migration_toolkit.cli.main import cli
    
    print("✅ Click CLI imported successfully!")
    print("\nCLI structure:")
    print(f"  - CLI group: {cli}")
    print(f"  - Commands: {list(cli.commands.keys())}")
    
    print("\n🎉 CLI test completed successfully!")
    
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Error testing CLI: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1) 