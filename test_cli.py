#!/usr/bin/env python3
"""
Test script to verify the Click CLI works correctly.
"""

import sys
import os

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Mock the dependencies to avoid import errors
class MockModule:
    def __getattr__(self, name):
        return lambda *args, **kwargs: None

# Mock the problematic modules
sys.modules['tqdm'] = MockModule()
sys.modules['requests'] = MockModule()

try:
    from adoc_migration_toolkit.cli.main import cli
    
    print("✅ Click CLI imported successfully!")
    print("\nTesting CLI help...")
    
    # Test the CLI help
    result = cli.main(['--help'], standalone_mode=False)
    print("✅ CLI help test completed!")
    
    print("\nTesting interactive command help...")
    result = cli.main(['interactive', '--help'], standalone_mode=False)
    print("✅ Interactive command help test completed!")
    
    print("\n🎉 All CLI tests passed!")
    
except Exception as e:
    print(f"❌ Error testing CLI: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1) 