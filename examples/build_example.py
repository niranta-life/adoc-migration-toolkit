#!/usr/bin/env python3
"""
Example script demonstrating PyInstaller build with integrity checks.

This script shows how to use the build system programmatically
and provides examples of different build configurations.
"""

import os
import subprocess
import sys
from pathlib import Path


def run_command(cmd, cwd=None, check=True):
    """Run a command and return the result."""
    print(f"Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(
            cmd, cwd=cwd, check=check, capture_output=True, text=True
        )
        if result.stdout:
            print("STDOUT:", result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e}")
        return False


def example_basic_build():
    """Example: Basic build with default settings."""
    print("=== Example: Basic Build ===")

    # Run the build script
    success = run_command(
        [sys.executable, "build_with_integrity.py", "--onefile", "--console", "--clean"]
    )

    if success:
        print("✅ Basic build completed successfully")

        # Test the executable
        executable = Path("dist/adoc-migration-toolkit")
        if executable.exists():
            print("Testing executable...")
            run_command([str(executable), "--help"], check=False)
    else:
        print("❌ Basic build failed")


def example_windowed_build():
    """Example: Build windowed executable (no console)."""
    print("\n=== Example: Windowed Build ===")

    success = run_command(
        [
            sys.executable,
            "build_with_integrity.py",
            "--onefile",
            "--windowed",
            "--clean",
        ]
    )

    if success:
        print("✅ Windowed build completed successfully")
    else:
        print("❌ Windowed build failed")


def example_directory_build():
    """Example: Build as directory with dependencies."""
    print("\n=== Example: Directory Build ===")

    success = run_command(
        [sys.executable, "build_with_integrity.py", "--onedir", "--console", "--clean"]
    )

    if success:
        print("✅ Directory build completed successfully")

        # Check the directory structure
        dist_dir = Path("dist/adoc-migration-toolkit")
        if dist_dir.exists():
            print("Directory contents:")
            for item in dist_dir.iterdir():
                print(f"  {item.name}")
    else:
        print("❌ Directory build failed")


def example_integrity_verification():
    """Example: Verify integrity of built executable."""
    print("\n=== Example: Integrity Verification ===")

    executable = Path("dist/adoc-migration-toolkit")
    if not executable.exists():
        print("❌ No executable found. Run a build first.")
        return

    print("Running integrity checks...")
    success = run_command([str(executable), "--help"], check=False)

    if success:
        print("✅ Integrity verification passed")
    else:
        print("❌ Integrity verification failed")


def example_custom_build():
    """Example: Custom build with specific options."""
    print("\n=== Example: Custom Build ===")

    # This would be a more complex build with custom options
    # For now, just show the concept
    print("Custom build options:")
    print("  - Custom data files")
    print("  - Specific hidden imports")
    print("  - Custom excludes")
    print("  - Code signing")

    # Example of how you might modify the build script
    print("\nTo customize the build, edit build_with_integrity.py:")
    print("  - Modify the datas list")
    print("  - Add custom hiddenimports")
    print("  - Configure excludes")
    print("  - Add code signing options")


def example_makefile_usage():
    """Example: Using Makefile for builds."""
    print("\n=== Example: Makefile Usage ===")

    makefile_targets = [
        "build",
        "build-onefile",
        "build-onedir",
        "build-windowed",
        "clean",
        "verify",
    ]

    print("Available Makefile targets:")
    for target in makefile_targets:
        print(f"  make {target}")

    print("\nExample usage:")
    print("  make clean && make build")
    print("  make build-onefile")
    print("  make verify")


def example_troubleshooting():
    """Example: Common troubleshooting steps."""
    print("\n=== Example: Troubleshooting ===")

    print("Common issues and solutions:")

    issues = [
        ("Build fails with import errors", "pip install -r requirements.txt"),
        ("Executable is too large", "Review excludes in build script"),
        ("Permission denied on Linux/macOS", "chmod +x dist/adoc-migration-toolkit"),
        ("Integrity check fails", "make clean && make build"),
    ]

    for issue, solution in issues:
        print(f"  {issue}: {solution}")


def main():
    """Main function to run all examples."""
    print("PyInstaller Build Examples")
    print("==========================")

    # Check if we're in the right directory
    if not Path("build_with_integrity.py").exists():
        print("❌ build_with_integrity.py not found")
        print("Please run this script from the project root directory")
        return

    # Run examples
    examples = [
        example_basic_build,
        example_windowed_build,
        example_directory_build,
        example_integrity_verification,
        example_custom_build,
        example_makefile_usage,
        example_troubleshooting,
    ]

    for example in examples:
        try:
            example()
        except Exception as e:
            print(f"❌ Example failed: {e}")

        print()  # Add spacing between examples

    print("=== Summary ===")
    print("Examples completed. Check the output above for results.")
    print("\nNext steps:")
    print("1. Review the build artifacts in dist/")
    print("2. Test the executable functionality")
    print("3. Customize the build configuration as needed")
    print("4. Use make commands for easier builds")


if __name__ == "__main__":
    main()
