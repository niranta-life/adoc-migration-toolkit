#!/usr/bin/env python3
"""
Test runner for the ADOC Migration Toolkit.

This script provides a comprehensive test runner with various options for running
tests, generating reports, and managing test execution using uv for environment management.
"""

import sys
import subprocess
import argparse
import os
from pathlib import Path
import shutil


def run_command(cmd, description, env=None):
    """Run a command and handle errors."""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {' '.join(cmd)}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=False, env=env)
        print(f"\n✅ {description} completed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n❌ {description} failed with exit code {e.returncode}")
        return False


def setup_uv_environment():
    """Set up uv environment and install dependencies."""
    print("🚀 Setting up uv test environment...")
    
    # Check if uv is installed
    print("📋 Checking if uv is installed...")
    try:
        result = subprocess.run(['uv', '--version'], check=True, capture_output=True, text=True)
        print(f"✅ uv is installed: {result.stdout.strip()}")
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ Error: uv is not installed. Please install uv first:")
        print("   curl -LsSf https://astral.sh/uv/install.sh | sh")
        return False
    
    # Check if pyproject.toml exists
    print("📋 Checking project configuration...")
    if not Path('pyproject.toml').exists():
        print("❌ Error: pyproject.toml not found. Please run this script from the project root.")
        return False
    print("✅ pyproject.toml found")
    
    # Check if .tvenv directory exists
    tvenv_path = Path('.tvenv')
    if tvenv_path.exists():
        print("📋 Found existing .tvenv test environment")
    else:
        print("📋 Creating new .tvenv test environment")
        # Create the virtual environment
        if not run_command(['uv', 'venv', '.tvenv', '--python', '3.13'], "Creating .tvenv virtual environment"):
            return False
    
    # Prepare environment variables for uv sync and test run
    env = os.environ.copy()
    env['VIRTUAL_ENV'] = str(tvenv_path.resolve())
    env['PATH'] = str(tvenv_path.resolve() / 'bin') + os.pathsep + env['PATH']
    
    # Sync dependencies to the .tvenv environment using --active
    print("📦 Installing test dependencies with uv...")
    if not run_command(['uv', 'sync', '--dev', '--all-extras', '--active'], "Installing test dependencies with uv", env=env):
        return False
    
    print("✅ uv test environment setup completed successfully!")
    return True


def cleanup_uv_environment():
    """Clean up uv test environment after tests."""
    print("\n🧹 Cleaning up uv test environment...")
    
    tvenv_path = Path('.tvenv')
    if tvenv_path.exists():
        try:
            shutil.rmtree(tvenv_path)
            print("✅ Removed .tvenv test environment")
        except Exception as e:
            print(f"⚠️  Warning: Could not remove .tvenv test environment: {e}")
    else:
        print("ℹ️  No .tvenv test environment found to clean up")
    
    print("✅ uv test environment cleanup completed!")


def main():
    """Main test runner function."""
    parser = argparse.ArgumentParser(
        description="Test runner for ADOC Migration Toolkit",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all tests with uv test environment
  python run_tests.py

  # Run only shared module tests
  python run_tests.py --shared-only

  # Run tests with coverage
  python run_tests.py --coverage

  # Run tests with verbose output
  python run_tests.py --verbose

  # Run specific test file
  python run_tests.py --file tests/test_shared_api_client.py

  # Run tests and generate HTML report
  python run_tests.py --coverage --html-report

  # Run tests excluding slow tests
  python run_tests.py --exclude-slow

  # Run integration tests only
  python run_tests.py --integration-only

  # Skip environment setup (if already set up)
  python run_tests.py --no-setup

  # Keep test environment after tests (don't clean up .tvenv)
  python run_tests.py --keep-env
        """
    )
    
    parser.add_argument(
        '--shared-only',
        action='store_true',
        help='Run only shared module tests'
    )
    
    parser.add_argument(
        '--coverage',
        action='store_true',
        help='Run tests with coverage reporting'
    )
    
    parser.add_argument(
        '--html-report',
        action='store_true',
        help='Generate HTML coverage report'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Run tests with verbose output'
    )
    
    parser.add_argument(
        '--file',
        type=str,
        help='Run specific test file'
    )
    
    parser.add_argument(
        '--exclude-slow',
        action='store_true',
        help='Exclude slow tests'
    )
    
    parser.add_argument(
        '--integration-only',
        action='store_true',
        help='Run only integration tests'
    )
    
    parser.add_argument(
        '--unit-only',
        action='store_true',
        help='Run only unit tests (exclude integration tests)'
    )
    
    parser.add_argument(
        '--clean',
        action='store_true',
        help='Clean up test artifacts before running'
    )
    
    parser.add_argument(
        '--no-setup',
        action='store_true',
        help='Skip environment setup (assumes .tvenv test environment is ready)'
    )
    
    parser.add_argument(
        '--keep-env',
        action='store_true',
        help='Keep test environment after tests (don\'t clean up .tvenv)'
    )
    
    args = parser.parse_args()
    
    # Check if we're in the right directory
    if not Path('pyproject.toml').exists():
        print("❌ Error: pyproject.toml not found. Please run this script from the project root.")
        sys.exit(1)
    
    # Set up uv test environment unless --no-setup is specified
    if not args.no_setup:
        if not setup_uv_environment():
            sys.exit(1)
    
    # Clean up test artifacts if requested
    if args.clean:
        print("Cleaning up test artifacts...")
        cleanup_items = ['htmlcov', '.coverage', '.pytest_cache', '__pycache__', 'tests/output']
        for item_name in cleanup_items:
            item_path = Path(item_name)
            if item_path.exists():
                try:
                    if item_path.is_file():
                        item_path.unlink()
                        print(f"Removed file {item_name}")
                    else:
                        shutil.rmtree(item_path)
                        print(f"Removed directory {item_name}")
                except Exception as e:
                    print(f"Warning: Could not remove {item_name}: {e}")
    
    # Prepare environment variables for test run
    tvenv_path = Path('.tvenv')
    env = os.environ.copy()
    env['VIRTUAL_ENV'] = str(tvenv_path.resolve())
    env['PATH'] = str(tvenv_path.resolve() / 'bin') + os.pathsep + env['PATH']

    # Build pytest command using the .tvenv Python interpreter
    cmd = ['.tvenv/bin/python', '-m', 'pytest']

    # Add test paths
    if args.file:
        cmd.append(args.file)
    elif args.shared_only:
        cmd.append('tests/test_shared/')
    elif args.integration_only:
        cmd.append('tests/test_shared/test_integration.py')
    elif args.unit_only:
        cmd.extend([
            'tests/test_shared/test_api_client.py',
            'tests/test_shared/test_logging.py',
            'tests/test_shared/test_file_utils.py',
            'tests/test_shared/test_globals.py'
        ])
    else:
        cmd.append('tests/')

    # Add pytest options
    if args.verbose:
        cmd.append('-v')

    if args.coverage:
        cmd.extend([
            '--cov=src/adoc_migration_toolkit',
            '--cov-report=term-missing',
            '--cov-report=xml:tests/output/coverage.xml'
        ])
        
        if args.html_report:
            cmd.append('--cov-report=html:tests/output/htmlcov')

    if args.exclude_slow:
        cmd.append('-m "not slow"')

    # Run the tests
    success = run_command(cmd, "Running tests", env=env)

    # Clean up test artifacts created during testing
    print("\n🧹 Cleaning up test artifacts created during testing...")
    
    # Clean up pytest artifacts
    pytest_artifacts = ['.pytest_cache', 'htmlcov', '.coverage']
    for artifact in pytest_artifacts:
        artifact_path = Path(artifact)
        if artifact_path.exists():
            try:
                if artifact_path.is_file():
                    artifact_path.unlink()
                    print(f"✅ Removed pytest artifact: {artifact}")
                else:
                    shutil.rmtree(artifact_path)
                    print(f"✅ Removed pytest artifact: {artifact}")
            except Exception as e:
                print(f"⚠️  Warning: Could not remove pytest artifact {artifact}: {e}")
    
    # Clean up application-generated directories (like adoc-migration-toolkit-202507*)
    import glob
    app_dirs = glob.glob('adoc-migration-toolkit-*')
    for app_dir in app_dirs:
        app_dir_path = Path(app_dir)
        if app_dir_path.is_dir():
            try:
                shutil.rmtree(app_dir_path)
                print(f"✅ Removed application directory: {app_dir}")
            except Exception as e:
                print(f"⚠️  Warning: Could not remove application directory {app_dir}: {e}")
    
    # Clean up any other common test artifacts
    other_artifacts = ['__pycache__', '*.pyc', '*.pyo', '*.pyd']
    for pattern in other_artifacts:
        if '*' in pattern:
            # Handle glob patterns
            for artifact in glob.glob(pattern):
                artifact_path = Path(artifact)
                if artifact_path.exists():
                    try:
                        if artifact_path.is_file():
                            artifact_path.unlink()
                            print(f"✅ Removed artifact: {artifact}")
                        else:
                            shutil.rmtree(artifact_path)
                            print(f"✅ Removed artifact: {artifact}")
                    except Exception as e:
                        print(f"⚠️  Warning: Could not remove artifact {artifact}: {e}")
        else:
            # Handle single file/directory
            artifact_path = Path(pattern)
            if artifact_path.exists():
                try:
                    if artifact_path.is_file():
                        artifact_path.unlink()
                        print(f"✅ Removed artifact: {pattern}")
                    else:
                        shutil.rmtree(artifact_path)
                        print(f"✅ Removed artifact: {pattern}")
                except Exception as e:
                    print(f"⚠️  Warning: Could not remove artifact {pattern}: {e}")
    
    print("✅ Test artifact cleanup completed!")

    # Clean up uv test environment unless --keep-env is specified
    if not args.keep_env and not args.no_setup:
        cleanup_uv_environment()

    if success:
        print("\n🎉 All tests completed successfully!")
        
        if args.coverage and args.html_report:
            print("\n📊 Coverage report generated:")
            print("   - HTML report: tests/output/htmlcov/index.html")
            print("   - XML report: tests/output/coverage.xml")
        
        return 0
    else:
        print("\n💥 Some tests failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main()) 