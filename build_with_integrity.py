#!/usr/bin/env python3
"""
Build script for ADOC Migration Toolkit with PyInstaller.

This script builds a standalone executable using PyInstaller.

Features:
- PyInstaller-based executable creation
- Cross-platform builds
- Automatic version embedding
- Wrapper script generation
- Versioned binary naming
"""

import argparse
import os
import platform
import shutil
import subprocess
import sys
import tomllib
from datetime import datetime
from pathlib import Path


class VersionManager:
    """Manages version information from pyproject.toml."""

    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.pyproject_path = project_root / "pyproject.toml"
        self.version = self._extract_version()

    def _extract_version(self) -> str:
        """Extract version from pyproject.toml."""
        try:
            with open(self.pyproject_path, "rb") as f:
                data = tomllib.load(f)
                return data.get("project", {}).get("version", "1.0.0")
        except Exception as e:
            print(f"⚠️  Warning: Could not extract version from pyproject.toml: {e}")
            return "1.0.0"

    def get_version_info(self) -> dict:
        """Get version information for embedding."""
        return {
            "version": self.version,
            "build_date": datetime.now().isoformat(),
            "platform": platform.system(),
            "architecture": platform.machine(),
            "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        }


class SimpleBuilder:
    """Simple PyInstaller builder without integrity checks."""

    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.dist_dir = self.project_root / "dist"
        self.build_dir = self.project_root / "build"
        self.spec_file = self.project_root / "adoc_migration_toolkit.spec"
        self.version_manager = VersionManager(self.project_root)
        self.version = self.version_manager.get_version_info()

    def clean_build_dirs(self) -> None:
        """Clean previous build artifacts."""
        print("🧹 Cleaning build directories...")
        for dir_path in [self.dist_dir, self.build_dir]:
            if dir_path.exists():
                shutil.rmtree(dir_path)
                print(f"   Removed {dir_path}")

        # Clean spec file
        if self.spec_file.exists():
            self.spec_file.unlink()
            print(f"   Removed {self.spec_file}")

    def install_dependencies(self) -> None:
        """Install PyInstaller and other build dependencies."""
        print("📦 Checking build dependencies...")

        dependencies = [
            "pyinstaller>=5.13.0",
            "pyinstaller-hooks-contrib>=2023.0",
        ]

        # Check if dependencies are already installed
        missing_deps = []
        for dep in dependencies:
            try:
                if dep.startswith("pyinstaller"):
                    import PyInstaller

                    print(
                        f"   ✅ PyInstaller {PyInstaller.__version__} already installed"
                    )
                else:
                    # For other dependencies, try to import them
                    module_name = dep.split(">=")[0].split("==")[0]
                    __import__(module_name)
                    print(f"   ✅ {dep} already installed")
            except ImportError:
                missing_deps.append(dep)
                print(f"   ⚠️  {dep} not found")

        if not missing_deps:
            print("   ✅ All dependencies are available")
            return

        print("📦 Installing missing dependencies...")

        # Try to install missing dependencies
        for dep in missing_deps:
            try:
                # First try with python -m pip
                subprocess.run(
                    [sys.executable, "-m", "pip", "install", dep],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                print(f"   ✅ Installed {dep}")
            except subprocess.CalledProcessError as e:
                print(f"   Failed to install {dep}: {e}")
                print("   Trying alternative installation method...")
                try:
                    # Try using pip directly if available
                    subprocess.run(
                        ["pip", "install", dep],
                        check=True,
                        capture_output=True,
                        text=True,
                    )
                    print(f"   ✅ Installed {dep} via direct pip")
                except (subprocess.CalledProcessError, FileNotFoundError):
                    print(f"   ❌ Could not install {dep}")
                    print(f"   Please install manually: pip install {dep}")
                    print(f"   Or activate your virtual environment and try again")
                    raise

    def create_version_file(self) -> Path:
        """Create a version file for embedding in the executable."""
        print("📋 Creating version file...")

        version_file = self.project_root / "version_info.txt"
        version_info = self.version_manager.get_version_info()

        version_content = f"""VSVersionInfo(
  ffi=FixedFileInfo(
    filevers=({version_info['version'].replace('.', ', ')}, 0, 0, 0),
    prodvers=({version_info['version'].replace('.', ', ')}, 0, 0, 0),
    mask=0x3f,
    flags=0x0,
    OS=0x40004,
    fileType=0x1,
    subtype=0x0,
    date=(0, 0)
  ),
  kids=[
    StringFileInfo([
      StringTable(
        u'040904B0',
        [StringStruct(u'CompanyName', u'Acceldata'),
         StringStruct(u'FileDescription', u'ADOC Migration Toolkit'),
         StringStruct(u'FileVersion', u'{version_info["version"]}'),
         StringStruct(u'InternalName', u'adoc-migration-toolkit'),
         StringStruct(u'LegalCopyright', u'Copyright (c) 2024 Acceldata'),
         StringStruct(u'OriginalFilename', u'adoc-migration-toolkit.exe'),
         StringStruct(u'ProductName', u'ADOC Migration Toolkit'),
         StringStruct(u'ProductVersion', u'{version_info["version"]}')])
    ]),
    VarFileInfo([VarStruct(u'Translation', [1033, 1200])])
  ]
)"""

        with open(version_file, "w") as f:
            f.write(version_content)

        print(f"   Created version file: {version_file}")
        return version_file

    def create_spec_file(self, onefile: bool = True, console: bool = True) -> None:
        """Create PyInstaller spec file."""
        print("📝 Creating PyInstaller spec file...")

        # Create version file
        version_file = self.create_version_file()

        # Determine target architecture for macOS
        target_arch = None
        if platform.system() == "Darwin":
            target_arch = "universal2"

        spec_content = f"""# -*- mode: python ; coding: utf-8 -*-

import os
import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(".").resolve()
sys.path.insert(0, str(project_root / "src"))

# Data files to include
datas = [
    ('config/config.env.example', 'config'),
    ('README.md', '.'),
    ('LICENSE', '.'),
]

# Hidden imports
hiddenimports = [
    'adoc_migration_toolkit',
    'adoc_migration_toolkit.cli',
    'adoc_migration_toolkit.core',
    'adoc_migration_toolkit.execution',
    'adoc_migration_toolkit.shared',
    'adoc_migration_toolkit.vcs',
    'click',
    'requests',
    'tqdm',
    'keyring',
    'json',
    'pathlib',
    'typing',
    'multiprocessing',
    'multiprocessing.pool',
    'multiprocessing.managers',
    'multiprocessing.synchronize',
    'multiprocessing.heap',
    'multiprocessing.resource_tracker',
    'multiprocessing.spawn',
    'multiprocessing.fork',
    'multiprocessing.forkserver',
]

# Exclude unnecessary modules to reduce size
excludes = [
    'matplotlib',
    'numpy',
    'pandas',
    'scipy',
    'PIL',
    'tkinter',
    'PyQt5',
    'PyQt6',
    'PySide2',
    'PySide6',
    'wx',
    'IPython',
    'jupyter',
    'notebook',
    'sphinx',
    'docutils',
    'pytest',
    'unittest',
    'test',
    'tests',
    'setuptools',
    'distutils',
    'pkg_resources',
]

# Runtime hooks for multiprocessing
runtime_hooks = []

a = Analysis(
    ['src/adoc_migration_toolkit/__main__.py'],
    pathex=[str(project_root)],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={{}},
    runtime_hooks=runtime_hooks,
    excludes=excludes,
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=None,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=None)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='adoc-migration-toolkit-{self.version["version"]}',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console={console},
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch={repr(target_arch)},
    codesign_identity=None,
    entitlements_file=None,
    icon=None,
    version_file='{version_file}',
    onefile={onefile},
)
"""

        with open(self.spec_file, "w") as f:
            f.write(spec_content)

        print(f"   Created spec file: {self.spec_file}")

    def build_executable(self, onefile: bool = True, console: bool = True) -> bool:
        """Build the executable using PyInstaller."""
        print("🔨 Building executable with PyInstaller...")

        try:
            # Create spec file
            self.create_spec_file(onefile=onefile, console=console)

            # Run PyInstaller with enhanced options
            cmd = [
                sys.executable,
                "-m",
                "PyInstaller",
                "--clean",
                "--noconfirm",
                "--log-level=INFO",
                str(self.spec_file),
            ]

            result = subprocess.run(cmd, cwd=self.project_root, check=True)

            if result.returncode == 0:
                print("✅ Executable built successfully")
                return True
            else:
                print("❌ Build failed")
                return False

        except subprocess.CalledProcessError as e:
            print(f"❌ Build failed with error: {e}")
            return False

    def create_wrapper_script(self) -> None:
        """Create a wrapper script for easier execution."""
        print("📜 Creating wrapper script...")

        # Look for the executable
        executable_name = f"adoc-migration-toolkit-{self.version['version']}"
        executable_path = self.dist_dir / executable_name

        if platform.system() == "Windows":
            executable_path = executable_path.with_suffix(".exe")

        if not executable_path.exists():
            # Fallback to non-versioned name
            fallback_path = self.dist_dir / "adoc-migration-toolkit"
            if platform.system() == "Windows":
                fallback_path = fallback_path.with_suffix(".exe")

            if fallback_path.exists():
                executable_path = fallback_path
            else:
                print("❌ Executable not found for wrapper script")
                return

        if platform.system() == "Windows":
            # Create batch file wrapper
            wrapper_content = f"""@echo off
REM Wrapper script for ADOC Migration Toolkit
REM Version: {self.version['version']}

set SCRIPT_DIR=%~dp0
set EXECUTABLE="%SCRIPT_DIR%{executable_path.name}"

if not exist %EXECUTABLE% (
    echo Error: Executable not found: %EXECUTABLE%
    exit /b 1
)

%EXECUTABLE% %*
"""
            wrapper_path = self.dist_dir / "adoc-migration-toolkit.bat"
        else:
            # Create shell script wrapper
            wrapper_content = f"""#!/bin/bash
# Wrapper script for ADOC Migration Toolkit
# Version: {self.version['version']}

SCRIPT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"
EXECUTABLE="$SCRIPT_DIR/{executable_path.name}"

if [ ! -f "$EXECUTABLE" ]; then
    echo "Error: Executable not found: $EXECUTABLE"
    exit 1
fi

# Make executable if needed
chmod +x "$EXECUTABLE" 2>/dev/null

# Run the executable with all arguments
exec "$EXECUTABLE" "$@"
"""
            wrapper_path = self.dist_dir / "adoc-migration-toolkit"

        with open(wrapper_path, "w") as f:
            f.write(wrapper_content)

        # Make wrapper executable on Unix systems
        if platform.system() != "Windows":
            os.chmod(wrapper_path, 0o755)

        print(f"   Created wrapper script: {wrapper_path}")

    def build(
        self, onefile: bool = True, console: bool = True, create_wrapper: bool = True
    ) -> bool:
        """Complete build process."""
        print("🚀 Starting build...")
        print(f"📦 Version: {self.version['version']}")
        print(f"🖥️  Platform: {self.version['platform']}")
        print(f"🏗️  Architecture: {self.version['architecture']}")

        try:
            # Clean previous builds
            self.clean_build_dirs()

            # Install dependencies
            self.install_dependencies()

            # Build executable
            if not self.build_executable(onefile=onefile, console=console):
                return False

            # Create wrapper script if requested
            if create_wrapper:
                self.create_wrapper_script()

            # Clean up temporary files
            version_file = self.project_root / "version_info.txt"
            if version_file.exists():
                version_file.unlink()
                print("   Cleaned up temporary version file")

            print("🎉 Build completed successfully!")
            print(f"📁 Executable location: {self.dist_dir}")

            # Show build summary
            self._show_build_summary()

            return True

        except Exception as e:
            print(f"❌ Build failed: {e}")
            import traceback

            traceback.print_exc()
            return False

    def _show_build_summary(self) -> None:
        """Show a summary of the build results."""
        print("\n📊 Build Summary:")
        print("=================")

        if self.dist_dir.exists():
            print(f"📁 Distribution directory: {self.dist_dir}")

            # List all files in dist directory
            for item in sorted(self.dist_dir.iterdir()):
                if item.is_file():
                    size = item.stat().st_size
                    print(f"   📄 {item.name} ({size:,} bytes)")
                elif item.is_dir():
                    print(f"   📁 {item.name}/")

        # Show executable info
        executable_name = f"adoc-migration-toolkit-{self.version['version']}"
        executable_path = self.dist_dir / executable_name

        if platform.system() == "Windows":
            executable_path = executable_path.with_suffix(".exe")

        if executable_path.exists():
            size = executable_path.stat().st_size
            print(f"\n🎯 Main executable: {executable_path.name}")
            print(f"   Size: {size:,} bytes ({size / (1024*1024):.1f} MB)")
            print(f"   Version: {self.version['version']}")
        else:
            print("\n⚠️  Main executable not found")


def main():
    """Main function for the build script."""
    parser = argparse.ArgumentParser(
        description="Build ADOC Migration Toolkit with PyInstaller"
    )
    parser.add_argument(
        "--onefile",
        action="store_true",
        default=True,
        help="Build as single executable file (default: True)",
    )
    parser.add_argument(
        "--onedir",
        action="store_true",
        help="Build as directory with executable and dependencies",
    )
    parser.add_argument(
        "--console",
        action="store_true",
        default=True,
        help="Include console window (default: True)",
    )
    parser.add_argument(
        "--windowed",
        action="store_true",
        help="Hide console window (Windows/macOS only)",
    )
    parser.add_argument(
        "--clean", action="store_true", help="Clean build directories before building"
    )
    parser.add_argument(
        "--no-wrapper", action="store_true", help="Skip creating wrapper script"
    )
    parser.add_argument("--version", action="version", version="%(prog)s 1.0.0")

    args = parser.parse_args()

    # Determine build options
    onefile = args.onefile and not args.onedir
    console = args.console and not args.windowed
    create_wrapper = not args.no_wrapper

    # Get project root
    project_root = Path(__file__).parent

    # Create builder
    builder = SimpleBuilder(project_root)

    # Clean if requested
    if args.clean:
        builder.clean_build_dirs()

    # Build
    success = builder.build(
        onefile=onefile, console=console, create_wrapper=create_wrapper
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
