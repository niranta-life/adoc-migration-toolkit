# ADOC Migration Toolkit Build Guide

This guide covers building standalone executables with PyInstaller.

## 🚀 Quick Start

### Prerequisites
- Python 3.8 or higher
- pip package manager
- Virtual environment (recommended)

### Basic Build
```bash
# Install dependencies and build
make build

# Or use the build script directly
python build_with_integrity.py --onefile --console --clean
```

## 📦 Build Options

### Onefile vs Onedir
```bash
# Single executable file (recommended for distribution)
python build_with_integrity.py --onefile --console --clean

# Directory with executable and dependencies
python build_with_integrity.py --onedir --console --clean

# Windowed executable (no console)
python build_with_integrity.py --onefile --windowed --clean

# Without wrapper script
python build_with_integrity.py --onefile --console --clean --no-wrapper
```

### Get Help
```bash
python build_with_integrity.py --help
```

## 🛠️ Build System Features

### Core Features
- **PyInstaller-based builds** - Reliable executable creation
- **Cross-platform support** - Windows, macOS, Linux
- **Version embedding** - Automatic version info from pyproject.toml
- **Wrapper scripts** - Easy execution on all platforms
- **Optimized builds** - Excludes unnecessary modules
- **Multiprocessing support** - Proper freeze support for parallel operations

### Build Optimizations
- Excludes large libraries (matplotlib, numpy, pandas, etc.)
- Includes only required hidden imports
- UPX compression for smaller executables
- Universal2 builds for macOS

## 📁 Build Output

### Onefile Build Output
```
dist/
├── adoc-migration-toolkit-1.0.0      # Main executable
├── adoc-migration-toolkit            # Wrapper script (Unix)
├── adoc-migration-toolkit.bat        # Wrapper script (Windows)
├── README.md                         # Documentation
└── LICENSE                           # License file
```

### Onedir Build Output
```
dist/
├── adoc-migration-toolkit-1.0.0/     # Executable directory
│   ├── adoc-migration-toolkit-1.0.0  # Main executable
│   └── [dependencies...]             # PyInstaller dependencies
├── adoc-migration-toolkit            # Wrapper script (Unix)
├── adoc-migration-toolkit.bat        # Wrapper script (Windows)
├── README.md                         # Documentation
└── LICENSE                           # License file
```

## 🔧 Configuration

### Customizing the Build

Edit the `create_spec_file()` method in `build_with_integrity.py`:

```python
# Add custom data files
datas = [
    ('config/config.env.example', 'config'),
    ('README.md', '.'),
    ('LICENSE', '.'),
    # Add your custom files here
]

# Add custom hidden imports
hiddenimports = [
    'adoc_migration_toolkit',
    'click',
    'requests',
    # Add your custom imports here
]

# Exclude unnecessary modules
excludes = [
    'matplotlib',
    'numpy',
    # Add modules to exclude
]
```

### Environment Variables
The build system respects standard PyInstaller environment variables:
- `PYTHONPATH` - Additional Python paths
- `PYINSTALLER_OPTS` - Additional PyInstaller options

## 🚀 Advanced Usage

### Makefile Targets
```bash
# Standard builds
make build                    # Default onefile build
make build-onefile           # Explicit onefile build
make build-onedir            # Directory build
make build-windowed          # Windowed build
make build-no-wrapper        # Without wrapper script

# Platform-specific builds
make build-linux             # Linux build
make build-macos             # macOS build
make build-windows           # Windows build

# Development
make dev-install             # Install development dependencies
make dev-test                # Run tests with coverage
make quick-build             # Fast development build

# Release
make release                 # Create release package
make rebuild                 # Clean and rebuild
```

### Command Line Options
```bash
# Build options
--onefile                    # Single executable file
--onedir                     # Directory with dependencies
--console                    # Include console window
--windowed                   # Hide console window
--clean                      # Clean before building
--no-wrapper                 # Skip wrapper script

# Information
--help                       # Show help
--version                    # Show version
```

## 🔍 Troubleshooting

### Common Issues

#### Build Failures
```bash
# Clean and rebuild
make clean && make build

# Check dependencies
make install-deps

# Verbose build
python build_with_integrity.py --onefile --console --clean
```

#### Import Errors
- Check that all required modules are in `hiddenimports`
- Verify that excluded modules aren't needed
- Use `--onedir` for debugging import issues

#### Large Executable Size
- Check `excludes` list for unnecessary modules
- Use UPX compression (enabled by default)
- Consider using `--onedir` for development

#### Platform-Specific Issues
- Build on the target platform when possible
- Use virtual environments to avoid conflicts
- Check platform-specific dependencies

### Debugging
```bash
# Show build information
make info

# Check PyInstaller version
python -c "import PyInstaller; print(PyInstaller.__version__)"

# Test the built executable
./dist/adoc-migration-toolkit-1.0.0 --help
```

### Getting Help
- Check the [troubleshooting section](#-troubleshooting)
- Review build logs in `build/` directory
- Test the executable manually

### Contributing
- Follow the existing code style
- Test builds on multiple platforms
- Update this documentation for new features

---

**Note:** This build system is designed for reliability and cross-platform compatibility. Always test builds on target platforms before distribution. 