# Makefile for ADOC Migration Toolkit with PyInstaller

.PHONY: help build build-onefile build-onedir clean install-deps test info release

# Default target
help:
	@echo "ADOC Migration Toolkit Build System"
	@echo "==================================="
	@echo ""
	@echo "Available targets:"
	@echo "  build          - Build executable (onefile)"
	@echo "  build-onefile  - Build as single executable file"
	@echo "  build-onedir   - Build as directory with executable and dependencies"
	@echo "  build-windowed - Build windowed executable (no console)"
	@echo "  build-no-wrapper - Build without wrapper script"
	@echo "  clean          - Clean build artifacts"
	@echo "  install-deps   - Install build dependencies"
	@echo "  test           - Run tests"
	@echo "  info           - Show build information"
	@echo "  release        - Create release package"
	@echo "  help           - Show this help message"
	@echo ""
	@echo "Examples:"
	@echo "  make build                    # Build with default settings"
	@echo "  make build-onefile            # Build with console window"
	@echo "  make build-windowed           # Build without console window"
	@echo "  make build-no-wrapper         # Build without wrapper script"
	@echo "  make clean && make build      # Clean build"
	@echo "  make release                  # Create release package"

# Install build dependencies
install-deps:
	@echo "📦 Installing build dependencies..."
	@# Try to upgrade pip, but don't fail if it's not available
	@$(shell which python3 || which python) -m pip install --upgrade pip 2>/dev/null || echo "⚠️  Could not upgrade pip, continuing..."
	@# Install PyInstaller and dependencies
	@$(shell which python3 || which python) -m pip install "pyinstaller>=5.13.0" "pyinstaller-hooks-contrib>=2023.0" || \
		(echo "❌ Failed to install PyInstaller. Trying alternative installation..." && \
		 $(shell which python3 || which python) -m ensurepip --upgrade && \
		 $(shell which python3 || which python) -m pip install "pyinstaller>=5.13.0" "pyinstaller-hooks-contrib>=2023.0")
	@echo "✅ Dependencies installed"

# Clean build artifacts
clean:
	@echo "🧹 Cleaning build artifacts..."
	rm -rf build/
	rm -rf dist/
	rm -f *.spec
	rm -f version_info.txt
	rm -rf adoc-migration-toolkit*
	@echo "✅ Cleaned build artifacts"

# Build (default: onefile)
build: install-deps
	@echo "🚀 Building executable..."
	python build_with_integrity.py --onefile --console --clean
	@echo "✅ Build completed"

# Build as single executable file
build-onefile: install-deps
	@echo "📦 Building single executable file..."
	python build_with_integrity.py --onefile --console --clean
	@echo "✅ Single file build completed"

# Build as directory with executable and dependencies
build-onedir: install-deps
	@echo "📁 Building directory with executable and dependencies..."
	python build_with_integrity.py --onedir --console --clean
	@echo "✅ Directory build completed"

# Build windowed executable (no console)
build-windowed: install-deps
	@echo "🪟 Building windowed executable..."
	python build_with_integrity.py --onefile --windowed --clean
	@echo "✅ Windowed build completed"

# Build without wrapper script
build-no-wrapper: install-deps
	@echo "🔧 Building without wrapper script..."
	python build_with_integrity.py --onefile --console --clean --no-wrapper
	@echo "✅ Build without wrapper completed"

# Run tests
test:
	@echo "🧪 Running tests..."
	python -m pytest tests/ -v
	@echo "✅ Tests completed"

# Build for different platforms
build-linux: install-deps
	@echo "🐧 Building for Linux..."
	python build_with_integrity.py --onefile --console --clean
	@echo "✅ Linux build completed"

build-macos: install-deps
	@echo "🍎 Building for macOS..."
	python build_with_integrity.py --onefile --console --clean
	@echo "✅ macOS build completed"

build-windows: install-deps
	@echo "🪟 Building for Windows..."
	python build_with_integrity.py --onefile --console --clean
	@echo "✅ Windows build completed"

# Development targets
dev-install:
	@echo "🔧 Installing development dependencies..."
	python -m pip install -e .
	python -m pip install -r requirements-dev.txt 2>/dev/null || echo "No requirements-dev.txt found"
	@echo "✅ Development environment ready"

dev-test:
	@echo "🧪 Running development tests..."
	python -m pytest tests/ -v --cov=src/adoc_migration_toolkit --cov-report=html
	@echo "✅ Development tests completed"

# Release targets
release-build: clean install-deps
	@echo "🚀 Building release version..."
	python build_with_integrity.py --onefile --console --clean
	@echo "✅ Release build completed"

release-package: release-build
	@echo "📦 Creating release package..."
	@mkdir -p release
	@# Copy versioned executables
	@cp dist/adoc-migration-toolkit-* release/ 2>/dev/null || echo "No versioned executables found"
	@# Copy non-versioned executables as fallback
	@cp dist/adoc-migration-toolkit* release/ 2>/dev/null || echo "No executables found"
	@# Copy wrapper scripts
	@cp dist/adoc-migration-toolkit.bat release/ 2>/dev/null || echo "No Windows wrapper found"
	@cp dist/adoc-migration-toolkit release/ 2>/dev/null || echo "No Unix wrapper found"
	@# Copy documentation
	@cp README.md LICENSE release/ 2>/dev/null || echo "No README or LICENSE found"
	@# Create release info
	@echo "Release created on $(shell date)" > release/RELEASE_INFO.txt
	@echo "Platform: $(shell python -c 'import platform; print(platform.system())')" >> release/RELEASE_INFO.txt
	@echo "Architecture: $(shell python -c 'import platform; print(platform.machine())')" >> release/RELEASE_INFO.txt
	@echo "✅ Release package created in release/ directory"
	@echo "📁 Release contents:"
	@ls -la release/

# Show build information
info:
	@echo "📊 Build Information"
	@echo "==================="
	@echo "Python version: $(shell python --version)"
	@echo "Platform: $(shell python -c 'import platform; print(platform.system())')"
	@echo "Architecture: $(shell python -c 'import platform; print(platform.machine())')"
	@echo "PyInstaller: $(shell python -c 'import PyInstaller; print(PyInstaller.__version__)' 2>/dev/null || echo 'Not installed')"
	@echo ""
	@echo "Project version: $(shell python -c 'import tomllib; print(tomllib.load(open("pyproject.toml", "rb"))["project"]["version"])' 2>/dev/null || echo 'Unknown')"
	@echo ""
	@echo "Project structure:"
	@ls -la src/adoc_migration_toolkit/
	@echo ""
	@if [ -d "dist" ]; then \
		echo "Build artifacts:"; \
		ls -la dist/; \
		echo ""; \
		echo "Executable details:"; \
		for exe in dist/adoc-migration-toolkit*; do \
			if [ -f "$$exe" ]; then \
				size=$$(stat -f%z "$$exe" 2>/dev/null || stat -c%s "$$exe" 2>/dev/null || echo "unknown"); \
				echo "   $$exe ($$size bytes)"; \
			fi; \
		done; \
	else \
		echo "No build artifacts found"; \
	fi

# Quick build for development
quick-build: install-deps
	@echo "⚡ Quick build for development..."
	python build_with_integrity.py --onefile --console --no-wrapper
	@echo "✅ Quick build completed"

# Build with all features
full-build: install-deps
	@echo "🎯 Full build with all features..."
	python build_with_integrity.py --onefile --console --clean
	@echo "✅ Full build completed"

# Clean and rebuild
rebuild: clean build
	@echo "🔄 Rebuild completed"

# Show help for build script
build-help:
	@echo "🔧 Build script help:"
	python build_with_integrity.py --help 
