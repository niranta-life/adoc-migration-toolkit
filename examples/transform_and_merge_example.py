#!/usr/bin/env python3
"""
Example script demonstrating the transform-and-merge command.

This script shows how to use the new transform-and-merge command to:
1. Read asset-all-source-export.csv and asset-all-target-export.csv
2. Apply string transformations to target_uid column
3. Merge records based on transformed UIDs
4. Generate asset-merged-export.csv with combined data

Prerequisites:
- Run 'asset-list-export' to generate asset-all-source-export.csv
- Run 'asset-list-export --target' to generate asset-all-target-export.csv
"""

import csv
import logging
import shutil

# Mock the necessary modules for demonstration
import sys
import tempfile
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from adoc_migration_toolkit.execution.asset_operations import (
    execute_transform_and_merge,
)
from adoc_migration_toolkit.shared import globals


def create_sample_csv_files(asset_export_dir: Path):
    """Create sample CSV files for demonstration."""

    # Create source CSV file (from source environment)
    source_file = asset_export_dir / "asset-all-source-export.csv"
    with open(source_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["source_uid", "source_id", "target_uid", "tags"])
        writer.writerow(
            ["prod-db-1", "1001", "PROD_DB.prod-db-1", "database:production"]
        )
        writer.writerow(
            ["prod-db-2", "1002", "PROD_DB.prod-db-2", "database:production"]
        )
        writer.writerow(["prod-api-1", "2001", "PROD_API.prod-api-1", "api:production"])
        writer.writerow(["prod-web-1", "3001", "PROD_WEB.prod-web-1", "web:production"])

    # Create target CSV file (from target environment)
    target_file = asset_export_dir / "asset-all-target-export.csv"
    with open(target_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["source_uid", "source_id", "target_uid", "tags"])
        writer.writerow(
            ["DEV_DB.dev-db-1", "101", "DEV_DB.dev-db-1", "database:development"]
        )
        writer.writerow(
            ["DEV_DB.dev-db-2", "102", "DEV_DB.dev-db-2", "database:development"]
        )
        writer.writerow(
            ["DEV_API.dev-api-1", "201", "DEV_API.dev-api-1", "api:development"]
        )
        writer.writerow(
            ["DEV_WEB.dev-web-1", "301", "DEV_WEB.dev-web-1", "web:development"]
        )

    print(f"✅ Created sample CSV files in {asset_export_dir}")
    print(f"📄 Source file: {source_file}")
    print(f"📄 Target file: {target_file}")


def demonstrate_transform_and_merge():
    """Demonstrate the transform-and-merge functionality."""

    # Create temporary directory for demonstration
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create asset-export directory structure
        asset_export_dir = temp_path / "asset-export"
        asset_export_dir.mkdir(parents=True, exist_ok=True)

        # Create sample CSV files
        create_sample_csv_files(asset_export_dir)

        # Set up logging
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

        # Mock global output directory
        globals.GLOBAL_OUTPUT_DIR = temp_path

        print("\n" + "=" * 80)
        print("TRANSFORM AND MERGE DEMONSTRATION")
        print("=" * 80)

        # Define string transformations
        string_transforms = {
            "PROD_DB": "DEV_DB",
            "PROD_API": "DEV_API",
            "PROD_WEB": "DEV_WEB",
        }

        print("🔄 String transformations:")
        for source, target in string_transforms.items():
            print(f"  '{source}' -> '{target}'")

        print("\n🚀 Executing transform-and-merge...")

        # Execute transform-and-merge
        execute_transform_and_merge(string_transforms, False, True, logger)

        # Display results
        asset_import_dir = asset_export_dir.parent / "asset-import"
        output_file = asset_import_dir / "asset-merged-all.csv"
        if output_file.exists():
            print(f"\n📊 Results from {output_file}:")
            print("-" * 80)
            with open(output_file, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader, 1):
                    print(
                        f"{i}. Source: {row['source_uid']} -> Target: {row['target_uid']}"
                    )
                    print(
                        f"   Source ID: {row['source_id']}, Target ID: {row['target_id']}"
                    )
                    print(f"   Tags: {row['tags']}")
                    print()
        else:
            print("❌ Output file was not created")


if __name__ == "__main__":
    demonstrate_transform_and_merge()
