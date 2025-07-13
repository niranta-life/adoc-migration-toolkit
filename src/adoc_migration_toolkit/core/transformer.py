"""
Policy transformer for JSON and ZIP file processing.

This module contains the PolicyTransformer class for processing JSON files
and ZIP archives with string replacement functionality.
"""

import argparse
import csv
import json
import logging
import os
import shutil
import sys
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from ..shared import globals


class PolicyTranformer:
    """Professional JSON string replacement tool with comprehensive error handling."""

    def __init__(
        self,
        input_dir: str,
        search_string: str,
        replace_string: str,
        output_dir: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize the PolicyExportFormatter with validation.

        Args:
            input_dir (str): Directory containing JSON files and ZIP files to process
            search_string (str): Substring to search for
            replace_string (str): Substring to replace with
            output_dir (str): Output directory (optional)
            logger (Logger): Logger instance (optional)

        Raises:
            ValueError: If input parameters are invalid
            FileNotFoundError: If input directory doesn't exist
        """
        self.logger = logger or logging.getLogger(__name__)

        # Validate input parameters
        if not input_dir or not input_dir.strip():
            raise ValueError("Input directory cannot be empty")

        if not search_string or not search_string.strip():
            raise ValueError("Search string cannot be empty")

        if replace_string is None:
            raise ValueError("Replace string cannot be None")

        # Setup paths
        self.input_dir = Path(input_dir).resolve()
        self.search_string = search_string.strip()
        self.replace_string = replace_string

        # Validate input directory
        if not self.input_dir.exists():
            raise FileNotFoundError(f"Input directory does not exist: {self.input_dir}")

        if not self.input_dir.is_dir():
            raise ValueError(f"Input path is not a directory: {self.input_dir}")

        # Setup output directory structure
        if output_dir:
            self.base_output_dir = Path(output_dir).resolve()
        else:
            # Use the same logic as other commands to find/create the output directory
            if globals.GLOBAL_OUTPUT_DIR:
                self.base_output_dir = globals.GLOBAL_OUTPUT_DIR
            else:
                from datetime import datetime

                self.base_output_dir = (
                    Path.cwd()
                    / f"adoc-migration-toolkit-{datetime.now().strftime('%Y%m%d%H%M')}"
                )

        # Create organized output directory structure
        self.output_dir = (
            self.base_output_dir / "policy-import"
        )  # For processed ZIP/JSON files
        self.asset_export_dir = (
            self.base_output_dir / "asset-export"
        )  # For asset_uids.csv
        self.policy_export_dir = (
            self.base_output_dir / "policy-export"
        )  # For segmented_spark_uids.csv

        # Create all output directories
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            self.asset_export_dir.mkdir(parents=True, exist_ok=True)
            self.policy_export_dir.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            raise PermissionError(
                f"Permission denied: Cannot create output directories"
            )
        except Exception as e:
            raise RuntimeError(f"Failed to create output directories: {e}")

        # Initialize statistics
        self.stats = {
            "files_investigated": 0,
            "changes_made": 0,
            "json_files_processed": 0,
            "zip_files_processed": 0,
            "errors": [],
            # Policy type statistics
            "segmented_spark_policies": 0,
            "segmented_jdbc_policies": 0,
            "non_segmented_policies": 0,
            "total_policies_processed": 0,
        }

        # Initialize data quality policy extraction tracking
        self.extracted_assets: Set[str] = set()
        self.all_asset_uids: Set[str] = set()  # Track all UIDs without filtering
        self.deep_scan_count: int = 0  # Track how many times deep scan is called

        self.logger.info("PolicyExportFormatter initialized successfully")
        self.logger.info(f"Input directory: {self.input_dir}")
        self.logger.info(f"Output directory (processed files): {self.output_dir}")
        self.logger.info(f"Asset export directory: {self.asset_export_dir}")
        self.logger.info(f"Policy export directory: {self.policy_export_dir}")
        self.logger.info(f"Search string: '{self.search_string}'")
        self.logger.info(f"Replace string: '{self.replace_string}'")

    def extract_data_quality_assets(self, data: Any) -> None:
        """Extract uid and backingAssetId from non-segmented data quality policies.

        Args:
            data: The JSON data to process
        """
        try:
            # Reset per-file statistics
            file_stats = {
                "segmented_spark_policies": 0,
                "segmented_jdbc_policies": 0,
                "non_segmented_policies": 0,
                "total_policies_processed": 0,
            }

            if isinstance(data, list):
                # Process array of policy definitions
                for policy in data:
                    if isinstance(policy, dict):
                        # Extract all UIDs without filtering
                        self._extract_all_assets_from_policy(policy)
                        # Extract filtered UIDs
                        self._extract_from_policy(policy)
                        # Update file stats based on the policy type
                        is_segmented = policy.get("isSegmented", False)
                        engine_type = policy.get("engineType", "")
                        file_stats["total_policies_processed"] += 1

                        if is_segmented and engine_type == "SPARK":
                            file_stats["segmented_spark_policies"] += 1
                        elif is_segmented and engine_type == "JDBC_SQL":
                            file_stats["segmented_jdbc_policies"] += 1
                        elif not is_segmented:
                            file_stats["non_segmented_policies"] += 1
            elif isinstance(data, dict):
                # Process single policy definition
                # Extract all UIDs without filtering
                self._extract_all_assets_from_policy(data)
                # Extract filtered UIDs
                self._extract_from_policy(data)
                # Update file stats
                is_segmented = data.get("isSegmented", False)
                engine_type = data.get("engineType", "")
                file_stats["total_policies_processed"] += 1

                if is_segmented and engine_type == "SPARK":
                    file_stats["segmented_spark_policies"] += 1
                elif is_segmented and engine_type == "JDBC_SQL":
                    file_stats["segmented_jdbc_policies"] += 1
                elif not is_segmented:
                    file_stats["non_segmented_policies"] += 1

            # Log per-file statistics
            if file_stats["total_policies_processed"] > 0:
                self.logger.info(f"File Policy Statistics:")
                self.logger.info(
                    f"  Total policies processed: {file_stats['total_policies_processed']}"
                )
                self.logger.info(
                    f"  Segmented SPARK policies: {file_stats['segmented_spark_policies']}"
                )
                self.logger.info(
                    f"  Segmented JDBC_SQL policies: {file_stats['segmented_jdbc_policies']}"
                )
                self.logger.info(
                    f"  Non-segmented policies: {file_stats['non_segmented_policies']}"
                )

            # Log asset extraction results
            self.logger.info(f"Asset extraction results:")
            self.logger.info(
                f"  Total unique assets found so far: {len(self.all_asset_uids)}"
            )
            if len(self.all_asset_uids) > 0:
                # Show first few assets found
                sample_assets = list(self.all_asset_uids)[:5]
                for i, asset in enumerate(sample_assets, 1):
                    self.logger.info(f"  Asset {i}: {asset}")
                if len(self.all_asset_uids) > 5:
                    self.logger.info(f"  ... and {len(self.all_asset_uids) - 5} more")

            # Also log at the end of each file processing to see incremental progress
            self.logger.info(
                f"=== End of file processing - Total assets: {len(self.all_asset_uids)} ==="
            )

        except Exception as e:
            self.logger.error(f"Error extracting data quality assets: {e}")
            self.stats["errors"].append(f"Data quality extraction error: {e}")

    def _extract_all_assets_from_policy(self, policy: Dict[str, Any]) -> None:
        """Extract all asset UIDs from a policy by deeply scanning all JSON fields.

        Args:
            policy: The policy definition dictionary
        """
        try:
            # Deep scan the entire policy object to find uid and parentAssetUid fields
            self._deep_scan_for_asset_uids(policy)

        except Exception as e:
            self.logger.error(f"Error extracting all assets from policy: {e}")
            self.stats["errors"].append(f"All assets extraction error: {e}")

    def _deep_scan_for_asset_uids(self, obj: Any, path: str = "") -> None:
        """Recursively scan any object to find uid and parentAssetUid fields.

        Args:
            obj: The object to scan (dict, list, or primitive)
            path: Current path in the object for debugging
        """
        try:
            self.deep_scan_count += 1
            if self.deep_scan_count % 1000 == 0:  # Log every 1000th scan
                self.logger.debug(
                    f"Deep scan count: {self.deep_scan_count}, current path: {path}"
                )
            if isinstance(obj, dict):
                # Check for uid and parentAssetUid fields in this dict
                for key, value in obj.items():
                    current_path = f"{path}.{key}" if path else key

                    # Check if this key is uid or parentAssetUid
                    if (
                        key in ["uid", "parentAssetUid"]
                        and isinstance(value, str)
                        and value.strip()
                    ):
                        uid = value.strip()
                        self.all_asset_uids.add(uid)
                        self.logger.debug(f"Found asset UID at {current_path}: {uid}")

                    # Also check for asset-related fields that might contain UIDs
                    if (
                        key
                        in [
                            "assetUid",
                            "asset_uid",
                            "assetUid",
                            "backingAssetUid",
                            "backingAssetId",
                        ]
                        and isinstance(value, str)
                        and value.strip()
                    ):
                        uid = value.strip()
                        self.all_asset_uids.add(uid)
                        self.logger.debug(f"Found asset UID at {current_path}: {uid}")

                    # Check for any field that contains "uid" in its name (case-insensitive)
                    if (
                        "uid" in key.lower()
                        and isinstance(value, str)
                        and value.strip()
                    ):
                        uid = value.strip()
                        self.all_asset_uids.add(uid)
                        self.logger.debug(f"Found asset UID at {current_path}: {uid}")

                    # Check for asset objects that might contain UIDs
                    if key in [
                        "asset",
                        "assets",
                        "backingAsset",
                        "backingAssets",
                    ] and isinstance(value, (dict, list)):
                        # This is likely an asset object or list, scan it more carefully
                        self.logger.debug(
                            f"Found asset object at {current_path}, scanning for UIDs"
                        )
                        self._deep_scan_for_asset_uids(value, current_path)

                    # Check for any field that contains "uid" in its name (case-insensitive)
                    if (
                        "uid" in key.lower()
                        and isinstance(value, str)
                        and value.strip()
                    ):
                        uid = value.strip()
                        self.all_asset_uids.add(uid)
                        self.logger.debug(f"Found asset UID at {current_path}: {uid}")
                        # Also log at info level for important finds
                        self.logger.info(f"Found asset UID at {current_path}: {uid}")
                        # Log the current total count after adding this UID
                        self.logger.info(
                            f"Total assets after adding {uid}: {len(self.all_asset_uids)}"
                        )

                    # Recursively scan the value
                    self._deep_scan_for_asset_uids(value, current_path)

            elif isinstance(obj, list):
                # Scan each item in the list
                for i, item in enumerate(obj):
                    current_path = f"{path}[{i}]"
                    self._deep_scan_for_asset_uids(item, current_path)

            # For primitive types (str, int, bool, etc.), no further scanning needed

        except Exception as e:
            self.logger.error(f"Error in deep scan at path {path}: {e}")
            self.stats["errors"].append(f"Deep scan error at {path}: {e}")

    def _extract_from_policy(self, policy: Dict[str, Any]) -> None:
        """Extract assets from a single policy definition.

        Args:
            policy: The policy definition dictionary
        """
        try:
            # Check if this is a segmented policy with SPARK engine
            is_segmented = policy.get("isSegmented", False)
            engine_type = policy.get("engineType", "")
            policy_name = policy.get("name", "unknown")

            # Track statistics
            self.stats["total_policies_processed"] += 1

            # Extract UIDs only when:
            # isSegmented=true AND engineType=SPARK
            should_extract = False

            if is_segmented and engine_type == "SPARK":
                should_extract = True
                self.stats["segmented_spark_policies"] += 1
                self.logger.debug(
                    f"Extracting from segmented SPARK policy: {policy_name}"
                )
            elif is_segmented and engine_type == "JDBC_SQL":
                self.stats["segmented_jdbc_policies"] += 1
                self.logger.debug(f"Skipping segmented JDBC_SQL policy: {policy_name}")
                return
            elif not is_segmented:
                self.stats["non_segmented_policies"] += 1
                self.logger.debug(f"Skipping non-segmented policy: {policy_name}")
                return
            else:
                self.logger.debug(
                    f"Skipping policy (isSegmented={is_segmented}, engineType={engine_type}): {policy_name}"
                )
                return

            if should_extract:
                # Extract backing assets
                backing_assets = policy.get("backingAssets", [])

                for asset in backing_assets:
                    if isinstance(asset, dict):
                        uid = asset.get("uid")

                        if uid is not None:
                            self.extracted_assets.add(uid)
                            self.logger.debug(f"Extracted asset uid: {uid}")
        except Exception as e:
            self.logger.error(f"Error extracting from policy: {e}")
            self.stats["errors"].append(f"Policy extraction error: {e}")

    def write_extracted_assets_csv(self) -> None:
        """Write extracted assets to CSV file."""
        if not self.extracted_assets:
            self.logger.info("No assets extracted, skipping CSV creation")
            return

        csv_file = self.policy_export_dir / "segmented_spark_uids.csv"

        try:
            with open(csv_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["source-env", "target-env"])

                # Sort by uid for consistent output
                sorted_assets = sorted(self.extracted_assets)

                for uid in sorted_assets:
                    # Apply the same string replacement logic to create target-env
                    target_env = uid.replace(self.search_string, self.replace_string)
                    writer.writerow([uid, target_env])

            self.logger.info(
                f"Extracted {len(self.extracted_assets)} unique assets to {csv_file}"
            )

        except Exception as e:
            error_msg = f"Failed to write CSV file {csv_file}: {e}"
            self.logger.error(error_msg)
            self.stats["errors"].append(error_msg)

    def write_all_assets_csv(self) -> None:
        """Write all asset UIDs to CSV file without filtering constraints."""
        if not self.all_asset_uids:
            self.logger.info("No assets found, skipping asset_uids.csv creation")
            return

        csv_file = self.asset_export_dir / "asset_uids.csv"

        try:
            with open(csv_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["source-env", "target-env"])

                # Sort by uid for consistent output
                sorted_assets = sorted(self.all_asset_uids)

                for uid in sorted_assets:
                    # Apply the same string replacement logic to create target-env
                    target_env = uid.replace(self.search_string, self.replace_string)
                    writer.writerow([uid, target_env])

            self.logger.info(
                f"Extracted {len(self.all_asset_uids)} unique assets to {csv_file}"
            )

        except Exception as e:
            error_msg = f"Failed to write CSV file {csv_file}: {e}"
            self.logger.error(error_msg)
            self.stats["errors"].append(error_msg)

    def replace_in_value(self, value: Any) -> Any:
        """Recursively replace substrings in a value with error handling.

        Args:
            value: The value to process (can be string, dict, list, or other types)

        Returns:
            The value with replacements made
        """
        try:
            if isinstance(value, str):
                # Replace substring in string values
                if self.search_string in value:
                    new_value = value.replace(self.search_string, self.replace_string)
                    self.stats["changes_made"] += 1
                    self.logger.debug(f"Replaced '{value}' -> '{new_value}'")
                    return new_value
                return value
            elif isinstance(value, dict):
                # Recursively process dictionary values
                return {key: self.replace_in_value(val) for key, val in value.items()}
            elif isinstance(value, list):
                # Recursively process list values
                return [self.replace_in_value(item) for item in value]
            else:
                # Return other types as-is (numbers, booleans, None, etc.)
                return value
        except Exception as e:
            self.logger.error(f"Error during string replacement: {e}")
            self.stats["errors"].append(f"String replacement error: {e}")
            return value  # Return original value on error

    def process_json_file(
        self, json_file_path: Path, relative_base_path: Optional[Path] = None
    ) -> bool:
        """Process a single JSON file with comprehensive error handling.

        Args:
            json_file_path (Path): Path to the JSON file to process
            relative_base_path (Path): Base path for calculating relative output path (for ZIP files)

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.stats["files_investigated"] += 1
            self.stats["json_files_processed"] += 1

            # Validate file exists and is readable
            if not json_file_path.exists():
                raise FileNotFoundError(f"JSON file does not exist: {json_file_path}")

            if not json_file_path.is_file():
                raise ValueError(f"Path is not a file: {json_file_path}")

            self.logger.info(f"Processing JSON file: {json_file_path}")

            # Read the JSON file with encoding detection
            try:
                with open(json_file_path, "r", encoding="utf-8") as file:
                    data = json.load(file)
            except UnicodeDecodeError:
                # Try with different encoding
                with open(json_file_path, "r", encoding="latin-1") as file:
                    data = json.load(file)

            # Check if this is a data quality policy definitions file
            file_name = json_file_path.name
            if file_name.startswith("data_quality_policy_definitions"):
                self.logger.info(
                    f"Processing data quality policy definitions file: {file_name}"
                )
                self.extract_data_quality_assets(data)

            # Process the data (existing functionality)
            modified_data = self.replace_in_value(data)

            # Determine output file path
            if relative_base_path:
                relative_path = json_file_path.relative_to(relative_base_path)
            else:
                relative_path = json_file_path.relative_to(self.input_dir)

            output_file_path = self.output_dir / relative_path

            # Create subdirectories if needed
            try:
                output_file_path.parent.mkdir(parents=True, exist_ok=True)
            except PermissionError:
                raise PermissionError(
                    f"Permission denied: Cannot create directory {output_file_path.parent}"
                )

            # Write the modified data
            with open(output_file_path, "w", encoding="utf-8") as file:
                json.dump(
                    modified_data, file, ensure_ascii=False, separators=(",", ":")
                )

            self.logger.info(
                f"Successfully processed: {json_file_path} -> {output_file_path}"
            )
            return True

        except json.JSONDecodeError as e:
            error_msg = f"Invalid JSON in {json_file_path}: {e}"
            self.logger.error(error_msg)
            self.stats["errors"].append(error_msg)
            return False
        except (FileNotFoundError, PermissionError, ValueError) as e:
            error_msg = f"File error processing {json_file_path}: {e}"
            self.logger.error(error_msg)
            self.stats["errors"].append(error_msg)
            return False
        except Exception as e:
            error_msg = f"Unexpected error processing {json_file_path}: {e}"
            self.logger.error(error_msg)
            self.stats["errors"].append(error_msg)
            return False

    def process_zip_file(self, zip_file_path: Path) -> bool:
        """Process a ZIP file with comprehensive error handling.

        Args:
            zip_file_path (Path): Path to the ZIP file to process

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.stats["files_investigated"] += 1
            self.stats["zip_files_processed"] += 1

            # Validate ZIP file
            if not zip_file_path.exists():
                raise FileNotFoundError(f"ZIP file does not exist: {zip_file_path}")

            if not zip_file_path.is_file():
                raise ValueError(f"Path is not a file: {zip_file_path}")

            self.logger.info(f"Processing ZIP file: {zip_file_path}")

            # Create a temporary directory for extraction
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                self.logger.debug(f"Created temporary directory: {temp_path}")

                # Extract the ZIP file
                try:
                    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
                        zip_ref.extractall(temp_path)
                except zipfile.BadZipFile as e:
                    raise zipfile.BadZipFile(f"Invalid ZIP file {zip_file_path}: {e}")
                except Exception as e:
                    raise RuntimeError(
                        f"Failed to extract ZIP file {zip_file_path}: {e}"
                    )

                self.logger.debug(f"Extracted ZIP file to: {temp_path}")

                # Get all files from the original ZIP to maintain structure
                try:
                    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
                        original_files = zip_ref.namelist()
                except Exception as e:
                    raise RuntimeError(
                        f"Failed to read ZIP file structure {zip_file_path}: {e}"
                    )

                self.logger.info(f"Original ZIP contains {len(original_files)} files")

                # Find all JSON files in the extracted content
                json_files = list(temp_path.rglob("*.json"))

                if not json_files:
                    self.logger.warning(f"No JSON files found in ZIP: {zip_file_path}")
                    # Still create the output ZIP with original content
                    return self._create_output_zip(
                        zip_file_path, temp_path, original_files
                    )

                self.logger.info(
                    f"Found {len(json_files)} JSON files in ZIP: {zip_file_path}"
                )

                # Log all JSON files found for debugging
                for i, json_file in enumerate(json_files, 1):
                    self.logger.info(f"  JSON file {i}: {json_file.name}")

                # Process each JSON file
                successful = 0
                failed = 0

                for json_file in json_files:
                    if self._process_json_file_in_zip(json_file, temp_path):
                        successful += 1
                    else:
                        failed += 1

                self.logger.info(
                    f"ZIP processing complete: {successful} successful, {failed} failed"
                )

                # Create output ZIP with all files (processed and unprocessed)
                return self._create_output_zip(zip_file_path, temp_path, original_files)

        except (zipfile.BadZipFile, FileNotFoundError, ValueError, RuntimeError) as e:
            error_msg = f"ZIP processing error for {zip_file_path}: {e}"
            self.logger.error(error_msg)
            self.stats["errors"].append(error_msg)
            return False
        except Exception as e:
            error_msg = f"Unexpected error processing ZIP file {zip_file_path}: {e}"
            self.logger.error(error_msg)
            self.stats["errors"].append(error_msg)
            return False

    def _process_json_file_in_zip(self, json_file_path: Path, temp_path: Path) -> bool:
        """Process a single JSON file within a ZIP extraction.

        Args:
            json_file_path (Path): Path to the JSON file to process
            temp_path (Path): Base path of the temporary extraction directory

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.stats["files_investigated"] += 1
            self.stats["json_files_processed"] += 1

            self.logger.debug(f"Processing JSON file in ZIP: {json_file_path}")

            # Read the JSON file
            try:
                with open(json_file_path, "r", encoding="utf-8") as file:
                    data = json.load(file)
            except UnicodeDecodeError:
                with open(json_file_path, "r", encoding="latin-1") as file:
                    data = json.load(file)

            # Process ALL JSON files for asset extraction, not just data_quality_policy_definitions
            file_name = json_file_path.name
            self.logger.info(
                f"Processing JSON file in ZIP for asset extraction: {file_name}"
            )

            # Extract assets from ALL JSON files (policies, configurations, etc.)
            self.logger.info(f"  Extracting assets from: {file_name}")

            # Log the type of file being processed for better debugging
            if "data_quality_policy_definitions" in file_name:
                self.logger.info(f"  File type: Data Quality Policy Definitions")
            elif "data_drift_policy_definitions" in file_name:
                self.logger.info(f"  File type: Data Drift Policy Definitions")
            elif "schema_drift_policy_definitions" in file_name:
                self.logger.info(f"  File type: Schema Drift Policy Definitions")
            elif "reconciliation_policy_definitions" in file_name:
                self.logger.info(f"  File type: Reconciliation Policy Definitions")
            elif "profile_anomaly_policy_definition" in file_name:
                self.logger.info(f"  File type: Profile Anomaly Policy Definition")
            elif "data_cadence_policy_definitions" in file_name:
                self.logger.info(f"  File type: Data Cadence Policy Definitions")
            elif "business_rules" in file_name:
                self.logger.info(f"  File type: Business Rules")
            elif "asset_udf_variables" in file_name:
                self.logger.info(f"  File type: Asset UDF Variables")
            elif "data_sources" in file_name:
                self.logger.info(f"  File type: Data Sources")
            elif "notification_settings" in file_name:
                self.logger.info(f"  File type: Notification Settings")
            elif "package_udf_definitions" in file_name:
                self.logger.info(f"  File type: Package UDF Definitions")
            elif "reference_asset" in file_name:
                self.logger.info(f"  File type: Reference Asset")
            else:
                self.logger.info(f"  File type: Other/Unknown")

            self.extract_data_quality_assets(data)

            # Process the data (existing functionality)
            modified_data = self.replace_in_value(data)

            # Write the modified data back to the same location in temp directory
            with open(json_file_path, "w", encoding="utf-8") as file:
                json.dump(
                    modified_data, file, ensure_ascii=False, separators=(",", ":")
                )

            self.logger.debug(f"Successfully processed: {json_file_path}")
            return True

        except json.JSONDecodeError as e:
            error_msg = f"Invalid JSON in {json_file_path}: {e}"
            self.logger.error(error_msg)
            self.stats["errors"].append(error_msg)
            return False
        except Exception as e:
            error_msg = f"Error processing {json_file_path}: {e}"
            self.logger.error(error_msg)
            self.stats["errors"].append(error_msg)
            return False

    def _create_output_zip(
        self, original_zip_path: Path, temp_path: Path, original_files: List[str]
    ) -> bool:
        """Create a new ZIP file with the processed content.

        Args:
            original_zip_path (Path): Path to the original ZIP file
            temp_path (Path): Path to the temporary directory with processed files
            original_files (List[str]): List of file paths from the original ZIP

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Create output ZIP filename with "-import-ready" suffix
            zip_name = original_zip_path.stem + "-import-ready.zip"
            output_zip_path = self.output_dir / zip_name

            self.logger.info(f"Creating output ZIP: {output_zip_path}")

            # Create the new ZIP file
            try:
                with zipfile.ZipFile(
                    output_zip_path, "w", zipfile.ZIP_DEFLATED
                ) as zip_ref:
                    # Add all files from the original ZIP structure
                    for file_path in original_files:
                        # Convert ZIP path to filesystem path
                        fs_path = temp_path / file_path

                        if fs_path.exists():
                            # Add file to ZIP, preserving the original path structure
                            zip_ref.write(fs_path, file_path)
                            self.logger.debug(f"Added to ZIP: {file_path}")
                        else:
                            self.logger.warning(
                                f"File not found in temp directory: {file_path}"
                            )
            except Exception as e:
                raise RuntimeError(f"Failed to create ZIP file {output_zip_path}: {e}")

            # Verify the output ZIP has the same number of files
            try:
                with zipfile.ZipFile(output_zip_path, "r") as zip_ref:
                    output_files = zip_ref.namelist()
            except Exception as e:
                raise RuntimeError(
                    f"Failed to verify output ZIP {output_zip_path}: {e}"
                )

            if len(output_files) == len(original_files):
                self.logger.info(
                    f"Successfully created ZIP with {len(output_files)} files: {output_zip_path}"
                )
                return True
            else:
                error_msg = f"File count mismatch: original={len(original_files)}, output={len(output_files)}"
                self.logger.error(error_msg)
                self.stats["errors"].append(error_msg)
                return False

        except Exception as e:
            error_msg = f"Error creating output ZIP {output_zip_path}: {e}"
            self.logger.error(error_msg)
            self.stats["errors"].append(error_msg)
            return False

    def process_directory(self) -> Dict[str, Any]:
        """Process all JSON files and ZIP files in the input directory.

        Returns:
            Dict[str, Any]: Statistics about the processing
        """
        try:
            # Find all JSON files and ZIP files
            json_files = list(self.input_dir.rglob("*.json"))
            zip_files = list(self.input_dir.rglob("*.zip"))

            total_files = len(json_files) + len(zip_files)

            if total_files == 0:
                self.logger.warning(f"No JSON or ZIP files found in {self.input_dir}")
                return {
                    "total_files": 0,
                    "json_files": 0,
                    "zip_files": 0,
                    "successful": 0,
                    "failed": 0,
                    "errors": self.stats["errors"],
                }

            self.logger.info(
                f"Found {len(json_files)} JSON files and {len(zip_files)} ZIP files to process"
            )

            # Process JSON files
            successful = 0
            failed = 0

            for json_file in json_files:
                if self.process_json_file(json_file):
                    successful += 1
                else:
                    failed += 1

            # Process ZIP files
            for zip_file in zip_files:
                if self.process_zip_file(zip_file):
                    successful += 1
                else:
                    failed += 1

            # Write extracted assets CSV at the end
            self.write_extracted_assets_csv()
            self.write_all_assets_csv()

            stats = {
                "total_files": total_files,
                "json_files": len(json_files),
                "zip_files": len(zip_files),
                "successful": successful,
                "failed": failed,
                "files_investigated": self.stats["files_investigated"],
                "changes_made": self.stats["changes_made"],
                "extracted_assets": len(self.extracted_assets),
                "all_assets": len(self.all_asset_uids),
                "errors": self.stats["errors"],
                # Policy statistics
                "total_policies_processed": self.stats["total_policies_processed"],
                "segmented_spark_policies": self.stats["segmented_spark_policies"],
                "segmented_jdbc_policies": self.stats["segmented_jdbc_policies"],
                "non_segmented_policies": self.stats["non_segmented_policies"],
            }

            self.logger.info(
                f"Processing complete: {successful} successful, {failed} failed"
            )
            return stats

        except Exception as e:
            error_msg = f"Directory processing error: {e}"
            self.logger.error(error_msg)
            self.stats["errors"].append(error_msg)
            return {
                "total_files": 0,
                "json_files": 0,
                "zip_files": 0,
                "successful": 0,
                "failed": 1,
                "files_investigated": self.stats["files_investigated"],
                "changes_made": self.stats["changes_made"],
                "extracted_assets": len(self.extracted_assets),
                "all_assets": len(self.all_asset_uids),
                "errors": self.stats["errors"],
                # Policy statistics
                "total_policies_processed": self.stats["total_policies_processed"],
                "segmented_spark_policies": self.stats["segmented_spark_policies"],
                "segmented_jdbc_policies": self.stats["segmented_jdbc_policies"],
                "non_segmented_policies": self.stats["non_segmented_policies"],
            }
