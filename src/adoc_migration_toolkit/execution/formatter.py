"""
Formatter execution functions.

This module contains execution functions for policy and asset formatter operations.
"""

import json
import logging
import sys
import argparse
import zipfile
import tempfile
import shutil
import os
import csv
from pathlib import Path
from typing import Any, Dict, List, Union, Optional, Set, Tuple
from datetime import datetime
from ..shared import globals


class PolicyExportFormatter:
    """Professional JSON string replacement tool with comprehensive error handling for policy transformations."""
    
    def __init__(self, input_dir: str, string_transforms: dict, 
                 output_dir: Optional[str] = None, logger: Optional[logging.Logger] = None):
        """Initialize the PolicyExportFormatter with validation.
        
        Args:
            input_dir (str): Directory containing JSON files and ZIP files to process
            string_transforms (dict): Dictionary of string transformations {source: target}
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
        
        if not string_transforms or not isinstance(string_transforms, dict):
            raise ValueError("String transforms must be a non-empty dictionary")
        
        # Setup paths
        self.input_dir = Path(input_dir).resolve()
        self.string_transforms = string_transforms
        
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
                self.base_output_dir = Path.cwd() / f"adoc-migration-toolkit-{datetime.now().strftime('%Y%m%d%H%M')}"
        
        # Create organized output directory structure
        self.output_dir = self.base_output_dir / "policy-import"  # For processed ZIP/JSON files
        self.asset_export_dir = self.base_output_dir / "asset-export"  # For asset_uids.csv
        self.policy_export_dir = self.base_output_dir / "policy-export"  # For segmented_spark_uids.csv
        
        # Create all output directories
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            self.asset_export_dir.mkdir(parents=True, exist_ok=True)
            self.policy_export_dir.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            raise PermissionError(f"Permission denied: Cannot create output directories")
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
            "total_policies_processed": 0
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
        self.logger.info(f"String transformations: {len(self.string_transforms)} transformations")
        for source, target in self.string_transforms.items():
            self.logger.info(f"  '{source}' -> '{target}'")
    
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
                "total_policies_processed": 0
            }
            
            # Process each policy in the data
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
                self.logger.info(f"  Total policies processed: {file_stats['total_policies_processed']}")
                self.logger.info(f"  Segmented SPARK policies: {file_stats['segmented_spark_policies']}")
                self.logger.info(f"  Segmented JDBC_SQL policies: {file_stats['segmented_jdbc_policies']}")
                self.logger.info(f"  Non-segmented policies: {file_stats['non_segmented_policies']}")
                
            # Log asset extraction results
            self.logger.info(f"Asset extraction results:")
            self.logger.info(f"  Total unique assets found so far: {len(self.all_asset_uids)}")
            if len(self.all_asset_uids) > 0:
                # Show first few assets found
                sample_assets = list(self.all_asset_uids)[:5]
                for i, asset in enumerate(sample_assets, 1):
                    self.logger.info(f"  Asset {i}: {asset}")
                if len(self.all_asset_uids) > 5:
                    self.logger.info(f"  ... and {len(self.all_asset_uids) - 5} more")
            
            # Also log at the end of each file processing to see incremental progress
            self.logger.info(f"=== End of file processing - Total assets: {len(self.all_asset_uids)} ===")
                
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
                self.logger.debug(f"Deep scan count: {self.deep_scan_count}, current path: {path}")
            if isinstance(obj, dict):
                # Check for uid and parentAssetUid fields in this dict
                for key, value in obj.items():
                    current_path = f"{path}.{key}" if path else key
                    
                    # Check if this key is uid or parentAssetUid
                    if key in ['uid', 'parentAssetUid'] and isinstance(value, str) and value.strip():
                        uid = value.strip()
                        self.all_asset_uids.add(uid)
                        self.logger.debug(f"Found asset UID at {current_path}: {uid}")
                    
                    # Also check for asset-related fields that might contain UIDs
                    if key in ['assetUid', 'asset_uid', 'assetUid', 'backingAssetUid', 'backingAssetId'] and isinstance(value, str) and value.strip():
                        uid = value.strip()
                        self.all_asset_uids.add(uid)
                        self.logger.debug(f"Found asset UID at {current_path}: {uid}")
                    
                    # Check for asset objects that might contain UIDs
                    if key in ['asset', 'assets', 'backingAsset', 'backingAssets'] and isinstance(value, (dict, list)):
                        # This is likely an asset object or list, scan it more carefully
                        self.logger.debug(f"Found asset object at {current_path}, scanning for UIDs")
                        self._deep_scan_for_asset_uids(value, current_path)
                    
                    # Check for any field that contains "uid" in its name (case-insensitive)
                    if 'uid' in key.lower() and isinstance(value, str) and value.strip():
                        uid = value.strip()
                        self.all_asset_uids.add(uid)
                        self.logger.debug(f"Found asset UID at {current_path}: {uid}")
                        # Also log at info level for important finds
                        self.logger.info(f"Found asset UID at {current_path}: {uid}")
                        # Log the current total count after adding this UID
                        self.logger.info(f"Total assets after adding {uid}: {len(self.all_asset_uids)}")
                    
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
                self.logger.debug(f"Extracting from segmented SPARK policy: {policy_name}")
            elif is_segmented and engine_type == "JDBC_SQL":
                self.stats["segmented_jdbc_policies"] += 1
                self.logger.debug(f"Skipping segmented JDBC_SQL policy: {policy_name}")
            elif not is_segmented:
                self.stats["non_segmented_policies"] += 1
                self.logger.debug(f"Skipping non-segmented policy: {policy_name}")
                return
            else:
                self.logger.debug(f"Skipping policy (isSegmented={is_segmented}, engineType={engine_type}): {policy_name}")
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
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['source-env', 'target-env'])
                
                # Sort by uid for consistent output
                sorted_assets = sorted(self.extracted_assets)
                
                for uid in sorted_assets:
                    # Apply string transformations to create target-env
                    target_env = uid
                    transformations_applied = []
                    for source, target in self.string_transforms.items():
                        if source in target_env and source != target:  # Only apply if strings are different
                            target_env = self.safe_replace(target_env, source, target)
                            transformations_applied.append(f"'{source}' -> '{target}'")
                    writer.writerow([uid, target_env])
            
            self.logger.info(f"Extracted {len(self.extracted_assets)} unique assets to {csv_file}")
            
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
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['source-env', 'target-env'])
                
                # Sort by uid for consistent output
                sorted_assets = sorted(self.all_asset_uids)
                
                # Track transformations for logging
                transformation_count = 0
                no_change_count = 0
                
                for uid in sorted_assets:
                    # Apply string transformations to create target-env
                    target_env = uid
                    original_uid = uid
                    transformations_applied = []
                    
                    for source, target in self.string_transforms.items():
                        if source in target_env:
                            target_env = self.safe_replace(target_env, source, target)
                            transformations_applied.append(f"'{source}' -> '{target}'")
                    
                    if transformations_applied:
                        transformation_count += 1
                        self.logger.debug(f"Transformed '{original_uid}' -> '{target_env}' (applied: {', '.join(transformations_applied)})")
                    else:
                        # No transformation applied
                        no_change_count += 1
                        self.logger.debug(f"No transformation applied to '{uid}'")
                    
                    writer.writerow([uid, target_env])
            
            self.logger.info(f"Extracted {len(self.all_asset_uids)} unique assets to {csv_file}")
            self.logger.info(f"String transformations applied: {transformation_count}, No changes: {no_change_count}")
            
        except Exception as e:
            error_msg = f"Failed to write CSV file {csv_file}: {e}"
            self.logger.error(error_msg)
            self.stats["errors"].append(error_msg)
    
    def process_asset_config_export_csv(self) -> bool:
        """Process the asset-config-export.csv file to replace source-env-string with target-env-string in the target_uid column (first column).
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Look for asset-config-export.csv in the asset-export directory
            asset_config_export_csv = self.asset_export_dir / "asset-config-export.csv"
            
            if not asset_config_export_csv.exists():
                self.logger.info(f"asset-config-export.csv not found at {asset_config_export_csv}")
                return True  # Not an error, just no file to process
            
            self.logger.info(f"Processing asset-config-export.csv: {asset_config_export_csv}")
            
            # Read the CSV file
            rows = []
            # The below limit is set to fix the error: field larger than field limit (131072), python csv read has a limitation.
            csv.field_size_limit(sys.maxsize)
            with open(asset_config_export_csv, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                rows = list(reader)
            
            if not rows:
                self.logger.warning("asset-config-export.csv is empty")
                return True
            
            # Process the target_uid column (first column, index 0), config_json column (second column, index 1), and source_uid column (third column, index 2)
            changes_made = 0
            for i, row in enumerate(rows):
                if len(row) >= 2:  # Ensure we have at least 2 columns (backward compatibility)
                    target_uid = row[0]
                    config_json = row[1]
                    source_uid = row[2] if len(row) > 2 else ''  # Handle both 2 and 3 column formats
                    original_target_uid = target_uid
                    original_config_json = config_json
                    
                    # Apply string transformations to target_uid
                    for source, target in self.string_transforms.items():
                        if source in target_uid and source != target:  # Only apply if strings are different
                            target_uid = self.safe_replace(target_uid, source, target)
                    
                    # Apply string transformations to config_json (for asset UIDs in JSON)
                    try:
                        if config_json.strip():
                            config_data = json.loads(config_json)
                            config_changed = False
                            
                            # Transform asset UIDs in the configuration
                            if "assetConfiguration" in config_data and config_data["assetConfiguration"]:
                                asset_config = config_data["assetConfiguration"]
                                
                                # Transform assetId if it exists
                                if "assetId" in asset_config:
                                    old_asset_id = str(asset_config["assetId"])
                                    for source, target in self.string_transforms.items():
                                        if source in old_asset_id and source != target:  # Only apply if strings are different
                                            asset_config["assetId"] = int(self.safe_replace(old_asset_id, source, target))
                                            config_changed = True
                                
                                # Transform freshnessColumnInfo.assetId if it exists
                                if "freshnessColumnInfo" in asset_config and asset_config["freshnessColumnInfo"]:
                                    freshness = asset_config["freshnessColumnInfo"]
                                    if "assetId" in freshness:
                                        old_freshness_asset_id = str(freshness["assetId"])
                                        for source, target in self.string_transforms.items():
                                            if source in old_freshness_asset_id and source != target:  # Only apply if strings are different
                                                freshness["assetId"] = int(self.safe_replace(old_freshness_asset_id, source, target))
                                                config_changed = True
                            
                            if config_changed:
                                config_json = json.dumps(config_data, ensure_ascii=False, separators=(',', ':'))
                    
                    except (json.JSONDecodeError, ValueError) as e:
                        self.logger.warning(f"Could not parse config JSON for row {i}: {e}")
                    
                    # Update row if any changes were made
                    if target_uid != original_target_uid or config_json != original_config_json:
                        rows[i][0] = target_uid
                        rows[i][1] = config_json
                        # Preserve source_uid if it exists
                        if len(rows[i]) > 2:
                            rows[i][2] = source_uid
                        changes_made += 1
                        self.logger.debug(f"Updated row {i}: target_uid={original_target_uid}->{target_uid}, config_json transformed")
            
            # Create asset-import directory if it doesn't exist
            asset_import_dir = self.base_output_dir / "asset-import"
            asset_import_dir.mkdir(parents=True, exist_ok=True)
            
            # Write the processed CSV to asset-import/asset-config-import-ready.csv
            output_csv = asset_import_dir / "asset-config-import-ready.csv"
            
            with open(output_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                writer.writerows(rows)
            
            self.logger.info(f"Processed asset-config-export.csv: {changes_made} changes made")
            self.logger.info(f"Output written to: {output_csv}")
            
            # Update statistics
            self.stats["changes_made"] += changes_made
            
            return True
            
        except Exception as e:
            error_msg = f"Error processing asset-config-export.csv: {e}"
            self.logger.error(error_msg)
            self.stats["errors"].append(error_msg)
            return False

    def process_asset_all_export_csv(self) -> bool:
        """Process the asset-all-export.csv file to replace source-env-string with target-env-string in the target_uid column.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Look for asset-all-export.csv in the asset-export directory
            asset_export_csv = self.asset_export_dir / "asset-all-export.csv"
            
            if not asset_export_csv.exists():
                self.logger.info(f"asset-all-export.csv not found at {asset_export_csv}")
                return True  # Not an error, just no file to process
            
            self.logger.info(f"Processing asset-all-export.csv: {asset_export_csv}")
            
            # Read the CSV file
            rows = []
            with open(asset_export_csv, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                rows = list(reader)
            
            if not rows:
                self.logger.warning("asset-all-export.csv is empty")
                return True
            
            # Process the target_uid column (column 2)
            changes_made = 0
            for i, row in enumerate(rows):
                if len(row) >= 3:  # Ensure we have at least 3 columns
                    target_uid = row[2]
                    original_target_uid = target_uid
                    # Apply string transformations
                    for source, target in self.string_transforms.items():
                        if source in target_uid and source != target:  # Only apply if strings are different
                            target_uid = self.safe_replace(target_uid, source, target)
                    
                    if target_uid != original_target_uid:
                        rows[i][2] = target_uid
                        changes_made += 1
                        self.logger.debug(f"Updated target_uid: {original_target_uid} -> {target_uid}")
            
            # Create asset-import directory if it doesn't exist
            asset_import_dir = self.base_output_dir / "asset-import"
            asset_import_dir.mkdir(parents=True, exist_ok=True)
            
            # Write the processed CSV to asset-import/asset-all-import-ready.csv
            output_csv = asset_import_dir / "asset-all-import-ready.csv"
            
            with open(output_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                writer.writerows(rows)
            
            self.logger.info(f"Processed asset-all-export.csv: {changes_made} changes made")
            self.logger.info(f"Output written to: {output_csv}")
            
            # Update statistics
            self.stats["changes_made"] += changes_made
            
            return True
            
        except Exception as e:
            error_msg = f"Error processing asset-all-export.csv: {e}"
            self.logger.error(error_msg)
            self.stats["errors"].append(error_msg)
            return False
    
    def replace_in_value(self, value: Any) -> Any:
        """Recursively replace substrings in a value with error handling.
        
        Args:
            value: The value to process (can be string, dict, list, or other types)
            
        Returns:
            The value with replacements made
        """
        try:
            if isinstance(value, str):
                # Apply all string transformations efficiently
                original_value = value
                modified_value = value
                changes_made = 0
                
                for source, target in self.string_transforms.items():
                    if source in modified_value and source != target:  # Only apply if strings are different
                        modified_value = self.safe_replace(modified_value, source, target)
                        changes_made += 1
                
                if changes_made > 0:
                    self.stats["changes_made"] += changes_made
                    self.logger.debug(f"Replaced '{original_value}' -> '{modified_value}'")
                    return modified_value
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
    
    def process_json_file(self, json_file_path: Path, relative_base_path: Optional[Path] = None) -> bool:
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
            
            self.logger.info(f"Processing JSON file: {json_file_path}")
            
            # Validate file exists and is readable
            if not json_file_path.exists():
                raise FileNotFoundError(f"JSON file does not exist: {json_file_path}")
            
            if not json_file_path.is_file():
                raise ValueError(f"Path is not a file: {json_file_path}")
            
            # Read the JSON file with encoding detection
            try:
                with open(json_file_path, 'r', encoding='utf-8') as file:
                    data = json.load(file)
            except UnicodeDecodeError:
                # Try with different encoding
                with open(json_file_path, 'r', encoding='latin-1') as file:
                    data = json.load(file)
            
            # Check if this is a data quality policy definitions file
            file_name = json_file_path.name
            if file_name.startswith("data_quality_policy_definitions"):
                self.logger.info(f"Processing data quality policy definitions file: {file_name}")
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
                raise PermissionError(f"Permission denied: Cannot create directory {output_file_path.parent}")
            
            # Write the modified data
            with open(output_file_path, 'w', encoding='utf-8') as file:
                json.dump(modified_data, file, ensure_ascii=False, separators=(',', ':'))
            
            self.logger.info(f"Successfully processed: {json_file_path} -> {output_file_path}")
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
            
            self.logger.info(f"Processing ZIP file: {zip_file_path}")
            
            # Validate ZIP file
            if not zip_file_path.exists():
                raise FileNotFoundError(f"ZIP file does not exist: {zip_file_path}")
            
            if not zip_file_path.is_file():
                raise ValueError(f"Path is not a file: {zip_file_path}")
            
            # Create a temporary directory for extraction
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                self.logger.debug(f"Created temporary directory: {temp_path}")
                
                # Extract the ZIP file
                try:
                    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                        zip_ref.extractall(temp_path)
                except zipfile.BadZipFile as e:
                    raise zipfile.BadZipFile(f"Invalid ZIP file {zip_file_path}: {e}")
                
                self.logger.debug(f"Extracted ZIP file to: {temp_path}")
                
                # Get all files from the original ZIP to maintain structure
                try:
                    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                        original_files = zip_ref.namelist()
                except Exception as e:
                    raise RuntimeError(f"Failed to read ZIP file structure {zip_file_path}: {e}")
                
                self.logger.info(f"Original ZIP contains {len(original_files)} files")
                
                # Find all JSON files in the extracted content
                json_files = list(temp_path.rglob("*.json"))
                
                if not json_files:
                    self.logger.warning(f"No JSON files found in ZIP: {zip_file_path}")
                    # Still create the output ZIP with original content
                    return self._create_output_zip(zip_file_path, temp_path, original_files)
                
                self.logger.info(f"Found {len(json_files)} JSON files in ZIP: {zip_file_path}")
                
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
                
                self.logger.info(f"ZIP processing complete: {successful} successful, {failed} failed")
                
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
            
            # Read the JSON file with encoding detection
            try:
                with open(json_file_path, 'r', encoding='utf-8') as file:
                    data = json.load(file)
            except UnicodeDecodeError:
                with open(json_file_path, 'r', encoding='latin-1') as file:
                    data = json.load(file)
            
            # Process ALL JSON files for asset extraction, not just data_quality_policy_definitions
            file_name = json_file_path.name
            self.logger.info(f"Processing JSON file in ZIP for asset extraction: {file_name}")
            
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
            with open(json_file_path, 'w', encoding='utf-8') as file:
                json.dump(modified_data, file, ensure_ascii=False, separators=(',', ':'))
            
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
    
    def _create_output_zip(self, original_zip_path: Path, temp_path: Path, original_files: List[str]) -> bool:
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
                with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_ref:
                    # Add all files from the original ZIP structure
                    for file_path in original_files:
                        # Convert ZIP path to filesystem path
                        fs_path = temp_path / file_path
                        
                        if fs_path.exists():
                            # Add file to ZIP, preserving the original path structure
                            zip_ref.write(fs_path, file_path)
                            self.logger.debug(f"Added to ZIP: {file_path}")
                        else:
                            self.logger.warning(f"File not found in temp directory: {file_path}")
            except Exception as e:
                raise RuntimeError(f"Failed to create ZIP file {output_zip_path}: {e}")
            
            # Verify the output ZIP has the same number of files
            try:
                with zipfile.ZipFile(output_zip_path, 'r') as zip_ref:
                    output_files = zip_ref.namelist()
            except Exception as e:
                raise RuntimeError(f"Failed to verify output ZIP {output_zip_path}: {e}")
            
            if len(output_files) == len(original_files):
                self.logger.info(f"Successfully created ZIP with {len(output_files)} files: {output_zip_path}")
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
                    "errors": self.stats["errors"]
                }
            
            self.logger.info(f"Found {len(json_files)} JSON files and {len(zip_files)} ZIP files to process")
            
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
            
            # Process asset-all-export.csv if it exists
            csv_processed = self.process_asset_all_export_csv()
            
            # Process asset-config-export.csv if it exists
            config_csv_processed = self.process_asset_config_export_csv()
            
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
                "csv_processed": csv_processed,
                "config_csv_processed": config_csv_processed,
                "errors": self.stats["errors"],
                # Policy statistics
                "total_policies_processed": self.stats["total_policies_processed"],
                "segmented_spark_policies": self.stats["segmented_spark_policies"],
                "segmented_jdbc_policies": self.stats["segmented_jdbc_policies"],
                "non_segmented_policies": self.stats["non_segmented_policies"],
                # Deep scan statistics
                "deep_scan_count": self.deep_scan_count
            }
            
            self.logger.info(f"Processing complete: {successful} successful, {failed} failed")
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
                "csv_processed": False,
                "config_csv_processed": False,
                "errors": self.stats["errors"],
                # Policy statistics
                "total_policies_processed": self.stats["total_policies_processed"],
                "segmented_spark_policies": self.stats["segmented_spark_policies"],
                "segmented_jdbc_policies": self.stats["segmented_jdbc_policies"],
                "non_segmented_policies": self.stats["non_segmented_policies"],
                # Deep scan statistics
                "deep_scan_count": self.deep_scan_count
            }


class AssetExportFormatter:
    """Professional asset transformation tool with comprehensive error handling for asset-specific operations."""
    
    def __init__(self, input_dir: str, string_transforms: dict, 
                 output_dir: Optional[str] = None, logger: Optional[logging.Logger] = None):
        """Initialize the AssetExportFormatter with validation.
        
        Args:
            input_dir (str): Directory containing asset CSV files to process
            string_transforms (dict): Dictionary of string transformations {source: target}
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
        
        # Setup paths
        self.input_dir = Path(input_dir).resolve()
        self.string_transforms = string_transforms or {}
        
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
                self.base_output_dir = Path.cwd() / f"adoc-migration-toolkit-{datetime.now().strftime('%Y%m%d%H%M')}"
        
        # Create organized output directory structure
        self.output_dir = self.base_output_dir / "asset-import"  # For processed asset files
        self.asset_export_dir = self.base_output_dir / "asset-export"  # For asset export files
        
        # Create all output directories
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            self.asset_export_dir.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            raise PermissionError(f"Permission denied: Cannot create output directories")
        except Exception as e:
            raise RuntimeError(f"Failed to create output directories: {e}")
        
        # Initialize statistics
        self.stats = {
            "files_investigated": 0,
            "changes_made": 0,
            "csv_files_processed": 0,
            "assets_processed": 0,
            "errors": []
        }
        
        self.logger.info(f"AssetExportFormatter initialized:")
        self.logger.info(f"  Input directory: {self.input_dir}")
        self.logger.info(f"  Output directory: {self.output_dir}")
        self.logger.info(f"  Asset export directory: {self.asset_export_dir}")
        self.logger.info(f"  String transformations: {len(self.string_transforms)} transformations")
        for source, target in self.string_transforms.items():
            self.logger.info(f"    '{source}' -> '{target}'")
    
    def process_directory(self) -> Dict[str, Any]:
        """Process all asset CSV files in the input directory.
        
        Returns:
            Dict[str, Any]: Statistics about the processing
        """
        try:
            # Find all CSV files
            csv_files = list(self.input_dir.rglob("*.csv"))
            
            total_files = len(csv_files)
            
            if total_files == 0:
                self.logger.warning(f"No CSV files found in {self.input_dir}")
                return {
                    "total_files": 0,
                    "csv_files": 0,
                    "successful": 0,
                    "failed": 0,
                    "assets_processed": 0,
                    "changes_made": 0,
                    "errors": self.stats["errors"]
                }
            
            self.logger.info(f"Found {len(csv_files)} CSV files to process")
            
            # Process CSV files
            successful = 0
            failed = 0
            
            for csv_file in csv_files:
                if self.process_csv_file(csv_file):
                    successful += 1
                else:
                    failed += 1
            
            stats = {
                "total_files": total_files,
                "csv_files": len(csv_files),
                "successful": successful,
                "failed": failed,
                "files_investigated": self.stats["files_investigated"],
                "changes_made": self.stats["changes_made"],
                "assets_processed": self.stats["assets_processed"],
                "errors": self.stats["errors"]
            }
            
            self.logger.info(f"Asset processing complete: {successful} successful, {failed} failed")
            return stats
            
        except Exception as e:
            error_msg = f"Directory processing error: {e}"
            self.logger.error(error_msg)
            self.stats["errors"].append(error_msg)
            return {
                "total_files": 0,
                "csv_files": 0,
                "successful": 0,
                "failed": 1,
                "files_investigated": self.stats["files_investigated"],
                "changes_made": self.stats["changes_made"],
                "assets_processed": self.stats["assets_processed"],
                "errors": self.stats["errors"]
            }
    
    def process_csv_file(self, csv_file_path: Path) -> bool:
        """Process a CSV file with asset data and apply string transformations.
        
        Args:
            csv_file_path (Path): Path to the CSV file to process
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.stats["files_investigated"] += 1
            self.stats["csv_files_processed"] += 1
            
            self.logger.info(f"Processing asset CSV file: {csv_file_path}")
            
            # Validate CSV file
            if not csv_file_path.exists():
                raise FileNotFoundError(f"CSV file does not exist: {csv_file_path}")
            
            if not csv_file_path.is_file():
                raise ValueError(f"Path is not a file: {csv_file_path}")
            
            # Read the CSV file
            rows = []
            with open(csv_file_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            if not rows:
                self.logger.warning(f"CSV file is empty: {csv_file_path}")
                return True
            
            self.logger.info(f"Found {len(rows)} rows in CSV file: {csv_file_path}")
            
            # Process each row
            processed_rows = []
            assets_processed = 0
            changes_made = 0
            
            for i, row in enumerate(rows, 1):
                try:
                    # Apply string transformations to all string fields
                    processed_row = {}
                    for key, value in row.items():
                        if isinstance(value, str):
                            transformed_value = self.apply_string_transforms(value)
                            if transformed_value != value:
                                changes_made += 1
                            processed_row[key] = transformed_value
                        else:
                            processed_row[key] = value
                    
                    processed_rows.append(processed_row)
                    assets_processed += 1
                    
                except Exception as e:
                    self.logger.error(f"Error processing row {i} in CSV {csv_file_path}: {e}")
                    # Keep original row if processing fails
                    processed_rows.append(row)
            
            # Write processed CSV file with proper naming convention
            if csv_file_path.stem == "asset-config-export":
                output_file = self.output_dir / "asset-config-import-ready.csv"
            elif csv_file_path.stem == "asset-profile-export":
                output_file = self.output_dir / "asset-profile-import-ready.csv"
            elif csv_file_path.stem == "asset-all-source-export":
                output_file = self.output_dir / "asset-all-source-import-ready.csv"
            elif csv_file_path.stem == "asset-all-target-export":
                output_file = self.output_dir / "asset-all-target-import-ready.csv"
            else:
                output_file = self.output_dir / f"{csv_file_path.stem}-ready.csv"
            
            if processed_rows:
                with open(output_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=processed_rows[0].keys())
                    writer.writeheader()
                    writer.writerows(processed_rows)
                
                self.logger.info(f"Processed CSV written to: {output_file}")
                self.logger.info(f"Assets processed: {assets_processed}")
                self.logger.info(f"Changes made: {changes_made}")
                
                # Update statistics
                self.stats["assets_processed"] += assets_processed
                self.stats["changes_made"] += changes_made
            
            return True
                
        except (FileNotFoundError, ValueError) as e:
            error_msg = f"CSV processing error for {csv_file_path}: {e}"
            self.logger.error(error_msg)
            self.stats["errors"].append(error_msg)
            return False
        except Exception as e:
            error_msg = f"Unexpected error processing CSV file {csv_file_path}: {e}"
            self.logger.error(error_msg)
            self.stats["errors"].append(error_msg)
            return False
    
    def safe_replace(self, text: str, source: str, target: str) -> str:
        """Safely replace source string with target string, preventing recursive replacements.
        
        Args:
            text (str): The text to transform
            source (str): The source string to replace
            target (str): The target string to replace with
            
        Returns:
            str: The transformed text
        """
        if source not in text or source == target:
            return text
        
        # Use a temporary placeholder to prevent recursive replacements
        # when target contains source string
        placeholder = f"__TEMP_PLACEHOLDER_{hash(source)}__"
        result = text.replace(source, placeholder)
        result = result.replace(placeholder, target)
        return result
    
    def apply_string_transforms(self, value: str) -> str:
        """Apply string transformations to a value.
        
        Args:
            value (str): The string value to transform
            
        Returns:
            str: The transformed string
        """
        if not self.string_transforms:
            return value
        
        transformed_value = value
        for source, target in self.string_transforms.items():
            transformed_value = self.safe_replace(transformed_value, source, target)
        
        return transformed_value


def validate_arguments(args: argparse.Namespace) -> None:
    """Validate command line arguments.
    
    Args:
        args: Parsed command line arguments
        
    Raises:
        ValueError: If arguments are invalid
    """
    if not args.input_dir or not args.input_dir.strip():
        raise ValueError("Input directory cannot be empty")
    
    if not args.string_transforms or not isinstance(args.string_transforms, dict):
        raise ValueError("String transforms must be a non-empty dictionary")
    
    # Check if input directory exists
    input_path = Path(args.input_dir)
    if not input_path.exists():
        raise FileNotFoundError(f"Input directory does not exist: {args.input_dir}")
    
    if not input_path.is_dir():
        raise ValueError(f"Input path is not a directory: {args.input_dir}")


def parse_formatter_command(command: str) -> tuple:
    """Parse policy-xfr command in interactive mode.
    Args:
        command (str): The command string
    Returns:
        tuple: (input_dir, string_transforms, output_dir, quiet_mode, verbose_mode)
    """
    try:
        args_str = command[len('policy-xfr'):].strip()
        input_dir = None
        string_transforms = {}
        output_dir = None
        quiet_mode = False
        verbose_mode = False
        args = args_str.split()
        i = 0
        while i < len(args):
            arg = args[i]
            if arg == '--input' and i + 1 < len(args):
                input_dir = args[i + 1]
                i += 2
            elif arg == '--output-dir' and i + 1 < len(args):
                output_dir = args[i + 1]
                i += 2
            elif arg == '--string-transform' and i + 1 < len(args):
                # Collect all args until next -- or end
                transform_parts = []
                j = i + 1
                while j < len(args) and not args[j].startswith('--'):
                    transform_parts.append(args[j])
                    j += 1
                
                if not transform_parts:
                    print("❌ Missing string transform argument")
                    print("💡 Expected format: --string-transform \"A\":\"B\", \"C\":\"D\", \"E\":\"F\"")
                    return None, None, None, None, False
                
                transform_arg = ' '.join(transform_parts)
                try:
                    # Parse format: "A":"B", "C":"D", "E":"F"
                    transforms = {}
                    # Remove outer quotes if present
                    if transform_arg.startswith('"') and transform_arg.endswith('"'):
                        transform_arg = transform_arg[1:-1]
                    
                    # Split by comma and process each pair
                    pairs = [pair.strip() for pair in transform_arg.split(',')]
                    for pair in pairs:
                        if ':' in pair:
                            source, target = pair.split(':', 1)
                            source = source.strip().strip('"')
                            target = target.strip().strip('"')
                            if source and target:
                                transforms[source] = target
                    
                    string_transforms.update(transforms)
                except Exception as e:
                    print(f"❌ Error parsing string transform argument: {e}")
                    print("💡 Expected format: --string-transform \"A\":\"B\", \"C\":\"D\", \"E\":\"F\"")
                    return None, None, None, None, False
                i = j
            elif arg == '--source-env-string' and i + 1 < len(args):
                # Legacy support for backward compatibility
                source_string = args[i + 1]
                target_string = args[i + 2] if i + 2 < len(args) else ""
                if target_string and not target_string.startswith('--'):
                    string_transforms[source_string] = target_string
                    i += 3
                else:
                    print("❌ Missing target string for --source-env-string")
                    print("💡 Use 'policy-xfr --help' for usage information")
                    return None, None, None, None, False
            elif arg == '--target-env-string' and i + 1 < len(args):
                # This should only be used with --source-env-string, skip here
                i += 2
            elif arg == '--quiet' or arg == '-q':
                quiet_mode = True
                i += 1
            elif arg == '--verbose' or arg == '-v':
                verbose_mode = True
                i += 1
            elif arg == '--help' or arg == '-h':
                print("\n" + "="*60)
                print("POLICY-XFR COMMAND HELP")
                print("="*60)
                print("Usage: policy-xfr [--input <input_dir>] [--string-transform \"A\":\"B\", \"C\":\"D\", \"E\":\"F\"] [options]")
                print("\nArguments:")
                print("  --string-transform <transforms>  Multiple string transformations [OPTIONAL]")
                print("                                   Format: \"A\":\"B\", \"C\":\"D\", \"E\":\"F\"")
                print("                                   If not provided, processes files without transformations")
                print("\nOptions:")
                print("  --input <dir>                 Input directory (auto-detected from policy-export if not specified)")
                print("  --output-dir <dir>            Output directory (defaults to organized subdirectories)")
                print("  --quiet, -q                   Quiet mode (minimal output)")
                print("  --verbose, -v                 Verbose mode (detailed output)")
                print("  --help, -h                    Show this help message")
                print("\nExamples:")
                print("  policy-xfr --string-transform \"PROD_DB\":\"DEV_DB\", \"PROD_URL\":\"DEV_URL\"")
                print("  policy-xfr --input data/samples --string-transform \"old\":\"new\", \"test\":\"prod\"")
                print("  policy-xfr --string-transform \"A\":\"B\", \"C\":\"D\", \"E\":\"F\" --verbose")
                print("\nLegacy Support:")
                print("  policy-xfr --source-env-string \"PROD_DB\" --target-env-string \"DEV_DB\"")
                print("="*60)
                return None, None, None, None, False
            else:
                print(f"❌ Unknown argument: {arg}")
                print("💡 Use 'policy-xfr --help' for usage information")
                return None, None, None, None, False
        
        # string_transforms can be empty (direct processing mode)
        return input_dir, string_transforms, output_dir, quiet_mode, verbose_mode
    except Exception as e:
        print(f"❌ Error parsing policy-xfr command: {e}")
        return None, None, None, None, False


def parse_asset_formatter_command(command: str) -> tuple:
    """Parse asset-xfr command in interactive mode.
    Args:
        command (str): The command string
    Returns:
        tuple: (input_dir, string_transforms, output_dir, quiet_mode, verbose_mode)
    """
    try:
        args_str = command[len('asset-xfr'):].strip()
        input_dir = None
        string_transforms = {}
        output_dir = None
        quiet_mode = False
        verbose_mode = False
        args = args_str.split()
        i = 0
        while i < len(args):
            arg = args[i]
            if arg == '--input' and i + 1 < len(args):
                input_dir = args[i + 1]
                i += 2
            elif arg == '--output-dir' and i + 1 < len(args):
                output_dir = args[i + 1]
                i += 2
            elif arg == '--string-transform' and i + 1 < len(args):
                # Collect all args until next -- or end
                transform_parts = []
                j = i + 1
                while j < len(args) and not args[j].startswith('--'):
                    transform_parts.append(args[j])
                    j += 1
                
                if not transform_parts:
                    print("❌ Missing string transform argument")
                    print("💡 Expected format: --string-transform \"A\":\"B\", \"C\":\"D\", \"E\":\"F\"")
                    return None, None, None, None, False
                
                transform_arg = ' '.join(transform_parts)
                try:
                    # Parse format: "A":"B", "C":"D", "E":"F"
                    transforms = {}
                    # Remove outer quotes if present
                    if transform_arg.startswith('"') and transform_arg.endswith('"'):
                        transform_arg = transform_arg[1:-1]
                    
                    # Split by comma and process each pair
                    pairs = [pair.strip() for pair in transform_arg.split(',')]
                    for pair in pairs:
                        if ':' in pair:
                            source, target = pair.split(':', 1)
                            source = source.strip().strip('"')
                            target = target.strip().strip('"')
                            if source and target:
                                transforms[source] = target
                    
                    string_transforms.update(transforms)
                except Exception as e:
                    print(f"❌ Error parsing string transform argument: {e}")
                    print("💡 Expected format: --string-transform \"A\":\"B\", \"C\":\"D\", \"E\":\"F\"")
                    return None, None, None, None, False
                i = j
            elif arg == '--source-env-string' and i + 1 < len(args):
                # Legacy support for backward compatibility
                source_string = args[i + 1]
                target_string = args[i + 2] if i + 2 < len(args) else ""
                if target_string and not target_string.startswith('--'):
                    string_transforms[source_string] = target_string
                    i += 3
                else:
                    print("❌ Missing target string for --source-env-string")
                    print("💡 Use 'asset-xfr --help' for usage information")
                    return None, None, None, None, False
            elif arg == '--target-env-string' and i + 1 < len(args):
                # This should only be used with --source-env-string, skip here
                i += 2
            elif arg == '--quiet' or arg == '-q':
                quiet_mode = True
                i += 1
            elif arg == '--verbose' or arg == '-v':
                verbose_mode = True
                i += 1
            elif arg == '--help' or arg == '-h':
                print("\n" + "="*60)
                print("ASSET-XFR COMMAND HELP")
                print("="*60)
                print("Usage: asset-xfr [--input <input_dir>] [--string-transform \"A\":\"B\", \"C\":\"D\", \"E\":\"F\"] [options]")
                print("\nArguments:")
                print("  --string-transform <transforms>  Multiple string transformations [OPTIONAL]")
                print("                                   Format: \"A\":\"B\", \"C\":\"D\", \"E\":\"F\"")
                print("                                   If not provided, processes files without transformations")
                print("\nOptions:")
                print("  --input <dir>                 Input directory (auto-detected from asset-export if not specified)")
                print("  --output-dir <dir>            Output directory (defaults to organized subdirectories)")
                print("  --quiet, -q                   Quiet mode (minimal output)")
                print("  --verbose, -v                 Verbose mode (detailed output)")
                print("  --help, -h                    Show this help message")
                print("\nExamples:")
                print("  asset-xfr --string-transform \"PROD_DB\":\"DEV_DB\", \"PROD_URL\":\"DEV_URL\"")
                print("  asset-xfr --input data/samples --string-transform \"old\":\"new\", \"test\":\"prod\"")
                print("  asset-xfr --string-transform \"A\":\"B\", \"C\":\"D\", \"E\":\"F\" --verbose")
                print("\nLegacy Support:")
                print("  asset-xfr --source-env-string \"PROD_DB\" --target-env-string \"DEV_DB\"")
                print("\nNotes:")
                print("  • Specifically designed for asset configuration and profile transformations")
                print("  • Input directory is auto-detected from asset-export if not specified")
                print("  • Output files are organized in asset-import and asset-export subdirectories")
                print("="*60)
                return None, None, None, None, False
            else:
                print(f"❌ Unknown argument: {arg}")
                print("💡 Use 'asset-xfr --help' for usage information")
                return None, None, None, None, False
        
        # string_transforms can be empty (direct processing mode)
        return input_dir, string_transforms, output_dir, quiet_mode, verbose_mode
    except Exception as e:
        print(f"❌ Error parsing asset-xfr command: {e}")
        return None, None, None, None, False

def execute_formatter(input_dir: str, string_transforms: dict, output_dir: str, 
                     quiet_mode: bool, verbose_mode: bool, logger):
    """Execute formatter command in interactive mode.
    Args:
        input_dir (str): Input directory (can be None for auto-detection)
        string_transforms (dict): Dictionary of string transformations {source: target}
        output_dir (str): Output directory (can be None for default)
        quiet_mode (bool): Quiet mode flag
        verbose_mode (bool): Verbose mode flag
        logger: Logger instance
    """
    try:
        if not input_dir:
            if globals.GLOBAL_OUTPUT_DIR:
                global_policy_export_dir = globals.GLOBAL_OUTPUT_DIR / "policy-export"
                if global_policy_export_dir.exists() and global_policy_export_dir.is_dir():
                    input_dir = str(global_policy_export_dir)
                    if not quiet_mode:
                        print(f"📁 Using global output directory: {input_dir}")
                else:
                    if not quiet_mode:
                        print(f"📁 Global output directory policy-export not found: {global_policy_export_dir}")
            if not input_dir:
                current_dir = Path.cwd()
                toolkit_dirs = [d for d in current_dir.iterdir() if d.is_dir() and d.name.startswith("adoc-migration-toolkit-")]
                if not toolkit_dirs:
                    print("❌ No adoc-migration-toolkit directory found.")
                    print("💡 Please specify an input directory or run 'policy-export' first to generate ZIP files")
                    return
                toolkit_dirs.sort(key=lambda x: x.stat().st_ctime, reverse=True)
                latest_toolkit_dir = toolkit_dirs[0]
                input_dir = str(latest_toolkit_dir / "policy-export")
                if not quiet_mode:
                    print(f"📁 Using input directory: {input_dir}")
        formatter = PolicyExportFormatter(
            input_dir=input_dir,
            string_transforms=string_transforms,
            output_dir=output_dir,
            logger=logger
        )
        stats = formatter.process_directory()
        if not quiet_mode:
            print("\n" + "="*60)
            print("PROCESSING SUMMARY")
            print("="*60)
            print(f"Input directory:     {input_dir}")
            print(f"Output directory:    {formatter.output_dir}")
            print(f"Asset export dir:    {formatter.asset_export_dir}")
            print(f"Policy export dir:   {formatter.policy_export_dir}")
            if not string_transforms:
                print(f"String transformations: None (direct processing mode)")
                print("  💡 No transformations provided - processing files without string replacements")
            else:
                print(f"String transformations: {len(string_transforms)} transformations")
                identical_transforms = 0
                for source, target in string_transforms.items():
                    if source == target:
                        print(f"  '{source}' -> '{target}' (identical - will be skipped)")
                        identical_transforms += 1
                    else:
                        print(f"  '{source}' -> '{target}'")
                if identical_transforms > 0:
                    print(f"  Note: {identical_transforms} identical transformation(s) will be skipped")
            print(f"Total files found:   {stats['total_files']}")
            if stats['json_files'] > 0:
                print(f"JSON files:          {stats['json_files']}")
            if stats['zip_files'] > 0:
                print(f"ZIP files:           {stats['zip_files']}")
            print(f"Files investigated:  {stats.get('files_investigated', 0)}")
            print(f"Changes made:        {stats.get('changes_made', 0)}")
            if not string_transforms:
                print("  ℹ️  Direct processing mode (no transformations provided)")
                print("  💡 This mode processes files without string replacements")
                print("     • Use this when no environment-specific string changes are needed")
                print("     • Files are copied and organized without modifications")
            elif stats.get('changes_made', 0) == 0:
                print("  ℹ️  No string transformations were applied")
                print("  💡 This could be because:")
                print("     • Source and target strings are identical (e.g., 'Snowflake':'Snowflake')")
                print("     • Source strings were not found in the files")
                print("     • No transformation was needed for this dataset")
            print(f"Successful:          {stats['successful']}")
            print(f"Failed:              {stats['failed']}")
            if stats.get('extracted_assets', 0) > 0:
                print(f"Assets extracted:    {stats['extracted_assets']}")
            if stats.get('all_assets', 0) > 0:
                print(f"All assets found:    {stats['all_assets']}")
            if stats.get('deep_scan_count', 0) > 0:
                print(f"Deep scan operations: {stats['deep_scan_count']}")
            if stats.get('csv_processed', False):
                print(f"CSV file processed:  asset-all-export.csv -> asset-all-import-ready.csv")
            if stats.get('config_csv_processed', False):
                print(f"CSV file processed:  asset-config-export.csv -> asset-config-import-ready.csv")
            if stats.get('total_policies_processed', 0) > 0:
                print(f"\nPolicy Statistics:")
                print(f"  Total policies processed: {stats['total_policies_processed']}")
                print(f"  Segmented SPARK policies: {stats['segmented_spark_policies']}")
                print(f"  Segmented JDBC_SQL policies: {stats['segmented_jdbc_policies']}")
                print(f"  Non-segmented policies: {stats['non_segmented_policies']}")
            if stats['errors']:
                print(f"\nErrors encountered:  {len(stats['errors'])}")
                for error in stats['errors'][:5]:
                    print(f"  - {error}")
                if len(stats['errors']) > 5:
                    print(f"  ... and {len(stats['errors']) - 5} more errors")
            print("="*60)
        if stats['failed'] > 0 or stats['errors']:
            print("⚠️  Processing completed with errors. Check log file for details.")
        else:
            print("✅ Formatter completed successfully!")
    except Exception as e:
        print(f"❌ Error executing formatter: {e}")
        logger.error(f"Error executing formatter: {e}")


def execute_asset_formatter(input_dir: str, string_transforms: dict, output_dir: str, 
                           quiet_mode: bool, verbose_mode: bool, logger):
    """Execute asset formatter command in interactive mode.
    Args:
        input_dir (str): Input directory (can be None for auto-detection)
        string_transforms (dict): Dictionary of string transformations {source: target}
        output_dir (str): Output directory (can be None for default)
        quiet_mode (bool): Quiet mode flag
        verbose_mode (bool): Verbose mode flag
        logger: Logger instance
    """
    try:
        if not input_dir:
            if globals.GLOBAL_OUTPUT_DIR:
                global_asset_export_dir = globals.GLOBAL_OUTPUT_DIR / "asset-export"
                if global_asset_export_dir.exists() and global_asset_export_dir.is_dir():
                    input_dir = str(global_asset_export_dir)
                    if not quiet_mode:
                        print(f"📁 Using global output directory: {input_dir}")
                else:
                    if not quiet_mode:
                        print(f"📁 Global output directory asset-export not found: {global_asset_export_dir}")
            if not input_dir:
                current_dir = Path.cwd()
                toolkit_dirs = [d for d in current_dir.iterdir() if d.is_dir() and d.name.startswith("adoc-migration-toolkit-")]
                if not toolkit_dirs:
                    print("❌ No adoc-migration-toolkit directory found.")
                    print("💡 Please specify an input directory or run 'asset-config-export' first to generate CSV files")
                    return
                toolkit_dirs.sort(key=lambda x: x.stat().st_ctime, reverse=True)
                latest_toolkit_dir = toolkit_dirs[0]
                input_dir = str(latest_toolkit_dir / "asset-export")
                if not quiet_mode:
                    print(f"📁 Using input directory: {input_dir}")
        
        formatter = AssetExportFormatter(
            input_dir=input_dir,
            string_transforms=string_transforms,
            output_dir=output_dir,
            logger=logger
        )
        stats = formatter.process_directory()
        
        if not quiet_mode:
            print("\n" + "="*60)
            print("ASSET PROCESSING SUMMARY")
            print("="*60)
            print(f"Input directory:     {input_dir}")
            print(f"Output directory:    {formatter.output_dir}")
            print(f"Asset export dir:    {formatter.asset_export_dir}")
            if not string_transforms:
                print(f"String transformations: None (direct processing mode)")
                print("  💡 No transformations provided - processing files without string replacements")
            else:
                print(f"String transformations: {len(string_transforms)} transformations")
                identical_transforms = 0
                for source, target in string_transforms.items():
                    if source == target:
                        print(f"  '{source}' -> '{target}' (identical - will be skipped)")
                        identical_transforms += 1
                    else:
                        print(f"  '{source}' -> '{target}'")
                if identical_transforms > 0:
                    print(f"  Note: {identical_transforms} identical transformation(s) will be skipped")
            print(f"Total files found:   {stats['total_files']}")
            if stats['csv_files'] > 0:
                print(f"CSV files:           {stats['csv_files']}")
            print(f"Files investigated:  {stats.get('files_investigated', 0)}")
            print(f"Changes made:        {stats.get('changes_made', 0)}")
            print(f"Assets processed:    {stats.get('assets_processed', 0)}")
            if not string_transforms:
                print("  ℹ️  Direct processing mode (no transformations provided)")
                print("  💡 This mode processes files without string replacements")
                print("     • Use this when no environment-specific string changes are needed")
                print("     • Files are copied and organized without modifications")
            elif stats.get('changes_made', 0) == 0:
                print("  ℹ️  No string transformations were applied")
                print("  💡 This could be because:")
                print("     • Source and target strings are identical (e.g., 'Snowflake':'Snowflake')")
                print("     • Source strings were not found in the files")
                print("     • No transformation was needed for this dataset")
            print(f"Successful:          {stats['successful']}")
            print(f"Failed:              {stats['failed']}")
            if stats['errors']:
                print(f"\nErrors encountered:  {len(stats['errors'])}")
                for error in stats['errors'][:5]:
                    print(f"  - {error}")
                if len(stats['errors']) > 5:
                    print(f"  ... and {len(stats['errors']) - 5} more errors")
            print("="*60)
        if stats['failed'] > 0 or stats['errors']:
            print("⚠️  Asset processing completed with errors. Check log file for details.")
        else:
            print("✅ Asset formatter completed successfully!")
    except Exception as e:
        print(f"❌ Error executing asset formatter: {e}")
        logger.error(f"Error executing asset formatter: {e}") 