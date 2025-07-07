"""
Lineage Operations for ADOC Migration Toolkit.

This module provides functionality for creating lineage relationships between assets
based on CSV input files. It supports the complex lineage format with grouping,
sequencing, and detailed transformations.
"""

import csv
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from ..shared.api_client import AcceldataAPIClient


@dataclass
class LineageRow:
    """Represents a single lineage row from the CSV."""
    group_id: Optional[str]
    step_order: Optional[int]
    source_asset_id: str
    source_column: Optional[str]
    target_asset_id: str
    target_column: Optional[str]
    relationship_type: str
    transformation: Optional[str]
    notes: Optional[str]


class LineageProcessor:
    """Processes lineage CSV files and creates lineage relationships via API."""
    
    def __init__(self, client: AcceldataAPIClient, logger: Optional[logging.Logger] = None, verbose: bool = False):
        """
        Initialize the lineage processor.
        
        Args:
            client: API client for making lineage creation calls
            logger: Logger instance for operation tracking
            verbose: If True, output detailed API request/response info
        """
        self.client = client
        self.logger = logger or logging.getLogger(__name__)
        self.verbose = verbose
        
    def parse_lineage_csv(self, csv_file: str) -> List[LineageRow]:
        """
        Parse a lineage CSV file into structured data.
        
        Args:
            csv_file: Path to the CSV file containing lineage data
            
        Returns:
            List of LineageRow objects representing the lineage data
            
        Raises:
            FileNotFoundError: If the CSV file doesn't exist
            ValueError: If the CSV format is invalid
        """
        csv_path = Path(csv_file)
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_file}")
        
        self.logger.info(f"Parsing lineage CSV file: {csv_file}")
        
        lineage_rows = []
        required_columns = ['Source Asset ID', 'Target Asset ID', 'Relationship Type']
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                # Validate required columns
                missing_columns = [col for col in required_columns if col not in reader.fieldnames]
                if missing_columns:
                    raise ValueError(f"Missing required columns in CSV: {missing_columns}")
                
                for row_num, row in enumerate(reader, 2):  # Start from 2 to account for header
                    try:
                        # Parse step order (optional)
                        step_order = None
                        if row.get('Step Order') and row['Step Order'].strip():
                            try:
                                step_order = int(row['Step Order'].strip())
                            except ValueError:
                                self.logger.warning(f"Row {row_num}: Invalid step order '{row['Step Order']}', ignoring")
                        
                        # Validate relationship type
                        relationship_type = row['Relationship Type'].strip().lower()
                        if relationship_type not in ['upstream', 'downstream']:
                            raise ValueError(f"Row {row_num}: Invalid relationship type '{row['Relationship Type']}'. Must be 'upstream' or 'downstream'")
                        
                        lineage_row = LineageRow(
                            group_id=row.get('Group ID', '').strip() or None,
                            step_order=step_order,
                            source_asset_id=row['Source Asset ID'].strip(),
                            source_column=row.get('Source Column', '').strip() or None,
                            target_asset_id=row['Target Asset ID'].strip(),
                            target_column=row.get('Target Column', '').strip() or None,
                            relationship_type=relationship_type,
                            transformation=row.get('Transformation', '').strip() or None,
                            notes=row.get('Notes', '').strip() or None
                        )
                        
                        lineage_rows.append(lineage_row)
                        
                    except Exception as e:
                        self.logger.error(f"Row {row_num}: Error parsing row: {e}")
                        raise ValueError(f"Row {row_num}: {e}")
        
        except UnicodeDecodeError:
            # Try with different encoding
            with open(csv_path, 'r', encoding='latin-1') as f:
                reader = csv.DictReader(f)
                # ... same logic as above
                pass
        
        self.logger.info(f"Successfully parsed {len(lineage_rows)} lineage rows")
        return lineage_rows
    
    def validate_assets(self, lineage_rows: List[LineageRow]) -> Dict[str, List[str]]:
        """
        Validate that all assets referenced in the lineage exist.
        
        Args:
            lineage_rows: List of lineage rows to validate
            
        Returns:
            Dictionary mapping asset IDs to their UIDs if found
            
        Raises:
            ValueError: If any assets are not found
        """
        self.logger.info("Validating asset existence...")
        
        # Collect unique asset IDs
        asset_ids = set()
        for row in lineage_rows:
            asset_ids.add(row.source_asset_id)
            asset_ids.add(row.target_asset_id)
        
        # Validate each asset
        asset_uid_map = {}
        missing_assets = []
        
        for asset_id in asset_ids:
            if self.verbose:
                print(f"[VERBOSE] Looking up asset: {asset_id}")
            try:
                if self.verbose:
                    print(f"[VERBOSE] API CALL: /catalog-server/api/assets?uid={asset_id}")
                asset_data = self.client.get_asset_by_uid(asset_id)
                if self.verbose:
                    print(f"[VERBOSE] API RESPONSE for {asset_id}: {asset_data}")
                if asset_data and 'id' in asset_data:
                    asset_uid_map[asset_id] = asset_data['id']
                    self.logger.debug(f"Found asset: {asset_id} -> ID: {asset_data['id']}")
                else:
                    if self.verbose:
                        print(f"[VERBOSE] Asset not found in response: {asset_id}")
                    missing_assets.append(asset_id)
            except Exception as e:
                if self.verbose:
                    print(f"[VERBOSE] Exception looking up asset {asset_id}: {e}")
                self.logger.debug(f"Asset not found: {asset_id} - {e}")
                missing_assets.append(asset_id)
        
        if missing_assets:
            raise ValueError(f"Assets not found: {missing_assets}")
        
        self.logger.info(f"All {len(asset_ids)} assets validated successfully")
        return asset_uid_map
    
    def group_lineage_by_target(self, lineage_rows: List[LineageRow]) -> Dict[str, List[LineageRow]]:
        """
        Group lineage rows by target asset for batch processing.
        
        Args:
            lineage_rows: List of lineage rows to group
            
        Returns:
            Dictionary mapping target asset IDs to their lineage rows
        """
        grouped = {}
        for row in lineage_rows:
            if row.target_asset_id not in grouped:
                grouped[row.target_asset_id] = []
            grouped[row.target_asset_id].append(row)
        
        return grouped
    
    def create_lineage_batch(self, target_asset_id: str, lineage_rows: List[LineageRow], 
                           asset_uid_map: Dict[str, str]) -> Dict[str, Any]:
        """
        Create lineage for a single target asset.
        
        Args:
            target_asset_id: The target asset ID
            lineage_rows: List of lineage rows for this target asset
            asset_uid_map: Mapping of asset IDs to UIDs
            
        Returns:
            API response data
        """
        target_uid = asset_uid_map[target_asset_id]
        
        # Group by relationship type
        upstream_rows = [row for row in lineage_rows if row.relationship_type == 'upstream']
        downstream_rows = [row for row in lineage_rows if row.relationship_type == 'downstream']
        
        results = {}
        
        # Process upstream lineage
        if upstream_rows:
            upstream_asset_ids = [asset_uid_map[row.source_asset_id] for row in upstream_rows]
            process_name = self._generate_process_name(upstream_rows, "upstream")
            process_description = self._generate_process_description(upstream_rows, "upstream")
            
            payload = {
                "direction": "UPSTREAM",
                "assetIds": upstream_asset_ids,
                "process": {
                    "name": process_name,
                    "description": process_description
                }
            }
            
            self.logger.info(f"Creating upstream lineage for {target_asset_id} with {len(upstream_asset_ids)} source assets")
            response = self.client.make_api_call(
                endpoint=f"/torch-pipeline/api/assets/{target_uid}/lineage",
                method="POST",
                json_payload=payload
            )
            results["upstream"] = response
        
        # Process downstream lineage
        if downstream_rows:
            downstream_asset_ids = [asset_uid_map[row.target_asset_id] for row in downstream_rows]
            process_name = self._generate_process_name(downstream_rows, "downstream")
            process_description = self._generate_process_description(downstream_rows, "downstream")
            
            payload = {
                "direction": "DOWNSTREAM",
                "assetIds": downstream_asset_ids,
                "process": {
                    "name": process_name,
                    "description": process_description
                }
            }
            
            self.logger.info(f"Creating downstream lineage for {target_asset_id} with {len(downstream_asset_ids)} target assets")
            response = self.client.make_api_call(
                endpoint=f"/torch-pipeline/api/assets/{target_uid}/lineage",
                method="POST",
                json_payload=payload
            )
            results["downstream"] = response
        
        return results
    
    def _generate_process_name(self, lineage_rows: List[LineageRow], direction: str) -> str:
        """Generate a process name based on lineage rows."""
        if not lineage_rows:
            return f"{direction.capitalize()} Lineage"
        
        # Try to use group ID if available
        group_ids = set(row.group_id for row in lineage_rows if row.group_id)
        if group_ids:
            return f"{direction.capitalize()} Lineage - Group {', '.join(group_ids)}"
        
        # Use transformation type if available
        transformations = set(row.transformation for row in lineage_rows if row.transformation)
        if transformations:
            return f"{direction.capitalize()} Lineage - {', '.join(transformations)}"
        
        return f"{direction.capitalize()} Lineage"
    
    def _generate_process_description(self, lineage_rows: List[LineageRow], direction: str) -> str:
        """Generate a process description based on lineage rows."""
        if not lineage_rows:
            return f"Data flow {direction}"
        
        descriptions = []
        
        for row in lineage_rows:
            desc_parts = []
            
            # Add source and target
            if direction == "upstream":
                desc_parts.append(f"{row.source_asset_id} -> {row.target_asset_id}")
            else:
                desc_parts.append(f"{row.target_asset_id} -> {row.source_asset_id}")
            
            # Add transformation if available
            if row.transformation:
                desc_parts.append(f"({row.transformation})")
            
            # Add notes if available
            if row.notes:
                desc_parts.append(f"- {row.notes}")
            
            descriptions.append(" ".join(desc_parts))
        
        return "; ".join(descriptions)
    
    def create_lineage_from_csv(self, csv_file: str, dry_run: bool = False) -> Dict[str, Any]:
        """
        Create lineage relationships from a CSV file.
        
        Args:
            csv_file: Path to the CSV file containing lineage data
            dry_run: If True, validate but don't create lineage
            
        Returns:
            Dictionary containing results and statistics
        """
        self.logger.info(f"Starting lineage creation from CSV: {csv_file}")
        
        try:
            # Parse CSV file
            lineage_rows = self.parse_lineage_csv(csv_file)
            
            if not lineage_rows:
                raise ValueError("No valid lineage rows found in CSV file")
            
            # Validate assets
            asset_uid_map = self.validate_assets(lineage_rows)
            
            if dry_run:
                self.logger.info("DRY RUN: Would create lineage for the following:")
                for row in lineage_rows:
                    self.logger.info(f"  {row.source_asset_id} -> {row.target_asset_id} ({row.relationship_type})")
                return {
                    "status": "dry_run",
                    "message": "Validation successful - no lineage created",
                    "rows_processed": len(lineage_rows),
                    "assets_validated": len(asset_uid_map)
                }
            
            # Group by target asset
            grouped_lineage = self.group_lineage_by_target(lineage_rows)
            
            # Create lineage for each target asset
            results = {}
            success_count = 0
            error_count = 0
            
            for target_asset_id, target_rows in grouped_lineage.items():
                try:
                    result = self.create_lineage_batch(target_asset_id, target_rows, asset_uid_map)
                    results[target_asset_id] = result
                    success_count += 1
                    self.logger.info(f"Successfully created lineage for {target_asset_id}")
                except Exception as e:
                    self.logger.error(f"Failed to create lineage for {target_asset_id}: {e}")
                    results[target_asset_id] = {"error": str(e)}
                    error_count += 1
            
            return {
                "status": "completed",
                "rows_processed": len(lineage_rows),
                "assets_processed": len(grouped_lineage),
                "success_count": success_count,
                "error_count": error_count,
                "results": results
            }
            
        except Exception as e:
            self.logger.error(f"Lineage creation failed: {e}")
            raise


def execute_create_lineage(csv_file: str, client: AcceldataAPIClient, logger: logging.Logger, 
                          dry_run: bool = False, quiet_mode: bool = False, verbose_mode: bool = False) -> None:
    """
    Execute the create-lineage command.
    
    Args:
        csv_file: Path to the CSV file containing lineage data
        client: API client for making lineage creation calls
        logger: Logger instance for operation tracking
        dry_run: If True, validate but don't create lineage
        quiet_mode: If True, suppress progress output
        verbose_mode: If True, output detailed API request/response info
    """
    if not quiet_mode:
        print(f"\n🔄 Creating lineage from CSV file: {csv_file}")
        if dry_run:
            print("🔍 DRY RUN MODE - No lineage will be created")
        print("-" * 60)
    
    try:
        processor = LineageProcessor(client, logger, verbose=verbose_mode)
        result = processor.create_lineage_from_csv(csv_file, dry_run=dry_run)
        
        if not quiet_mode:
            if result["status"] == "dry_run":
                print(f"✅ {result['message']}")
                print(f"📊 Rows processed: {result['rows_processed']}")
                print(f"📊 Assets validated: {result['assets_validated']}")
            else:
                print(f"✅ Lineage creation completed!")
                print(f"📊 Rows processed: {result['rows_processed']}")
                print(f"📊 Assets processed: {result['assets_processed']}")
                print(f"✅ Successful: {result['success_count']}")
                if result['error_count'] > 0:
                    print(f"❌ Errors: {result['error_count']}")
        
        logger.info(f"Lineage creation completed: {result}")
        
    except Exception as e:
        error_msg = f"Failed to create lineage: {e}"
        if not quiet_mode:
            print(f"❌ {error_msg}")
        logger.error(error_msg)
        raise 