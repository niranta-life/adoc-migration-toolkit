"""
Guided Migration System for ADOC Export Import Tool.

This module provides a stateful guided migration system that walks users through
the complete migration process step by step, with validation and helpful guidance.
"""

import os
import json
import pickle
import csv
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
import logging


class MigrationState:
    """Represents the state of a guided migration session."""
    
    def __init__(self, name: str):
        self.name = name
        self.created_at = datetime.now()
        self.current_step = 0
        self.completed_steps = []
        self.data = {}
        self.errors = []
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert state to dictionary for serialization."""
        return {
            'name': self.name,
            'created_at': self.created_at.isoformat(),
            'current_step': self.current_step,
            'completed_steps': self.completed_steps,
            'data': self.data,
            'errors': self.errors
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MigrationState':
        """Create state from dictionary."""
        state = cls(data['name'])
        state.created_at = datetime.fromisoformat(data['created_at'])
        state.current_step = data['current_step']
        state.completed_steps = data['completed_steps']
        state.data = data['data']
        state.errors = data['errors']
        return state


class GuidedMigration:
    """Guided migration system with state management."""
    
    STEPS = [
        {
            'id': 1,
            'name': 'setup',
            'title': 'Migration Setup',
            'description': 'Configure migration parameters and validate prerequisites'
        },
        {
            'id': 2,
            'name': 'export_policies',
            'title': 'Export Policies from Source',
            'description': 'Manual step: Export policies from Acceldata UI'
        },
        {
            'id': 3,
            'name': 'process_formatter',
            'title': 'Process ZIP Files with Formatter',
            'description': 'Run formatter to translate environment strings and extract assets'
        },
        {
            'id': 4,
            'name': 'export_profiles',
            'title': 'Export Asset Profiles',
            'description': 'Export profile configurations from source environment'
        },
        {
            'id': 5,
            'name': 'import_profiles',
            'title': 'Import Asset Profiles',
            'description': 'Import profile configurations to target environment'
        },
        {
            'id': 6,
            'name': 'export_configs',
            'title': 'Export Asset Configurations',
            'description': 'Export detailed asset configurations from source environment'
        },
        {
            'id': 7,
            'name': 'import_configs',
            'title': 'Import Asset Configurations',
            'description': 'Import detailed configurations to target environment'
        },
        {
            'id': 8,
            'name': 'handle_segments',
            'title': 'Handle Segmented Assets',
            'description': 'Export and import segment configurations for SPARK assets'
        },
        {
            'id': 9,
            'name': 'completion',
            'title': 'Migration Complete',
            'description': 'Final validation and cleanup'
        }
    ]
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.state_file_dir = Path.home() / '.adoc-migrations'
        self.state_file_dir.mkdir(exist_ok=True)
    
    def get_state_file_path(self, name: str) -> Path:
        """Get the state file path for a migration."""
        return self.state_file_dir / f'.{name}-guide'
    
    def save_state(self, state: MigrationState) -> bool:
        """Save migration state to file."""
        try:
            state_file = self.get_state_file_path(state.name)
            with open(state_file, 'wb') as f:
                pickle.dump(state.to_dict(), f)
            self.logger.info(f"Migration state saved to {state_file}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to save migration state: {e}")
            return False
    
    def load_state(self, name: str) -> Optional[MigrationState]:
        """Load migration state from file."""
        try:
            state_file = self.get_state_file_path(name)
            if not state_file.exists():
                return None
            
            with open(state_file, 'rb') as f:
                data = pickle.load(f)
            state = MigrationState.from_dict(data)
            self.logger.info(f"Migration state loaded from {state_file}")
            return state
        except Exception as e:
            self.logger.error(f"Failed to load migration state: {e}")
            return None
    
    def delete_state(self, name: str) -> bool:
        """Delete migration state file."""
        try:
            state_file = self.get_state_file_path(name)
            if state_file.exists():
                state_file.unlink()
                self.logger.info(f"Migration state deleted: {state_file}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to delete migration state: {e}")
            return False
    
    def list_migrations(self) -> List[str]:
        """List all available migrations."""
        migrations = []
        for state_file in self.state_file_dir.glob('.*-guide'):
            name = state_file.stem.replace('-guide', '')
            migrations.append(name)
        return sorted(migrations)
    
    def get_current_step_info(self, state: MigrationState) -> Dict[str, Any]:
        """Get information about the current step."""
        if state.current_step >= len(self.STEPS):
            return self.STEPS[-1]
        return self.STEPS[state.current_step]
    
    def validate_step_prerequisites(self, step_name: str, state: MigrationState) -> Tuple[bool, List[str]]:
        """Validate prerequisites for a specific step."""
        errors = []
        
        if step_name == 'setup':
            # No prerequisites for setup
            pass
        elif step_name == 'export_policies':
            # Manual step - no file validation needed
            pass
        elif step_name == 'process_formatter':
            # Check if input directory exists and contains ZIP files
            input_dir = state.data.get('input_directory')
            if not input_dir:
                errors.append("Input directory not configured")
            elif not Path(input_dir).exists():
                errors.append(f"Input directory does not exist: {input_dir}")
            else:
                zip_files = list(Path(input_dir).glob('*.zip'))
                if not zip_files:
                    errors.append(f"No ZIP files found in {input_dir}")
        
        elif step_name == 'export_profiles':
            # Check if asset_uids.csv exists
            asset_uids_file = state.data.get('asset_uids_file')
            exists, error_msg = self._verify_file_exists(asset_uids_file, "Asset UIDs file")
            if not exists:
                errors.append(error_msg)
            else:
                # Validate CSV format - formatter generates source-env,target-env columns
                if not self._validate_csv_format(asset_uids_file, ['source-env', 'target-env']):
                    errors.append(f"Invalid CSV format in {asset_uids_file}")
        
        elif step_name == 'import_profiles':
            # Check if asset-profiles-import-ready.csv exists
            profiles_file = asset_uids_file.parent / 'asset-profiles-import-ready.csv'
            if not profiles_file.exists():
                errors.append(f"Asset profiles file not found: {profiles_file}")
            else:
                # Check if file has content
                try:
                    with open(profiles_file, 'r') as f:
                        lines = f.readlines()
                        if len(lines) < 2:  # Header + at least one data row
                            errors.append(f"Asset profiles file is empty or has no data: {profiles_file}")
                except Exception as e:
                    errors.append(f"Error reading asset profiles file: {e}")
        
        elif step_name == 'export_configs':
            # Check if asset_uids.csv exists
            asset_uids_file = state.data.get('asset_uids_file')
            exists, error_msg = self._verify_file_exists(asset_uids_file, "Asset UIDs file")
            if not exists:
                errors.append(error_msg)
            else:
                # Validate CSV format - formatter generates source-env,target-env columns
                if not self._validate_csv_format(asset_uids_file, ['source-env', 'target-env']):
                    errors.append(f"Invalid CSV format in {asset_uids_file}")
        
        elif step_name == 'import_configs':
            # Check if asset-config-export.csv exists
            configs_file = state.data.get('configs_export_file')
            exists, error_msg = self._verify_file_exists(configs_file, "Asset configs export file")
            if not exists:
                errors.append(error_msg)
            else:
                # Check if export step was completed
                if 'export_configs' not in state.completed_steps:
                    errors.append("Export Asset Configurations step must be completed before importing")
                else:
                    # Validate CSV format
                    validation_result = self._validate_csv_format_with_details(configs_file, ['target-env', 'config_json'])
                    if not validation_result['valid']:
                        errors.append(f"Invalid CSV format in {configs_file}: {validation_result['error']}")
        
        elif step_name == 'handle_segments':
            # Check if segmented_spark_uids.csv exists
            segments_file = asset_uids_file.parent.parent / 'policy-export' / 'segmented_spark_uids.csv'
            if not segments_file.exists():
                errors.append(f"Segmented SPARK UIDs file not found: {segments_file}")
            else:
                # Check if file has content
                try:
                    with open(segments_file, 'r') as f:
                        lines = f.readlines()
                        if len(lines) < 2:  # Header + at least one data row
                            errors.append(f"Segmented SPARK UIDs file is empty or has no data: {segments_file}")
                except Exception as e:
                    errors.append(f"Error reading segmented SPARK UIDs file: {e}")
        
        return len(errors) == 0, errors
    
    def _validate_csv_format(self, file_path: str, expected_columns: List[str]) -> bool:
        """Validate CSV file format."""
        try:
            with open(file_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if not header:
                    return False
                
                # Check if all expected columns are present
                for col in expected_columns:
                    if col not in header:
                        return False
                
                # Check if file has at least one data row
                try:
                    next(reader)
                    return True
                except StopIteration:
                    return False
        except Exception:
            return False
    
    def _validate_csv_format_with_details(self, file_path: str, expected_columns: List[str]) -> Dict[str, Any]:
        """Validate CSV file format and return details."""
        try:
            with open(file_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if not header:
                    return {'valid': False, 'error': 'CSV file is empty'}
                
                # Check if all expected columns are present
                for col in expected_columns:
                    if col not in header:
                        return {'valid': False, 'error': f"Missing column: {col}"}
                
                # Check if file has at least one data row
                try:
                    next(reader)
                    return {'valid': True, 'error': None}
                except StopIteration:
                    return {'valid': False, 'error': 'CSV file has no data rows (only header)'}
        except Exception as e:
            return {'valid': False, 'error': str(e)}
    
    def _verify_file_exists(self, file_path: str, file_description: str) -> Tuple[bool, str]:
        """Verify that a file exists and provide helpful error message."""
        if not file_path:
            return False, f"{file_description} not configured"
        
        path = Path(file_path)
        if not path.exists():
            return False, f"{file_description} does not exist: {file_path}"
        
        if not path.is_file():
            return False, f"{file_description} is not a file: {file_path}"
        
        return True, ""
    
    def get_step_help(self, step_name: str) -> str:
        """Get help information for a specific step."""
        help_texts = {
            'setup': """
Migration Setup Help:
- Configure source and target environment strings
- Set input directory for policy export ZIP files
- Validate environment configuration
- Set output directory for processed files

Required Information:
- Source environment string (e.g., "PROD_DB")
- Target environment string (e.g., "DEV_DB")
- Input directory path containing ZIP files
- Output directory path (optional)
            """,
            
            'export_policies': """
Export Policies Help:
This is a manual step that must be completed using the Acceldata UI.

Steps:
1. Navigate to your source Acceldata environment
2. Go to the Policies section
3. Select the policies you want to migrate
4. Export them as ZIP files
5. Download the ZIP files to your local machine
6. Place them in the input directory configured in the setup step

After completing this step, type 'continue' to proceed.
            """,
            
            'process_formatter': """
Process Formatter Help:
This step runs the formatter to translate environment strings and extract asset information.

The formatter will:
- Read ZIP files from the input directory
- Extract JSON files while maintaining structure
- Replace source environment strings with target strings
- Generate import-ready ZIP files
- Create specialized CSV files for asset management

Output files:
- *_import_ready/ directories with translated ZIP files
- segmented_spark_uids.csv - UIDs of segmented SPARK assets (source-env,target-env format)
- asset_uids.csv - All asset UIDs for profile/configuration management (source-env,target-env format)

Command that will be executed:
python -m adoc_export_import formatter --input {input_dir} --source-env-string "{source_string}" --target-env-string "{target_string}" --verbose
            """,
            
            'export_profiles': """
Export Asset Profiles Help:
This step exports profile configurations from the source environment.

The guided migration will:
- Read UIDs from asset_uids.csv (source-env column)
- Make API calls to get profile configurations
- Export profile configurations to CSV format
- Verify the export file was created successfully

Input file: asset_uids.csv (generated by formatter with source-env,target-env columns)
Output file: asset-profiles-export.csv

This step will execute the actual asset-profile-export command with verbose output.
            """,
            
            'import_profiles': """
Import Asset Profiles Help:
This step imports profile configurations to the target environment.

Prerequisites:
- Export Asset Profiles step must be completed first
- asset-profiles-import-ready.csv file must exist and contain data

The guided migration will:
- Verify that asset-profiles-import-ready.csv exists and contains data
- Count the number of profiles available for import
- Prepare for actual import (currently in verification mode)

Note: Full import functionality is being implemented. This step currently verifies the export file.
            """,
            
            'export_configs': """
Export Asset Configurations Help:
This step exports detailed asset configurations from the source environment.

The guided migration will:
- Read UIDs from asset_uids.csv (source-env column)
- Create placeholder file for asset configurations
- Prepare for actual export (currently in development)

Input file: asset_uids.csv (generated by formatter with source-env,target-env columns)
Output file: asset-config-export.csv

Note: Full export functionality is being implemented. This step currently creates a placeholder file.
            """,
            
            'import_configs': """
Import Asset Configurations Help:
This step imports detailed configurations to the target environment.

Prerequisites:
- Export Asset Configurations step must be completed first
- asset-config-export.csv file must exist and contain data

The guided migration will:
- Verify that asset-config-export.csv exists
- Prepare for actual import (currently in verification mode)

Note: Full import functionality is being implemented. This step currently verifies the export file.
            """,
            
            'handle_segments': """
Handle Segmented Assets Help:
This step handles segment configurations for SPARK assets.

The guided migration will:
- Verify that segmented_spark_uids.csv exists and contains data
- Count the number of segments available for processing
- Prepare for actual export/import (currently in verification mode)

Input file: segmented_spark_uids.csv (generated by formatter with source-env,target-env columns)

Note: Full segments functionality is being implemented. This step currently verifies the input file.
            """,
            
            'completion': """
Migration Complete Help:
Congratulations! Your migration has been completed successfully.

Final steps:
1. Verify that all assets have been migrated correctly
2. Test the migrated policies in the target environment
3. Clean up temporary files if needed
4. Delete migration state file if no longer needed

You can delete the migration state with: delete-migration {name}
            """
        }
        
        return help_texts.get(step_name, "No help available for this step.")
    
    def execute_step(self, step_name: str, state: MigrationState, client=None) -> Tuple[bool, str]:
        """Execute a specific migration step."""
        if step_name == 'setup':
            return self._execute_setup(state)
        elif step_name == 'export_policies':
            return self._execute_export_policies(state)
        elif step_name == 'process_formatter':
            return self._execute_process_formatter(state)
        elif step_name == 'export_profiles':
            return self._execute_export_profiles(state, client)
        elif step_name == 'import_profiles':
            return self._execute_import_profiles(state, client)
        elif step_name == 'export_configs':
            return self._execute_export_configs(state, client)
        elif step_name == 'import_configs':
            return self._execute_import_configs(state, client)
        elif step_name == 'handle_segments':
            return self._execute_handle_segments(state, client)
        elif step_name == 'completion':
            return self._execute_completion(state)
        else:
            return False, f"Unknown step: {step_name}"
    
    def _execute_setup(self, state: MigrationState) -> Tuple[bool, str]:
        """Execute setup step."""
        # This step is handled interactively in the main loop
        return True, "Setup completed successfully"
    
    def _execute_export_policies(self, state: MigrationState) -> Tuple[bool, str]:
        """Execute export policies step."""
        # This is a manual step - just mark as completed
        return True, "Manual export step completed"
    
    def _execute_process_formatter(self, state: MigrationState) -> Tuple[bool, str]:
        """Execute process formatter step."""
        try:
            input_dir = state.data.get('input_directory')
            source_string = state.data.get('source_env_string')
            target_string = state.data.get('target_env_string')
            output_dir = state.data.get('output_directory')
            
            # Import here to avoid circular imports
            from .core import PolicyExportFormatter
            
            formatter = PolicyExportFormatter(
                input_dir=input_dir,
                search_string=source_string,
                replace_string=target_string,
                output_dir=output_dir,
                logger=self.logger
            )
            
            stats = formatter.process_directory()
            
            # Update state with generated file paths using the new directory structure
            # segmented_spark_uids.csv is in policy-export directory
            segmented_spark_file = formatter.policy_export_dir / "segmented_spark_uids.csv"
            if segmented_spark_file.exists():
                state.data['segmented_spark_uids_file'] = str(segmented_spark_file)
            
            # asset_uids.csv is in asset-export directory
            asset_uids_file = formatter.asset_export_dir / "asset_uids.csv"
            if asset_uids_file.exists():
                state.data['asset_uids_file'] = str(asset_uids_file)
            
            return True, f"Formatter completed successfully. Processed {stats['total_files']} files."
        except Exception as e:
            return False, f"Formatter failed: {e}"
    
    def _execute_export_profiles(self, state: MigrationState, client) -> Tuple[bool, str]:
        """Execute export profiles step."""
        try:
            asset_uids_file = state.data.get('asset_uids_file')
            if not asset_uids_file:
                return False, "Asset UIDs file not configured"
            
            output_file = Path(asset_uids_file).parent / 'asset-profiles-import-ready.csv'
            
            # Import the actual export function from execution module
            from .execution import execute_asset_profile_export_guided
            
            # Execute the actual export
            success, message = execute_asset_profile_export_guided(
                csv_file=asset_uids_file,
                client=client,
                logger=self.logger,
                output_file=str(output_file),
                quiet_mode=False,
                verbose_mode=True
            )
            
            if not success:
                return False, message
            
            # Verify the file was created
            if not output_file.exists():
                return False, f"Export completed but file was not created: {output_file}"
            
            # Store the file path in state
            state.data['profiles_export_file'] = str(output_file)
            return True, f"Asset profiles exported to {output_file}"
        except Exception as e:
            return False, f"Export profiles failed: {e}"
    
    def _execute_import_profiles(self, state: MigrationState, client) -> Tuple[bool, str]:
        """Execute import profiles step."""
        try:
            profiles_file = state.data.get('profiles_export_file')
            if not profiles_file:
                return False, "Asset profiles export file not configured"
            
            if not Path(profiles_file).exists():
                return False, f"Asset profiles export file does not exist: {profiles_file}"
            
            # For now, just verify the file exists and has content
            # TODO: Implement actual import functionality
            with open(profiles_file, 'r') as f:
                reader = csv.reader(f)
                header = next(reader)
                row_count = sum(1 for row in reader)
            
            if row_count == 0:
                return False, f"Asset profiles export file is empty: {profiles_file}"
            
            return True, f"Asset profiles import verified (dry-run mode) - {row_count} profiles found in {profiles_file}"
        except Exception as e:
            return False, f"Import profiles failed: {e}"
    
    def _execute_export_configs(self, state: MigrationState, client) -> Tuple[bool, str]:
        """Execute export configs step."""
        try:
            asset_uids_file = state.data.get('asset_uids_file')
            if not asset_uids_file:
                return False, "Asset UIDs file not configured"
            
            output_file = Path(asset_uids_file).parent / 'asset-config-export.csv'
            
            # TODO: Implement actual config export functionality
            # For now, create a placeholder file
            output_file.write_text("target-env,config_json\n")
            
            # Store the file path in state
            state.data['configs_export_file'] = str(output_file)
            return True, f"Asset configurations export placeholder created at {output_file}"
        except Exception as e:
            return False, f"Export configs failed: {e}"
    
    def _execute_import_configs(self, state: MigrationState, client) -> Tuple[bool, str]:
        """Execute import configs step."""
        try:
            configs_file = state.data.get('configs_export_file')
            if not configs_file:
                return False, "Asset configs export file not configured"
            
            if not Path(configs_file).exists():
                return False, f"Asset configs export file does not exist: {configs_file}"
            
            # TODO: Implement actual config import functionality
            return True, f"Asset configurations import verified (dry-run mode) from {configs_file}"
        except Exception as e:
            return False, f"Import configs failed: {e}"
    
    def _execute_handle_segments(self, state: MigrationState, client) -> Tuple[bool, str]:
        """Execute handle segments step."""
        try:
            asset_uids_file = state.data.get('asset_uids_file')
            if not asset_uids_file:
                return False, "Asset UIDs file not configured"
            
            # Check if segmented_spark_uids.csv exists
            segments_file = asset_uids_file.parent.parent / 'policy-export' / 'segmented_spark_uids.csv'
            if not segments_file.exists():
                return False, f"Segmented SPARK UIDs file not found: {segments_file}"
            
            # Check if file has content
            try:
                with open(segments_file, 'r') as f:
                    lines = f.readlines()
                    if len(lines) < 2:  # Header + at least one data row
                        return False, f"Segmented SPARK UIDs file is empty or has no data: {segments_file}"
            except Exception as e:
                return False, f"Error reading segmented SPARK UIDs file: {e}"
            
            return True, f"Segmented SPARK UIDs file validated: {segments_file}"
        except Exception as e:
            return False, f"Handle segments failed: {e}"
    
    def _execute_completion(self, state: MigrationState) -> Tuple[bool, str]:
        """Execute completion step."""
        return True, "Migration completed successfully!" 