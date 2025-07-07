# Lineage Creation Guide

This document describes how to use the `create-lineage` command to create lineage relationships between assets in ADOC using CSV input files.

## Overview

The `create-lineage` command allows you to create complex lineage relationships between assets by providing a CSV file that describes the relationships, transformations, and flow of data. This is particularly useful for:

- Documenting data lineage for compliance and governance
- Creating complex multi-step data transformations
- Mapping data flows between different systems
- Supporting data lineage visualization and analysis

## Command Usage

```bash
create-lineage <csv_file> [--dry-run] [--quiet] [--verbose]
```

### Arguments

- `csv_file`: Path to the CSV file containing lineage data (required)
- `--dry-run`: Validate CSV and assets without creating lineage
- `--quiet`: Suppress console output, show only summary
- `--verbose`: Show detailed output including API calls

### Examples

```bash
# Basic lineage creation
create-lineage lineage_data.csv

# Validate without creating (dry run)
create-lineage lineage.csv --dry-run --verbose

# Quiet mode for batch processing
create-lineage data/lineage.csv --quiet
```

## CSV Format

The CSV file must contain the following columns:

### Required Columns

- **Source Asset ID**: The source asset identifier (e.g., table name, dataset ID)
- **Target Asset ID**: The target asset identifier
- **Relationship Type**: Either `upstream` (source → target) or `downstream` (target ← source)

### Optional Columns

- **Group ID**: Links rows for multi-column transformations (e.g., joins)
- **Step Order**: Numeric sequence for multi-step lineage
- **Source Column**: Source field name (blank for asset-level)
- **Target Column**: Target field name (blank for asset-level)
- **Transformation**: Describes the transformation logic
- **Notes**: Free-text context and documentation

### CSV Example

```csv
Group ID,Step Order,Source Asset ID,Source Column,Target Asset ID,Target Column,Relationship Type,Transformation,Notes
G1,1,table_456,col1,table_123,key1,upstream,join,Join on col1, col2
G1,1,table_456,col2,table_123,key2,upstream,join,Join on col1, col2
G2,1,table_457,id,table_123,join_id,upstream,join,
,2,table_123,key1,table_789,final_key,downstream,filter(key1 > 100),Filtered output
,,table_789,,table_999,,downstream,,,Full table
```

## Advanced Features

### Multi-Column Mappings

Use the **Group ID** to tie multiple rows into a single transformation. For example, a join operation that uses multiple columns:

```csv
Group ID,Source Asset ID,Source Column,Target Asset ID,Target Column,Relationship Type,Transformation
G1,table_456,col1,table_123,key1,upstream,join
G1,table_456,col2,table_123,key2,upstream,join
```

### Multi-Step Lineage

Use **Step Order** to sequence dependencies in multi-step processes:

```csv
Group ID,Step Order,Source Asset ID,Target Asset ID,Relationship Type,Transformation
G2,1,table_457,table_123,upstream,join
,2,table_123,table_789,downstream,filter(key1 > 100)
```

### Complex Transformations

The **Transformation** field supports various transformation types:

- `join`: Join operations
- `filter(condition)`: Filter operations with conditions
- `aggregate`: Aggregation operations
- `transform`: Data transformation operations
- `SQL: SELECT...`: Custom SQL transformations

## Validation and Error Handling

The command performs comprehensive validation:

1. **CSV Format Validation**: Checks for required columns and valid data types
2. **Asset Existence**: Validates that all referenced assets exist in the environment
3. **Relationship Type Validation**: Ensures relationship types are valid
4. **Data Consistency**: Checks for logical consistency in the lineage data

### Common Validation Errors

- **Missing required columns**: CSV must contain Source Asset ID, Target Asset ID, and Relationship Type
- **Invalid relationship type**: Must be either 'upstream' or 'downstream'
- **Asset not found**: Referenced assets must exist in the ADOC environment
- **Invalid step order**: Step order must be numeric and sequential

## API Integration

The command uses the ADOC lineage API:

```
POST /torch-pipeline/api/assets/:assetId/lineage
```

### Request Payload

```json
{
    "direction": "UPSTREAM|DOWNSTREAM",
    "assetIds": [123, 456, 789],
    "process": {
        "name": "Generated Process Name",
        "description": "Generated Process Description"
    }
}
```

### Process Name Generation

The system automatically generates process names based on:

1. **Group ID**: If available, includes group information
2. **Transformation Type**: Uses transformation information
3. **Direction**: Includes upstream/downstream context

### Process Description Generation

Process descriptions are generated from:

1. **Source/Target Mapping**: Shows the data flow
2. **Transformation Details**: Includes transformation logic
3. **Notes**: Incorporates user-provided notes

## Best Practices

### CSV File Preparation

1. **Use consistent asset IDs**: Ensure asset IDs match exactly with ADOC
2. **Group related operations**: Use Group ID for multi-column operations
3. **Sequence complex flows**: Use Step Order for multi-step processes
4. **Document transformations**: Use the Notes field for context
5. **Validate before creation**: Use `--dry-run` to validate without creating

### Command Usage

1. **Start with dry run**: Always validate with `--dry-run` first
2. **Use verbose mode**: Use `--verbose` for debugging and validation
3. **Batch processing**: Use `--quiet` for large-scale operations
4. **Monitor results**: Check the output for success/error counts

### Error Recovery

1. **Check asset existence**: Verify all assets exist before creating lineage
2. **Review CSV format**: Ensure all required columns are present
3. **Validate relationships**: Check that relationship types are correct
4. **Incremental creation**: Create lineage in smaller batches for complex scenarios

## Troubleshooting

### Common Issues

**Asset not found errors**
- Verify asset IDs exist in the ADOC environment
- Check for typos in asset names
- Ensure you're connected to the correct environment

**CSV parsing errors**
- Check CSV format and encoding
- Verify required columns are present
- Ensure no special characters in column headers

**API errors**
- Check authentication and permissions
- Verify network connectivity
- Review API response for specific error details

### Debug Mode

Use `--verbose` flag to get detailed information:

```bash
create-lineage lineage.csv --dry-run --verbose
```

This will show:
- CSV parsing details
- Asset validation results
- API request/response details
- Process name/description generation

## Examples

### Simple Lineage

```csv
Source Asset ID,Target Asset ID,Relationship Type,Transformation
table_456,table_123,upstream,join
table_123,table_789,downstream,filter
```

### Complex Multi-Step Lineage

```csv
Group ID,Step Order,Source Asset ID,Target Asset ID,Relationship Type,Transformation,Notes
G1,1,raw_data,staged_data,upstream,extract,Extract from source system
G1,2,staged_data,cleaned_data,downstream,clean,Apply data quality rules
G1,3,cleaned_data,final_table,downstream,aggregate,Create final aggregation
```

### Column-Level Lineage

```csv
Source Asset ID,Source Column,Target Asset ID,Target Column,Relationship Type,Transformation
table_456,customer_id,table_123,user_id,upstream,map
table_456,email,table_123,email_address,upstream,validate
```

## Integration with Other Commands

The `create-lineage` command works well with other ADOC Migration Toolkit commands:

1. **Asset Export**: Use `asset-list-export` to get asset IDs for lineage
2. **Asset Profiles**: Use `asset-profile-export` to understand asset structure
3. **Policy Management**: Combine with policy export/import for comprehensive migration

## Support

For issues or questions about lineage creation:

1. Check the validation output for specific error messages
2. Use `--verbose` mode for detailed debugging information
3. Review the CSV format requirements
4. Verify asset existence in the ADOC environment 