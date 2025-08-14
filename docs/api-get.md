# GET

Make GET request to API endpoint.

## Synopsis

```bash
GET <endpoint> [--target]
```

## Description

The `GET` command makes HTTP GET requests to Acceldata API endpoints. This command provides direct access to the Acceldata API for querying data, checking status, and retrieving information from both source and target environments.

## Arguments

- `endpoint` (required): API endpoint path (e.g., `/catalog-server/api/assets?uid=123`)
- `--target`: Use target environment authentication and tenant

## Examples

```bash
# Get asset information from source environment
GET /catalog-server/api/assets?uid=123

# Get asset information from target environment
GET /catalog-server/api/assets?uid=snowflake_krish_test.DEMO_DB.CS_DEMO.Customer_Sample --target

# Get policies from source environment
GET /catalog-server/api/policies

# Get specific policy details
GET /catalog-server/api/rules/123

# Get asset configuration
GET /catalog-server/api/assets/456/config
```

## Behavior

### Authentication
- Uses source environment authentication by default
- Use `--target` flag to switch to target environment
- Automatically includes access keys and tenant information
- Handles authentication headers transparently

### Response Formatting
- Returns formatted JSON response
- Supports query parameters in endpoint URL
- Handles pagination automatically
- Pretty-prints JSON for readability

### Environment Selection
- **Source Environment** (default): Uses source access key, secret key, and tenant
- **Target Environment** (`--target`): Uses target access key, secret key, and tenant

## Use Cases

1. **Data Verification**: Check if assets exist in target environment
2. **Configuration Inspection**: View asset configurations and settings
3. **Status Monitoring**: Check policy and asset status
4. **Debugging**: Investigate API responses and errors
5. **Data Discovery**: Explore available data and endpoints

## Related Commands

- [PUT](api-put.md) - Make PUT requests to API endpoints
- [show-config](show-config.md) - Display current configuration
- [set-http-config](set-http-config.md) - Configure HTTP settings

## Tips

- Use `--target` when you need to query the target environment
- Include query parameters in the endpoint URL
- The response is automatically formatted for readability
- Use this command to verify data before import operations
- Check the response for error messages and status codes

## Error Handling

- Network errors are retried automatically
- Authentication errors will fail immediately
- Invalid endpoints will return appropriate HTTP status codes
- JSON parsing errors are handled gracefully

## Output

### Success Response
- Formatted JSON output
- HTTP status code
- Response headers (if verbose mode is enabled)

### Error Response
- Error message with details
- HTTP status code
- Suggested troubleshooting steps

## Common Endpoints

### Assets
- `/catalog-server/api/assets` - List all assets
- `/catalog-server/api/assets?uid=<uid>` - Get specific asset
- `/catalog-server/api/assets/<id>/config` - Get asset configuration
- `/catalog-server/api/assets/<id>/profile` - Get asset profile

### Policies
- `/catalog-server/api/rules` - List all policies
- `/catalog-server/api/rules/<id>` - Get specific policy
- `/catalog-server/api/rules/<id>/tags` - Get policy tags

### Assemblies
- `/catalog-server/api/assemblies` - List all assemblies
- `/catalog-server/api/assemblies/<id>` - Get specific assembly

## Query Parameters

Common query parameters include:
- `uid=<asset_uid>` - Filter by asset UID
- `page=<number>` - Page number for pagination
- `size=<number>` - Page size for pagination
- `type=<type>` - Filter by type
- `engineType=<engine>` - Filter by engine type 