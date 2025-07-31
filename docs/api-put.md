# PUT

Make PUT request to API endpoint with JSON payload.

## Synopsis

```bash
PUT <endpoint> <json_payload> [--target]
```

## Description

The `PUT` command makes HTTP PUT requests to Acceldata API endpoints with JSON payloads. This command provides direct access to the Acceldata API for updating data, creating resources, and modifying configurations in both source and target environments.

## Arguments

- `endpoint` (required): API endpoint path
- `json_payload` (required): JSON data to send (e.g., `{"key": "value"}`)
- `--target`: Use target environment authentication and tenant

## Examples

```bash
# Update asset configuration in source environment
PUT /catalog-server/api/assets {"name": "test", "type": "database"}

# Update asset configuration in target environment
PUT /catalog-server/api/policies {"policy": "data"} --target

# Update asset profile
PUT /catalog-server/api/assets/123/profile {"profile": "configuration"}

# Update policy configuration
PUT /catalog-server/api/rules/456 {"rule": "settings"}
```

## Behavior

### Authentication
- Uses source environment authentication by default
- Use `--target` flag to switch to target environment
- Automatically includes access keys and tenant information
- Handles authentication headers transparently

### JSON Payload
- JSON payload must be valid JSON format
- Supports complex nested objects and arrays
- Automatically handles JSON escaping
- Validates JSON syntax before sending

### Response Formatting
- Returns formatted JSON response
- Shows HTTP status code
- Displays response headers (if verbose mode is enabled)

### Environment Selection
- **Source Environment** (default): Uses source access key, secret key, and tenant
- **Target Environment** (`--target`): Uses target access key, secret key, and tenant

## Use Cases

1. **Configuration Updates**: Update asset configurations and settings
2. **Profile Management**: Modify asset profiles and metadata
3. **Policy Updates**: Update policy configurations and rules
4. **Data Creation**: Create new resources and configurations
5. **Testing**: Test API endpoints and payloads

## Related Commands

- [GET](api-get.md) - Make GET requests to API endpoints
- [show-config](show-config.md) - Display current configuration
- [set-http-config](set-http-config.md) - Configure HTTP settings

## Tips

- Use `--target` when you need to update the target environment
- Ensure JSON payload is properly formatted
- Use this command to test API endpoints before automation
- Check the response for success/error status
- Use GET first to understand the current data structure

## Error Handling

- Invalid JSON will cause the command to fail
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
- `/catalog-server/api/assets/<id>` - Update asset
- `/catalog-server/api/assets/<id>/config` - Update asset configuration
- `/catalog-server/api/assets/<id>/profile` - Update asset profile

### Policies
- `/catalog-server/api/rules/<id>` - Update policy
- `/catalog-server/api/rules/<id>/tags` - Update policy tags

### Assemblies
- `/catalog-server/api/assemblies/<id>` - Update assembly

## JSON Payload Examples

### Asset Configuration
```json
{
  "name": "Updated Asset Name",
  "description": "Updated description",
  "type": "database"
}
```

### Asset Profile
```json
{
  "profile": {
    "engineType": "SPARK",
    "connectionString": "updated_connection",
    "settings": {
      "key": "value"
    }
  }
}
```

### Policy Configuration
```json
{
  "name": "Updated Policy",
  "type": "data_quality",
  "settings": {
    "threshold": 0.95,
    "enabled": true
  }
}
```

## Best Practices

1. **Test First**: Use GET to understand current data structure
2. **Validate JSON**: Ensure JSON is properly formatted
3. **Use Target Flag**: Use `--target` for target environment operations
4. **Check Response**: Always verify the response for success/error
5. **Backup Data**: Consider backing up data before major updates 