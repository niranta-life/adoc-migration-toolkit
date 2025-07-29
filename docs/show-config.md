# show-config

Display current configuration for HTTP, logging, environment, and output settings.

## Synopsis

```bash
show-config
```

## Description

The `show-config` command displays the current configuration settings for the ADOC Migration Toolkit, including HTTP settings, logging configuration, environment settings, and output directory configuration. This command is useful for troubleshooting and verifying configuration.

## Arguments

- None (no arguments required)

## Examples

```bash
# Display current configuration
show-config
```

## Behavior

### Configuration Sections
The command displays configuration in organized sections:

#### HTTP Configuration
- **Timeout**: Request timeout in seconds
- **Retry**: Number of retry attempts
- **Proxy**: Proxy URL (if configured)

#### Logging Configuration
- **Log Level**: Current log level (ERROR, WARNING, INFO, DEBUG)
- **Log File**: Log file location and settings

#### Environment Configuration
- **Host**: Acceldata host URL
- **Source Tenant**: Source environment tenant
- **Target Tenant**: Target environment tenant
- **Access Keys**: Partially masked access keys (first 8 characters)
- **Secret Keys**: Partially masked secret keys (first 8 characters)

#### Output Configuration
- **Output Directory**: Current output directory setting
- **Directory Status**: Whether directory exists and is writable

### Security Features
- **Masked Keys**: Only shows first 8 characters of sensitive keys
- **Secure Display**: Prevents exposure of full credentials
- **Clear Formatting**: Uses emojis and formatting for readability

## Use Cases

1. **Troubleshooting**: Verify configuration before operations
2. **Environment Verification**: Check environment settings
3. **Security Audit**: Verify authentication settings
4. **Configuration Review**: Review all settings in one place

## Related Commands

- [set-output-dir](set-output-dir.md) - Set global output directory
- [set-log-level](set-log-level.md) - Change log level dynamically
- [set-http-config](set-http-config.md) - Configure HTTP settings

## Tips

- Run this command before starting migration operations
- Use this to verify environment configuration
- Check that output directory is set correctly
- Verify that authentication keys are properly configured

## Output Format

### HTTP Configuration Section
```
🔧 HTTP Configuration:
   Timeout: 10 seconds
   Retry: 3 attempts
   Proxy: None
```

### Logging Configuration Section
```
📝 Logging Configuration:
   Log Level: INFO
   Log File: /path/to/log/file.log
```

### Environment Configuration Section
```
🌐 Environment Configuration:
   Host: https://tenant.acceldata.com
   Source Tenant: source_tenant
   Target Tenant: target_tenant
   Source Access Key: 12345678...
   Source Secret Key: 87654321...
   Target Access Key: 11111111...
   Target Secret Key: 22222222...
```

### Output Configuration Section
```
📁 Output Configuration:
   Output Directory: /path/to/output
   Status: Directory exists and is writable
```

## Error Handling

- Missing configuration files are handled gracefully
- Invalid configuration values are reported
- Network connectivity issues are detected
- File permission issues are reported

## Configuration Sources

The configuration is loaded from multiple sources:
1. **Environment File**: `config.env` file
2. **User Config**: `~/.adoc_migration_config.json`
3. **Default Settings**: Built-in defaults
4. **Session Settings**: Runtime configuration changes 