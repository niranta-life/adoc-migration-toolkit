# ADOC Migration Toolkit - Frequently Asked Questions (FAQ)

This FAQ addresses common questions, troubleshooting scenarios, and solutions for the ADOC Migration Toolkit.

## Table of Contents

- [Installation and Setup](#installation-and-setup)
- [Authentication and Configuration](#authentication-and-configuration)
- [Command Execution](#command-execution)
- [File and Data Issues](#file-and-data-issues)
- [Performance and Optimization](#performance-and-optimization)
- [API and Network Issues](#api-and-network-issues)
- [Migration Workflow](#migration-workflow)
- [Error Messages and Solutions](#error-messages-and-solutions)

## Installation and Setup

### Q: How do I install the ADOC Migration Toolkit?

**A:** Follow these steps:

```bash
# Clone the repository
git clone https://github.com/niranta-life/adoc-migration-toolkit.git
cd adoc-migration-toolkit

# Install in development mode
pip install -e .

# Make scripts executable
chmod +x bin/adoc-migration-toolkit
chmod +x bin/sjson
```

### Q: What are the system requirements?

**A:** 
- Python 3.9 or higher
- Access to Acceldata environments (source and target)
- Proper authentication credentials
- Sufficient disk space for export files
- Network connectivity to Acceldata APIs

### Q: How do I start the toolkit?

**A:** Use the binary script (recommended):

```bash
./bin/adoc-migration-toolkit interactive --env-file=config/config.env
```

Or use the Python module:

```bash
python -m adoc_migration_toolkit interactive --env-file=config/config.env
```

### Q: The script is not executable. What should I do?

**A:** Make the scripts executable:

```bash
chmod +x bin/adoc-migration-toolkit
chmod +x bin/sjson
```

## Authentication and Configuration

### Q: How do I set up my configuration file?

**A:** 

1. Copy the example configuration:
   ```bash
   cp config/config.env.example config/config.env
   ```

2. Edit the configuration file with your details:
   ```bash
   # Acceldata Environment Configuration
   AD_HOST=https://${tenant}.your-acceldata-host.com
   
   # Source Environment
   AD_SOURCE_ACCESS_KEY=your_source_access_key
   AD_SOURCE_SECRET_KEY=your_source_secret_key
   AD_SOURCE_TENANT=your_source_tenant
   
   # Target Environment
   AD_TARGET_ACCESS_KEY=your_target_access_key
   AD_TARGET_SECRET_KEY=your_target_secret_key
   AD_TARGET_TENANT=your_target_tenant
   ```

### Q: I'm getting authentication errors. What should I check?

**A:** Verify these items:

1. **Access Keys**: Ensure access keys are correct and active
2. **Secret Keys**: Verify secret keys match the access keys
3. **Tenant Names**: Check that tenant names are correct
4. **Host URL**: Verify the host URL format and accessibility
5. **Network Connectivity**: Ensure you can reach the Acceldata environment

### Q: How do I verify my configuration?

**A:** Use the configuration check command:

```bash
./bin/adoc-migration-toolkit --check-env
```

Or use the interactive command:

```bash
show-config
```

### Q: My configuration file is not being read. What's wrong?

**A:** Check these common issues:

1. **File Path**: Ensure the path to your config file is correct
2. **File Permissions**: Make sure the file is readable
3. **File Format**: Verify the file uses the correct format (no spaces around `=`)
4. **File Encoding**: Ensure the file is saved in UTF-8 encoding

## Command Execution

### Q: Commands are running slowly. How can I speed them up?

**A:** Use parallel processing:

```bash
# Add --parallel flag to most commands
asset-list-export --parallel
policy-list-export --parallel
asset-profile-export --parallel
```

### Q: How do I preview changes before making them?

**A:** Use the `--dry-run` flag:

```bash
asset-profile-import --dry-run --verbose
asset-config-import --dry-run --verbose
segments-import segments.csv --dry-run
```

### Q: I want to see detailed output. How do I enable verbose mode?

**A:** Add the `--verbose` flag:

```bash
asset-list-export --verbose
policy-list-export --verbose
GET /catalog-server/api/assets --verbose
```

### Q: How do I set the output directory for all commands?

**A:** Use the `set-output-dir` command:

```bash
set-output-dir /path/to/my/migration/project
```

## File and Data Issues

### Q: My CSV files are not being found. What should I check?

**A:** Verify these items:

1. **File Path**: Ensure the file path is correct
2. **File Permissions**: Check that files are readable
3. **File Format**: Verify CSV files are properly formatted
4. **Default Locations**: Check if files are in expected default locations

### Q: CSV files are corrupted or have wrong format. What should I do?

**A:** 

1. **Check File Encoding**: Ensure files are UTF-8 encoded
2. **Verify CSV Format**: Check that columns match expected format
3. **Validate Data**: Use a CSV viewer to inspect the file
4. **Re-export**: Re-export the data if necessary

### Q: How do I check what files are being created?

**A:** Use the `show-config` command to see the output directory, then list files:

```bash
show-config
ls -la <output-directory>
```

### Q: I'm getting "file not found" errors. What should I do?

**A:** 

1. **Check Default Locations**: Verify files are in expected directories
2. **Set Output Directory**: Use `set-output-dir` to specify location
3. **Use Absolute Paths**: Try using absolute file paths
4. **Check Permissions**: Ensure you have read/write permissions

## Performance and Optimization

### Q: How can I optimize performance for large datasets?

**A:** 

1. **Use Parallel Processing**: Add `--parallel` flag to commands
2. **Adjust Page Size**: Use `--page-size` for asset exports
3. **Use Filters**: Apply filters to reduce data volume
4. **Set Output Directory**: Avoid repeated file path specifications

### Q: Commands are timing out. What should I do?

**A:** 

1. **Increase Timeout**: Use `set-http-config --timeout 60`
2. **Increase Retries**: Use `set-http-config --retry 5`
3. **Use Smaller Batches**: Reduce page size or batch size
4. **Check Network**: Verify network connectivity and stability

### Q: How do I monitor progress of long-running commands?

**A:** 

1. **Use Verbose Mode**: Add `--verbose` to see detailed progress
2. **Check Progress Bars**: Parallel commands show progress bars
3. **Monitor Logs**: Check log files for detailed information
4. **Use History**: Check command history for completion status

## API and Network Issues

### Q: I'm getting network connection errors. What should I check?

**A:** 

1. **Network Connectivity**: Test connection to Acceldata host
2. **Proxy Settings**: Configure proxy if behind corporate firewall
3. **SSL Certificates**: Verify SSL certificate validity
4. **Firewall Rules**: Check if firewall is blocking connections

### Q: How do I configure proxy settings?

**A:** Use the HTTP configuration command:

```bash
set-http-config --proxy http://proxy.company.com:8080
```

### Q: API calls are failing with 401/403 errors. What does this mean?

**A:** 

- **401 Unauthorized**: Authentication credentials are invalid
- **403 Forbidden**: Credentials are valid but lack required permissions

**Solutions:**
1. Verify access keys and secret keys
2. Check tenant permissions
3. Ensure API endpoints are accessible
4. Contact Acceldata support if issues persist

### Q: How do I test API connectivity?

**A:** Use the GET command to test basic connectivity:

```bash
GET /catalog-server/api/assets
```

## Migration Workflow

### Q: What's the recommended migration workflow?

**A:** Follow this professional workflow:

1. **Setup**: Start toolkit and verify configuration
2. **Export**: Export assets and policies from source
3. **Transform**: Transform data for target environment
4. **Import**: Import to target environment with dry-run first
5. **Verify**: Check profiling and notifications
6. **Validate**: Verify successful migration

### Q: How do I handle environment-specific configurations?

**A:** 

1. **Use String Transformations**: Replace environment-specific values
2. **Use Policy XFR**: Format policy exports for target environment
3. **Use Transform and Merge**: Transform asset mappings
4. **Verify Results**: Always verify transformations worked correctly

### Q: What should I do if migration fails partway through?

**A:** 

1. **Check Logs**: Review error logs for specific issues
2. **Verify Configuration**: Ensure source/target environments are accessible
3. **Use Dry Run**: Test remaining steps with `--dry-run`
4. **Resume from Last Success**: Start from the last successful step
5. **Contact Support**: If issues persist, contact Acceldata support

### Q: How do I verify that migration was successful?

**A:** 

1. **Check Asset Counts**: Compare asset counts between environments
2. **Verify Profiles**: Check that asset profiles are properly imported
3. **Test Notifications**: Use `notifications-check` to verify notifications
4. **Check Profiling**: Use `profile-check` to verify profiling status
5. **Test API Access**: Use GET commands to verify data accessibility

## Error Messages and Solutions

### Q: "Authentication failed" - What should I do?

**A:** 

1. **Verify Credentials**: Check access keys and secret keys
2. **Check Tenant**: Ensure tenant name is correct
3. **Test Connectivity**: Verify network connectivity to Acceldata
4. **Check Permissions**: Ensure credentials have required permissions

### Q: "File not found" - How do I resolve this?

**A:** 

1. **Check File Path**: Verify the file path is correct
2. **Set Output Directory**: Use `set-output-dir` to specify location
3. **Check Permissions**: Ensure file is readable
4. **Use Absolute Paths**: Try using absolute file paths

### Q: "Network timeout" - What should I do?

**A:** 

1. **Increase Timeout**: Use `set-http-config --timeout 60`
2. **Check Network**: Verify network connectivity
3. **Use Retries**: Use `set-http-config --retry 5`
4. **Check Proxy**: Configure proxy if behind firewall

### Q: "Invalid JSON" - How do I fix this?

**A:** 

1. **Check File Format**: Verify JSON is properly formatted
2. **Validate JSON**: Use a JSON validator to check syntax
3. **Re-export Data**: Re-export the data if necessary
4. **Check Encoding**: Ensure file is UTF-8 encoded

### Q: "Permission denied" - What should I do?

**A:** 

1. **Check File Permissions**: Ensure files are readable/writable
2. **Check Directory Permissions**: Verify output directory permissions
3. **Use Different Location**: Try a different output directory
4. **Check User Permissions**: Ensure user has required permissions

### Q: "API endpoint not found" - How do I resolve this?

**A:** 

1. **Check URL**: Verify the API endpoint URL is correct
2. **Check Version**: Ensure you're using the correct API version
3. **Check Permissions**: Verify credentials have access to the endpoint
4. **Contact Support**: If endpoint should exist, contact Acceldata support

## Best Practices

### Q: What are the best practices for using the toolkit?

**A:** 

1. **Always Use Dry Run**: Test with `--dry-run` before production
2. **Use Parallel Processing**: Add `--parallel` for better performance
3. **Set Output Directory**: Use `set-output-dir` early in workflow
4. **Monitor Progress**: Use `--verbose` for detailed monitoring
5. **Keep Logs**: Maintain logs for troubleshooting
6. **Verify Results**: Always verify migration success

### Q: How do I handle large datasets efficiently?

**A:** 

1. **Use Parallel Processing**: Enable parallel processing
2. **Use Filters**: Apply filters to reduce data volume
3. **Use Appropriate Page Sizes**: Adjust page size for optimal performance
4. **Monitor Resources**: Watch memory and CPU usage
5. **Use Batch Processing**: Process data in manageable batches

### Q: How do I troubleshoot performance issues?

**A:** 

1. **Check Network**: Verify network connectivity and speed
2. **Monitor Resources**: Check CPU, memory, and disk usage
3. **Use Verbose Mode**: Enable verbose logging for detailed analysis
4. **Adjust Settings**: Modify timeout, retry, and batch settings
5. **Use Parallel Processing**: Enable parallel processing where available

## Getting Help

### Q: Where can I get additional help?

**A:** 

1. **Interactive Help**: Use `help` and `help <command>` in the toolkit
2. **Documentation**: Review the comprehensive documentation
3. **Command History**: Use `history` to review previous commands
4. **Logs**: Check log files for detailed error information
5. **Support**: Contact Acceldata support for complex issues

### Q: How do I report bugs or issues?

**A:** 

1. **Check Logs**: Review log files for error details
2. **Reproduce Issue**: Document steps to reproduce the problem
3. **Gather Information**: Collect configuration and error details
4. **Contact Support**: Report issues to Acceldata support with full details

This FAQ covers the most common questions and issues. For additional help, refer to the command-specific documentation or contact Acceldata support. 