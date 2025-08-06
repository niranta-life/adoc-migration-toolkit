# Set HTTP Config Command

The `set-http-config` command configures HTTP timeout, retry, and proxy settings for API operations.

## Overview

This command allows you to adjust HTTP connection settings dynamically during interactive sessions. It's useful for optimizing API performance, handling network issues, and configuring proxy settings for enterprise environments.

## Usage

```bash
set-http-config [--timeout x] [--retry x] [--proxy url]
```

## Parameters

### Optional Parameters

- `--timeout x`: HTTP timeout in seconds (default: 30)
- `--retry x`: Number of retry attempts for failed requests (default: 3)
- `--proxy url`: Proxy server URL (e.g., `http://proxy.company.com:8080`)

## Examples

### Set Timeout

```bash
set-http-config --timeout 30
```

### Set Retry Count

```bash
set-http-config --retry 5
```

### Set Proxy

```bash
set-http-config --proxy http://proxy.company.com:8080
```

### Combined Settings

```bash
set-http-config --timeout 20 --retry 3 --proxy http://proxy:8080
```

## Configuration Options

### Timeout Settings
- **Purpose**: Controls how long to wait for API responses
- **Default**: 30 seconds
- **Range**: 1-300 seconds
- **Use Case**: Adjust for slow networks or large data transfers

### Retry Settings
- **Purpose**: Number of attempts for failed requests
- **Default**: 3 attempts
- **Range**: 0-10 attempts
- **Use Case**: Handle temporary network issues

### Proxy Settings
- **Purpose**: Configure corporate proxy server
- **Format**: `http://host:port` or `https://host:port`
- **Authentication**: Supports username/password if needed
- **Use Case**: Enterprise network environments

## Use Cases

### Slow Network Optimization
For slow network connections:
```bash
set-http-config --timeout 60 --retry 5
```

### Enterprise Proxy Setup
For corporate proxy environments:
```bash
set-http-config --proxy http://proxy.company.com:8080
```

### High-Reliability Operations
For critical operations requiring reliability:
```bash
set-http-config --timeout 45 --retry 5
```

### Performance Testing
For performance-focused operations:
```bash
set-http-config --timeout 10 --retry 1
```

## Network Scenarios

### Corporate Network
Typical enterprise setup:
```bash
set-http-config --timeout 30 --retry 3 --proxy http://proxy.company.com:8080
```

### High-Latency Network
For satellite or international connections:
```bash
set-http-config --timeout 120 --retry 5
```

### Unstable Network
For unreliable connections:
```bash
set-http-config --timeout 60 --retry 8
```

### Local Network
For fast local connections:
```bash
set-http-config --timeout 15 --retry 2
```

## Error Handling

The command includes comprehensive error handling:

- **Invalid Timeout**: Validates timeout values (1-300 seconds)
- **Invalid Retry**: Validates retry values (0-10 attempts)
- **Invalid Proxy**: Validates proxy URL format
- **Network Issues**: Handles proxy connection failures

## Performance Impact

### Timeout Impact
- **Low Timeout**: Faster failure detection, may cause timeouts
- **High Timeout**: Slower failure detection, more reliable for slow networks
- **Recommendation**: Start with 30 seconds, adjust based on network

### Retry Impact
- **Low Retry**: Faster operation, may fail on temporary issues
- **High Retry**: More reliable, slower operation
- **Recommendation**: Use 3-5 retries for most scenarios

### Proxy Impact
- **No Proxy**: Direct connection, faster for local networks
- **With Proxy**: Additional hop, required for corporate networks
- **Recommendation**: Use proxy only when required

## Tips

- **Start Conservative**: Begin with default settings
- **Monitor Performance**: Watch for timeout or retry patterns
- **Network Testing**: Test settings with simple API calls first
- **Environment Specific**: Use different settings for different environments
- **Documentation**: Keep track of optimal settings for your environment

## Troubleshooting

### Timeout Issues
If you encounter frequent timeouts:
1. Increase timeout value: `set-http-config --timeout 60`
2. Check network connectivity
3. Consider using proxy if in corporate environment

### Retry Issues
If operations fail frequently:
1. Increase retry count: `set-http-config --retry 5`
2. Check for network instability
3. Verify API endpoint availability

### Proxy Issues
If proxy doesn't work:
1. Verify proxy URL format
2. Check proxy server availability
3. Test proxy with simple HTTP request

## Related Commands

- [set-log-level](set-log-level.md) - Change log level dynamically
- [show-config](show-config.md) - Display current configuration
- [help](help.md) - Show command help 