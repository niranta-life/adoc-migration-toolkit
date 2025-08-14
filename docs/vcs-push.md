# VCS Push Command

The `vcs-push` command pushes local changes to the remote repository with authentication.

## Overview

This command sends local commits and changes to the remote repository configured with `vcs-config`. It supports all authentication methods including HTTPS, SSH, and proxy configurations.

## Usage

```bash
vcs-push
```

## Parameters

This command has no parameters. It uses the configuration set by `vcs-config`.

## Examples

### Basic Push

```bash
vcs-push
```

### Push with Verbose Output

```bash
vcs-push --verbose
```

## Prerequisites

Before using this command, you must:

1. **Initialize Repository**: Use `vcs-init` to create a local repository
2. **Configure Remote**: Use `vcs-config` to set up remote repository settings
3. **Make Changes**: Have local changes to push
4. **Commit Changes**: Commit local changes before pushing

## Authentication Methods

The command supports the same authentication methods as `vcs-config`:

### HTTPS Authentication
- **Username/Password**: Basic authentication
- **Token**: Personal access token or API key
- **OAuth**: OAuth token for enterprise systems

### SSH Authentication
- **SSH Key**: Private key file authentication
- **Passphrase**: Optional passphrase for SSH keys
- **Agent**: SSH agent authentication

### Proxy Authentication
- **HTTP Proxy**: Corporate proxy server
- **Proxy Credentials**: Username/password for proxy
- **SSL/TLS**: Secure proxy connections

## Workflow Integration

This command is typically used in the following workflow:

1. **Initialize Repository**: Use `vcs-init` to create local repository
2. **Configure Remote**: Use `vcs-config` to set up remote repository
3. **Make Changes**: Work with local files
4. **Commit Changes**: Stage and commit local changes
5. **Push Changes**: Use this command to send changes to remote

## Error Handling

The command includes comprehensive error handling:

- **Missing Configuration**: Provides guidance on required `vcs-config` setup
- **Authentication Errors**: Handles authentication failures gracefully
- **Network Issues**: Retries failed connections with exponential backoff
- **Permission Errors**: Handles permission issues with clear guidance
- **Repository Issues**: Validates repository state before pushing

## Safety Features

- **Conflict Detection**: Detects and reports push conflicts
- **Validation**: Validates repository state before pushing
- **Backup**: Creates backup of local changes before pushing
- **Rollback**: Provides guidance for rolling back problematic pushes

## Performance

- **Incremental Uploads**: Only uploads changed files
- **Compression**: Uses compression for efficient transfers
- **Parallel Uploads**: Uploads multiple files in parallel
- **Caching**: Caches authentication and connection information

## Tips

- **Regular Pushes**: Push frequently to avoid large changes
- **Commit Messages**: Use descriptive commit messages
- **Testing**: Test changes before pushing
- **Communication**: Coordinate with team members for shared repositories
- **Backup**: Always backup important changes before pushing

## Common Use Cases

### Daily Workflow
Push daily changes to shared repository:
```bash
vcs-push
```

### Team Collaboration
Share changes with team members:
```bash
vcs-push --verbose
```

### Migration Data Updates
Push migration data updates:
```bash
vcs-push
```

## Troubleshooting

### Authentication Issues
If you encounter authentication errors:
1. Check your `vcs-config` settings
2. Verify tokens and passwords are correct
3. Test with `vcs-config` in interactive mode

### Permission Issues
For permission problems:
1. Verify you have write access to the remote repository
2. Check repository settings and permissions
3. Contact repository administrator if needed

### Network Issues
For network connectivity problems:
1. Check proxy settings in `vcs-config`
2. Verify network connectivity
3. Try with different authentication methods

### Push Conflicts
For push conflicts:
1. Pull latest changes first with `vcs-pull`
2. Resolve any merge conflicts
3. Push again after resolving conflicts

## Best Practices

### Before Pushing
1. **Test Changes**: Ensure all changes work correctly
2. **Commit Messages**: Write clear, descriptive commit messages
3. **Pull First**: Pull latest changes to avoid conflicts
4. **Review Changes**: Review what you're about to push

### After Pushing
1. **Verify Upload**: Check that changes were uploaded successfully
2. **Notify Team**: Inform team members of significant changes
3. **Document Changes**: Update documentation if needed

## Related Commands

- [vcs-config](vcs-config.md) - Configure VCS repository settings
- [vcs-init](vcs-init.md) - Initialize a VCS repository
- [vcs-pull](vcs-pull.md) - Pull updates from remote repository
- [set-output-dir](set-output-dir.md) - Set output directory for operations 