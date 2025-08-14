# VCS Config Command

The `vcs-config` command configures enterprise VCS (Version Control System) settings for Git, Mercurial, or Subversion repositories with support for HTTPS, SSH, and proxy configurations.

## Overview

This command sets up VCS repository configuration including remote URLs, authentication methods, and proxy settings. It supports both interactive and command-line configuration modes for enterprise environments.

## Usage

```bash
vcs-config [--vcs-type <type>] [--remote-url <url>] [--username <user>] [--token <token>] [options]
```

## Parameters

### Optional Parameters

- `--vcs-type <type>`: Version control system type (`git`, `hg`, `svn`)
- `--remote-url <url>`: Remote repository URL
- `--username <user>`: Username for authentication
- `--token <token>`: Authentication token or password
- `--ssh-key-path <path>`: Path to SSH private key file
- `--ssh-passphrase <phrase>`: SSH key passphrase
- `--proxy-url <url>`: Proxy server URL
- `--proxy-username <user>`: Proxy username
- `--proxy-password <pass>`: Proxy password

## Examples

### Interactive Mode

```bash
vcs-config  # Interactive mode
```

### Git with HTTPS

```bash
vcs-config --vcs-type git --remote-url https://github.com/user/repo.git
```

### Git with SSH

```bash
vcs-config --vcs-type git --remote-url git@github.com:user/repo.git --ssh-key-path ~/.ssh/id_rsa
```

### Enterprise GitLab with Token

```bash
vcs-config --vcs-type git --remote-url https://enterprise.gitlab.com/repo.git --username user --token <token>
```

### With Proxy Configuration

```bash
vcs-config --vcs-type git --remote-url https://github.com/user/repo.git --proxy-url http://proxy.company.com:8080 --proxy-username proxy_user --proxy-password proxy_pass
```

## Authentication Methods

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

## Configuration Storage

The command stores configuration in:
- **Config File**: `~/.adoc-vcs-config.json`
- **Environment Variables**: For sensitive data
- **SSH Config**: For SSH key configurations

## Security Features

- **Encrypted Storage**: Sensitive data is encrypted at rest
- **Environment Variables**: Tokens and passwords can use environment variables
- **SSH Key Management**: Secure SSH key handling
- **Proxy Security**: Secure proxy authentication

## Enterprise Features

### Corporate Proxy Support
```bash
vcs-config --vcs-type git --remote-url https://github.com/user/repo.git --proxy-url http://proxy.company.com:8080
```

### Enterprise GitLab Integration
```bash
vcs-config --vcs-type git --remote-url https://gitlab.company.com/user/repo.git --username user --token <token>
```

### SSH Key Management
```bash
vcs-config --vcs-type git --remote-url git@github.com:user/repo.git --ssh-key-path ~/.ssh/id_rsa --ssh-passphrase <phrase>
```

## Error Handling

The command includes comprehensive error handling:

- **Invalid URLs**: Validates repository URLs
- **Authentication Errors**: Handles authentication failures gracefully
- **Network Issues**: Retries failed connections
- **Configuration Errors**: Provides clear error messages

## Tips

- **Interactive Mode**: Use interactive mode for guided configuration
- **Environment Variables**: Use environment variables for sensitive data
- **SSH Keys**: Ensure SSH keys have correct permissions (600)
- **Proxy Settings**: Configure proxy settings for corporate networks
- **Testing**: Test configuration with `vcs-pull` after setup

## Common Use Cases

### GitHub Repository Setup
```bash
vcs-config --vcs-type git --remote-url https://github.com/user/repo.git --username user --token <github_token>
```

### Enterprise GitLab Setup
```bash
vcs-config --vcs-type git --remote-url https://gitlab.company.com/user/repo.git --username user --token <gitlab_token>
```

### SSH Repository Setup
```bash
vcs-config --vcs-type git --remote-url git@github.com:user/repo.git --ssh-key-path ~/.ssh/id_rsa
```

### Corporate Network Setup
```bash
vcs-config --vcs-type git --remote-url https://github.com/user/repo.git --proxy-url http://proxy.company.com:8080 --proxy-username user --proxy-password pass
```

## Related Commands

- [vcs-init](vcs-init.md) - Initialize a VCS repository
- [vcs-pull](vcs-pull.md) - Pull updates from remote repository
- [vcs-push](vcs-push.md) - Push changes to remote repository 