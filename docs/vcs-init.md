# VCS Init Command

The `vcs-init` command initializes a VCS (Version Control System) repository in the output directory or specified directory.

## Overview

This command creates a new Git or Mercurial repository in the specified directory. It's typically used to set up version control for migration data and configurations. The repository can then be used with `vcs-pull` and `vcs-push` commands.

## Usage

```bash
vcs-init [<base directory>]
```

## Parameters

### Optional Parameters

- `<base directory>`: Directory where the repository should be initialized (defaults to current output directory)

## Examples

### Initialize in Output Directory

```bash
vcs-init
```

### Initialize in Specific Directory

```bash
vcs-init /path/to/repository
```

### Initialize in Current Directory

```bash
vcs-init .
```

## Repository Types

The command supports the following VCS types:

### Git Repository
- **Initialization**: Creates `.git` directory and initial commit
- **Configuration**: Sets up basic Git configuration
- **Remote**: Can be configured with `vcs-config` command

### Mercurial Repository
- **Initialization**: Creates `.hg` directory and initial commit
- **Configuration**: Sets up basic Mercurial configuration
- **Remote**: Can be configured with `vcs-config` command

## Directory Structure

After initialization, the directory contains:
- **VCS Files**: `.git/` or `.hg/` directory
- **Initial Commit**: First commit with basic structure
- **Configuration**: Basic VCS configuration files

## Workflow Integration

This command is typically used in the following workflow:

1. **Set Output Directory**: Use `set-output-dir` to specify output location
2. **Initialize Repository**: Use this command to create VCS repository
3. **Configure Remote**: Use `vcs-config` to set up remote repository
4. **Version Control**: Use `vcs-pull` and `vcs-push` for version control

## Error Handling

The command includes comprehensive error handling:

- **Directory Permissions**: Checks write permissions for target directory
- **Existing Repository**: Handles cases where repository already exists
- **VCS Installation**: Validates VCS tools are installed
- **Configuration Errors**: Provides clear error messages

## Safety Features

- **Directory Validation**: Ensures target directory is valid
- **Permission Checks**: Validates write permissions
- **Existing Repository**: Warns if repository already exists
- **Backup**: Preserves existing files during initialization

## Tips

- **Output Directory**: Set output directory first with `set-output-dir`
- **Permissions**: Ensure you have write permissions to the target directory
- **Existing Files**: Repository can be initialized in directories with existing files
- **Remote Setup**: Use `vcs-config` after initialization to set up remote repository

## Common Use Cases

### Migration Data Repository
Initialize repository for migration data:
```bash
set-output-dir /path/to/migration-data
vcs-init
```

### Configuration Repository
Initialize repository for configurations:
```bash
vcs-init /path/to/config-repo
```

### Project Repository
Initialize repository in current project:
```bash
vcs-init .
```

## Related Commands

- [vcs-config](vcs-config.md) - Configure VCS repository settings
- [vcs-pull](vcs-pull.md) - Pull updates from remote repository
- [vcs-push](vcs-push.md) - Push changes to remote repository
- [set-output-dir](set-output-dir.md) - Set output directory for operations 