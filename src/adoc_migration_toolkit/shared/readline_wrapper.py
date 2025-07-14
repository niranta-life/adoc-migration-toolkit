"""
Cross-platform readline wrapper.

This module provides a unified interface for readline functionality
that works on both Windows and Unix-like systems.
"""

import json
import os
import sys
from pathlib import Path
from typing import Optional, Dict, List, Any

# Try to import readline for Unix-like systems
try:
    import readline

    READLINE_AVAILABLE = True
except ImportError:
    READLINE_AVAILABLE = False
# Try to import prompt_toolkit for Windows
try:
    from prompt_toolkit import PromptSession
    from prompt_toolkit.history import FileHistory
    from prompt_toolkit.completion import Completer, Completion

    PROMPT_TOOLKIT_AVAILABLE = True
except ImportError:
    PROMPT_TOOLKIT_AVAILABLE = False

    # Define stub classes for when prompt_toolkit is not available
    class Completer:
        def get_completions(self, document, complete_event):
            return []

    class Completion:
        def __init__(self, text, start_position=0):
            self.text = text
            self.start_position = start_position


# Determine the platform
IS_WINDOWS = sys.platform.startswith("win")


class ADOCCompleter(Completer):
    """Custom completer for ADOC interactive commands."""

    def __init__(self):
        # Load API endpoints configuration
        self.api_config = self._load_api_config()
        
        # Define all available commands with their options and argument types
        self.commands = {
            "segments-export": {"options": ["--parallel", "--quiet"], "args": ["csv_file"]},
            "segments-import": {"options": ["--parallel", "--quiet"], "args": ["csv_file"]},
            "asset-profile-export": {"options": ["--parallel", "--quiet"], "args": ["csv_file"]},
            "asset-profile-import": {"options": ["--parallel", "--quiet"], "args": ["csv_file"]},
            "asset-config-export": {"options": ["--parallel", "--quiet"], "args": ["csv_file"]},
            "asset-config-import": {"options": ["--parallel", "--quiet"], "args": ["csv_file"]},
            "asset-list-export": {"options": ["--parallel", "--quiet"], "args": []},
            "asset-tag-import": {"options": ["--parallel", "--quiet"], "args": ["csv_file"]},
            "policy-list-export": {"options": ["--parallel", "--quiet"], "args": []},
            "policy-export": {"options": ["--parallel", "--quiet"], "args": []},
            "policy-import": {"options": ["--parallel", "--quiet"], "args": ["zip_file"]},
            "policy-xfr": {"options": ["--parallel", "--quiet"], "args": []},
            "rule-tag-export": {"options": ["--parallel", "--quiet"], "args": []},
            "vcs-config": {"options": ["--vcs-type", "--remote-url", "--username", "--token", "--ssh-key-path", "--ssh-passphrase", "--proxy-url", "--proxy-username", "--proxy-password", "--help", "-h"], "args": []},
            "vcs-init": {"options": [], "args": ["directory"]},
            "vcs-pull": {"options": [], "args": []},
            "vcs-push": {"options": [], "args": []},
            "get": {"options": ["--target"], "args": []},
            "put": {"options": ["--target"], "args": []},
            "set-output-dir": {"options": [], "args": ["directory"]},
            "set-log-level": {"options": ["ERROR", "WARNING", "INFO", "DEBUG"], "args": []},
            "set-http-config": {"options": ["--timeout", "--retry", "--proxy"], "args": []},
            "show-config": {"options": [], "args": []},
            "help": {"options": [], "args": []},
            "history": {"options": [], "args": []},
            "exit": {"options": [], "args": []},
            "quit": {"options": [], "args": []},
            "q": {"options": [], "args": []},
        }

        # Create a flat list of all commands and options for basic completion
        self.all_completions = list(self.commands.keys())
        for cmd_data in self.commands.values():
            if isinstance(cmd_data, dict):
                self.all_completions.extend(cmd_data.get("options", []))
            else:
                # Legacy format support
                self.all_completions.extend(cmd_data)

        # Remove duplicates while preserving order
        seen = set()
        self.all_completions = [
            x for x in self.all_completions if not (x in seen or seen.add(x))
        ]

    def _load_api_config(self) -> Dict[str, Any]:
        """Load API endpoints configuration from file."""
        try:
            # Try to find config file relative to this module
            current_dir = Path(__file__).parent
            config_path = current_dir.parent.parent.parent / "config" / "api_endpoints.json"
            
            if config_path.exists():
                with open(config_path, 'r') as f:
                    return json.load(f)
        except Exception:
            pass
        
        # Return empty config if file not found or invalid
        return {"endpoints": {"GET": [], "PUT": []}, "parameter_values": {}, "id_examples": {}}

    def _get_api_completions(self, command: str, current_word: str, words: List[str]) -> List[str]:
        """Generate API endpoint completions for GET/PUT commands."""
        completions = []
        
        if command.upper() not in self.api_config.get("endpoints", {}):
            return completions
        
        endpoints = self.api_config["endpoints"][command.upper()]
        
        # Check if we're completing query parameters (current word contains ?)
        if "?" in current_word:
            return self._get_query_parameter_completions(current_word, endpoints)
        
        # If current word starts with /, complete API paths
        if current_word.startswith("/") or (not current_word and len(words) == 2):
            for endpoint in endpoints:
                path = endpoint["path"]
                
                # Check if this is a partial match that could be expanded
                if self._is_expandable_path(current_word, path):
                    # If user has entered values for placeholders, expand them
                    expanded_path = self._expand_user_path(current_word, path)
                    if expanded_path and expanded_path.startswith(current_word):
                        completions.append(expanded_path)
                elif path.startswith(current_word):
                    completions.append(path)
        
        return completions
    
    def _is_expandable_path(self, user_input: str, template_path: str) -> bool:
        """Check if user input matches a template path pattern."""
        if not user_input.startswith("/"):
            return False
        
        user_parts = user_input.split("/")
        template_parts = template_path.split("/")
        
        # Must have same number of parts up to the current input
        if len(user_parts) > len(template_parts):
            return False
        
        for i, (user_part, template_part) in enumerate(zip(user_parts, template_parts)):
            if i == len(user_parts) - 1:
                # Last part - can be partial match
                if template_part.startswith(":"):
                    # User is typing a placeholder value
                    return True
                elif not template_part.startswith(user_part):
                    return False
            else:
                # Full parts must match exactly (or be placeholder replacements)
                if template_part.startswith(":"):
                    # User has entered a value for this placeholder
                    continue
                elif user_part != template_part:
                    return False
        
        return True
    
    def _expand_user_path(self, user_input: str, template_path: str) -> str:
        """Expand template path with user-entered values and suggest completion."""
        user_parts = user_input.split("/")
        template_parts = template_path.split("/")
        
        result_parts = []
        
        for i, template_part in enumerate(template_parts):
            if i < len(user_parts):
                if template_part.startswith(":"):
                    # Use user's value for this placeholder
                    result_parts.append(user_parts[i])
                else:
                    # Use template part
                    result_parts.append(template_part)
            else:
                # Beyond user input - use template as-is
                result_parts.append(template_part)
        
        return "/".join(result_parts)

    def _get_query_parameter_completions(self, current_word: str, endpoints: List[Dict[str, Any]]) -> List[str]:
        """Generate query parameter completions for API endpoints."""
        completions = []
        
        # Extract the path part before the ?
        if "?" not in current_word:
            return completions
        
        path_part, query_part = current_word.split("?", 1)
        
        # Find the matching endpoint
        matching_endpoint = None
        for endpoint in endpoints:
            if endpoint["path"] == path_part:
                matching_endpoint = endpoint
                break
        
        if not matching_endpoint or "parameters" not in matching_endpoint:
            return completions
        
        parameters = matching_endpoint["parameters"]
        
        # If no query part yet, show all parameters
        if not query_part:
            # Build complete query string with all parameters
            param_strings = []
            for param_name, param_type in parameters.items():
                param_strings.append(f"{param_name}=<{param_type}>")
            
            if param_strings:
                query_string = "&".join(param_strings)
                completions.append(f"{path_part}?{query_string}")
        else:
            # Parse existing parameters and suggest missing ones
            existing_params = set()
            query_parts = query_part.split("&")
            
            for part in query_parts:
                if "=" in part:
                    param_name = part.split("=")[0]
                    existing_params.add(param_name)
            
            # Find parameters not yet used
            missing_params = []
            for param_name, param_type in parameters.items():
                if param_name not in existing_params:
                    missing_params.append(f"{param_name}=<{param_type}>")
            
            # Add completions for missing parameters
            for missing_param in missing_params:
                if query_part.endswith("&") or not query_part:
                    # Ready to add new parameter
                    completions.append(f"{path_part}?{query_part}{missing_param}")
                else:
                    # Need to add & first
                    completions.append(f"{path_part}?{query_part}&{missing_param}")
        
        return completions

    def _get_json_completions(self, endpoint_path: str) -> List[str]:
        """Generate JSON template completions for PUT commands."""
        completions = []
        
        put_endpoints = self.api_config.get("endpoints", {}).get("PUT", [])
        
        for endpoint in put_endpoints:
            if endpoint["path"] == endpoint_path:
                # Add JSON template examples
                if "json_examples" in endpoint:
                    for example in endpoint["json_examples"]:
                        completions.append(json.dumps(example, indent=2))
                break
        
        return completions


    def get_path_completions(self, path_prefix: str, arg_type: str):
        """Generate file/directory path completions."""
        try:
            # Handle empty path or just starting to type
            if not path_prefix:
                path_prefix = "./"
            
            # Handle relative vs absolute paths
            if path_prefix.startswith("/"):
                search_dir = "/" if path_prefix == "/" else os.path.dirname(path_prefix) or "/"
                filename_prefix = os.path.basename(path_prefix)
            else:
                if "/" in path_prefix:
                    search_dir = os.path.dirname(path_prefix) or "."
                    filename_prefix = os.path.basename(path_prefix)
                else:
                    search_dir = "."
                    filename_prefix = path_prefix
            
            # Make sure search directory exists
            if not os.path.exists(search_dir):
                return
            
            try:
                entries = os.listdir(search_dir)
            except (PermissionError, OSError):
                return
            
            for entry in sorted(entries):
                # Skip hidden files unless user is explicitly typing them
                if entry.startswith(".") and not filename_prefix.startswith("."):
                    continue
                
                if entry.startswith(filename_prefix):
                    full_path = os.path.join(search_dir, entry)
                    
                    # Build the completion text with proper path prefix
                    if search_dir == ".":
                        # For current directory, just use the entry name
                        base_completion = entry
                    else:
                        # For subdirectories, preserve the path structure
                        if path_prefix.startswith("/"):
                            # Absolute path
                            base_completion = os.path.join(search_dir, entry)
                        else:
                            # Relative path - rebuild from original path_prefix
                            if "/" in path_prefix:
                                dir_part = os.path.dirname(path_prefix)
                                base_completion = os.path.join(dir_part, entry)
                            else:
                                base_completion = entry
                    
                    # For directory arguments, only show directories
                    if arg_type == "directory":
                        if os.path.isdir(full_path):
                            completion_text = base_completion + "/"
                            yield completion_text
                    # For file arguments, show both files and directories
                    elif arg_type in ["csv_file", "zip_file"]:
                        if os.path.isdir(full_path):
                            completion_text = base_completion + "/"
                            yield completion_text
                        elif arg_type == "csv_file" and entry.endswith((".csv", ".CSV")):
                            yield base_completion
                        elif arg_type == "zip_file" and entry.endswith((".zip", ".ZIP")):
                            yield base_completion
                        elif arg_type == "csv_file" and not filename_prefix:
                            # Show all files when no prefix to help discovery
                            yield base_completion
                    else:
                        # Default: show everything
                        if os.path.isdir(full_path):
                            completion_text = base_completion + "/"
                            yield completion_text
                        else:
                            yield base_completion
                            
        except Exception:
            # Fail silently on path completion errors
            pass

    def get_completions(self, document, complete_event):
        """Generate completions for the current document."""
        text = document.text_before_cursor
        words = text.split()

        if not words or (len(words) == 1 and not text.endswith(" ")):
            # Complete command names
            for command in self.commands.keys():
                if command.startswith(text.lower()):
                    yield Completion(command, start_position=-len(text))
        elif len(words) >= 1:
            command = words[0].lower()
            if command in self.commands:
                cmd_data = self.commands[command]
                current_word = words[-1] if not text.endswith(" ") else ""
                
                # Special handling for GET and PUT commands (API endpoints)
                if command in ["get", "put"]:
                    # Handle options first
                    if current_word.startswith("--"):
                        for option in cmd_data.get("options", []):
                            if option.startswith(current_word) and option not in words:
                                yield Completion(option, start_position=-len(current_word))
                        return
                    
                    # Count non-option arguments for API commands
                    non_option_args = [w for w in words[1:] if not w.startswith("--")]
                    
                    if text.endswith(" "):
                        arg_position = len(non_option_args)
                    else:
                        arg_position = len(non_option_args) - 1 if non_option_args else 0
                    
                    # First argument: API endpoint path
                    if arg_position == 0:
                        api_completions = self._get_api_completions(command, current_word, words)
                        for completion in api_completions:
                            # Show paths with placeholders as-is
                            if completion.startswith(current_word):
                                yield Completion(completion, start_position=-len(current_word))
                    
                    # Second argument for PUT: JSON payload
                    elif arg_position == 1 and command == "put":
                        # Get the endpoint path from previous argument
                        if len(non_option_args) > 0:
                            endpoint_path = non_option_args[0]
                            json_completions = self._get_json_completions(endpoint_path)
                            for json_completion in json_completions:
                                if current_word == "" or json_completion.startswith(current_word):
                                    yield Completion(json_completion, start_position=-len(current_word))
                    
                    # Additional options if no more positional arguments
                    else:
                        for option in cmd_data.get("options", []):
                            if option.startswith(current_word) and option not in words:
                                yield Completion(option, start_position=-len(current_word))
                    
                    return
                
                # Regular command handling for non-API commands
                # Count non-option arguments so far
                non_option_args = [w for w in words[1:] if not w.startswith("--")]
                if text.endswith(" "):
                    arg_position = len(non_option_args)
                else:
                    # If current word is an option, complete options
                    if current_word.startswith("--"):
                        for option in cmd_data.get("options", []):
                            if option.startswith(current_word) and option not in words:
                                yield Completion(option, start_position=-len(current_word))
                        return
                    else:
                        arg_position = len(non_option_args) - 1
                
                # Check if we should complete an argument (file/directory)
                args = cmd_data.get("args", [])
                if arg_position < len(args):
                    arg_type = args[arg_position]
                    # Generate path completions
                    for path_completion in self.get_path_completions(current_word, arg_type):
                        yield Completion(path_completion, start_position=-len(current_word))
                else:
                    # Complete options if no more positional arguments
                    for option in cmd_data.get("options", []):
                        if option.startswith(current_word) and option not in words:
                            yield Completion(option, start_position=-len(current_word))


class CrossPlatformReadline:
    """Cross-platform readline wrapper that works on Windows and Unix-like systems."""

    def __init__(self):
        self.history_file = None
        self.history = []
        self.history_length = 1000
        self.completer = ADOCCompleter()

        # Initialize the appropriate backend - prefer prompt_toolkit for auto-completion
        if PROMPT_TOOLKIT_AVAILABLE:
            self.backend = "prompt_toolkit"
            self.session = None
        elif READLINE_AVAILABLE:
            self.backend = "readline"
        else:
            self.backend = "fallback"
            print("Warning: No readline support available. Using basic input.")

    def set_history_file(self, history_file: str):
        """Set the history file path."""
        self.history_file = history_file

        if self.backend == "prompt_toolkit":
            # Create directory if it doesn't exist
            Path(history_file).parent.mkdir(parents=True, exist_ok=True)
            self.session = PromptSession(
                history=FileHistory(history_file), completer=self.completer
            )
        elif self.backend == "readline":
            readline.set_history_length(self.history_length)

    def parse_and_bind(self, binding: str):
        """Parse and bind readline configuration."""
        if self.backend == "readline":
            readline.parse_and_bind(binding)

    def read_history_file(self, history_file: str):
        """Read history from file."""
        if self.backend == "readline":
            try:
                readline.read_history_file(history_file)
            except FileNotFoundError:
                pass  # File doesn't exist yet, that's okay

    def write_history_file(self, history_file: str):
        """Write history to file."""
        if self.backend == "readline":
            readline.write_history_file(history_file)

    def clear_history(self):
        """Clear the command history."""
        if self.backend == "readline":
            readline.clear_history()
        elif self.backend == "prompt_toolkit":
            self.history.clear()

    def add_history(self, command: str):
        """Add a command to history."""
        if self.backend == "readline":
            readline.add_history(command)
        elif self.backend == "prompt_toolkit":
            self.history.append(command)

    def get_current_history_length(self) -> int:
        """Get the current history length."""
        if self.backend == "readline":
            return readline.get_current_history_length()
        elif self.backend == "prompt_toolkit":
            return len(self.history)
        return 0

    def get_history_item(self, index: int) -> Optional[str]:
        """Get a history item by index."""
        if self.backend == "readline":
            return readline.get_history_item(index)
        elif self.backend == "prompt_toolkit":
            if 0 <= index - 1 < len(self.history):
                return self.history[index - 1]
        return None

    def get_line_buffer(self) -> str:
        """Get the current line buffer."""
        if self.backend == "readline":
            return readline.get_line_buffer()
        return ""

    def input(self, prompt: str = "") -> str:
        """Get user input with history support."""
        if self.backend == "prompt_toolkit":
            # Create session if it doesn't exist
            if self.session is None:
                if self.history_file:
                    Path(self.history_file).parent.mkdir(parents=True, exist_ok=True)
                    self.session = PromptSession(
                        history=FileHistory(self.history_file), completer=self.completer
                    )
                else:
                    self.session = PromptSession(completer=self.completer)
            return self.session.prompt(prompt)
        else:
            # Fallback to basic input
            return input(prompt)


# Global instance
readline_wrapper = CrossPlatformReadline()


# Convenience functions that mirror the readline API
def set_history_file(history_file: str):
    """Set the history file path."""
    readline_wrapper.set_history_file(history_file)


def parse_and_bind(binding: str):
    """Parse and bind readline configuration."""
    readline_wrapper.parse_and_bind(binding)


def read_history_file(history_file: str):
    """Read history from file."""
    readline_wrapper.read_history_file(history_file)


def write_history_file(history_file: str):
    """Write history to file."""
    readline_wrapper.write_history_file(history_file)


def clear_history():
    """Clear the command history."""
    readline_wrapper.clear_history()


def add_history(command: str):
    """Add a command to history."""
    readline_wrapper.add_history(command)


def get_current_history_length() -> int:
    """Get the current history length."""
    return readline_wrapper.get_current_history_length()


def get_history_item(index: int) -> Optional[str]:
    """Get a history item by index."""
    return readline_wrapper.get_history_item(index)


def get_line_buffer() -> str:
    """Get the current line buffer."""
    return readline_wrapper.get_line_buffer()


def input_with_history(prompt: str = "") -> str:
    """Get user input with history support."""
    return readline_wrapper.input(prompt)
