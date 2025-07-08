"""
Cross-platform readline wrapper.

This module provides a unified interface for readline functionality
that works on both Windows and Unix-like systems.
"""

import os
import sys
from typing import List, Optional, Callable
from pathlib import Path

# Try to import readline for Unix-like systems
try:
    import readline
    READLINE_AVAILABLE = True
except ImportError:
    READLINE_AVAILABLE = False

# Try to import prompt_toolkit for Windows
try:
    from prompt_toolkit import PromptSession
    from prompt_toolkit.completion import Completer, Completion
    from prompt_toolkit.history import FileHistory
    PROMPT_TOOLKIT_AVAILABLE = True
except ImportError:
    PROMPT_TOOLKIT_AVAILABLE = False

# Determine the platform
IS_WINDOWS = sys.platform.startswith('win')


class CrossPlatformReadline:
    """Cross-platform readline wrapper that works on Windows and Unix-like systems."""
    
    def __init__(self):
        self.history_file = None
        self.completer = None
        self.history = []
        self.history_length = 1000
        
        # Initialize the appropriate backend
        if IS_WINDOWS and PROMPT_TOOLKIT_AVAILABLE:
            self.backend = 'prompt_toolkit'
            self.session = None
        elif READLINE_AVAILABLE:
            self.backend = 'readline'
        else:
            self.backend = 'fallback'
            print("Warning: No readline support available. Using basic input.")
    
    def set_history_file(self, history_file: str):
        """Set the history file path."""
        self.history_file = history_file
        
        if self.backend == 'prompt_toolkit':
            # Create directory if it doesn't exist
            Path(history_file).parent.mkdir(parents=True, exist_ok=True)
            self.session = PromptSession(history=FileHistory(history_file))
        elif self.backend == 'readline':
            readline.set_history_length(self.history_length)
    
    def set_completer(self, completer_func: Callable):
        """Set the completer function."""
        self.completer = completer_func
        
        if self.backend == 'readline':
            readline.set_completer(completer_func)
    
    def parse_and_bind(self, binding: str):
        """Parse and bind readline configuration."""
        if self.backend == 'readline':
            readline.parse_and_bind(binding)
    
    def read_history_file(self, history_file: str):
        """Read history from file."""
        if self.backend == 'readline':
            try:
                readline.read_history_file(history_file)
            except FileNotFoundError:
                pass  # File doesn't exist yet, that's okay
    
    def write_history_file(self, history_file: str):
        """Write history to file."""
        if self.backend == 'readline':
            readline.write_history_file(history_file)
    
    def clear_history(self):
        """Clear the command history."""
        if self.backend == 'readline':
            readline.clear_history()
        elif self.backend == 'prompt_toolkit':
            self.history.clear()
    
    def add_history(self, command: str):
        """Add a command to history."""
        if self.backend == 'readline':
            readline.add_history(command)
        elif self.backend == 'prompt_toolkit':
            self.history.append(command)
    
    def get_current_history_length(self) -> int:
        """Get the current history length."""
        if self.backend == 'readline':
            return readline.get_current_history_length()
        elif self.backend == 'prompt_toolkit':
            return len(self.history)
        return 0
    
    def get_history_item(self, index: int) -> Optional[str]:
        """Get a history item by index."""
        if self.backend == 'readline':
            return readline.get_history_item(index)
        elif self.backend == 'prompt_toolkit':
            if 0 <= index - 1 < len(self.history):
                return self.history[index - 1]
        return None
    
    def get_line_buffer(self) -> str:
        """Get the current line buffer."""
        if self.backend == 'readline':
            return readline.get_line_buffer()
        return ""
    
    def input(self, prompt: str = "") -> str:
        """Get user input with completion and history support."""
        if self.backend == 'prompt_toolkit':
            if self.completer:
                # Create a prompt_toolkit completer from our readline-style completer
                class PromptToolkitCompleter(Completer):
                    def __init__(self, readline_completer):
                        self.readline_completer = readline_completer
                    
                    def get_completions(self, document, complete_event):
                        text = document.text_before_cursor
                        state = 0
                        while True:
                            completion = self.readline_completer(text, state)
                            if completion is None:
                                break
                            yield Completion(completion, start_position=0)
                            state += 1
                
                pt_completer = PromptToolkitCompleter(self.completer)
                return self.session.prompt(prompt, completer=pt_completer)
            else:
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

def set_completer(completer_func: Callable):
    """Set the completer function."""
    readline_wrapper.set_completer(completer_func)

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
    """Get user input with completion and history support."""
    return readline_wrapper.input(prompt) 