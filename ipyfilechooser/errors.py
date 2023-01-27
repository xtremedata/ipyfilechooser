"""Exception classes."""
import os
from typing import Optional

# Local Imports
from .utils_sources import SupportedSources


class ParentPathError(Exception):
    """ParentPathError class."""

    def __init__(self, path: str, parent_path: str, message: Optional[str] = None):
        self.path = path
        self.sandbox_path = parent_path
        self.message = message or f'{path} is not a part of {parent_path}'
        super().__init__(self.message)


class InvalidPathError(Exception):
    """InvalidPathError class."""

    def __init__(self, path: str, message: Optional[str] = None):
        self.path = path
        self.message = message or f'{path} does not exist'
        super().__init__(self.message)


class InvalidFileNameError(Exception):
    """InvalidFileNameError class."""
    invalid_str = [os.sep, os.pardir]

    if os.altsep:
        invalid_str.append(os.altsep)

    def __init__(self, filename: str, message: Optional[str] = None):
        self.filename = filename
        self.message = message or f'{filename} cannot contain {self.invalid_str}'
        super().__init__(self.message)


class InvalidSourceError(Exception):
    """InvalidSourceError class."""
    valid_str = SupportedSources.names()

    def __init__(self, source: SupportedSources, message: Optional[str] = None):
        self.source = source
        self.message = message \
                or f'{source} is not supported/known, supported: Enum:{self.valid_str}'
        super().__init__(self.message)


#class RuntimeError(Exception):
#    """RuntimeError class."""
#
#    def __init__(self, reason: str, message: Optional[str] = None):
#        self.message = message \
#                or f'Runtime error, reason: {reason}'
#        super().__init__(self.message)
