from typing import TypeAlias


EnvVariableType: TypeAlias = str | int | float | bool | None  # TODO: support lists and dicts?
"""Type alias for environment variable value types"""

EnvKeyLookupErrors = (KeyError, ValueError)
"""Tuple of errors that can be raised when looking up an environment variable"""
