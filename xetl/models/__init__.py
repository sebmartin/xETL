from typing import TypeAlias

EnvVariableType: TypeAlias = str | int | float | bool | None
"""Type alias for environment variable value types"""

EnvKeyLookupErrors = (KeyError, ValueError)
"""Tuple of errors that can be raised when looking up an environment variable"""
