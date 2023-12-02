from typing import Any
from pydantic import BaseModel, field_validator

from metl.core.models.utils import conform_env_key

ArgumentType = str | int | float | bool


class Step(BaseModel):
    """
    A step is an instruction for executing a transform. An app is composed of jobs, which are composed of
    one or more steps. Each step defines the input and output parameters for executing a single transform.
    """

    name: str | None = None
    """An optional name for the step. This can be any string value."""

    description: str | None = None
    """
    An optional description of the step. This can be any string value. This is purely metadata and has no
    functional impact on the app.
    """

    transform: str
    """
    The name of the transform to execute. The transform needs to be discovered by the runner in order
    to be referenced by name and be found. The name matching is case insensitive.

    See the `metl.core.models.transform.discover_transforms` function for more information on the transform
    discovery process.
    """

    env: dict[str, ArgumentType] = {}
    """
    Set of ENV variables that will be set when executing the transform. The keys in this dictionary must
    match the names of the `env` keys defined (and described) in the transform's manifest.
    """

    skip: bool = False
    """
    If `True`, the step will be skipped when running the app. This can be useful for temporarily disabling
    steps during development without removing them from the app. It's akin to commenting out the step
    however the variable resolution will still occur (e.g. future steps can still reference this step's
    values).
    """  # TODO: this has no tests in test_runner, only skip_to=

    @field_validator("env", mode="before")
    @classmethod
    def conform_env_keys(cls, value: Any):
        if not isinstance(value, dict):
            return value
        return {conform_env_key(key): value for key, value in value.items()}
