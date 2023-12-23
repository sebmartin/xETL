from typing import Any

from pydantic import BaseModel, field_validator

from xetl.models import EnvVariableType
from xetl.models.utils.dicts import conform_env_key


class Task(BaseModel):
    """
    A task is a directive to execute a specific command. A job consists of one or more such tasks. Each task
    stipulates the input values required for the execution of its associated command.
    """

    name: str | None = None
    """An optional name for the task. This can be any string value."""

    description: str | None = None
    """
    An optional description of the task. This can be any string value. This is purely metadata and has no
    functional impact on the job.
    """

    command: str
    """
    The name of the command to execute. The command needs to be discovered by the engine in order
    to be referenced by name and be found. The name matching is case insensitive.

    See the `xetl.models.command.discover_commands` function for more information on the command
    discovery process.
    """

    env: dict[str, EnvVariableType] = {}
    """
    Set of ENV variables that will be set when executing the command. The keys in this dictionary must
    match the names of the `env` keys defined (and described) in the command's manifest.
    """

    skip: bool = False
    """
    If `True`, the task will be skipped when executing the job. This can be useful for temporarily disabling
    tasks during development without removing them from the job. It's akin to commenting out the task
    however the variable resolution will still occur (e.g. future tasks can still reference this task's
    values).
    """

    @field_validator("env", mode="before")
    @classmethod
    def conform_env_keys(cls, value: Any):
        if not isinstance(value, dict):
            return value
        return {conform_env_key(key): value for key, value in value.items()}

    @field_validator("name", mode="before")
    @classmethod
    def validate_name(cls, value: Any):
        # TODO: test this
        if not isinstance(value, str):
            return value
        valid_characters = set("abcdefghijklmnopqrstuvwxyz0123456789-_")
        if set(value.lower()) - valid_characters:
            raise ValueError(
                f"Task name '{value}' contains invalid characters. Only letters, numbers, dashes, and underscores are allowed."
            )
        return value
