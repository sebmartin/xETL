from typing import Any

from pydantic import BaseModel, field_validator

from xetl.models import EnvVariableType
from xetl.models.utils.dicts import conform_env_key


class Command(BaseModel):
    """
    A command is a directive to execute a specific task. A job consists of one or more such commands. Each command
    stipulates the input values required for the execution of its associated task.
    """

    name: str | None = None
    """An optional name for the command. This can be any string value."""

    description: str | None = None
    """
    An optional description of the command. This can be any string value. This is purely metadata and has no
    functional impact on the job.
    """

    task: str
    """
    The name of the task to execute. The task needs to be discovered by the engine in order
    to be referenced by name and be found. The name matching is case insensitive.

    See the `xetl.models.task.discover_tasks` function for more information on the task
    discovery process.
    """

    env: dict[str, EnvVariableType] = {}
    """
    Set of ENV variables that will be set when executing the task. The keys in this dictionary must
    match the names of the `env` keys defined (and described) in the task's manifest.
    """

    skip: bool = False
    """
    If `True`, the command will be skipped when executing the job. This can be useful for temporarily disabling
    commands during development without removing them from the job. It's akin to commenting out the command
    however the variable resolution will still occur (e.g. future commands can still reference this command's
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
                f"Command name '{value}' contains invalid characters. Only letters, numbers, dashes, and underscores are allowed."
            )
        return value
