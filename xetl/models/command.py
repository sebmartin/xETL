import logging
from typing import Any

from pydantic import BaseModel, field_validator
import yaml
from xetl.logging import LogContext, log_context

from xetl.models import EnvVariableType
from xetl.models.task import Task, TaskFailure, UnknownTaskError
from xetl.models.utils.dicts import conform_env_key

logger = logging.getLogger(__name__)


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
    The name of the task to execute. The task needs to be discovered by the job in order
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
        valid_characters = set("abcdefghijklmnopqrstuvwxyz0123456789-_")
        if isinstance(value, str) and set(value.lower()) - valid_characters:
            raise ValueError(
                f"Command name '{value}' contains invalid characters. Only letters, numbers, dashes, and underscores are allowed."
            )
        return value

    def execute(self, tasks: dict[str, Task], dryrun: bool, index: int, total: int):
        context_header = (
            f"Executing command {f'{index + 1}'} of {total}"
            if self.name is None
            else f"Executing command: {self.name} ({f'{index + 1}'} of {total})"
        )
        with log_context(LogContext.TASK, context_header):
            for line in yaml.dump(self.model_dump(), indent=2, sort_keys=False).strip().split("\n"):
                logger.info("  " + line)
            with log_context(LogContext.COMMAND, f"Executing task: {self.task}") as log_footer:
                returncode = self.get_task(tasks).execute(self.env, dryrun)
                log_footer(f"Return code: {returncode}")
            if index < total - 1:
                logger.info("")  # leave a blank line between commands

        if returncode != 0:
            raise TaskFailure(returncode=returncode)

    def get_task(self, tasks: dict[str, Task]) -> Task:
        """
        Get the task associated with this command from the provided tasks dictionary.

        Args:
            tasks: A dictionary of tasks where the keys are the task names and the values are the task objects.

        Returns:
            The task object associated with this command.
        """
        task_name = self.task

        if task := tasks.get(task_name):
            return task
        else:
            raise UnknownTaskError(f"Unknown task `{task_name}`, should be one of: {sorted(tasks.keys())}")
