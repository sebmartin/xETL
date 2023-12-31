import logging
import os
import re
import tempfile
from collections import OrderedDict
from typing import Any, Iterable

from pydantic import BaseModel, field_validator, model_validator

from xetl.models import EnvKeyLookupErrors, EnvVariableType
from xetl.models.command import Command
from xetl.models.utils.dicts import fuzzy_lookup
from xetl.models.utils.io import parse_yaml, parse_yaml_file

logger = logging.getLogger(__name__)


class Job(BaseModel):
    """
    A job serves as the primary model that outlines the structure of the work to be performed. It is comprised of
    multiple commands. Each command runs a specific task, with arguments that are defined within the command itself and
    supplied to the task during execution.
    """

    name: str
    """A name for the job used in logging and other places to identify the job. This can be any string value."""

    description: str | None = None
    """
    An optional description of the job. This can be any string value. This is purely metadata and has no
    functional impact on the job.
    """

    data: str
    """
    The root directory where the job will store its data. If the directory does not exist, it will be created.
    This value can be referenced in commands using the `$data` placeholder.

    e.g.
    ```
        commands:
          - task: my-task
            env:
              INPUT: $data/input.csv
              OUTPUT: $data/output.csv
    ```
    """

    host_env: list[str] | None = None
    """
    A list of environment variable names that should be inherited from the host environment. If the value is `*`
    or is a list that contains `*`, then all environment variables will be inherited from the host environment.
    Otherwise, only the environment variables that are explicitly listed will be inherited.

    If the value is `None` (or omitted), then no environment variables will be inherited from the host
    environment.
    """

    env: dict[str, EnvVariableType] = {}
    """
    A dictionary of environment variables that can be referenced in job commands. This can be useful for
    declaring values that are used in multiple commands such as database connection strings.

    These values will be available as env variables when running the task for each command in the job
    without having to be redefined in each command's env. However, the command's env always takes precedence so
    it's possible to override the job's env value by specifying a different value in a command's env.
    """

    tasks: list[str] = []
    """
    A path or list of paths containing the tasks that are used in the job. Paths can be an absolute
    path or a path relative to the job manifest file. If the directory does not exist,
    """

    commands: list[Command]
    """
    A dictionary of commands. Each command executes a command with env variables that are defined in each command
    and passed to the command at runtime.
    """

    @classmethod
    def from_file(cls, path: str) -> "Job":
        logger.info("Loading job manifest at: {}".format(path))
        return cls(**parse_yaml_file(path))

    @classmethod
    def from_yaml(cls, yaml_content: str) -> "Job":
        return cls(**parse_yaml(yaml_content))

    @model_validator(mode="before")
    @classmethod
    def load_host_env(cls, data: Any) -> Any:
        if isinstance(data, dict) and (host_env := data.get("host-env", data.get("host_env"))):
            data["host_env"] = [host_env] if isinstance(host_env, str) else host_env
        return data

    @model_validator(mode="after")
    def resolve_placeholders(self) -> "Job":
        inherit_env(self)
        propagate_env(self)
        resolve_placeholders(self)
        return self

    @field_validator("tasks", mode="before")
    @classmethod
    def validate_tasks(cls, value: Any):
        if isinstance(value, str):
            value = [value]
        return value


# Validation


def inherit_env(job: Job):
    base_env = {}
    host_env = job.host_env or []
    if "*" in host_env:
        if len(host_env) > 1:
            logger.warning(
                "The `*` value in `host-env` was specified alongside other values. "
                "All host environment variables will be inherited."
            )
        base_env = dict(os.environ)
    else:
        base_env = {key: os.environ.get(key) for key in host_env if key in os.environ}
        if missing_keys := set(host_env) - set(base_env.keys()):
            logger.warning(
                "The following host environment variables were not found: {}".format(", ".join(missing_keys))
            )
    base_env = {**base_env, **job.env}
    job.env = base_env


def propagate_env(job: Job):
    if not job.env:
        return
    for command in job.commands:
        command.env = {**job.env, **command.env}


def resolve_placeholders(job: Job):
    def temp_directory(root) -> str:
        if not os.path.exists(root):
            os.makedirs(root)
        return tempfile.mkdtemp("__", dir=root)

    def temp_file(root) -> str:
        if not os.path.exists(root):
            os.makedirs(root)
        fd, path = tempfile.mkstemp("__", dir=root)
        os.close(fd)
        return path

    def get_key_value(obj: BaseModel | dict, keys: list[str], match: str) -> EnvVariableType:
        incomplete_key_error = ValueError(
            f"Incomplete key path, variable must reference a leaf value: `{match}`"
            " -- did you forget to wrap the variable names in curly braces?"
        )
        if not keys:
            raise incomplete_key_error

        try:
            value = fuzzy_lookup(obj, keys[0], raise_on_missing=True)
            if isinstance(value, BaseModel | dict) and len(keys) > 1:
                # TODO: Can variable values be objects? -- we might not need this with the simplified implementation
                return get_key_value(value, keys[1:], match)  # TODO: test this
            if len(keys) <= 1:
                return value
            raise incomplete_key_error  # TODO: test this

        except EnvKeyLookupErrors:
            valid_keys = ", ".join(
                sorted(
                    f"`{key}`" for key in (list(obj.keys()) if isinstance(obj, dict) else list(obj.model_dump().keys()))
                )
            )
            raise ValueError(f"Invalid placeholder `{keys[0]}` in {match}. Valid keys are: {valid_keys}")

    def variable_value(
        names: Iterable[str], current_command: Command, named_commands: dict[str, Command], match: str
    ) -> EnvVariableType:
        # Make case insensitive
        names = [n.lower() for n in names]

        # Check for the `data` variable
        if names == ["data"]:
            return job.data

        # Check for $tmp variables
        tmpdir = os.path.join(job.data, "tmp")
        if tuple(names) == ("tmp", "dir"):
            return temp_directory(tmpdir)
        elif tuple(names) == ("tmp", "file"):
            return temp_file(tmpdir)

        # Check current command's env
        try:
            return get_key_value(current_command.env, names, match)
        except EnvKeyLookupErrors:
            pass

        # Check for `previous`
        if names[0] == "previous" and "previous" not in named_commands:
            raise ValueError("Cannot use $previous placeholder on the first command")

        # Resolve from previous command's env
        if command := fuzzy_lookup(named_commands, names[0]):
            if isinstance(command, Command):
                return get_key_value(command.env, names[1:], match)

        # Could not resolve
        env_keys = ", ".join(sorted(current_command.env.keys()))
        command_keys = ", ".join(sorted(named_commands.keys()))
        raise ValueError(
            f"Invalid name `{names[0]}` in `{match}`. The first must be one of:\n"
            f" - variable in the current command's env: {env_keys or 'No env variables defined'}\n"
            f" - name of a previous command: {command_keys or 'No previous commands defined'}"
        )

    def resolve(string: str, current_command: Command, named_commands: dict[str, Command]) -> EnvVariableType:
        """
        Parse the placeholder string and resolve it to a value.
        """
        # Find literal dollar signs ($$), replace them with a single dollar sign ($), and track their
        # positions so that we can skip them when they match later on
        literal_pattern = re.compile(r"\$\$")
        literals = set()
        pos = 0
        while match := literal_pattern.search(string, pos):
            string = string[: match.start()] + "$" + string[match.end() :]
            literals.add(match.start())
            pos = match.start() + 1

        # Find placeholders in `string` and replace them with their variable values
        # but skip matches that start where we found literals
        #        w/ curly braces <━┳━━━━━━━━━━━━━━━━━━━━━━━━━┓      ┏━━━━━━━━┳━> w/o curly braces
        pattern = re.compile(r"(?:\${([\w_-]+(?:[.][\w_-]+)*)})|(?:\$([\w_-]+))")
        pos = 0
        string_length_delta = 0  # accounts for length changes made to `string` along the way
        while match := pattern.search(string, pos):
            if match.start() - string_length_delta in literals:
                pos = match.start() + 1
                continue
            matched_names = match[1] or match[2]
            names = matched_names.split(".")

            resolved = variable_value(names, current_command, named_commands, match[0])
            if match.span() == (0, len(string)):
                # We matched the entire string, retain the original type (e.g. int, float, etc)
                return resolved
            # Match is embedded in the string
            resolved = "null" if resolved is None else str(resolved)
            string_length_delta += len(resolved) - len(match[0])
            string = string[: match.start()] + resolved + string[match.end() :]
            pos = match.start() + len(resolved)
        return string

    def traverse_object(model: BaseModel | dict, current_command: Command, named_commands: dict[str, Command]):
        """
        Look for placeholders (e.g. $previous.OUTPUT) in property (models) or dict values and resolve them.
        """
        items = model.items() if isinstance(model, dict) else model.model_dump().items()
        for key, value in items:
            if isinstance(value, BaseModel | dict):
                traverse_object(value, current_command, named_commands)
            elif isinstance(value, str):
                if "$" in value:
                    value = resolve(value, current_command, named_commands)
                if isinstance(value, str) and value.startswith("~/"):
                    # assume it's a path and expand it
                    value = os.path.expanduser(value)
            else:
                continue

            if isinstance(model, dict):
                model[key] = value
            elif isinstance(model, BaseModel):
                setattr(model, key, value)

    # Resolve all job placeholders
    job.data = os.path.abspath(job.data)
    named_commands = OrderedDict({})
    for command in job.commands:
        traverse_object(command, command, named_commands)
        if command.name:
            named_commands[command.name] = command
        named_commands["previous"] = command
