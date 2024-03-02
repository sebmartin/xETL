import logging
import os
import re
import tempfile
from collections import OrderedDict
from typing import Any, Iterable

from pydantic import BaseModel, field_validator, model_validator
import yaml
from xetl.logging import LogContext, log_context

from xetl.models import EnvKeyLookupErrors, EnvVariableType
from xetl.models.command import Command
from xetl.models.task import discover_tasks
from xetl.models.utils.dicts import conform_key, fuzzy_lookup
from xetl.models.utils.io import parse_yaml, parse_yaml_file

logger = logging.getLogger(__name__)


class NoAliasDumper(yaml.SafeDumper):
    def ignore_aliases(self, data):
        return True


class JobDataDirectoryNotFound(Exception):
    pass


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

    basedir: str | None = None
    """
    The path used as the working directory for the job. This is used to resolve relative paths and defaults to the
    directory containing the job manifest file.

    This value can be referenced in commands using the `${job.basedir}` placeholder.

    e.g.
    ```
        commands:
          - task: my-task
            env:
              INPUT: ${job.basedir}/files/input.csv
              OUTPUT: ${job.basedir}/files/output.csv
    ```
    """

    data: str
    """
    The root directory where the job will store its data. If the directory does not exist, it will be created.

    This value can be referenced in commands using the `${job.data}` placeholder.

    e.g.
    ```
        commands:
          - task: my-task
            env:
              INPUT: ${job.data}/input.csv
              OUTPUT: ${job.data}/output.csv
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
        job = parse_yaml_file(path)
        return cls(**{**job, "basedir": os.path.dirname(path)})

    @classmethod
    def from_yaml(cls, yaml_content: str) -> "Job":
        return cls(**parse_yaml(yaml_content))

    @model_validator(mode="before")
    @classmethod
    def load_host_env(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if not set(data.keys()) & {"host_env", "host-env"}:
                host_env = list(data.get("env", {}).keys())
            else:
                host_env = data.get("host_env", data.get("host-env"))
            data["host_env"] = [host_env] if isinstance(host_env, str) else host_env
        return data

    @model_validator(mode="after")
    def resolve_placeholders(self):
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

    def execute(self, commands: list[str] | str | None = None, dryrun=False):
        if commands is not None:
            if isinstance(commands, str):
                commands = [cmd.strip() for cmd in commands.split(",")]
            if not isinstance(commands, Iterable):
                raise ValueError(
                    "The `commands` argument must be a list of strings or a comma-separated string of command names"
                )
            if not commands:
                logger.warning("No commands to execute")
                return
            commands = [conform_key(name.strip()) for name in commands]

        with log_context(LogContext.JOB, "Executing job: {}".format(self.name)):
            if dryrun:
                logger.info("Manifest parsed as:")
                for line in (
                    yaml.dump(
                        self.model_dump(exclude_unset=True),
                        Dumper=NoAliasDumper,
                        sort_keys=False,
                    )
                    .strip()
                    .split("\n")
                ):
                    logger.info("  " + line)
            else:
                logger.info("Parsed manifest for job: {}".format(self.name))

            if tasks_repo_paths := self.tasks:
                logger.info(f"Discovering tasks at paths: {tasks_repo_paths}")
                available_tasks = discover_tasks(tasks_repo_paths)
                if not available_tasks:
                    logger.error("Could not find any tasks at paths {}".format(tasks_repo_paths))
                    return
            else:
                logger.warning("The property `tasks` is not defined in the job manifest, no tasks will be available")
                available_tasks = {}
            logger.info("Available tasks detected:")
            for cmd in available_tasks.values():
                logger.info(f" - {cmd.name}")

            filtered_commands = []
            for command in self.commands:
                if commands is None or command.name and conform_key(command.name) in commands:
                    filtered_commands.append(command)
                else:
                    logger.warning(f"Skipping command `{command.name}`")

            if not dryrun:
                self._verify_data_dir(self.data)

            # Validate all inputs before executing any command to fail fast
            for command in filtered_commands:
                command.get_task(available_tasks).validate_inputs(command.env, critical_only=True)

            # Execute all commands in order
            for i, command in enumerate(filtered_commands):
                if command.skip:
                    logger.warning(f"Skipping command `{command.name or f'#{i + 1}'}` from job '{self.name}'")
                    continue
                command.execute(available_tasks, dryrun, i, len(self.commands))

            logger.info("Done! \\o/")

    def _verify_data_dir(self, data_dir: str):
        if not os.path.exists(data_dir):
            logger.fatal(f"The job's `data` directory does not exist: {data_dir}")
            raise JobDataDirectoryNotFound


# Validation


def expand_path(path: str, base_path: str | None) -> str:
    if path is None or path.startswith(os.path.sep):
        return path

    if not base_path:
        raise ValueError(f"Relative paths cannot be used when the job manifest is loaded from a string: {path}")

    # Expand data path relative to the job's manifest file
    return os.path.abspath(os.path.join(base_path, path))


def inherit_env(job: Job):
    """
    Inherit environment variables from the host environment. Host environment variables that are named in `host_env`
    override values specified in `job.env`.
    """
    os_env = {}
    allowlist = job.host_env or []
    if "*" in allowlist:
        if len(allowlist) > 1:
            logger.warning(
                "The `*` value in `job.host_env` was specified alongside other values. "
                "All host environment variables will be inherited."
            )
        os_env = dict(os.environ)
    else:
        os_env = {key: os.environ[key] for key in allowlist if key in os.environ}
        if missing_keys := set(allowlist) - set(os_env.keys()) - set(job.env.keys()):
            logger.warning(
                "The following host environment variables did not receive a value: {}".format(", ".join(missing_keys))
            )
    os_env = {**job.env, **os_env}
    job.env = os_env


def propagate_env(job: Job):
    """Propagates the job's env values to each command's env."""
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

    def get_key_value(obj: BaseModel | dict, keys: list[str], match: str) -> Any:
        incomplete_key_error = ValueError(
            f"Incomplete key path, variable must reference a leaf value: `{match}`"
            " -- did you forget to wrap the variable names in curly braces?"
        )
        if not keys:
            raise incomplete_key_error

        try:
            value = fuzzy_lookup(obj, keys[0], raise_on_missing=True)
        except EnvKeyLookupErrors:
            valid_keys = ", ".join(
                sorted(
                    f"`{key}`" for key in (list(obj.keys()) if isinstance(obj, dict) else list(obj.model_dump().keys()))
                )
            )
            raise ValueError(f"Invalid placeholder `{keys[0]}` in {match}. Valid keys are: {valid_keys}")

        if len(keys) <= 1:
            return value
        if isinstance(value, BaseModel | dict):
            return get_key_value(value, keys[1:], match)
        if isinstance(value, list):
            return get_key_value(value[int(keys[1])], keys[2:], match)

        raise ValueError(
            f"Invalid placeholder in {match}. Could not drill in beyond `{keys[0]}` as it does not refer to an object or a list."
        )

    def variable_value(
        names: Iterable[str], current_model: Job | Command, references: dict[str, Job | Command], match: str
    ) -> EnvVariableType:
        # Make case insensitive
        names = [n.lower() for n in names]

        # Check for reserved names
        tmpdir = os.path.join(job.data, "tmp")
        match list(names):
            case ["tmp", *rest]:
                match rest:
                    case ["dir"]:
                        return temp_directory(tmpdir)
                    case ["file"]:
                        return temp_file(tmpdir)
                    case _:
                        raise ValueError(
                            f"Invalid use of ${{tmp}} placeholder in `{match}`. Expected `tmp.dir` or `tmp.file`"
                        )
            case ["job", *rest]:
                return get_key_value(job, rest, match)
            case ["previous", *rest]:
                if "previous" not in references:
                    raise ValueError("Cannot use ${previous} placeholder on the first command")
            case _:
                if len(names) == 1:
                    # Check current command's env
                    try:
                        return get_key_value(current_model.env, names, match)
                    except EnvKeyLookupErrors:
                        pass

        # Resolve named commands or `previous`
        if placeholder := fuzzy_lookup(references, names[0]):
            return get_key_value(placeholder, names[1:], match)

        # Could not resolve
        env_keys = ", ".join(sorted(current_model.env.keys()))
        command_keys = ", ".join(sorted({name for name, p in references.items()} - {"previous"}))
        raise ValueError(
            f"Invalid name `{names[0]}` in `{match}`. The first name must be one of:\n"
            f" - variable name in the current command's env: {env_keys or 'No env variables defined'}\n"
            f" - name of a previous command: {command_keys or 'No previous commands defined'}\n"
            " - `self` to reference the current command (e.g. ${self.name})\n"
            " - `job` to reference the Job (e.g. ${job.data})\n"
            " - `previous` to reference the previous command (e.g. ${previous.OUTPUT})\n"
            " - `tmp.dir` to create a temporary directory\n"
            " - `tmp.file` to create a temporary file"
        )

    def resolve(string: str, current_model: Job | Command, references: dict[str, Job | Command]) -> EnvVariableType:
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

            resolved = variable_value(names, current_model, references, match[0])
            if match.span() == (0, len(string)):
                # We matched the entire string, retain the original type (e.g. int, float, etc)
                return resolved
            # Match is embedded in the string
            resolved = "null" if resolved is None else str(resolved)
            string_length_delta += len(resolved) - len(match[0])
            string = string[: match.start()] + resolved + string[match.end() :]
            pos = match.start() + len(resolved)
        return string

    def traverse(
        model: BaseModel | dict | list,
        key_path: Iterable[str],
        references: dict[str, Job | Command],
        current_model: Job | Command,
    ):
        """
        Look for placeholders (e.g. $previous.env.OUTPUT) in property (models) or dict values and resolve them.
        """

        def get(obj: BaseModel | dict | list, key: str | int) -> Any:
            if isinstance(obj, dict):
                return obj[key]
            if isinstance(obj, list):
                return obj[int(key)]
            if isinstance(obj, BaseModel):
                return getattr(obj, str(key))

        def set(obj: BaseModel | dict | list, key: str | int, value: Any) -> None:
            if isinstance(obj, dict):
                obj[key] = value
            if isinstance(obj, list):
                obj[int(key)] = value
            elif isinstance(obj, BaseModel):
                setattr(obj, str(key), value)

        def keys(obj: BaseModel | dict | list) -> Iterable[str | int]:
            if isinstance(obj, dict):
                return obj.keys()
            if isinstance(obj, BaseModel):
                return obj.model_fields.keys()
            if isinstance(obj, list):
                return range(len(obj))

        current_model = model if isinstance(model, Job | Command) else current_model
        references = dict(references)
        queue: list[tuple[BaseModel | dict | list, Iterable[str]]] = []

        for key in keys(model):
            value = get(model, key)
            item_key_path = tuple(key_path) + (key,) if isinstance(key, str) else tuple(key_path)
            if not value:
                continue
            if isinstance(value, str):
                if "$" in value:
                    value = resolve(value, current_model, references)
                if isinstance(value, str):
                    if value.startswith("~/"):
                        # assume it's a path and expand it
                        value = os.path.expanduser(value)

                    if item_key_path in (
                        ("job", "data"),
                        ("job", "tasks"),
                    ):
                        value = expand_path(value, job.basedir)

                set(model, key, value)
            elif isinstance(value, BaseModel | dict | list):
                queue.append((value, item_key_path))
            else:
                continue

        for item, key_path in queue:
            traverse(item, key_path, references, current_model)

            # set command name reference and advance the `previous` placeholder reference
            if key_path == ("job", "commands") and isinstance(item, Command) and item.name:
                references[item.name.lower()] = item
                references["previous"] = item
            elif "previous" in references:
                del references["previous"]

    # Resolve all job placeholders
    traverse(job, key_path=("job",), references=OrderedDict(), current_model=job)
