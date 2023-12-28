import logging
import os
import shlex
import subprocess
from enum import Enum
from typing import Any, Type

from pydantic import BaseModel, ValidationError, field_validator, model_validator

from xetl.models import EnvVariableType
from xetl.models.command import Command
from xetl.models.utils.dicts import conform_env_key, conform_key
from xetl.models.utils.io import InvalidManifestError, ManifestLoadError, load_file, parse_yaml

logger = logging.getLogger(__name__)


class UnknownTaskError(Exception):
    pass


class TaskFailure(Exception):
    def __init__(self, returncode: int) -> None:
        super().__init__()
        self.returncode = returncode


class EnvType(Enum):
    """
    Types of environments that a task can run in.
    """

    PYTHON = "python"
    BASH = "bash"


class InputDetails(BaseModel):
    description: str | None = None
    required: bool = True
    default: Any | None = None
    type: Type[EnvVariableType] | None = None

    @model_validator(mode="before")
    @classmethod
    def set_defaults(cls, data: Any) -> Any:
        if isinstance(data, dict):
            data = {conform_key(key): value for key, value in data.items()}
            if "optional" in data:
                if "required" in data:
                    raise ValueError("Cannot specify both `required` and `optional`")
                data["required"] = not data.pop("optional")
        return data

    @field_validator("type", mode="before")
    def valid_type(cls, value: Any) -> Any:
        if isinstance(value, str):
            mapping = {
                "str": str,
                "string": str,
                "int": int,
                "integer": int,
                "float": float,
                "decimal": float,
                "bool": bool,
                "boolean": bool,
            }
            if type_ := mapping.get(value.lower()):
                return type_
        return value


class Task(BaseModel):
    """
    A task is a single unit of work that can be executed in an job. You can think of a task as a
    mini-job that accepts inputs in the form of ENV variables, does some work, and produces some
    output. These inputs are variable which makes tasks re-usable in different contexts.
    """

    name: str
    """
    The name of the task. This is used to identify the task from a job command. The name matching is
    case insensitive (`MyTask` is equivalent to `mytask` but not `my-task`). The name should
    contain only:
        - alphanumeric characters
        - underscores
        - dashes
    This is not (yet) enforced but is good practice to avoid issues with matching commands to their tasks.
    """

    description: str | None = None
    """
    An optional description of the task. This can be any string value. This is purely metadata and has no
    functional impact on the task.
    """

    path: str
    """
    The path to the directory containing the task. This will be used as the working directory when the
    task is executed.
    """

    env_type: EnvType  # TODO: do we need this or just stick to bash?
    """
    The type of environment that the task will run in. This determines how the run task is executed
    and can currently accept one of the following values:
        - `python`: The run task is executed as a python script.
        - `bash`: The run task is executed as a bash task.
    """

    env: dict[str, InputDetails] = {}
    """
    A dictionary of environment variable inputs for the task's run task. This instructs the runtime
    engine which environment variables to pass to the task when executing it. When the task is
    executed, the runtime engine will pass the values of these environment variables (provided by the job
    command) to the task as environment variables. The task can then use these values to control its
    behaviour.

    In its simplest form, the keys are the names of the ENV variables and the values are a text description
    for each variable. The descriptions are purely metadata and have no functional impact on the task.

    e.g.
    ```
        env:
            MY_INPUT: A value that is use as the input for the task
            MY_OUTPUT: A path where the task will save its output
    ```

    It's also possible to specify additional details for each input. The following details can be specified:
        - `description`: A text description of the input. This is purely metadata and has no functional
            impact on the task.
        - `required`: A boolean value indicating whether the input is required. If `True`, the runtime engine
            will raise an error if the input is not provided by the job command. If `False`, the runtime engine
            will not raise an error if the input is not provided by the job command and the ENV variable will not
            be set when executing the task.
        - `optional`: A boolean value indicating whether the input is optional. This is just a convenience
            alias for `required` and is mutually exclusive with `required`.
        - `default`: The default value to use for the input if it is not provided by the job command. This value
            is only used if the input is not required and is not provided by the job command. An exception is
            raised if a default value is specified for a required input.
        - `type`: The data type of the input. This is used to validate the value of the input provided by the job
            command (at runtime). If the value provided by the job command is not of the specified type, an exception
            is raised. The following types are supported:
                - `string`: A string value (also accepts `str`)
                - `integer`: An integer value (also accepts `int`)
                - `decimal`: A floating point value (also accepts `float`)
                - `boolean`: A boolean value (also accepts `bool`)

            e.g.
            ```
                env:
                    MY_INPUT:
                        description: A value that is use as the input for the task
                        required: true
                        type: string
                    MY_OUTPUT:
                        description: A path where the task will save its output
                        required: true
                        type: string
            ```
    """

    run_task: str
    """
    The task to execute when running the task. The task is executed in with the "working directory"
    set to the value in the `path` property (typically the directory containing the task's manifest.yml file)
    and executes with the ENV variables set (see the `env` property). The task depends on the `env_type`
    which has some impact on the environment in which the task is executed.

    The inputs are accessed via environment variables. Each input name is converted to a naming convention that
    is compatible with environment variables. The naming convention is as follows:
        - all uppercase
        - all dashes replaced with underscores

    Any `env` name that does not follow this convention will be converted.
    """

    test_task: str | None = None
    """
    An optional task to execute when running the task's tests. This is currently experimental and not
    completely implemented. The idea is to be able to build some ergonomics around being able to run all tests
    for all tasks regardless of their implementation (python, bash, rust, etc.)
    """  # TODO: add a task for this to the CLI

    @classmethod
    def from_file(cls, path: str) -> "Task":
        logger.info(f"Loading task at: {path}")
        yaml_content = load_file(path)
        try:
            return cls.from_yaml(yaml_content, path=os.path.dirname(path))
        except Exception as e:
            raise ManifestLoadError(f"Could not load YAML file at path: {path}") from e

    @classmethod
    def from_yaml(cls, yaml_content: str, path: str) -> "Task":
        manifest = parse_yaml(yaml_content)
        manifest["path"] = path
        return cls(**manifest)

    @model_validator(mode="before")
    @classmethod
    def conform_root_keys_to_snakecase(cls, data: Any) -> Any:
        """
        Converts root keys in incoming dictionary to snake case so that it matches the
        pydantic model properties.
        e.g. My-Favourite-Variable -> my_favourite_variable
        """
        return {conform_key(key): value for key, value in data.items()}

    @field_validator("env", mode="before")
    @classmethod
    def conform_env(cls, data: Any) -> dict[str, InputDetails]:
        def conform_value(value: Any) -> InputDetails:
            if isinstance(value, dict):
                return InputDetails(**value)
            return InputDetails(description=str(value))

        if isinstance(data, list):
            invalid_keys = [str(key) for key in data if not isinstance(key, str)]
            if invalid_keys:
                raise ValueError(
                    f"Task env names must be strings, the following are invalid: {', '.join(invalid_keys)}"
                )
            data = {key: "N/A" for key in data}
        return {conform_env_key(key): conform_value(value) for key, value in data.items()}

    @field_validator("env_type", mode="before")
    @classmethod
    def convert_env_type_lowercase(cls, data: Any) -> Any:
        if isinstance(data, str):
            return data.lower()
        return data

    def validate_inputs(self, command: Command) -> None:
        """
        Validates that the task can be executed with the given command as an input.
        """
        if unknown_inputs := [input for input in command.env.keys() if conform_env_key(input) not in self.env]:
            logger.warning(
                f"Ignoring unknown env variable{'s' if len(unknown_inputs) > 1 else ''} for task `{self.name}`: {', '.join(unknown_inputs)}. "
                f"Valid names are: {', '.join(self.env.keys())}"
            )

        if missing_inputs := [
            input for input, details in self.env.items() if details.required and input not in command.env
        ]:
            raise ValueError(
                f"Missing required input{'s' if len(missing_inputs) > 1 else ''} for task `{self.name}`: {', '.join(missing_inputs)}"
            )

        if invalid_envs := [
            (env_key, value, expected_type)
            for env_key, value, expected_type in [
                (env_key, value, self.env[conform_env_key(env_key)].type)
                for env_key, value in command.env.items()
                if conform_env_key(env_key) in self.env
            ]
            if expected_type not in (Any, None) and not isinstance(value, expected_type)
        ]:
            details = [
                f" - {env_key}: expected `{expected_type.__name__}`, received `{type(value).__name__}`"
                for env_key, value, expected_type in invalid_envs
            ]
            raise ValueError(f"Invalid env values for task `{self.name}`:\n" + "\n".join(details))

    def execute(self, command: Command, dryrun: bool = False) -> int:
        """
        Execute the task with inputs from a given command.
        """

        self.validate_inputs(command)
        inputs_env = {conform_env_key(key): str(value) for (key, value) in command.env.items()}

        # TODO: see if we can satisfy the python usecase with only bash (including custom venv)
        match self.env_type:
            case EnvType.PYTHON:
                task = shlex.split(self.run_task)
            case EnvType.BASH:
                task = ["/bin/bash", "-c", self.run_task]

        if dryrun:
            logger.info("DRYRUN: Would execute with:")
            logger.info(f"  task: {task}")
            logger.info(f"  cwd: {self.path}")
            logger.info(f"  env: {', '.join(f'{k}={v}' for k,v in inputs_env.items())}")
            return 0
        else:
            env = dict(os.environ)
            env.update(inputs_env)
            process = subprocess.Popen(
                task,
                cwd=self.path,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            assert process.stdout is not None, "Process should have been opened with stdout=PIPE"

            try:
                while True:
                    output = process.stdout.readline()
                    if output == "" and process.poll() is not None:
                        break
                    if output:
                        logger.info(output.strip())
            finally:
                if process.poll() is None:
                    process.kill()  # tas test this
                if process.stdin:
                    process.stdin.close()
                if process.stdout:
                    process.stdout.close()
                if process.stderr:
                    process.stderr.close()
            return process.returncode


def discover_tasks(tasks_repo_path: str | list[str]) -> dict[str, Task]:
    """
    Walks a directory and loads all tasks found in subdirectories. Tasks are identified by the presence of a
    manifest.yml file in the directory. The manifest file must contain a `name` and `run-task` field.
    Returns a dictionary of tasks keyed by their name.
    """
    tasks: dict[str, Task] = {}

    # handle multiple paths
    if isinstance(tasks_repo_path, list):
        for path in tasks_repo_path:
            tasks.update(discover_tasks(path))
        return tasks

    # handle single path
    tasks_paths = [path[0] for path in os.walk(tasks_repo_path) if "manifest.yml" in path[2]]
    for path in tasks_paths:
        # ignore manifests in tests directories
        if path.endswith("/tests"):
            continue
        if "/tests/" in path and path.split("/tests/")[0] in tasks_paths:
            continue

        try:
            task = Task.from_file(f"{path}/manifest.yml")
            tasks[task.name] = task
        except ValidationError as e:
            logger.warning(f"Skipping task due to validation error: {e}")  # TODO: test this
        except (ManifestLoadError, InvalidManifestError) as e:
            logger.warning(f"Skipping task due to error: {str(e)}")  # TODO: test this
        except Exception as e:
            logger.warning(f"Skipping task due to unexpected error: {e}")  # TODO: test this

    return tasks
