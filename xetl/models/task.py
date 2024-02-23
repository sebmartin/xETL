import logging
import os
import subprocess
from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    field_validator,
    model_validator,
)

from xetl.models import EnvVariableType
from xetl.models.task_input_details import TaskInputDetails
from xetl.models.task_test_case import TaskTestCase
from xetl.models.utils.dicts import conform_env_key, conform_key
from xetl.models.utils.io import (
    InvalidManifestError,
    ManifestLoadError,
    load_file,
    parse_yaml,
)
from xetl.models.utils.run import parse_run_command

logger = logging.getLogger(__name__)


class UnknownTaskError(Exception):
    pass


class TaskFailure(Exception):
    def __init__(self, returncode: int) -> None:
        super().__init__()
        self.returncode = returncode


class Task(BaseModel):
    model_config = ConfigDict(extra="allow")

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

    basedir: str
    """
    The path to the directory containing the task. This will be used as the working directory when the
    task is executed. It's also used to resolve relative paths.
    """

    run: list[str]
    """
    The command to run when the task executes. It's a list of strings which will be passed as an argument
    to `Popen` when executed. However, in YAML it can actually
    take two forms which will get converted to a list of strings during model validation:

    1. A string containing an executable and its arguments. For example:
     - run: python -m mymodule --foo $MY_ENV_VAR
     - run: ./my-script.sh --foo $MY_ENV_VAR

    2. An object with two keys: `interpreter` and `script`. The script can be multi-line and will be
    passed to the interpreter as an argument. For example:

    ```
    run:
      interpreter: /bin/bash -c
      script: |
        echo "Hello"
        echo $MY_ENV_VAR
    ```

    If the interpreter value is left unspecified, the current process' python interpreter will be used.
    For example:

    ```
    run:
      script: |
        print("I'm a python script!")
        print(f"This will execute with the interpreter: {sys.executable}")
    ```

    For both forms, the current working directory is set to the value of `Task.path`. This makes it
    possible to use relative paths that are relative to the task's directory.

    All input values that come from the job's `Command` will be set as environment variables and can be
    used as you would any ENV variable.  Each input name is converted to be compatible with POSIX naming
    convention for environment variables which is:
      - all uppercase
      - all dashes replaced with underscores
    """

    env: dict[str, TaskInputDetails] = {}
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

    tests: dict[str, TaskTestCase] = {}
    """
    A dictionary of test cases for the task. The dictionary keys are the names of the test cases. The values
    represent the `env` that is used to execute the task as well as a verification (`verify`) command or script
    to validate the test's outputs. The verification command/script is executed in the task's directory.

    e.g.
    ```
      tests:
        my-test:
          env:
            INPUT1: foo
            INPUT2: bar
          verify: python -m mymodule --foo $MY_INPUT
    ```

    The format for the `verify` property is similar to the `run` property. It can also be expressed
    as a script object with an optional `interpreter` property. If the interpreter is not specified,
    the current process' python interpreter will be used.

    e.g.
    ```
      tests:
        my-test:
          env:
            INPUT1: foo
            INPUT2: bar
          verify:
            interpreter: /bin/bash -c
            script: |
              diff -u ./test/results.txt ./expected/results.txt
    ```
    """

    @classmethod
    def from_file(cls, path: str, silent=False) -> "Task":
        if not silent:
            logger.info(f"Loading task at: {path}")
        yaml_content = load_file(path)
        try:
            return cls.from_yaml(yaml_content, path=os.path.dirname(path))
        except Exception as e:
            raise ManifestLoadError(f"Could not load YAML file at path: {path}") from e

    @classmethod
    def from_yaml(cls, yaml_content: str, path: str) -> "Task":
        manifest = parse_yaml(yaml_content)
        manifest["basedir"] = path
        return cls(**manifest)

    # -- Validation

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
    def conform_env(cls, data: Any) -> dict[str, TaskInputDetails]:
        if isinstance(data, list):
            invalid_keys = [str(key) for key in data if not isinstance(key, str)]
            if invalid_keys:
                raise ValueError(
                    f"Task env names must be strings, the following are invalid: {', '.join(invalid_keys)}"
                )
            data = {key: None for key in data}

        def conform_value(value: Any) -> TaskInputDetails:
            if isinstance(value, dict):
                return TaskInputDetails(**value)
            return TaskInputDetails(description=str(value) if value is not None else None)

        data = {conform_env_key(key): conform_value(value) for key, value in data.items()}

        if required_with_defaults := [
            key for key, value in data.items() if value.required and value.default is not None
        ]:
            raise ValueError(
                f"The following task env variables are required but specify a default value which is invalid: {', '.join(required_with_defaults)}"
            )

        return data

    @field_validator("run", mode="before")
    @classmethod
    def generate_run_command(cls, data: Any) -> list[str]:
        if run_command := parse_run_command(data):
            return run_command
        raise ValueError(f"Task run command must be a string, a list of strings, or a script object, received: {data}")

    # -- Execution

    def validate_inputs(self, env: dict[str, EnvVariableType], critical_only: bool = False) -> None:
        """
        Validates that the task can be executed with the given command as an input.
        """
        if not critical_only and (
            unknown_inputs := [input for input in env.keys() if conform_env_key(input) not in self.env]
        ):
            logger.warning(
                f"Ignoring unexpected env variable{'s' if len(unknown_inputs) > 1 else ''} for task `{self.name}`: {', '.join(unknown_inputs)}."
                + (f" Valid names are: {', '.join(self.env.keys())}" if self.env else "")
            )

        if missing_inputs := [input for input, details in self.env.items() if details.required and input not in env]:
            raise ValueError(
                f"Missing required input{'s' if len(missing_inputs) > 1 else ''} for task `{self.name}`: {', '.join(missing_inputs)}"
            )

        if invalid_envs := [
            (env_key, value, expected_type)
            for env_key, value, expected_type in [
                (env_key, value, self.env[conform_env_key(env_key)].type)
                for env_key, value in env.items()
                if conform_env_key(env_key) in self.env
            ]
            if expected_type not in (Any, None) and not isinstance(value, expected_type)
        ]:
            details = [
                f" - {env_key}: expected `{expected_type.__name__}`, received `{type(value).__name__}`"
                for env_key, value, expected_type in invalid_envs
            ]
            raise ValueError(f"Invalid env values for task `{self.name}`:\n" + "\n".join(details))

    def execute(self, env: dict[str, EnvVariableType], dryrun: bool = False) -> int:
        """
        Execute the task with inputs from a given command.
        """
        env = {conform_env_key(key): value for key, value in env.items()}
        self.validate_inputs(env)

        # Start with defaults for envs that are not provided
        inputs_env = {
            conform_env_key(key): value.default
            for (key, value) in self.env.items()
            if not value.required and value.default is not None
        }
        # Override with values from the command
        inputs_env.update(
            {
                conform_env_key(key): str(value) if value is not None else self.env.get("default", None)
                for (key, value) in env.items()
            }
        )

        if dryrun:
            logger.info("DRYRUN: Would execute with:")
            logger.info(f"  run: {' '.join(self.run)}")
            logger.info(f"  cwd: {self.basedir}")
            logger.info(f"  env: {', '.join(f'{k}={v}' for k,v in inputs_env.items())}")
            return 0
        else:
            final_env = dict(os.environ)
            final_env.update(inputs_env)
            process = subprocess.Popen(
                self.run,
                cwd=self.basedir,
                env=final_env,
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
                    process.kill()
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
    for path, dirs, files in os.walk(tasks_repo_path):
        # ignore test directories
        dirs[:] = [d for d in dirs if d.lower() != "tests"]

        # walk subdirectories in alphabetical order
        dirs.sort()

        # ignore directories that don't contain a manifest file
        if not ({"manifest.yml", "manifest.yaml"} & {f.lower() for f in files}):
            continue

        try:
            task = Task.from_file(f"{path}/manifest.yml")
            tasks[task.name] = task
        except (ManifestLoadError, InvalidManifestError) as e:
            logger.warning(f"Skipping task at `{path}` due to error: {str(e)}")
        except Exception as e:
            logger.error(f"Skipping task at `{path}` due to unexpected error: {e}")

    return tasks
