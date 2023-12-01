from enum import Enum
import logging
import os
import shlex
import subprocess
from typing import Any, Type
from pydantic import BaseModel, ValidationError, field_validator, model_validator
from metl.core.models.step import Step

from metl.core.models.utils import conform_env_key, conform_key, load_yaml

logger = logging.getLogger(__name__)


class LoadTransformManifestError(Exception):
    pass


class TransformFailure(Exception):
    def __init__(self, returncode: int) -> None:
        super().__init__()
        self.returncode = returncode


class EnvType(Enum):
    """
    Types of environments that a transform can run in.
    """

    PYTHON = "python"
    BASH = "bash"


class InputDetails(BaseModel):
    description: str | None = None
    required: bool = True
    default: Any | None = None
    type: Type[str | int | float | bool | Any] = Any

    @model_validator(mode="before")
    @classmethod
    def set_defaults(cls, data: Any) -> Any:
        if isinstance(data, str):
            return {"description": data}
        if isinstance(data, dict):
            data = {conform_key(key): value for key, value in data.items()}
            if "optional" in data:
                if "required" in data:
                    raise ValueError("Cannot specify both `required` and `optional`")
                data["required"] = not data.pop("optional")
        return data

    @field_validator("type", mode="before")
    def valid_type(cls, value: Any) -> Type[str | int | float]:
        if value in (str, int, float, bool):
            return value
        if isinstance(value, str):
            mapping = {
                "str": str,
                "string": str,
                "int": int,
                "integer": int,
                "float": float,
                "bool": bool,
                "boolean": bool,
            }
            if type_ := mapping.get(value.lower()):
                return type_
        raise ValueError(f"Invalid input type: {value}")


class Transform(BaseModel):
    """
    A transform is a single unit of work that can be run in a pipeline. You can think of a transform as a
    mini-application that takes some input, does some work, and produces some output. The inputs are variable
    which makes transforms re-usable in different contexts.
    """

    name: str
    """
    The name of the transform. This is used to identify the transform from a job step. The name matching is
    case insensitive (`MyTransform` is equivalent to `mytransform` but not `my-transform`). The name should
    contain only:
        - alphanumeric characters
        - underscores
        - dashes
    This not (yet) enforced but is good practice to avoid issues with matching jobs to their transforms.
    """

    description: str | None = None
    """
    An optional description of the transform. This can be any string value. This is purely metadata and has no
    functional impact on the transform.
    """

    path: str
    """
    The path to the directory containing the transform. This is used as the working directory when executing
    the transform's run command.
    """

    env_type: EnvType
    """
    The type of environment that the transform will run in. This determines how the run command is executed
    and can be currently be one of the following:
        - `python`: The run command is executed as a python script.
        - `bash`: The run command is executed as a bash command.
    """

    env: dict[str, InputDetails] = {}
    """
    A dictionary of environment variable inputs for the transform's run command. The keys are the names of
    the ENV variables and the values are their text descriptions. The values will be provided by the job step
    that executes the transform. The descriptions are for documentation purposes only and serve no functional
    purpose.
    """
    # TODO: specify basic type annotations for each

    output: str | None = None
    """
    The transform's output. The value of this property is a text description of what the transform expects
    to receive as the value for its output.

    The output value is an opaque string that is specified by the Step to control where the
    output of the transform will be stored. It can also be referenced by future steps to use as their
    inputs.

    For example,
        - if the transform is a python script that outputs a CSV file, this value could be the path
          to the CSV file
        - if a transform produces multiple files, this can be a parent directory where the files are stored
        - if the output is saved to a database, this could be a connection string to that database

    The output value is passed to the transform at runtime as an environment variable named `OUTPUT`.
    """
    # TODO: what is an "output" parameter on the transform? Is it the description of what the output should be?
    # TODO: would be nice to add typing information

    run_command: str
    """
    The command to execute when running the transform. The command is executed in the transform's directory
    and is provided with the inputs and outputs as environment variables. The command depends on the `env_type`
    which has some impact on the environment in which the command is executed.

    The inputs and outputs can be accessed via environment variables. Each input and output name is converted
    to a naming convention that is compatible with environment variables. The naming convention is as follows:
        - all uppercase
        - all dashes replaced with underscores

    In addition to this, in order to avoid naming conflicts between inputs and outputs, the environment variable
    names are prefixed with `I_` for inputs and `O_` for outputs. For example, if a transform has an input named
    `my-input` and an output named `my-output`, the environment variables would be:
        - `I_MY_INPUT`
        - `O_MY_OUTPUT`
    """  # TODO: fix this to remove bit about the prefixes

    test_command: str | None = None
    """
    An optional command to execute when running the transform's tests. This is currently experimental and not
    completely implemented. The idea is to be able to build some ergonomics around being able to run all tests
    for all transforms regardless of their implementation (python, bash, rust, etc.)
    """
    # TODO: add a command for this to the CLI

    @classmethod
    def from_file(cls, path: str) -> "Transform":
        logger.info("Loading transform at: {}".format(path))
        if manifest := load_yaml(path):
            return cls(
                **{
                    **manifest,
                    "path": os.path.dirname(path),
                }
            )
        raise LoadTransformManifestError(f"Invalid app manifest at {path}")

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
        # TODO: support a list as an input for "env" and assert on the type when
        #   getting inputs from the job step
        def conform_value(value: Any) -> InputDetails:
            if isinstance(value, str):
                return InputDetails(description=value)
            if isinstance(value, dict):
                return InputDetails(**value)
            raise ValueError(f"Invalid input value: {value}")

        if isinstance(data, list):
            data = {key: "N/A" for key in data}
        return {conform_env_key(key): conform_value(value) for key, value in data.items()}

    @field_validator("env_type", mode="before")
    @classmethod
    def convert_env_type_lowercase(cls, data: Any) -> Any:
        if not isinstance(data, str):
            return data
        return data.lower()

    def execute(self, step: Step, dryrun: bool = False) -> int:
        """
        Execute the transform in the context of a given step.
        """

        if unknown_inputs := [input for input in step.env.keys() if conform_env_key(input) not in self.env]:
            raise ValueError(
                f"Invalid input for transform `{self.name}`: {', '.join(unknown_inputs)}. "
                f"Valid inputs are: {', '.join(self.env.keys())}"
            )

        inputs_env = {conform_env_key(key): str(value) for (key, value) in step.env.items()}
        command = shlex.split(self.run_command)

        if dryrun:
            logger.info("DRYRUN: Would execute with:")
            logger.info(f"  command: {command}")
            logger.info(f"  cwd: {self.path}")
            logger.info(f"  env: {', '.join(f'{k}={v}' for k,v in inputs_env.items())}")
            return 0
        else:
            # TODO check the type for each env value in the transform definition
            #   and raise if the value is invalid
            env = dict(os.environ)
            env.update(inputs_env)
            process = subprocess.Popen(
                command,
                cwd=self.path,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                shell=self.env_type == EnvType.BASH,
            )
            assert process.stdout is not None, "Process should have been opened with stdout=PIPE"
            assert process.stderr is not None, "Process should have been opened with stderr=PIPE"

            try:
                while True:
                    output = process.stdout.readline()
                    if output == "" and process.poll() is not None:
                        break
                    if output:
                        logger.info(output.strip())
                if lines := process.stderr.readlines():
                    # TODO try to figure out how to get stderr and stdout in the correct order
                    # use the thread trick with StringIO?
                    for line in lines:
                        if line:
                            logger.error(line.rstrip())
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


def discover_transforms(transforms_repo_path: str) -> dict[str, Transform]:
    """
    Walks a directory and loads all transforms found in subdirectories. Transforms are identified by the presence of a
    manifest.yml file in the directory. The manifest file must contain a `name` and `run-command` field.
    Returns a dictionary of transforms keyed by their name.
    """

    transforms_paths = [path[0] for path in os.walk(transforms_repo_path) if "manifest.yml" in path[2]]
    transforms: dict[str, Transform] = {}
    for path in transforms_paths:
        if path.endswith("/tests"):
            continue  # ignore manifests in tests directories
        if "/tests/" in path and path.split("/tests/")[0] in transforms_paths:
            continue  # ignore manifests in tests directories

        try:
            transform = Transform.from_file(f"{path}/manifest.yml")
            transforms[transform.name] = transform
        except ValidationError as e:
            logger.warning(f"Skipping transform due to validation error: {e}")
        except LoadTransformManifestError as e:
            logger.warning(f"Skipping transform due to error: {e}")

    return transforms
