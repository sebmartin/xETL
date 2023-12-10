from enum import Enum
import logging
import os
import shlex
import subprocess
from typing import Any, Type
from pydantic import BaseModel, ValidationError, field_validator, model_validator
import yaml
from metl.models.step import Step

from metl.models.utils import (
    InvalidManifestError,
    ManifestLoadError,
    conform_env_key,
    conform_key,
    load_file,
    parse_yaml,
)

logger = logging.getLogger(__name__)


class UnknownTransformError(Exception):
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

    @staticmethod
    def _yaml_representer(dumper: yaml.Dumper, data):
        return dumper.represent_str(data.value)


yaml.add_representer(EnvType, EnvType._yaml_representer)


class InputDetails(BaseModel):
    description: str | None = None
    required: bool = True
    default: Any | None = None
    type: Type[str | int | float | bool | Any] = Any

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
                "decimal": float,
                "bool": bool,
                "boolean": bool,
            }
            if type_ := mapping.get(value.lower()):
                return type_
        raise ValueError(f"Invalid input type: {value}")  # TODO: test this


class Transform(BaseModel):
    """
    A transform is a single unit of work that can be executed in an app. You can think of a transform as a
    mini-application that accepts inputs in the form of ENV variables, does some work, and produces some
    output. These inputs are variable which makes transforms re-usable in different contexts.
    """

    name: str
    """
    The name of the transform. This is used to identify the transform from a job step. The name matching is
    case insensitive (`MyTransform` is equivalent to `mytransform` but not `my-transform`). The name should
    contain only:
        - alphanumeric characters
        - underscores
        - dashes
    This is not (yet) enforced but is good practice to avoid issues with matching jobs to their transforms.
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
    and can currently accept one of the following values:
        - `python`: The run command is executed as a python script.
        - `bash`: The run command is executed as a bash command.
    """

    env: dict[str, InputDetails] = {}
    """
    A dictionary of environment variable inputs for the transform's run command. This instructs the runtime
    engine which environment variables to pass to the transform when executing it. When the transform is
    executed, the runtime engine will pass the values of these environment variables (provided by the job
    step) to the transform as environment variables. The transform can then use these values to control its
    behaviour.

    In its simplest form, the keys are the names of the ENV variables and the values are a text description
    for each variable. The descriptions are purely metadata and have no functional impact on the transform.

    e.g.
    ```
        env:
            MY_INPUT: A value that is use as the input for the transform
            MY_OUTPUT: A path where the transform will save its output
    ```

    It's also possible to specify additional details for each input. The following details can be specified:
        - `description`: A text description of the input. This is purely metadata and has no functional
            impact on the transform.
        - `required`: A boolean value indicating whether the input is required. If `True`, the runtime engine
            will raise an error if the input is not provided by the job step. If `False`, the runtime engine
            will not raise an error if the input is not provided by the job step and the ENV variable will not
            be set when executing the transform.
        - `optional`: A boolean value indicating whether the input is optional. This is just a convenience
            alias for `required` and is mutually exclusive with `required`.
        - `default`: The default value to use for the input if it is not provided by the job step. This value
            is only used if the input is not required and is not provided by the job step. An exception is
            raised if a default value is specified for a required input.
        - `type`: The data type of the input. This is used to validate the value of the input provided by the job
            step (at runtime). If the value provided by the job step is not of the specified type, an exception
            is raised. The following types are supported:
                - `string`: A string value (also accepts `str`)
                - `integer`: An integer value (also accepts `int`)
                - `decimal`: A floating point value (also accepts `float`)
                - `boolean`: A boolean value (also accepts `bool`)

            e.g.
            ```
                env:
                    MY_INPUT:
                        description: A value that is use as the input for the transform
                        required: true
                        type: string
                    MY_OUTPUT:
                        description: A path where the transform will save its output
                        required: true
                        type: string
            ```
    """  # TODO: implement and test

    run_command: str
    """
    The command to execute when running the transform. The command is executed in with the "working directory"
    set to the value in the `path` property (typically the directory containing the transform's manifest.yml file)
    and executes with the ENV variables set (see the `env` property). The command depends on the `env_type`
    which has some impact on the environment in which the command is executed.

    The inputs are accessed via environment variables. Each input name is converted to a naming convention that
    is compatible with environment variables. The naming convention is as follows:
        - all uppercase
        - all dashes replaced with underscores

    Any `env` name that does not follow this convention will be converted.
    """

    test_command: str | None = None
    """
    An optional command to execute when running the transform's tests. This is currently experimental and not
    completely implemented. The idea is to be able to build some ergonomics around being able to run all tests
    for all transforms regardless of their implementation (python, bash, rust, etc.)
    """  # TODO: add a command for this to the CLI

    @classmethod
    def from_file(cls, path: str) -> "Transform":
        logger.info(f"Loading transform at: {path}")
        yaml_content = load_file(path)
        try:
            return cls.from_yaml(yaml_content, path=os.path.dirname(path))
        except Exception as e:
            raise ManifestLoadError(f"Could not load YAML file at path: {path}") from e

    @classmethod
    def from_yaml(cls, yaml_content: str, path: str) -> "Transform":
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
                    f"Transform env names must be strings, the following are invalid: {', '.join(invalid_keys)}"
                )
            data = {key: "N/A" for key in data}
        return {conform_env_key(key): conform_value(value) for key, value in data.items()}

    @field_validator("env_type", mode="before")
    @classmethod
    def convert_env_type_lowercase(cls, data: Any) -> Any:
        if isinstance(data, str):
            return data.lower()
        return data

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
        except (ManifestLoadError, InvalidManifestError) as e:
            logger.warning(f"Skipping transform due to error: {str(e)}")
        except Exception as e:
            logger.warning(f"Skipping transform due to unexpected error: {e}")

    return transforms
