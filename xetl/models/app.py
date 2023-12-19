import logging
import os
import re
import tempfile
from collections import OrderedDict
from typing import Any, Iterable

from pydantic import BaseModel, field_validator, model_validator

from xetl.models import EnvKeyLookupErrors, EnvVariableType
from xetl.models.step import Step
from xetl.models.utils.dicts import fuzzy_lookup
from xetl.models.utils.io import parse_yaml, parse_yaml_file

logger = logging.getLogger(__name__)


class App(BaseModel):
    """
    The app is the top level model that defines the structure of the pipeline. An app is composed of jobs,
    which are composed of steps. Each step executes a transform with arguments that are defined in each step
    and passed to the transform at runtime.
    """

    name: str
    """A name for the app used in logging and other places to identify the app. This can be any string value."""

    description: str | None = None
    """
    An optional description of the app. This can be any string value. This is purely metadata and has no
    functional impact on the app.
    """

    data: str
    """
    The root directory where the app will store its data. If the directory does not exist, it will be created.
    This value can be referenced in steps using the `$data` placeholder.

    e.g.
    ```
        jobs:
          some-job:
            - transform: my-transform
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
    A dictionary of environment variables that can be referenced in job steps. This can be useful for
    declaring values that are used in multiple steps such as database connection strings.

    These values will be available as env variables when running the transform for each step in the app
    without having to be redefined in each step's env. However, the step's env always takes precedence so
    it's possible to override the app's env value by specifying a different value in a step's env.
    """

    transforms: list[str] = []
    """
    A path or list of paths containing the transforms that are used in the app. Paths can be an absolute
    path or a path relative to the app manifest file. If the directory does not exist,
    """

    jobs: dict[str, list["Step"]]
    """
    A dictionary of jobs. Each job is a list of steps. Each step executes a transform with env variables
    that are defined in each step and passed to the transform at runtime.
    """

    @classmethod
    def from_file(cls, path: str) -> "App":
        logger.info("Loading app manifest at: {}".format(path))
        return cls(**parse_yaml_file(path))

    @classmethod
    def from_yaml(cls, yaml_content: str) -> "App":
        return cls(**parse_yaml(yaml_content))

    @model_validator(mode="before")
    @classmethod
    def load_host_env(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data  # TODO: test this
        if host_env := data.get("host-env", data.get("host_env")):
            data["host_env"] = [host_env] if isinstance(host_env, str) else host_env
        return data

    @model_validator(mode="after")
    def resolve_placeholders(self) -> "App":
        inherit_env(self)
        propagate_env(self)
        resolve_placeholders(self)
        return self

    @field_validator("transforms", mode="before")
    @classmethod
    def validate_transforms(cls, value: Any):
        if isinstance(value, str):
            value = [value]
        return value


# Validation


def inherit_env(app: App):
    base_env = {}
    host_env = app.host_env or []
    if "*" in host_env:
        if len(host_env) > 1:
            logger.warning(
                "The `*` value in `host-env` was specified alongside other values. "
                "All host environment variables will be inherited."
            )
        base_env = dict(os.environ)
    else:
        base_env = {key: os.environ.get(key) for key in host_env}
        if missing_keys := set(host_env) - set(base_env.keys()):
            # TODO: test this
            logger.warning(
                "The following host environment variables were not found: {}".format(", ".join(missing_keys))
            )
    base_env = {**base_env, **app.env}
    app.env = base_env


def propagate_env(app: App):
    if not app.env:
        return
    for steps in app.jobs.values():
        for step in steps:
            step.env = {**app.env, **step.env}


def resolve_placeholders(app: App):
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
        names: Iterable[str], current_step: Step, named_steps: dict[str, Step], match: str
    ) -> EnvVariableType:
        # Make case insensitive
        names = [n.lower() for n in names]

        # Check for the `data` variable
        if names == ["data"]:
            return app.data

        # Check for $tmp variables
        tmpdir = os.path.join(app.data, "tmp")
        if tuple(names) == ("tmp", "dir"):
            return temp_directory(tmpdir)
        elif tuple(names) == ("tmp", "file"):
            return temp_file(tmpdir)

        # Check current step's env
        try:
            return get_key_value(current_step.env, names, match)
        except EnvKeyLookupErrors:
            pass

        # Check for `previous`
        if names[0] == "previous" and "previous" not in named_steps:
            raise ValueError("Cannot use $previous placeholder on the first step")

        # Resolve from previous step's env
        if step := fuzzy_lookup(named_steps, names[0]):
            if isinstance(step, Step):
                return get_key_value(step.env, names[1:], match)

        # Could not resolve
        env_keys = ", ".join(sorted(current_step.env.keys()))
        step_keys = ", ".join(sorted(named_steps.keys()))
        raise ValueError(
            f"Invalid name `{names[0]}` in `{match}`. The first must be one of:\n"
            f" - variable in the current step's env: {env_keys or 'No env variables defined'}\n"
            f" - name of a previous step: {step_keys or 'No previous steps defined'}"
        )

    def resolve(string: str, current_step: Step, named_steps: dict[str, Step]) -> EnvVariableType:
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

            resolved = variable_value(names, current_step, named_steps, match[0])
            if match.span() == (0, len(string)):
                # We matched the entire string, retain the original type (e.g. int, float, etc)
                return resolved
            # Match is embedded in the string
            resolved = "null" if resolved is None else str(resolved)
            string_length_delta += len(resolved) - len(match[0])
            string = string[: match.start()] + resolved + string[match.end() :]
            pos = match.start() + len(resolved)
        return string

    def traverse_object(model: BaseModel | dict, current_step: Step, named_steps: dict[str, Step]):
        """
        Look for placeholders (e.g. $previous.OUTPUT) in property (models) or dict values and resolve them.
        """
        items = model.items() if isinstance(model, dict) else model.model_dump().items()
        for key, value in items:
            if isinstance(value, BaseModel | dict):
                traverse_object(value, current_step, named_steps)
            elif isinstance(value, str):
                if "$" in value:
                    value = resolve(value, current_step, named_steps)
                if isinstance(value, str) and value.startswith("~/"):
                    # assume it's a path and expand it
                    value = os.path.expanduser(value)
            else:
                continue

            if isinstance(model, dict):
                model[key] = value
            elif isinstance(model, BaseModel):
                setattr(model, key, value)

    # Resolve all app placeholders
    app.data = os.path.abspath(app.data)
    for steps in app.jobs.values():
        named_steps = OrderedDict({})
        for step in steps:
            traverse_object(step, step, named_steps)
            if step.name:
                named_steps[step.name] = step
            named_steps["previous"] = step
