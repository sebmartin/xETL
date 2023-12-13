import logging
import os
import re
import tempfile
from collections import OrderedDict
from typing import Any, Iterable

import yaml
from pydantic import BaseModel, field_validator, model_validator
from metl.models.step import ArgumentType, Step

from metl.models.utils import parse_yaml, parse_yaml_file

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

    env: dict[str, ArgumentType] = {}  # TODO: implement and test
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

    @model_validator(mode="after")
    def resolve_placeholders(self) -> "App":
        resolve_placeholders(self)
        return self

    @field_validator("transforms", mode="before")
    @classmethod
    def validate_transforms(cls, value: Any):
        if isinstance(value, str):
            value = [value]
        elif not isinstance(value, list):
            raise ValueError("transforms must be a string or a list of strings")
        return value


# Validation


def resolve_placeholders(app: App):
    def temp_directory(root):
        if not os.path.exists(root):
            os.makedirs(root)
        return tempfile.mkdtemp("__", dir=root)

    def temp_file(root):
        if not os.path.exists(root):
            os.makedirs(root)
        fd, path = tempfile.mkstemp("__", dir=root)
        os.close(fd)
        return path

    def get_key_value(obj: BaseModel | dict, keys: list[str], match: str):
        def get(obj, key):
            key = key.lower()
            if isinstance(obj, dict):
                return obj.get(key, obj.get(key.upper()))
            return getattr(obj, key, None)

        if (value := get(obj, keys[0])) is None:
            valid_keys = ", ".join(
                sorted(
                    f"`{key}`" for key in (list(obj.keys()) if isinstance(obj, dict) else list(obj.model_dump().keys()))
                )
            )
            raise ValueError(f"Invalid placeholder `{keys[0]}` in {match}. Valid keys are: {valid_keys}")
        if len(keys) == 1:
            if isinstance(value, BaseModel | dict):
                raise ValueError(f"Incomplete key path, variable must reference a leaf value: {match}")
            return value
        return get_key_value(value, keys[1:], match)

    def variable_value(names: Iterable[str], named_steps: dict[str, Step], match: str):
        # Make case insensitive
        names = [n.lower() for n in names]

        # Check for properties on the app
        if len(names) == 1:
            return get_key_value(app, names, match)

        # Check for $tmp variables
        tmpdir = os.path.join(app.data, "tmp")
        if tuple(names) == ("tmp", "dir"):
            return temp_directory(tmpdir)
        elif tuple(names) == ("tmp", "file"):
            return temp_file(tmpdir)

        # Check for `previous`
        if names[0] == "previous" and "previous" not in named_steps:
            raise ValueError("Cannot use $previous placeholder on the first step")

        # Resolve from previous steps
        if step := named_steps.get(names[0]):
            return get_key_value(step, names[1:], match)
        else:
            valid_keys = ", ".join(sorted(f"`{key}`" for key in named_steps.keys()))
            raise ValueError(
                f"Invalid placeholder `{names[0]}` in {match}."
                + (f" Valid keys are: {valid_keys}" if valid_keys else " There are no steps to reference.")
            )

    def resolve(string: str, named_steps: dict[str, Step]):
        # First, find literal dollar signs ($$)
        literal_pattern = re.compile(r"\$\$")
        literals = set()
        pos = 0
        while match := literal_pattern.search(string, pos):
            string = string[: match.start()] + "$" + string[match.end() :]
            literals.add(match.start())
            pos = match.start() + 1

        # Second, find placeholders in `string`` and replace them with their variable values
        # but skip matches that start where we found literals
        #        w/ curly braces <━┳━━━━━━━━━━━━━━━━━┓      ┏━━━━┳━> w/o curly braces
        pattern = re.compile(r"(?:\${(\w+(?:[.]\w+)*)})|(?:\$(\w+))")
        pos = 0
        while match := pattern.search(string, pos):
            if match.start() in literals:
                pos = match.start() + 1
                continue
            matched_names = match[1] or match[2]
            names = matched_names.split(".")
            if resolved := variable_value(names, named_steps, match[0]):
                string = string[: match.start()] + resolved + string[match.end() :]
                pos = match.start() + len(resolved)
            else:
                # TODO: we always raise now if a match does not have a value so we
                #   should never hit this line
                pos = match.start() + 1
        return string

    def traverse_object(model: BaseModel | dict, named_steps: dict[str, Step]):
        items = model.items() if isinstance(model, dict) else model.model_dump().items()
        for key, value in items:
            if isinstance(value, BaseModel | dict):
                traverse_object(value, named_steps)
            elif isinstance(value, str):
                if "$" in value:
                    value = resolve(value, named_steps)
                if value.startswith("~/"):
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
            traverse_object(step, named_steps)
            if step.name:
                named_steps[step.name] = step
            named_steps["previous"] = step
