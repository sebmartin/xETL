import logging
import os
import re
import tempfile
from collections import OrderedDict
from typing import Iterable
from traitlets import Any

import yaml
from pydantic import BaseModel, field_validator, model_validator

from metl.core.models.utils import conform_env_key, load_yaml

logger = logging.getLogger(__name__)

ArgumentType = str | int | float | bool


class LoadAppManifestError(Exception):
    pass


class App(BaseModel):
    """
    The app is the top level object that defines the structure of the pipeline. An app is composed of jobs,
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
    """The root directory where the app will store its data. If the directory does not exist, it will be created."""

    vars: dict[str, ArgumentType] = {}  # TODO: implement and test
    """
    A dictionary of variables that can be referenced in job steps. This can be useful for declaring values
    that are used in multiple steps such as database connection strings.
    """

    jobs: dict[str, list["Step"]]
    """
    A dictionary of jobs. Each job is a list of steps. Each step executes a transform with arguments that are
    defined in each step and passed to the transform at runtime.
    """

    @classmethod
    def from_file(cls, path: str) -> "App":
        logger.info("Loading app manifest at: {}".format(path))
        if manifest := load_yaml(path):
            return cls(**manifest)
        raise LoadAppManifestError(f"Invalid app manifest at {path}")

    @classmethod
    def from_yaml(cls, yaml_content: str) -> "App":
        manifest = yaml.load(yaml_content, Loader=yaml.FullLoader)
        return cls(**manifest)

    @model_validator(mode="after")
    def resolve_placeholders(self) -> "App":
        resolve_placeholders(self)
        return self


class Step(BaseModel):
    """
    A step is an instruction for executing a transform. An app is composed of jobs, which are composed of
    one or more steps. Each step defines the input and output parameters for executing a single transform.
    """

    name: str | None = None
    """An optional name for the step. This can be any string value."""

    description: str | None = None
    """An optional description of the step. This can be any string value."""

    transform: str
    """
    The name of the transform to execute. The transformed needs to be discovered by the runner in order
    to be referenced by name and be found. See the `metl.core.models.transform.discover_transforms` function
    for more information on the transform discovery process.
    """

    env: dict[str, ArgumentType] = {}
    """
    Set of ENV variables that will be set when executing the transform. The keys in this dictionary must
    match the names of the `env` keys defined (and described) in the transform's manifest.
    """

    skip: bool = False
    """
    If `True`, the step will be skipped when the app is run. This can be useful for temporarily disabling
    steps during development without removing them from the app. It's akin to commenting out the step
    however the variable resolution will still occur (e.g. future steps can still reference this step's
    values). # TODO: add a test to confirm this statement
    """

    @field_validator("env", mode="before")
    @classmethod
    def conform_env_keys(cls, value: Any):
        # TODO: add a test for this
        if not isinstance(value, dict):
            return value
        return {conform_env_key(key): value for key, value in value.items()}


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
            # TODO: should this raise if key is not found?
            valid_keys = ", ".join(
                sorted(
                    f"`{key}`" for key in (list(obj.keys()) if isinstance(obj, dict) else list(obj.model_dump().keys()))
                )
            )
            raise Exception(f"Invalid placeholder `{keys[0]}` in {match}. Valid keys are: {valid_keys}")
        if len(keys) == 1:
            if isinstance(value, BaseModel | dict):
                raise Exception("Incomplete key path")  # TODO improve
            return value
        return get_key_value(value, keys[1:], match)

    def variable_value(names: Iterable[str], named_steps: dict[str, Step], match: str):
        # Make case insensitive
        names = [n.lower() for n in names]

        # Check for properties on the app
        if len(names) == 1:
            return getattr(app, names[0], None)

        # Check for $tmp variables
        tmpdir = os.path.join(app.data, "tmp")
        if tuple(names) == ("tmp", "dir"):
            return temp_directory(tmpdir)
        elif tuple(names) == ("tmp", "file"):
            return temp_file(tmpdir)

        # Check for `previous`
        if names[0] == "previous" and "previous" not in named_steps:
            raise Exception("Cannot use $previous placeholder on the first step")

        # Resolve from previous steps
        if step := named_steps.get(names[0]):
            return get_key_value(step, names[1:], match)
        else:
            # TODO: define an exception for this, and set that exception in the test
            valid_keys = ", ".join(sorted(f"`{key}`" for key in named_steps.keys()))
            raise Exception(
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
