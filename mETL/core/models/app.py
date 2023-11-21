import logging
import os
import re
import tempfile
from collections import OrderedDict
from typing import Any, Iterable

import yaml
from pydantic import BaseModel, model_validator

from metl.core.models.utils import load_yaml

logger = logging.getLogger(__name__)

ArgumentType = str | int | float | bool


class LoadAppManifestError(Exception):
    pass


class App(BaseModel):
    name: str
    data: str
    jobs: dict[str, list["Step"]]
    description: str | None = None

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
    name: str | None = None
    description: str | None = None
    transform: str
    args: dict[str, ArgumentType] = {}
    output: str
    skip: bool = False


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

    def get_key_value(obj: object | dict, keys: list[str], match: str):
        def get(obj, key):
            if isinstance(obj, dict):
                return obj.get(key)
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
