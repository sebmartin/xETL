from enum import Enum
import logging
import os
import shlex
import subprocess
from typing import Any
from pydantic import BaseModel, ValidationError, model_validator
from metl.core.models.app import Step

from metl.core.models.utils import load_yaml

logger = logging.getLogger(__name__)


class LoadTransformManifestError(Exception):
    pass


class EnvType(Enum):
    """
    Types of environments that a transform can run in.
    """

    PYTHON = "python"
    BASH = "bash"


class Transform(BaseModel):
    """
    A transform is a single unit of work that can be run in a pipeline.
    """

    name: str
    path: str
    env_type: EnvType
    options: dict[str, str] = {}
    run_command: str
    test_command: str
    description: str | None = None

    @classmethod
    def from_file(cls, path: str) -> "Transform":
        logger.info("Loading transform manifest at: {}".format(path))
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
    def convert_to_underscores(cls, data: Any) -> Any:
        """Converts keys in incoming dictionary to underscores instead of hyphens."""
        return {key.replace("-", "_"): value for key, value in data.items()}

    def execute(self, step: Step, dryrun: bool = False):
        """
        Execute the transform in the context of a given step.
        """

        if unknown_args := [
            option for option in step.args.keys() if option.lower().replace("_", "-") not in self.options
        ]:
            raise ValueError(
                f"Invalid arguments for transform `{self.name}`: {', '.join(unknown_args)}. "
                f"Valid arguments are: {', '.join(self.options)}"
            )

        args = {option.upper().replace("-", "_"): str(value) for (option, value) in step.args.items()}
        command = shlex.split(self.run_command)

        if dryrun:
            logger.info("DRYRUN: Would execute with:")
            logger.info(f"  command: {command}")
            logger.info(f"  cwd: {self.path}")
            logger.info(f"  args: {args}")
        else:
            # TODO: feature: support more than a directory as the output (e.g. direct to postgres?, etc.)
            if not os.path.exists(step.output):
                os.makedirs(step.output)

            env = dict(os.environ)
            env.update(args)
            process = subprocess.Popen(
                command,
                cwd=self.path,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                shell=self.env_type == EnvType.BASH,
            )

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
            logger.info(f"Return code: {process.returncode}")


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

        load_transform_at_path(path, transforms)
    return transforms


def load_transform_at_path(path: str, transforms: dict[str, Transform]):
    try:
        transform = Transform.from_file(f"{path}/manifest.yml")
    except ValidationError as e:
        # TODO: test this
        logger.warning(f"Skipping transform due to validation error: {e}")
        return
    except LoadTransformManifestError as e:
        # TODO: test this
        logger.warning(f"Skipping transform due to error: {e}")
        return

    transforms[transform.name] = transform
