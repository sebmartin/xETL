import argparse
import logging
import os
import pathlib
import re
import subprocess
import tempfile
from collections import OrderedDict
from pprint import pprint
from typing import Optional

import yaml

from metl.core.logging import LogContext, log_context

TRANSFORMS_REPO_PATH = os.path.abspath(os.path.dirname(__file__) + "/../transforms")
REQUIRED_TRANSFORM_FIELDS = {"name", "run-command"}

logger = logging.getLogger(__name__)


def load_yaml(path) -> Optional[dict]:
    with open(path, "r") as fd:
        try:
            manifest = yaml.load(fd, Loader=yaml.FullLoader)
            return manifest if isinstance(manifest, dict) else None
        except yaml.YAMLError:
            return None


def discover_transforms(transforms_repo_path):
    """
    Walks a directory and loads all transforms found in subdirectories. Transforms are identified by the presence of a
    manifest.yml file in the directory. The manifest file must contain a `name` and `run-command` field.
    Returns a dictionary of transforms keyed by their name.
    """
    transforms_paths = [path[0] for path in os.walk(transforms_repo_path) if "manifest.yml" in path[2]]
    transforms = {}
    for path in transforms_paths:
        if path.endswith("/tests"):
            continue  # ignore manifests in tests directories
        if "/tests/" in path and path.split("/tests/")[0] in transforms_paths:
            continue  # ignore manifests in tests directories

        load_manifest_at_path(path, transforms)
    return transforms


def load_manifest_at_path(path, transforms):
    manifest = load_yaml("{}/manifest.yml".format(path))

    if not manifest or REQUIRED_TRANSFORM_FIELDS - set(manifest.keys()):
        logger.warning(f"Skipping manifest due to missing required fields: {path}")
        return

    transforms[manifest["name"]] = {"path": path, "manifest": manifest}


def execute_job_steps(job_name, steps, transforms, dryrun):
    for i, step in enumerate(steps):
        with log_context(LogContext.STEP, f"Running transform: {step['transform']}"):
            if step.get("skip"):
                logger.warning(
                    "Skipping step '{}' from job '{}'".format(step.get("name", "#{}".format(i + 1)), job_name)
                )
                continue
            if "transform" in step:
                execute_transform(step, transforms, dryrun)


def execute_transform(step, transforms, dryrun):
    name = step["transform"]
    options = {name: value for (name, value) in step.items() if name != "transform"}

    assert name in transforms, "Unknown transform: {}, should be one of: {}".format(name, set(transforms.keys()))
    path = transforms[name]["path"]
    manifest = transforms[name]["manifest"]

    command = "{options} {command}".format(
        options=" ".join("{}={}".format(option.upper(), value) for (option, value) in options.items()),
        command=manifest["run-command"],
    )

    options = {option.upper(): str(value) for (option, value) in options.items()}
    command = manifest["run-command"]
    if dryrun:
        logger.info("DRYRUN: Would execute the following command:")
        logger.info(
            "  {options} cd {path} && {cmd}".format(
                options=" ".join("{}={}".format(name, value) for (name, value) in options.items()),
                path=path,
                cmd=command,
            )
        )
    else:
        if "output" in options:
            output_path = options["output"]
            if not os.path.exists(output_path):
                os.makedirs(output_path)
        env = dict(os.environ)
        env.update(options)
        command = command.split(" ")

        process = subprocess.Popen(
            command, cwd=path, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

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
        if lines := process.stderr.readlines():
            # TODO try to figure out how to get stderr and stdout in the correct order
            # use the thread trick with StringIO?
            for line in lines:
                if line:
                    logger.error(line.rstrip())
        logger.info(f"Return code: {process.returncode}")


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


def resolve_manifest_placeholders(manifest):
    assert manifest.get("data"), "App manifest does not have a 'data' path"
    manifest["data"] = os.path.abspath(manifest["data"])
    tmpdir = os.path.join(manifest["data"], "tmp")

    def variable_value(name_tuple, named_steps):
        if name_tuple == ("tmp", "dir"):
            return value.replace("$tmp.dir", temp_directory(tmpdir))
        if name_tuple == ("tmp", "file"):
            return value.replace("$tmp.file", temp_file(tmpdir))
        if name_tuple[0].lower() == "previous":
            if "previous" not in named_steps:
                raise Exception("Cannot use $previous placeholder on the first step")
            previous_step = named_steps["previous"]
            if name_tuple[1] not in previous_step:
                raise Exception(
                    'No property named "{}" defined in previous step. Possible values are: {}'.format(
                        name_tuple[1], list(previous_step.keys())
                    )
                )

        step_name, option = name_tuple
        assert step_name in named_steps, "Invalid placeholder: ${}; valid names include: {}".format(
            step_name, sorted(list(named_steps.keys()))
        )
        assert option in named_steps[step_name], "Invalid placeholder: ${}.{}; valid option names include: {}".format(
            step_name, option, sorted(list(named_steps[step_name].keys()))
        )
        return named_steps[step_name][option]

    def resolve_placeholders(value, named_steps):
        core_pattern = re.compile(r"\$({\w+}|\w+)")
        parens_pattern = re.compile(r"\${(\w+)\.(\w+)}")
        simple_pattern = re.compile(r"\$(\w+)\.(\w+)")
        pos = 0
        value = value.strip()
        while True:
            if match := parens_pattern.search(value, pos) or simple_pattern.match(value, pos):
                resolved = variable_value(match.groups(), named_steps)
            elif match := core_pattern.search(value, pos):
                key = match.groups()[0]
                if key not in manifest:
                    # It does not match a key in the root, ignore and keep intact
                    pos = match.start() + 1
                    continue
                resolved = manifest[match.groups()[0]]
            else:
                break

            value = value[: match.start()] + resolved + value[match.end() :]
            pos = match.start()
        return value

    for _, steps in manifest["jobs"].items():
        named_steps = OrderedDict({})
        for step in steps:
            for name, value in step.items():
                if not isinstance(value, str):
                    continue
                if "$" in value:
                    value = resolve_placeholders(value, named_steps)
                    step[name] = value
                if value.startswith("~/"):
                    # assume it's a path and expand it
                    step[name] = str(pathlib.PosixPath(value).expanduser())
            if "name" in step:
                named_steps[step["name"]] = step
            named_steps["previous"] = step


def run_app(manifest: str, skip_to: str | None = None, dryrun=False, transforms_repo_path=None):
    logger.info("Parsing app manifest")
    if isinstance(manifest, str):
        # assume a path to a YAML file
        logger.info(" ┖ Loading YAML at: {}".format(manifest))
        manifest = load_yaml(manifest) or {}
    resolve_manifest_placeholders(manifest)
    if dryrun:
        logger.info("Manifest parsed as:")
        pprint(manifest, width=140)

    logger.info("Parsed manifest for app: {}".format(manifest["name"]))
    logger.info("Discovering steps...")
    transforms_repo_path = transforms_repo_path or TRANSFORMS_REPO_PATH
    transforms = discover_transforms(transforms_repo_path)

    if not transforms:
        logger.error("Could not find any transforms at {}".format(transforms_repo_path))
        return

    if dryrun:
        logger.info("Available transforms detected:")
        pprint(transforms, width=140)

    for job_name, steps in manifest["jobs"].items():
        with log_context(LogContext.JOB, f"Running job: {job_name}"):
            if skip_to:
                if job_name != skip_to and f"{job_name}." not in skip_to:
                    logger.warning("Skipping this job...")
                    continue

                if "." in skip_to:
                    while steps:
                        if skip_to.endswith(f'.{steps[0]["transform"]}'):
                            break
                        logger.warning(f'Skipping step: {steps[0]["transform"]}')
                        del steps[0]
                skip_to = None

            execute_job_steps(job_name, steps, transforms, dryrun)

    logger.info("Done! \o/")


def main():
    parser = argparse.ArgumentParser("App runner")
    parser.add_argument("manifest", help="Path to app manifest YAML file")
    parser.add_argument("skip-to", default=None, help="Name of job (and optionally step) to skip to")
    parser.add_argument("--dryrun", action="store_true", help="Print the transform commands instead of executing them")
    args = parser.parse_args()

    manifest_path = os.path.abspath(args.manifest)
    if not os.path.exists(manifest_path):
        print("File does not exist: {}".format(manifest_path))
        exit(code=1)

    with log_context(LogContext.APP, "Running app: {}".format(manifest_path)):
        run_app(manifest_path, skip_to=args.skip_to, dryrun=args.dryrun)


if __name__ == "__main__":
    main()
