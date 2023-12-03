import logging
import os
from pprint import pprint

import yaml

from metl.logging import LogContext, log_context
from metl.models.app import App
from metl.models.step import Step
from metl.models.transform import Transform, TransformFailure, UnknownTransformError, discover_transforms

TRANSFORMS_REPO_PATH = os.path.abspath(os.path.dirname(__file__) + "/transforms")

logger = logging.getLogger(__name__)


# TODO: move to models.app?
def run_app(manifest_path: str, skip_to: str | None = None, dryrun=False):
    app = App.from_file(manifest_path)
    if dryrun:
        logger.info("Manifest parsed as:")
        pprint(app, width=140)  # TODO: this doesn't get logged
    else:
        logger.info("Parsed manifest for app: {}".format(app.name))

    if transforms_repo_path := app.transforms_path:
        logger.info(f"Discovering transforms at: {transforms_repo_path}")
        transforms = discover_transforms(transforms_repo_path)
        if not transforms:
            logger.error("Could not find any transforms at {}".format(transforms_repo_path))
            return
    else:
        logger.warning(
            "The property `transforms_path` is not defined in the app manifest, no transforms will be available"
        )
        transforms = {}

    if dryrun:
        logger.info("Available transforms detected:")
        pprint(transforms, width=140)  # TODO: this doesn't get logged

    for job_name, steps in app.jobs.items():
        with log_context(LogContext.JOB, f"Running job: {job_name}"):
            if skip_to:
                if job_name != skip_to and f"{job_name}." not in skip_to:
                    logger.warning("Skipping this job...")
                    continue

                if "." in skip_to:
                    while steps:
                        if skip_to.endswith(f".{steps[0].name}"):
                            break
                        logger.warning(f"Skipping step: {steps[0].name or steps[0].transform}")
                        del steps[0]
                skip_to = None

            execute_job_steps(job_name, steps, transforms, dryrun)

    logger.info("Done! \\o/")


def execute_job_steps(job_name: str, steps: list[Step], transforms: dict[str, Transform], dryrun: bool):
    for i, step in enumerate(steps):
        if i > 0:
            logger.info("")
        logger.info(f"Running step: {f'#{i + 1}'}")
        for line in yaml.dump(step.model_dump(), indent=2, sort_keys=False).strip().split("\n"):
            logger.info("  " + line)
        with log_context(LogContext.STEP, f"Running transform: {step.transform}") as tail:
            if step.skip:
                logger.warning(f"Skipping step `{step.name or f'#{i + 1}'}` from job '{job_name}'")
                continue
            returncode = execute_job_step(step, transforms, dryrun)
            tail(f"Return code: {returncode}")

        if returncode != 0:
            raise TransformFailure(returncode=returncode)


def execute_job_step(step: Step, transforms: dict[str, Transform], dryrun) -> int:
    name = step.transform

    if transform := transforms.get(name):
        return transform.execute(step, dryrun)
    else:
        raise UnknownTransformError(f"Unknown transform `{name}`, should be one of: {sorted(transforms.keys())}")
