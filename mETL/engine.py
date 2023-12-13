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


class NoAliasDumper(yaml.SafeDumper):
    def ignore_aliases(self, data):
        return True


def execute_app(manifest_path: str, skip_to: str | None = None, dryrun=False):
    app = App.from_file(manifest_path)
    with log_context(LogContext.APP, "Executing app: {}".format(app.name)):
        if dryrun:
            logger.info("Manifest parsed as:")
            for line in (
                yaml.dump(
                    app.model_dump(exclude_unset=True),
                    Dumper=NoAliasDumper,
                    sort_keys=False,
                )
                .strip()
                .split("\n")
            ):
                logger.info("  " + line)
        else:
            logger.info("Parsed manifest for app: {}".format(app.name))

        if transforms_repo_paths := app.transforms:
            logger.info(f"Discovering transforms at paths: {transforms_repo_paths}")
            transforms = discover_transforms(transforms_repo_paths)
            if not transforms:
                # TODO: test this
                logger.error("Could not find any transforms at {}".format(transforms_repo_paths))
                return
        else:
            logger.warning(
                "The property `transforms` is not defined in the app manifest, no transforms will be available"
            )
            transforms = {}
        logger.info(f"Available transforms detected:")
        for t in transforms.values():
            logger.info(f" - {t.name}")

        for job_name, steps in app.jobs.items():
            with log_context(LogContext.JOB, f"Executing job: {job_name}"):
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
        logger.info(f"Executing step {f'{i + 1}'} of {len(steps)}")
        for line in yaml.dump(step.model_dump(), indent=2, sort_keys=False).strip().split("\n"):
            logger.info("  " + line)
        with log_context(LogContext.STEP, f"Executing transform: {step.transform}") as tail:
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
