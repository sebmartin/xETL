import argparse
import logging
import os
from pprint import pprint

from metl.core.logging import LogContext, log_context
from metl.core.models.app import App, Step
from metl.core.models.transform import Transform, discover_transforms

TRANSFORMS_REPO_PATH = os.path.abspath(os.path.dirname(__file__) + "/../transforms")

logger = logging.getLogger(__name__)


# TODO: move to models.app?
def run_app(manifest_path: str, skip_to: str | None = None, dryrun=False, transforms_repo_path=None):
    app = App.from_file(manifest_path)
    if dryrun:
        logger.info("Manifest parsed as:")
        pprint(app, width=140)

    logger.info("Parsed manifest for app: {}".format(app.name))
    logger.info("Discovering steps...")
    transforms_repo_path = transforms_repo_path or TRANSFORMS_REPO_PATH
    transforms = discover_transforms(transforms_repo_path)

    if not transforms:
        logger.error("Could not find any transforms at {}".format(transforms_repo_path))
        return

    if dryrun:
        logger.info("Available transforms detected:")
        pprint(transforms, width=140)

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

    logger.info("Done! \o/")


def execute_job_steps(job_name: str, steps: list[Step], transforms: dict[str, Transform], dryrun: bool):
    for i, step in enumerate(steps):
        with log_context(LogContext.STEP, f"Running transform: {step.transform}"):
            if step.skip:
                logger.warning(f"Skipping step `{step.name or f'#{i + 1}'}` from job '{job_name}'")
                continue
            execute_transform(step, transforms, dryrun)


def execute_transform(step: Step, transforms: dict[str, Transform], dryrun):
    name = step.transform

    # TODO: add test for this this --v
    assert name in transforms, "Unknown transform: {}, should be one of: {}".format(name, set(transforms.keys()))
    transforms[name].execute(step, dryrun)


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
