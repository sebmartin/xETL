import argparse
import logging
from os.path import abspath, exists

from xetl.engine import execute_job
from xetl.models.task import TaskFailure

logger = logging.getLogger(__name__)


def argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser("Let's Go!")
    parser.add_argument(
        "manifest",
        help="Path to job manifest YAML file. Relative paths are resolved relative to the current working directory.",
    )
    parser.add_argument(
        "-c",
        "--commands",
        default=None,
        help="Comma-separated list of commands to execute. Commands will be executed in the order defined by the job, regardless of the order in this list.",
    )
    parser.add_argument("--dryrun", action="store_true", help="Print the task details instead of executing them")
    return parser


def main():
    args = argument_parser().parse_args()

    manifest_path = abspath(args.manifest)
    if not exists(manifest_path):
        print("Job manifest file does not exist: {}".format(manifest_path))
        exit(code=1)

    try:
        execute_job(manifest_path, commands=args.commands, dryrun=args.dryrun)
    except TaskFailure as e:
        logger.fatal("Task failed, terminating job.")
        exit(code=e.returncode)


if __name__ == "__main__":
    main()
