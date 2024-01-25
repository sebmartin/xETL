import argparse
import logging
from os.path import abspath, exists

from xetl.engine import execute_job
from xetl.logging import LogStyle, configure_logging
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
    parser.add_argument(
        "-l",
        "--log-style",
        default="gaudy",
        choices=[1, 2, 3, "minimal", "moderate", "gaudy"],
        help="Sets the amount to decoration to add around logs from 1 (minimal) to 3 (gaudy).",
    )
    parser.add_argument(
        "-t",
        "--no-timestamps",
        action="store_true",
        help="Sets the amount to decoration to add around logs from 1 (minimal) to 3 (gaudy).",
    )
    parser.add_argument("--dryrun", action="store_true", help="Print the task details instead of executing them")
    return parser


if __name__ == "__main__":
    args = argument_parser().parse_args()

    match args.log_style:
        case "minimal" | 1:
            log_style = LogStyle.MINIMAL
        case "moderate" | 2:
            log_style = LogStyle.MODERATE
        case _:
            log_style = LogStyle.GAUDY
    configure_logging(root_logger=logging.getLogger(), style=log_style, timestamps=not args.no_timestamps)

    manifest_path = abspath(args.manifest)
    if not exists(manifest_path):
        print("Job manifest file does not exist: {}".format(manifest_path))
        exit(code=1)

    try:
        execute_job(manifest_path, commands=args.commands, dryrun=args.dryrun)
    except TaskFailure as e:
        logger.fatal("Task failed, terminating job.")
        exit(code=e.returncode)
