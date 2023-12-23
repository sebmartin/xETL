import argparse
import logging
import os
from xetl.models.command import CommandFailure
from xetl.engine import execute_job

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser("Let's Go!")
    parser.add_argument("manifest", help="Path to job manifest YAML file")
    parser.add_argument("--skip-to", default=None, help="Name of job (and optionally task) to skip to")
    parser.add_argument("--dryrun", action="store_true", help="Print the command details instead of executing them")
    args = parser.parse_args()

    manifest_path = os.path.abspath(args.manifest)
    if not os.path.exists(manifest_path):
        print("File does not exist: {}".format(manifest_path))
        exit(code=1)

    try:
        execute_job(manifest_path, skip_to=args.skip_to, dryrun=args.dryrun)
    except CommandFailure as e:
        logger.fatal("Command failed, terminating job.")
        exit(code=e.returncode)


if __name__ == "__main__":
    main()
