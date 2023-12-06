import argparse
import logging
import os
from metl.logging import LogContext, log_context
from metl.models.transform import TransformFailure
from metl.engine import execute_app

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser("mETL")
    parser.add_argument("manifest", help="Path to app manifest YAML file")
    parser.add_argument("--skip-to", default=None, help="Name of job (and optionally step) to skip to")
    parser.add_argument("--dryrun", action="store_true", help="Print the transform commands instead of executing them")
    args = parser.parse_args()

    manifest_path = os.path.abspath(args.manifest)
    if not os.path.exists(manifest_path):
        print("File does not exist: {}".format(manifest_path))
        exit(code=1)

    try:
        with log_context(LogContext.APP, "Executing app: {}".format(manifest_path)):
            execute_app(manifest_path, skip_to=args.skip_to, dryrun=args.dryrun)
    except TransformFailure as e:
        logger.fatal("Transform failed, terminating job.")
        exit(code=e.returncode)


if __name__ == "__main__":
    main()
