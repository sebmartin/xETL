import argparse
import logging
import os
import subprocess

from xetl.models.task import Task

logger = logging.getLogger(__name__)


def argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser("xETL")
    parser.add_argument(
        dest="task_path",
        help="Path to job manifest YAML file. Relative paths are resolved relative to the current working directory.",
    )
    parser.add_argument(
        "-n",
        "--name",
        default=None,
        help="Name of test case to run. If not provided, all tests will be run.",
    )
    return parser


if __name__ == "__main__":
    args = argument_parser().parse_args()
    task = Task.from_file(args.task_path)

    if args.name:
        if test_case := task.tests.get(args.name):
            test_cases = {args.name: test_case}
        else:
            logger.error(f"Test case {args.name} not found in task {task.name}")
            logger.info("Available test cases:")
            for name in task.tests.keys():
                logger.info(f"  {name}")
            exit(code=1)

    if test_case := task.tests.get(args.name):
        test_cases = {args.name: test_case}
    else:
        test_cases = task.tests

    cwd = os.path.dirname(args.task_path)
    for name, test_case in test_cases.items():
        # TODO: Need to reset the output directory for each test case
        if test_case.setup:
            logger.info(f"Running setup for test case: {name}")
            subprocess.run(test_case.setup, cwd=cwd)

        try:
            logger.info(f"Running test case: {name}")
            if return_code := task.execute(test_case.env):
                logger.error(f"❌ Test case {name} failed, task failed with return code {return_code}")
                exit(code=return_code)

            logger.info("Verifying...")

            result = subprocess.run(test_case.verify, cwd=cwd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, encoding="utf-8")
            if result.returncode != 0:
                logger.error(f"❌ Test case {name} failed")
                for line in result.stdout.splitlines():
                    logger.info(line)
                exit(code=result.returncode)

            logger.info(f"✅ Test case {name} passed")
        finally:
            if test_case.teardown:
                logger.info(f"Running teardown for test case: {name}")
                subprocess.run(test_case.teardown, cwd=cwd)
