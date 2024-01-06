import os
import re
import sys
from argparse import ArgumentParser as StdlibArgumentParser

from xetl.models.task import Task


def arg_name_for_env(env_name: str) -> str:
    """
    Convert an environment variable name to a task line argument name. For example, the environment variable
    `MY_ENV_VAR` will be converted to `--my-env-var`.
    """
    long_name = f"{env_name.lower().replace('_', '-')}"
    return long_name


def add_arguments(argparser: "ArgumentParser", task: Task):
    for var, var_info in task.env.items():
        argparser.add_argument(
            f"--{arg_name_for_env(var)}",
            type=var_info.type or str,
            required=var_info.required,
            default=var_info.default,
            help=var_info.description,
        )


class ArgumentParser(StdlibArgumentParser):
    def __init__(self, task: Task | str, name: str | None = None):
        """
        Create a preconfigured argument parser from a task's manifest file.
        """
        self._task = task if isinstance(task, Task) else Task.from_file(task, silent=True)
        super().__init__(name or self._task.run_command, description=self._task.description)
        add_arguments(self, self._task)

    def parse_args(self, args: list[str] | None = None, namespace=None):
        args = args if args is not None else sys.argv[1:]
        arg_name_matcher = re.compile(r"^--([a-zA-Z0-9-_]+)=")
        provided_arg_names = [arg_name[0] for arg in args or [] if (arg_name := arg_name_matcher.match(arg))]
        env_args = [
            f"--{arg_name_for_env(var)}={os.environ[var]}"
            for var in self._task.env.keys()
            if var in os.environ and arg_name_for_env(var) not in provided_arg_names
        ]
        return super().parse_args(env_args + args, namespace)
