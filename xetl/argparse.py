import os
from argparse import ArgumentParser as StdlibArgumentParser
import re

from xetl.models.command import Command


def arg_name_for_env(env_name: str) -> str:
    """
    Convert an environment variable name to a command line argument name. For example, the environment variable
    `MY_ENV_VAR` will be converted to `--my-env-var`.
    """
    long_name = f"{env_name.lower().replace('_', '-')}"
    return long_name


def add_arguments(argparser: "ArgumentParser", command: Command):
    for var, var_info in command.env.items():
        argparser.add_argument(
            f"--{arg_name_for_env(var)}",
            type=var_info.type or str,
            required=var_info.required,
            default=var_info.default,
            help=var_info.description,
        )


class ArgumentParser(StdlibArgumentParser):
    def __init__(self, command: Command | str, name: str | None = None):
        """
        Create a preconfigured argument parser from a command's manifest file.
        """
        self._command = command if isinstance(command, Command) else Command.from_file(command)
        super().__init__(name or self._command.run_command, description=self._command.description)
        add_arguments(self, self._command)

    def parse_args(self, args: list[str] | None = None, namespace=None):
        args = args or []
        arg_name_matcher = re.compile(r"^--([a-zA-Z0-9-_]+)=")
        provided_arg_names = [arg_name[0] for arg in args or [] if (arg_name := arg_name_matcher.match(arg))]
        env_args = [
            f"--{arg_name_for_env(var)}={os.environ[var]}"
            for var in self._command.env.keys()
            if var in os.environ and arg_name_for_env(var) not in provided_arg_names
        ]
        return super().parse_args(env_args + args, namespace)
