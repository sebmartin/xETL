from argparse import ArgumentParser as StdlibArgumentParser

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
        command = command if isinstance(command, Command) else Command.from_file(command)
        super().__init__(name or command.run_command, description=command.description)
        add_arguments(self, command)
        # TODO: configure parser with command.env

    def parse_args(self, args=None, namespace=None):
        # TODO: check environment and merge with sys.argv
        args = super().parse_args(args, namespace)
        return args
