from argparse import ArgumentParser as StdlibArgumentParser

from xetl.models.transform import Transform


def arg_name_for_env(env_name: str) -> str:
    """
    Convert an environment variable name to a command line argument name. For example, the environment variable
    `MY_ENV_VAR` will be converted to `--my-env-var`.
    """
    long_name = f"{env_name.lower().replace('_', '-')}"
    return long_name


def add_arguments(argparser: "ArgumentParser", transform: Transform):
    for var, var_info in transform.env.items():
        argparser.add_argument(
            f"--{arg_name_for_env(var)}",
            type=var_info.type or str,
            required=var_info.required,
            default=var_info.default,
            help=var_info.description,
        )


class ArgumentParser(StdlibArgumentParser):
    def __init__(self, transform: Transform | str, name: str | None = None):
        """
        Create a preconfigured argument parser from a transform's manifest file.
        """
        transform = transform if isinstance(transform, Transform) else Transform.from_file(transform)
        super().__init__(name or transform.run_command, description=transform.description)
        add_arguments(self, transform)
        # TODO: configure parser with transform.env

    def parse_args(self, args=None, namespace=None):
        # TODO: check environment and merge with sys.argv
        args = super().parse_args(args, namespace)
        return args
