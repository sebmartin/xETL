import logging
import yaml

logger = logging.getLogger(__name__)


def load_yaml(path) -> dict | None:
    with open(path, "r") as fd:
        try:
            manifest = yaml.load(fd, Loader=yaml.FullLoader)
            return manifest if isinstance(manifest, dict) else None
        except yaml.YAMLError as e:
            logger.warning(f"Could not load yaml file: {path}")
            logger.warning(e)
            return None


def conform_key(key: str):
    """
    All properties in app and transform manifests are converted to snake case. The only exception is
    the `env` dictionary in Transforms and Step models which are converted to upper case with underscores.
    See `conform_env_key` for more information.
    """
    return key.lower().replace("-", "_")


def conform_env_key(key: str):
    """
    The keys in env dictionaries are converted to upper case with underscores. This is done to match the
    convention of environment variables. For example, the following env dictionary:

        env:
          my-env-var: foo

    will be converted to:

        env:
          MY_ENV_VAR: foo
    """
    return key.upper().replace("-", "_")
