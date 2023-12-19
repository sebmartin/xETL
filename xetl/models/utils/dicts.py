from pydantic import BaseModel

from xetl.models import EnvVariableType


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


def fuzzy_lookup(obj: BaseModel | dict, key: str, raise_on_missing: bool = False) -> EnvVariableType:
    """
    Look up a key in a model or dict using a case insensitive match that also allows underscores to be used
    in place of dashes (and vice-versa)
    """

    d = obj if isinstance(obj, dict) else obj.model_dump()
    normalized_dict = {conform_key(k): v for k, v in d.items()}
    if raise_on_missing:
        return normalized_dict[conform_key(key)]
    return normalized_dict.get(conform_key(key))
