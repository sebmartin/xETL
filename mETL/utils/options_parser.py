import os
import inspect
import argparse
import yaml

# TODO maybe move this to core.tranform?
# TODO add tests

def __load_yaml(path):
    with open(path, 'r') as fd:
        try:
            manifest = yaml.load(fd, Loader=yaml.FullLoader)
            return manifest if isinstance(manifest, dict) else None
        except yaml.YAMLError:
            return None


def from_manifest(manifest_file=None, defaults=None):
    def guess_manifest_path():
        frame = inspect.stack()[2]
        filename = frame[0].f_code.co_filename
        transform_path = os.path.dirname(os.path.join(os.getcwd(), filename))
        return os.path.join(transform_path, 'manifest.yml')

    manifest_file = manifest_file or guess_manifest_path()
    manifest = __load_yaml(manifest_file)
    defaults = defaults or {}

    options = manifest.get('options', {}).keys()
    env = {
        option: os.environ.get(option.upper())
        for option in options
        if option.upper() in os.environ
    }
    missing_options = set(options) - set(env) - set(defaults)
    if missing_options:
        raise Exception('Missing required options: {}'.format(missing_options))
    return env
