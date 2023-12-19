import yaml


class ChainedException(Exception):
    def __str__(self) -> str:
        return super().__str__() + ("; " + str(self.__cause__) if self.__cause__ else "")


class ManifestLoadError(ChainedException):
    pass


class InvalidManifestError(ChainedException):
    pass


def load_file(path: str) -> str:
    try:
        with open(path, "r") as fd:
            return fd.read()
    except Exception as e:
        raise ManifestLoadError(f"Failed to load file") from e


def parse_yaml(yaml_content: str) -> dict:
    try:
        manifest = yaml.load(yaml_content, Loader=yaml.FullLoader)
        if isinstance(manifest, dict):
            return manifest
        raise InvalidManifestError("Failed to parse YAML, expected a dictionary")
    except yaml.YAMLError as e:
        raise InvalidManifestError("Failed to parse YAML") from e


def parse_yaml_file(path: str) -> dict:
    yaml_content = load_file(path)
    try:
        return parse_yaml(yaml_content)
    except Exception as e:
        raise ManifestLoadError(f"Error while parsing YAML at path: {path}") from e
