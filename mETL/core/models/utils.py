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
