import os
import sys
import logging
from pathlib import Path
from typing import Iterable

import yaml

from .schemas import ConfigFileSchema, ProfileSchema

CONFIG_ENV_VAR = "STACKER_CONFIG_FILE"

CONFIG_FILE_PRECEDENCE = [
    "./stacker.yaml",
    "./stacker.yml",
    "./.stacker.yaml",
    "./.stacker.yml",
    "~/.stacker.yaml",
    "~/.stacker.yml",
    "~/.config/stacker.yaml",
    "~/.config/stacker.yml",
]

GLOBAL_DEFAULT_PROFILE = {
    "elasticsearch": {"base_url": "https://localhost:9200"},
    "kibana": {"base_url": "https://localhost:5601"},
    "options": {
        "data_directory": "./stacker_data",
    },
    "log": {"level": "WARN", "ecs": False},
}

logger = logging.getLogger("elastic_stacker")


def find_config():
    """
    Find the config file based on environment variables and the specified
    search order.
    """
    env_path = os.getenv(CONFIG_ENV_VAR)
    if env_path is not None:
        env_path = Path(env_path).expanduser()
        if not env_path.exists():
            raise FileNotFoundError(env_path)
        elif env_path.is_dir():
            raise IsADirectoryError(env_path)
        else:
            return env_path
    else:
        for path in CONFIG_FILE_PRECEDENCE:
            path = Path(path).expanduser()
            if path.exists() and path.is_file():
                return path
        raise FileNotFoundError(", ".join(CONFIG_FILE_PRECEDENCE))


def read_config(path: Path = None):
    """
    Load the config file as YAML.
    """
    logger.info("Reading configuration from file {}".format(path))
    with path.open("r") as fh:
        raw_config = yaml.safe_load(fh)
    return raw_config


def validate_config(raw_config: dict):
    """
    Validate the config object using the Marshmallow schema.
    """
    schema = ConfigFileSchema()
    config = schema.load(raw_config)
    return config


def load_config(path: os.PathLike = None):
    """
    Find, load and validate the config file.
    """
    if path is None:
        path = find_config()
    else:
        path = Path(path)
    raw_config = read_config(path)
    config = validate_config(raw_config)
    return config


def chain_configs(configs: Iterable[dict], keys: Iterable[str] = None):
    """
    overlay several mapping types on top of each other. Works somewhat like
    collections.ChainMap, but returns a single flat dict at the end.
    If `keys` is provided, will extract the value at each key in each mapping,
    and use that as the value to overlay.
    Useful for complex hierarchies of defaults which override one another.
    """
    result = {}
    for config in configs:
        if keys is not None:
            for key in reversed(keys):
                result.update(config.get(key, {}))
        else:
            result.update(config)
    return result


def make_profile(config: dict, overrides: dict = {}, profile_name: str = None):
    """
    The configuration file for Stacker includes the notion of "configuration profiles"
    which can be selected by the user at runtime. It also includes several levels of
    defaults which can be overriden by the level above them.
    """
    schema = ProfileSchema()

    global_defaults = schema.load(GLOBAL_DEFAULT_PROFILE)
    overrides = schema.load(overrides)

    user_defaults = config.get("default", {})

    if not user_defaults:
        logger.warn("No user-specified defaults found; using application defaults.")

    profiles = config.get("profiles", {})
    selected_profile = profiles.get(profile_name, {})

    if selected_profile == {} and profile_name is not None:
        logger.error("Profile {} not found in config; exiting.".format(profile_name))
        sys.exit(1)

    # this is the order of precedence for configuration:
    configs = [
        global_defaults,  # hardcoded
        user_defaults,  # ["default"]
        selected_profile,  # ["pre"] etc.
        overrides,  # specified at command line
    ]
    # the only other layer here is that for API clients (Elasticsearch, Kibana)
    # app-specific configs override general client configs
    # for example, profile.kibana overrides profile.client

    final_profile = dict(selected_profile)

    final_profile["elasticsearch"] = chain_configs(
        configs, keys=["client", "elasticsearch"]
    )
    final_profile["kibana"] = chain_configs(configs, keys=["client", "kibana"])

    final_profile["options"] = chain_configs(configs, keys=["options"])

    final_profile["substitutions"] = chain_configs(configs, keys=["substitutions"])

    final_profile["log"] = chain_configs(configs, keys=["log"])

    final_profile = schema.load(final_profile)

    return final_profile
