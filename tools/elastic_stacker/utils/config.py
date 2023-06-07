import os
import sys
import logging
import pathlib
from collections import ChainMap
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
    "dump": {"include_managed": False},
    "load": {
        "temp_copy": False,
        "delete_after_import": False,
        "allow_failure": False,
        "retries": 0,
    },
    "io": {
        "data_directory": "./stacker_data",
    },
}

logger = logging.getLogger("elastic_stacker")


def find_config():
    env_path = os.getenv(CONFIG_ENV_VAR)
    if env_path is not None:
        env_path = pathlib.Path(env_path).expanduser()
        if not env_path.exists():
            raise FileNotFoundError(env_path)
        elif env_path.is_dir():
            raise IsADirectoryError(env_path)
        else:
            return env_path
    else:
        for path in CONFIG_FILE_PRECEDENCE:
            path = pathlib.Path(path).expanduser()
            if path.exists() and path.is_file():
                return path
        raise FileNotFoundError(", ".join(CONFIG_FILE_PRECEDENCE))


def read_config(path: pathlib.Path = None):
    logger.info("Reading configuration from file {}".format(path))
    with path.open("r") as fh:
        raw_config = yaml.safe_load(fh)
    return raw_config


def validate_config(raw_config: dict):
    schema = ConfigFileSchema()
    config = schema.load(raw_config)
    return config


def load_config(path: os.PathLike = None):
    if path is None:
        path = find_config()
    else:
        path = pathlib.Path(path)
    raw_config = read_config(path)
    config = validate_config(raw_config)
    return config


def chain_configs(configs: Iterable[dict], keys: Iterable[str] = None):
    chain = []
    for config in configs:
        if keys is not None:
            for key in keys:
                chain.append(config.get(key, {}))
        else:
            chain.append(config)
    return ChainMap(*chain)


def make_profile(config: dict, overrides: dict = {}, profile_name: str = None):
    """
    The configuration file for Stacker includes the notion of "configuration profiles"
    which can be selected by the user at runtime. It also includes several levels of
    defaults which can be overriden by the level above them. In order to implement this,
    we generate a final profile using the collections.ChainMap class. For more details:
    https://docs.python.org/3/library/collections.html#collections.ChainMap
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

    final_profile["load"] = chain_configs(configs, keys=["io", "load"])
    final_profile["dump"] = chain_configs(configs, keys=["io", "dump"])

    final_profile = schema.load(dict(final_profile))

    return final_profile
