#! /bin/env python3

# stdlib
import json
from pathlib import Path

# PyPI
import fire

# local project
from kibana import KibanaController
from elasticsearch import ElasticsearchController
from utils.config import load_config, make_profile
from utils.client import APIClient


class Stacker:
    def __init__(
        self,
        config: str = None,
        profile: str = None,
        data_directory=None,
        elasticsearch: str = None,
        kibana: str = None,
        ca: str = None,
    ):
        global_config = load_config(config)  # if None, checks list of default locations
        overrides = {
            # TODO: add CLI arguments here to override configuration values, for example
            "elasticsearch": {"base_url": elasticsearch},
            "kibana": {"base_url": kibana},
            "client": {"verify": ca},
            "io": {"data_directory": data_directory},
        }
        self.profile = make_profile(
            global_config, profile_name=profile, overrides=overrides
        )

    def show_profile(self):
        print(self.profile)


def main():
    fire.Fire(Stacker)


if __name__ == "__main__":
    main()
