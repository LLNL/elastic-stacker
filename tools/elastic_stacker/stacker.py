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
    elasticsearch: ElasticsearchController
    kibana: KibanaController

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
        self._elasticsearch_client = APIClient(**self.profile["elasticsearch"])
        self._kibana_client = APIClient(**self.profile["kibana"])

        self.elasticsearch = ElasticsearchController(self._elasticsearch_client)
        self.kibana = KibanaController(self._kibana_client)
        # steal the attrs from the controllers so we can do
        # "stacker.py watches dump" instead of "stacker.py elasticsearch watches dump"
        # for controller in (self._elasticsearch, self._kibana):
        #     for name, attr in controller.__dict__.items():
        #         if not name.startswith("_"):
        #             self.__setattr__(name, attr)

    def nop(self):
        pass


def main():
    fire.Fire(Stacker)


if __name__ == "__main__":
    main()
