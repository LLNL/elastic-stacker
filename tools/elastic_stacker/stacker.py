#! /bin/env python3

# stdlib
import json
from pathlib import Path

# PyPI
import fire

# local project
from kibana.saved_objects import SavedObjectController
from kibana.agent_policies import AgentPolicyController
from kibana.package_policies import PackagePolicyController
from elasticsearch.pipelines import PipelineController
from elasticsearch.transforms import TransformController
from elasticsearch.watches import WatchController
from elasticsearch.enrich_policies import EnrichPolicyController

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

        # https://www.elastic.co/guide/en/kibana/master/api.html#api-request-headers
        self.profile["kibana"]["headers"] = self.profile["kibana"].get(
            "headers", {}
        ) | {"kbn-xsrf": "true"}

        kibana_client = APIClient(**self.profile["kibana"])
        elasticsearch_client = APIClient(**self.profile["elasticsearch"])
        data_directory = self.profile["io"]["data_directory"]

        self.saved_objects = SavedObjectController(kibana_client, data_directory)
        self.agent_policies = AgentPolicyController(kibana_client, data_directory)
        self.package_policies = PackagePolicyController(kibana_client, data_directory)
        self.pipelines = PipelineController(elasticsearch_client, data_directory)
        self.transforms = TransformController(elasticsearch_client, data_directory)
        self.watches = WatchController(elasticsearch_client, data_directory)
        self.enrich_policies = EnrichPolicyController(
            elasticsearch_client, data_directory
        )

    def nop(self):
        pass


def main():
    fire.Fire(Stacker)


if __name__ == "__main__":
    main()
