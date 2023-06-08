#! /bin/env python3

# stdlib
import logging
import json
import tempfile
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
from utils.controller import GenericController

logger = logging.getLogger("elastic_stacker")


class Stacker:
    def __init__(
        self,
        config: str = None,
        profile: str = None,
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

    def _members_with_method(self, method_name: str):
        return {
            name: obj
            for name, obj in self.__dict__.items()
            if hasattr(obj, method_name) and callable(getattr(obj, method_name))
        }

    def system_dump(self, *types: str, include_managed: bool = True):
        # find every object that's part of self which has a method called dump
        dumpables = self._members_with_method("dump")
        invalid_types = set(types).difference(set(dumpables))
        if invalid_types:
            raise Exception()
        if len(types) == 0:
            types = dumpables.keys()

        dump_arguments = {"include_managed": include_managed}

        for type_name in types:
            logger.warn("exporting {}".format(type_name))
            controller = getattr(self, type_name)
            controller.dump(**dump_arguments)

    def system_load(
        self,
        temp: bool = True,
        delete: bool = True,
        retries: bool = True,
    ):
        pass


def main():
    fire.Fire(Stacker)


if __name__ == "__main__":
    main()
