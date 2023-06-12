#! /bin/env python3

# stdlib
import logging
import json
import os
import shutil
import tempfile
from pathlib import Path

# PyPI
import fire

# local project
from kibana.saved_objects import SavedObjectController
from kibana.agent_policies import AgentPolicyController
from elasticsearch.pipelines import PipelineController
from elasticsearch.transforms import TransformController
from elasticsearch.watches import WatchController
from elasticsearch.enrich_policies import EnrichPolicyController

# Removed pending clarification on the Fleet API data structures.
# from kibana.package_policies import PackagePolicyController

from utils.config import load_config, make_profile
from utils.client import APIClient
from utils.controller import GenericController

logger = logging.getLogger("elastic_stacker")


class Stacker:
    profile: dict
    # package_policies: PackagePolicyController
    saved_objects: SavedObjectController
    agent_policies: AgentPolicyController
    pipelines: PipelineController
    transforms: TransformController
    watches: WatchController
    enrich_policies: EnrichPolicyController

    def __init__(
        self,
        config: os.PathLike = None,
        profile: str = None,
        elasticsearch: str = None,
        kibana: str = None,
        ca: os.PathLike = None,
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
        self._data_directory = self.profile["io"]["data_directory"]

        # self.package_policies = PackagePolicyController(kibana_client, self._data_directory)
        self.saved_objects = SavedObjectController(kibana_client, self._data_directory)
        self.agent_policies = AgentPolicyController(kibana_client, self._data_directory)
        self.pipelines = PipelineController(elasticsearch_client, self._data_directory)
        self.transforms = TransformController(
            elasticsearch_client, self._data_directory
        )
        self.watches = WatchController(elasticsearch_client, self._data_directory)
        self.enrich_policies = EnrichPolicyController(
            elasticsearch_client, self._data_directory
        )
        self._controllers = {
            # "package_policies": self.package_policies,
            "saved_objects": self.saved_objects,
            "agent_policies": self.agent_policies,
            "watches": self.watches,
            "pipelines": self.pipelines,
            "transforms": self.transforms,
            "watches": self.watches,
            "enrich_policies": self.enrich_policies,
        }

    def system_dump(self, *types: str, include_managed: bool = True):
        invalid_types = set(types).difference(set(self._controllers.keys()))
        if invalid_types:
            raise Exception("types {} are invalid".format(invalid_types))
        if len(types) == 0:
            types = self._controllers.keys()

        dump_arguments = {"include_managed": include_managed}

        for type_name in types:
            logger.warning("exporting {}".format(type_name))
            controller = getattr(self, type_name)
            controller.dump(**dump_arguments)

    def system_load(
        self,
        *types,
        temp: bool = False,
        data_directory: os.PathLike = None,
        delete_after_import: bool = False,
        retries: int = 0,
        allow_failure: bool = False,
        stubborn: bool = False
    ):
        if stubborn:
            delete_after_import = True
            temp = True
            allow_failure = True
            if retries is not None:
                retries = 5

        invalid_types = set(types).difference(set(self._controllers.keys()))
        if invalid_types:
            raise Exception("types {} are invalid".format(invalid_types))
        if len(types) == 0:
            types = self._controllers.keys()

        if data_directory is None:
            data_directory = self._data_directory

        if temp:
            working_data_directory = tempfile.mkdtemp(prefix="stacker_data_")
            shutil.copytree(data_directory, working_data_directory)
        else:
            working_data_directory = data_directory

        load_arguments = {
            "allow_failure": allow_failure,
            "data_directory": working_data_directory,
            "delete_after_import": delete_after_import,
        }

        for type_name in types:
            logger.warning("importing {}".format(type_name))
            controller = getattr(self, type_name)
            controller.load(**load_arguments)


def main():
    fire.Fire(Stacker)


if __name__ == "__main__":
    main()
