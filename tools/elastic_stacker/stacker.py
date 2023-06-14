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
from elasticsearch.indices import IndexController
from elasticsearch.pipelines import PipelineController
from elasticsearch.transforms import TransformController
from elasticsearch.watches import WatchController
from elasticsearch.enrich_policies import EnrichPolicyController
from kibana.saved_objects import SavedObjectController
from kibana.agent_policies import AgentPolicyController
from kibana.package_policies import PackagePolicyController


from utils.config import load_config, make_profile
from utils.client import APIClient
from utils.controller import GenericController

logger = logging.getLogger("elastic_stacker")


class Stacker:
    profile: dict
    package_policies: PackagePolicyController
    agent_policies: AgentPolicyController
    indices: IndexController
    saved_objects: SavedObjectController
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
        self._options = self.profile["options"]

        subs = self.profile.get("substitutions", {})

        kibana_client = APIClient(**self.profile["kibana"])
        elasticsearch_client = APIClient(**self.profile["elasticsearch"])

        self.package_policies = PackagePolicyController(
            kibana_client, subs=subs, **self._options
        )
        self.agent_policies = AgentPolicyController(
            kibana_client, subs=subs, **self._options
        )
        self.indices = IndexController(elasticsearch_client, subs=subs, **self._options)
        self.saved_objects = SavedObjectController(
            kibana_client, subs=subs, **self._options
        )
        self.pipelines = PipelineController(
            elasticsearch_client, subs=subs, **self._options
        )
        self.transforms = TransformController(
            elasticsearch_client, subs=subs, **self._options
        )
        self.watches = WatchController(elasticsearch_client, subs=subs, **self._options)
        self.enrich_policies = EnrichPolicyController(
            elasticsearch_client, subs=subs, **self._options
        )
        self._controllers = {
            "indices": self.indices,
            "saved_objects": self.saved_objects,
            "watches": self.watches,
            "pipelines": self.pipelines,
            "transforms": self.transforms,
            "watches": self.watches,
            "enrich_policies": self.enrich_policies,
        }
        self._experimental_controllers = {
            "package_policies": self.package_policies,
            "agent_policies": self.agent_policies,
        }

    def system_dump(
        self,
        *types: str,
        include_managed: bool = False,
        include_experimental: bool = False
    ):
        valid_controllers = self._controllers
        if include_experimental:
            logger.warning(
                "Including experimental objects which do not have a load function yet."
            )
            valid_controllers.update(self._experimental_controllers)

        invalid_types = set(types).difference(set(valid_controllers.keys()))
        if invalid_types:
            raise Exception("types {} are invalid".format(invalid_types))
        if len(types) == 0:
            types = valid_controllers.keys()

        dump_arguments = {"include_managed": include_managed}

        for type_name in types:
            logger.warning("exporting {}".format(type_name))
            controller = valid_controllers[type_name]
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
            data_directory = self._options["data_directory"]

        if temp:
            working_data_directory = tempfile.mkdtemp(prefix="stacker_data_")
            shutil.copytree(data_directory, working_data_directory, dirs_exist_ok=True)
        else:
            working_data_directory = data_directory

        load_arguments = {
            "allow_failure": allow_failure,
            "data_directory": working_data_directory,
            "delete_after_import": delete_after_import,
        }
        for i in range(retries + 1):
            logger.info("beginning attempt no.")
            for type_name in types:
                logger.warning("importing {}".format(type_name))
                controller = self._controllers[type_name]
                controller.load(**load_arguments)


def main():
    fire.Fire(Stacker)


if __name__ == "__main__":
    main()
