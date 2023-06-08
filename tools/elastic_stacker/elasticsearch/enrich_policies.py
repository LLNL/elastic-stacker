import logging
import json
import os
from pathlib import Path

from utils.controller import ElasticsearchAPIController

logger = logging.getLogger("elastic_stacker")


class EnrichPolicyController(ElasticsearchAPIController):
    _resource_directory = "enrich_policies"

    def _build_endpoint(self, *names: str) -> str:
        return "_enrich/policy/{}".format(",".join(names))

    def get(self, *names):
        endpoint = self._build_endpoint(*names)
        response = self._client.get(endpoint)
        return response.json()

    def create(self, name: str, policy: dict):
        endpoint = self._build_endpoint(name)
        response = self._client.put(endpoint, json=policy)
        response_data = response.json()
        if "error" in response_data:
            if response_data["error"]["type"] == "resource_already_exists_exception":
                # Elasticsearch won't let you modify enrich policies after creation,
                # and the process for replacing an old one with a new one is a massive pain in the neck
                # so changing existing policies is not supported in version 1, but the user
                # should be warned that the policy hasn't been changed.
                logger.warn(response_data["reason"])
        return response_data

    def dump(self, data_directory: os.PathLike = None, **kwargs):
        working_directory = self._get_working_dir(data_directory, create=True)
        for policy in self.get()["policies"]:
            filename = policy["config"]["match"]["name"] + ".json"
            policy_file = working_directory / filename
            policy = policy["config"]
            policy["match"].pop("name")
            with policy_file.open("w") as fh:
                fh.write(json.dumps(policy, sort_keys=True, indent=4))

    def load(
        self,
        data_directory: os.PathLike = None,
        allow_failure: bool = False,
        delete_after_import: bool = False,
        **kwargs
    ):
        working_directory = self._get_working_dir(data_directory, create=True)

        if working_directory.is_dir():
            for policy_file in self._working_directory.glob("*.json"):
                with policy_file.open("r") as fh:
                    policy = json.load(fh)
                policy_name = policy_file.stem
                self.create(policy_name, policy)
