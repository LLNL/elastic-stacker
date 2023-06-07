import logging
import json
import pathlib

from .generic import GenericElasticsearchController

logger = logging.getLogger("elastic_stacker")


class EnrichPolicyController(GenericElasticsearchController):
    resource_directory = "enrich_policies"

    def _build_endpoint(self, names: str) -> str:
        return "_enrich/policy/{}".format(",".join(names))

    def get(self, *names):
        endpoint = self._build_endpoint(*names)
        response = self.client.get(endpoint)
        return response.json()

    def create(self, name: str, policy: dict):
        endpoint = self._build_endpoint(name)
        response = self.client.put(endpoint, json=policy)
        response_data = response.json()
        if "error" in response_data:
            if response_data["error"]["type"] == "resource_already_exists_exception":
                # Elasticsearch won't let you modify enrich policies after creation,
                # and the process for replacing an old one with a new one is a massive pain in the neck
                # so changing existing policies is not supported in version 1, but the user
                # should be warned that the policy hasn't been changed.
                logger.warn(response_data["reason"])
        return response_data

    def dump(
        self,
        data_directory: pathlib.Path,
    ):
        enrich_policies_directory = data_directory / self.resource_directory
        enrich_policies_directory.mkdir(exist_ok=True, parents=True)

        for policy in self.get()["policies"]:
            filename = policy["config"]["match"]["name"] + ".json"
            policy_file = enrich_policies_directory / filename
            policy = policy["config"]
            policy["match"].pop("name")
            with policy_file.open("w") as fh:
                fh.write(json.dumps(policy, sort_keys=True, indent=4))

    def load(
        self,
        data_directory: pathlib.Path,
        allow_failure: bool = False,
        delete_after_import: bool = False,
    ):
        enrich_policies_directory = data_directory / self.resource_directory
        if enrich_policies_directory.is_dir():
            for policy_file in enrich_policies_directory.glob("*.json"):
                with policy_file.open("r") as fh:
                    policy = json.load(fh)
                policy_name = policy_file.stem
                self.create(policy_name, policy)
