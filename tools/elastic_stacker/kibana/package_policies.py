import logging
import json
import pathlib

from slugify import slugify

from .generic import GenericKibanaController

logger = logging.getLogger("elastic_stacker")


class PackagePolicyController(GenericKibanaController):
    base_endpoint = "/api/fleet/package_policies"
    resource_directory = "package_policies"

    def _build_endpoint(self, id: str):
        return self.base_endpoint if not id else self.base_endpoint + "/" + id

    def get(self, id: str = None):
        endpoint = self._build_endpoint(id)
        response = self.client.get(endpoint)
        return response.json()

    def create(self, id: str, policy: dict):
        endpoint = self._build_endpoint(id)
        response = self.client.put(endpoint, json=policy)
        return response.json()

    def load(
        self,
        data_directory: pathlib.Path,
        delete_after_import: bool = False,
        allow_failure: bool = False,
    ):
        package_policies_directory = data_directory / self.resource_directory
        for policy_file in package_policies_directory.glob("*.json"):
            with policy_file.open("r") as fh:
                policy = json.load(fh)
            policy_id = policy["id"]
            self.create(id=policy_id, policy=policy)

    def dump(self, data_directory: pathlib.Path):
        package_policies_directory = data_directory / self.resource_directory
        package_policies_directory.mkdir(exist_ok=True, parents=True)
        for policy in self.get()["items"]:
            filename = slugify(policy["name"]) + ".json"
            file_path = package_policies_directory / filename
            with file_path.open("w") as file:
                file.write(json.dumps(policy, indent=4, sort_keys=True))
