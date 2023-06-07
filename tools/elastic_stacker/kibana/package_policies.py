import logging
import json
from pathlib import Path

from slugify import slugify

from .generic import GenericKibanaController

logger = logging.getLogger("elastic_stacker")


class PackagePolicyController(GenericKibanaController):
    _base_endpoint = "/api/fleet/package_policies"
    _resource_directory = "package_policies"

    def _build_endpoint(self, id: str):
        return self._base_endpoint if not id else self._base_endpoint + "/" + id

    def get(self, id: str = None):
        endpoint = self._build_endpoint(id)
        response = self._client.get(endpoint)
        return response.json()

    def create(self, id: str, policy: dict):
        endpoint = self._build_endpoint(id)
        response = self._client.put(endpoint, json=policy)
        return response.json()

    def load(
        self,
        delete_after_import: bool = False,
        allow_failure: bool = False,
    ):
        for policy_file in self._working_directory.glob("*.json"):
            with policy_file.open("r") as fh:
                policy = json.load(fh)
            policy_id = policy["id"]
            self.create(id=policy_id, policy=policy)

    def dump(self):
        self._create_working_dir()
        for policy in self.get()["items"]:
            filename = slugify(policy["name"]) + ".json"
            file_path = self._working_directory / filename
            with file_path.open("w") as file:
                file.write(json.dumps(policy, indent=4, sort_keys=True))
