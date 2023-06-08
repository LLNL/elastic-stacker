import os
import logging
import json
from pathlib import Path

from slugify import slugify

from utils.controller import FleetAPIController

logger = logging.getLogger("elastic_stacker")


class PackagePolicyController(FleetAPIController):
    _base_endpoint = "/api/fleet/package_policies"
    _resource_directory = "package_policies"

    def _build_endpoint(self, id: str):
        return self._base_endpoint if not id else self._base_endpoint + "/" + id

    def get(self, ids: str = None, perPage: int = None, page: int = None):
        """
        page and perPage are actually not documented at all in the API docs...:man_facepalming:
        But, this *is* a paginated API, and it works the same way as agent_policies (where the
        pagination is documented properly.)
        """
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
        data_directory: os.PathLike = None,
    ):
        working_directory = self._get_working_dir(data_directory, create=False)
        for policy_file in working_directory.glob("*.json"):
            with policy_file.open("r") as fh:
                policy = json.load(fh)
            policy_id = policy["id"]
            self.create(id=policy_id, policy=policy)

    def dump(self, data_directory: os.PathLike = None):
        working_directory = self._get_working_dir(data_directory, create=True)
        for policy in self._depaginate(self.get):
            filename = slugify(policy["name"]) + ".json"
            file_path = working_directory / filename
            with file_path.open("w") as file:
                file.write(json.dumps(policy, indent=4, sort_keys=True))
