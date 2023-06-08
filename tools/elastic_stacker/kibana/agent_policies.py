import logging
import json
import os
from pathlib import Path

from slugify import slugify

from utils.controller import FleetAPIController

logger = logging.getLogger("elastic_stacker")


class AgentPolicyController(FleetAPIController):
    _base_endpoint = "/api/fleet/agent_policies"
    _resource_directory = "agent_policies"

    def get(
        self,
        perPage: int = None,
        page: int = None,
        kuery: str = None,
        full: bool = None,
        noAgentCount: bool = None,
    ):
        query_params = {
            "perPage": perPage,
            "page": page,
            "full": full,
            "noAgentCount": noAgentCount,
            "kuery": kuery,
        }
        query_params = self._clean_params(query_params)
        response = self._client.get(self._base_endpoint, params=query_params)
        return response.json()

    # TODO
    def create(self):
        pass

    # TODO
    def load(self, data_directory: os.PathLike = None, **kwargs):
        pass

    def dump(
        self,
        include_managed: bool = False,
        data_directory: os.PathLike = None,
        **kwargs
    ):
        working_directory = self._get_working_dir(data_directory, create=True)

        for policy in self._depaginate(self.get):
            if include_managed or not policy["is_managed"]:
                filename = slugify(policy["name"]) + ".json"
                file_path = working_directory / filename
                with file_path.open("w") as file:
                    file.write(json.dumps(policy, indent=4, sort_keys=True))
