import logging
import json
import pathlib

from slugify import slugify

from .generic import GenericKibanaController

logger = logging.getLogger("elastic_stacker")


class AgentPolicyController(GenericKibanaController):
    base_endpoint = "/api/fleet/agent_policies"
    resource_directory = "agent_policies"

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
        response = self.client.get(self.base_endpoint, params=query_params)
        return response.json()

    # TODO
    def create(self):
        pass

    # TODO
    def load(self):
        pass

    def dump(
        self,
        output_directory: pathlib.Path,
        include_managed: bool = False,
    ):
        agent_policies_directory = output_directory / self.resource_directory
        agent_policies_directory.mkdir(exist_ok=True)
        for policy in self._depaginate(self.get):
            if include_managed or not policy["is_managed"]:
                filename = slugify(policy["name"]) + ".json"
                file_path = agent_policies_directory / filename
                with file_path.open("w") as file:
                    file.write(json.dumps(policy, indent=4, sort_keys=True))
