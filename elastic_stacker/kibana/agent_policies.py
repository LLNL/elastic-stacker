import logging
import os

from slugify import slugify

from elastic_stacker.utils.controller import FleetAPIController

logger = logging.getLogger("elastic_stacker")


class AgentPolicyController(FleetAPIController):
    """
    AgentPolicyController manages the import and export of Agent policies
    from the Fleet Server.
    Because the Fleet API is in tech preview, the functionality is incomplete.
    https://www.elastic.co/guide/en/fleet/master/fleet-apis.html
    """

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
        raise NotImplementedError

    # TODO
    def load(
        self,
        data_directory: os.PathLike = None,
        allow_failure: bool = False,
        delete_after_import: bool = False,
    ):
        raise NotImplementedError

    def dump(
        self,
        include_managed: bool = False,
        data_directory: os.PathLike = None,
        purge: bool = False,
        force_purge: bool = False,
    ):
        working_directory = self._get_working_dir(data_directory, create=True)

        for policy in self._depaginate(self.get):
            if include_managed or not policy["is_managed"]:
                # Policy name often has spaces in it, so slugify it first
                filename = slugify(policy["name"]) + ".json"
                file_path = working_directory / filename
                self._write_file(file_path, policy)
        if purge or force_purge:
            self._purge_untouched_files(force=force_purge)
