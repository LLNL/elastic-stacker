import os
import logging
import json
from httpx import HTTPStatusError

from slugify import slugify

from elastic_stacker.utils.controller import FleetAPIController

logger = logging.getLogger("elastic_stacker")


class PackagePolicyController(FleetAPIController):
    """
    PackagePolicyController manages the import and export of package policies
    from the Fleet Server.
    Because the Fleet API is in tech preview, the functionality is incomplete.
    https://www.elastic.co/guide/en/fleet/master/fleet-apis.html
    """

    _base_endpoint = "/api/fleet/package_policies"
    _resource_directory = "package_policies"

    def _build_endpoint(self, id: str):
        return self._base_endpoint if not id else self._base_endpoint + "/" + id

    def get(self, id: str = None, perPage: int = None, page: int = None):
        """
        page and perPage are actually not documented at all in the API docs...:man_facepalming:
        But, this *is* a paginated API, and it works the same way as agent_policies (where the
        pagination is documented properly.)
        """
        endpoint = self._build_endpoint(id)
        query_params = {"page": page, "perPage": perPage}
        query_params = self._clean_params(query_params)
        response = self._client.get(endpoint, params=query_params)
        return response.json()

    def create(self, id: str, policy: dict):
        endpoint = self._build_endpoint()
        response = self._client.post(endpoint, json=policy)
        return response.json()

    def update(self, id: str, policy: dict):
        endpoint = self._build_endpoint(id)
        response = self._client.put(endpoint, json=policy)
        return response.json()

    def upsert(self, id: str, policy: dict):
        try:
            self.update(id, policy)
        except HTTPStatusError as e:
            if e.response.status_code == "404":
                self.insert(id, policy)
            else:
                raise e

    def load(
        self,
        delete_after_import: bool = False,
        allow_failure: bool = False,
        data_directory: os.PathLike = None,
        **kwargs,
    ):
        raise NotImplementedError()
        working_directory = self._get_working_dir(data_directory, create=False)
        for policy_file in working_directory.glob("*.json"):
            with policy_file.open("r") as fh:
                policy = json.load(fh)
            policy_id = policy.pop("id")
            try:
                self.upsert(id=policy_id, policy=policy)
            except HTTPStatusError as e:
                if allow_failure:
                    logger.info(
                        "Experienced an error; continuing because allow_failure is True"
                    )
                else:
                    raise e
            else:
                if delete_after_import:
                    policy_file.unlink()

    def dump(
        self,
        data_directory: os.PathLike = None,
        purge: bool=False,
        force_purge:bool=False,
        **kwargs
    ):
        working_directory = self._get_working_dir(data_directory, create=True)
        for policy in self._depaginate(self.get):
            filename = slugify(policy["name"]) + ".json"
            file_path = working_directory / filename
            # clean unimportable keys
            for key in ["created_at", "created_by", "revision"]:
                policy.pop(key)
            if "title" in policy.get("package", {}):
                policy["package"].pop("title")
            self._write_file(file_path, policy)
        if purge or force_purge:
            self._purge_untouched_files(force=force_purge)

