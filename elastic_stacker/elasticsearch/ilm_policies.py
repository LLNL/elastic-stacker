import logging
import os

from httpx import HTTPStatusError

from elastic_stacker.utils.controller import ElasticsearchAPIController

logger = logging.getLogger("elastic_stacker")


class ILMPolicyController(ElasticsearchAPIController):
    """
    ILMPolicyController manages the import/export of Elasticsearch's Index
    Lifecycle Management policies.
    https://www.elastic.co/guide/en/elasticsearch/reference/current/index-lifecycle-management-api.html
    """

    _base_endpoint = "/_ilm/policy/"
    _resource_directory = "ilm_policies"

    def _build_endpoint(self, name: str = "") -> str:
        return self._base_endpoint if name is None else self._base_endpoint + name

    def get(self, policy_id: str = None) -> dict:
        """
        Get one or all of the ILM policies on the system.
        https://www.elastic.co/guide/en/elasticsearch/reference/current/ilm-get-lifecycle.html
        """
        endpoint = self._build_endpoint(name)
        response = self._client.get(endpoint)
        return response.json()

    def create(
        self,
        policy_name: str,
        policy: dict,
    ):
        """
        Create or update a new ILM policy.
        https://www.elastic.co/guide/en/elasticsearch/reference/current/ilm-put-lifecycle.html
        """
        endpoint = self._build_endpoint(name)

        response = self._client.put(endpoint, json=role)
        return response.json()

    def dump(
        self,
        include_managed: bool = False,
        data_directory: os.PathLike = None,
        **kwargs,
    ):
        """
        Dump all ILM policies on the system to files in the data directory.
        """
        working_directory = self._get_working_dir(data_directory, create=True)
        policies = self.get()
        for policy_id, policy in policies.items():
            # if include_managed or not role.get("metadata", {}).get("_reserved"):
            file_path = working_directory / (name + ".json")
            self._write_file(file_path, role)

    def load(
        self,
        data_directory: os.PathLike = None,
        delete_after_import: bool = False,
        allow_failure: bool = False,
        **kwargs,
    ):
        """
        Read ILM policies from files and load them into
        Elasticsearch.
        """
        working_directory = self._get_working_dir(data_directory, create=False)

        for policy_file in working_directory.glob("*.json"):
            role = self._read_file(policy_file)
            role_name = policy_file.stem
            try:
                self.create(role_name, role)
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
