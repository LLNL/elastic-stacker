import logging
import os

from httpx import HTTPStatusError

from elastic_stacker.utils.controller import ElasticsearchAPIController

logger = logging.getLogger("elastic_stacker")


class RoleController(ElasticsearchAPIController):
    """
    PipelineController manages the import/export of security roles.
    https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api.html#security-role-apis
    """

    _base_endpoint = "/_security/role/"
    _resource_directory = "roles"

    def _build_endpoint(self, name: str = "") -> str:
        return self._base_endpoint if name is None else self._base_endpoint + name

    def get(self, name: str = None) -> dict:
        """
        Get one or all of the roles on the system.
        https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-get-role.html
        """
        endpoint = self._build_endpoint(name)
        response = self._client.get(endpoint)
        return response.json()

    def create(
        self,
        name: str,
        role: dict,
    ):
        """
        Create a new role.
        https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-put-role.html
        """
        endpoint = self._build_endpoint(name)

        response = self._client.put(endpoint, json=role)
        return response.json()

    def dump(
        self,
        include_managed: bool = False,
        data_directory: os.PathLike = None,
        purge: bool = False,
        force_purge: bool = False,
    ):
        """
        Dump all roles on the system to files in the data directory
        """
        working_directory = self._get_working_dir(data_directory, create=True)
        roles = self.get()
        for name, role in roles.items():
            if include_managed or not role.get("metadata", {}).get("_reserved"):
                file_path = working_directory / (name + ".json")
                self._write_file(file_path, role)
        if purge or force_purge:
            self._purge_untouched_files(force=force_purge)

    def load(
        self,
        data_directory: os.PathLike = None,
        delete_after_import: bool = False,
        allow_failure: bool = False,
    ):
        """
        Read role configurations from files and load them into
        Elasticsearch.
        """
        working_directory = self._get_working_dir(data_directory, create=False)

        for role_file in working_directory.glob("*.json"):
            role = self._read_file(role_file)
            role_name = role_file.stem
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
                    role_file.unlink()
