import logging
import os

from httpx import HTTPStatusError

from elastic_stacker.utils.controller import ElasticsearchAPIController

logger = logging.getLogger("elastic_stacker")


class RoleMappingController(ElasticsearchAPIController):
    """
    RoleMappingController manages the import/export of security role mappings.
    https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api.html#security-role-mapping-apis
    """

    _base_endpoint = "/_security/role_mapping/"
    _resource_directory = "role_mappings"

    def _build_endpoint(self, name: str = "") -> str:
        return self._base_endpoint if name is None else self._base_endpoint + name

    def get(self, name: str = None) -> dict:
        """
        Get one or all of the role mappings on the system.
        https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-get-role-mapping.html
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
        https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-put-role_mapping.html
        """
        endpoint = self._build_endpoint(name)

        response = self._client.put(endpoint, json=role)
        return response.json()

    def dump(
        self,
        data_directory: os.PathLike = None,
        **kwargs,
    ):
        """
        Dump all roles on the system to files in the data directory
        """
        working_directory = self._get_working_dir(data_directory, create=True)
        role_mappings = self.get()
        for name, role_mapping in role_mappings.items():
            file_path = working_directory / (name + ".json")
            self._write_file(file_path, role_mapping)

    def load(
        self,
        data_directory: os.PathLike = None,
        delete_after_import: bool = False,
        allow_failure: bool = False,
        **kwargs,
    ):
        """
        Read role configurations from files and load them into
        Elasticsearch.
        """
        working_directory = self._get_working_dir(data_directory, create=False)

        for role_mapping_file in working_directory.glob("*.json"):
            role_mapping = self._read_file(role_mapping_file)
            role_mapping_name = role_mapping_file.stem
            try:
                self.create(role_mapping_name, role_mapping)
            except HTTPStatusError as e:
                if allow_failure:
                    logger.info(
                        "Experienced an error; continuing because allow_failure is True"
                    )
                else:
                    raise e
            else:
                if delete_after_import:
                    role_mapping_file.unlink()
