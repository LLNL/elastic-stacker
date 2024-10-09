import logging
import os

from httpx import HTTPStatusError

from elastic_stacker.utils.controller import ElasticsearchAPIController

logger = logging.getLogger("elastic_stacker")


class IndexTemplateController(ElasticsearchAPIController):
    """
    ComponentTemplateController manages the import/export of index templates.
    https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-index-template.html
    This is shockingly similar to the ComponentTemplateController...perhaps they ought ti be condensed.
    """

    _base_endpoint = "_index_template"
    _resource_directory = "index_templates"

    def _build_endpoint(self, name: str) -> str:
        endpoint = (
            self._base_endpoint
            if name is None
            else "{}/{}".format(self._base_endpoint, name)
        )
        return endpoint

    def get(
        self,
        name: str = None,
        flat_settings: bool = None,
        local_only: bool = None,
        master_timeout: str = None,
        include_defaults: bool = None,
    ) -> dict:
        """
        Get one or all of the index templates on the system.
        """
        endpoint = self._build_endpoint(name)
        query_params = {
            "flat_settings": flat_settings,
            "local": local_only,
            "master_timeout": master_timeout,
            "include_defaults": include_defaults,
        }
        response = self._client.get(endpoint, params=self._clean_params(query_params))
        return response.json()

    def create(
        self,
        name: str,
        template: dict,
        create_only: bool = None,
        master_timeout: str = None,
    ):
        """
        Create a new index template.
        """
        endpoint = self._build_endpoint(name)

        query_params = {
            "create": create_only,
            "master_timeout": master_timeout,
        }

        response = self._client.put(
            endpoint, json=template, params=self._clean_params(query_params)
        )
        return response.json()

    def dump(
        self,
        include_managed: bool = False,
        data_directory: os.PathLike = None,
        purge: bool = False,
        force_purge: bool = False,
        **kwargs,
    ):
        """
        Dump all index templates on the system to files in the data directory
        """
        working_directory = self._get_working_dir(data_directory, create=True)
        templates = self.get()["index_templates"]
        for template in templates:
            # the managed field is very deeply nested and all of the keys above it may or may not exist
            template_managed = (
                template["index_template"]
                .get("template", {})
                .get("mappings", {})
                .get("_meta", {})
                .get("managed", False)
            )
            if include_managed or not template_managed:
                file_path = working_directory / (template["name"] + ".json")
                self._write_file(file_path, template)
        if purge or force_purge:
            self._purge_untouched_files(force=force_purge)

    def load(
        self,
        data_directory: os.PathLike = None,
        delete_after_import: bool = False,
        allow_failure: bool = False,
        **kwargs,
    ):
        """
        Load index template configurations from files and load them into
        Elasticsearch.
        """
        working_directory = self._get_working_dir(data_directory, create=False)

        for template_file in working_directory.glob("*.json"):
            contents = self._read_file(template_file)
            template = contents["index_template"]
            template_name = contents["name"]
            try:
                self.create(template_name, template)
            except HTTPStatusError as e:
                if allow_failure:
                    logger.info(
                        "Experienced an error; continuing because allow_failure is True"
                    )
                else:
                    raise e
            else:
                if delete_after_import:
                    template_file.unlink()
