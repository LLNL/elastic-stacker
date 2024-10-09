import logging
import os

from httpx import HTTPStatusError

from elastic_stacker.utils.controller import ElasticsearchAPIController

logger = logging.getLogger("elastic_stacker")


class ComponentTemplateController(ElasticsearchAPIController):
    """
    ComponentTemplateController manages the import/export of component templates.
    https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-component-template.html
    """

    _base_endpoint = "_component_template"
    _resource_directory = "component_templates"

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
        Get one or all of the component templates on the system.
        https://www.elastic.co/guide/en/elasticsearch/reference/current/getting-component-templates.html
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
        Create a new component template.
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
        purge: bool=False,
        purge_prompt: bool=True,
        **kwargs,
    ):
        """
        Dump all component templates on the system to files in the data directory
        """
        working_directory = self._get_working_dir(data_directory, create=True)
        templates = self.get()["component_templates"]
        for template in templates:
            name = template["name"]
            managed = template["component_template"].get("_meta", {}).get("managed")
            if include_managed or not managed:
                file_path = working_directory / (name + ".json")
                self._write_file(file_path, template)
        if purge:
            self._purge_untouched_files(prompt=purge_prompt)


    def load(
        self,
        data_directory: os.PathLike = None,
        delete_after_import: bool = False,
        allow_failure: bool = False,
        **kwargs,
    ):
        """
        Load component template configurations from files and load them into
        Elasticsearch.
        """
        working_directory = self._get_working_dir(data_directory, create=False)

        for template_file in working_directory.glob("*.json"):
            contents = self._read_file(template_file)
            template = contents["component_template"]
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
