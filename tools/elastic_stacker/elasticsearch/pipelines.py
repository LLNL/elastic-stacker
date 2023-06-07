import logging
import json
import pathlib

from .generic import GenericElasticsearchController

logger = logging.getLogger("elastic_stacker")


class PipelineController(GenericElasticsearchController):
    base_endpoint = "_ingest/pipeline"
    resource_directory = "pipelines"

    def _build_endpoint(self, id: str):
        endpoint = (
            self.base_endpoint if id is None else "{}/{}".format(self.base_endpoint, id)
        )
        return endpoint

    def get(self, id: str = None, master_timeout: str = None):
        endpoint = self._build_endpoint(id)
        query_params = {"master_timeout": master_timeout} if master_timeout else {}
        pipelines_response = self.client.get(endpoint, params=query_params)
        return pipelines_response.json()

    def create(
        self,
        id: str,
        pipeline: dict,
        if_version: int = None,
        master_timeout: str = None,
        timeout: str = None,
    ):
        endpoint = self._build_endpoint(id)

        query_params = {
            "if_version": if_version,
            "master_timeout": master_timeout,
            "timeout": timeout,
        }

        response = self.client.put(endpoint, json=pipeline, params=self._clean_headers(query_params))
        return response.json()

    def dump(
        self,
        data_directory: pathlib.Path,
        include_managed: bool = False
    ):
        pipelines_directory = data_directory / self.resource_directory
        pipelines_directory.mkdir(exist_ok=True)

        for name, pipeline in self.get().items():
            if include_managed or not pipeline.get("_meta", {}).get("managed"):
                file_path = pipelines_directory / (name + ".json")
                with file_path.open("w") as file:
                    file.write(json.dumps(pipeline, indent=4))

    def load(
        self,
        data_directory: pathlib.Path,
        delete_after_import: bool = False
    ):
        pipelines_directory = data_directory / self.resource_directory
        if pipelines_directory.is_dir():
            for pipeline_file in pipelines_directory.glob("*.json"):
                with pipeline_file.open("r") as fh:
                    pipeline = json.load(fh)
                pipeline_id = pipeline_file.stem
                self.client.create_pipeline(pipeline_id, pipeline)
