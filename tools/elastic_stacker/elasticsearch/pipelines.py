import logging
import json
from pathlib import Path

from utils.controller import GenericController

logger = logging.getLogger("elastic_stacker")


class PipelineController(GenericController):
    _base_endpoint = "_ingest/pipeline"
    _resource_directory = "pipelines"

    def _build_endpoint(self, id: str) -> str:
        endpoint = (
            self._base_endpoint
            if id is None
            else "{}/{}".format(self._base_endpoint, id)
        )
        return endpoint

    def get(self, id: str = None, master_timeout: str = None) -> dict:
        endpoint = self._build_endpoint(id)
        query_params = {"master_timeout": master_timeout} if master_timeout else {}
        response = self._client.get(endpoint, params=self._clean_params(query_params))
        return response.json()

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

        response = self._client.put(
            endpoint, json=pipeline, params=self._clean_params(query_params)
        )
        return response.json()

    def dump(self, include_managed: bool = False):
        self._create_working_dir()
        pipelines = self.get()
        for name, pipeline in pipelines.items():
            if include_managed or not pipeline.get("_meta", {}).get("managed"):
                file_path = self._working_directory / (name + ".json")
                with file_path.open("w") as file:
                    file.write(json.dumps(pipeline, indent=4, sort_keys=True))

    def load(self, data_directory: Path, delete_after_import: bool = False):
        if self._working_directory.is_dir():
            for pipeline_file in self._working_directory.glob("*.json"):
                with pipeline_file.open("r") as fh:
                    pipeline = json.load(fh)
                pipeline_id = pipeline_file.stem
                self.create(pipeline_id, pipeline)
