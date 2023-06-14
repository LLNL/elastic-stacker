import logging
import json
import os
from pathlib import Path

from httpx import HTTPStatusError

from utils.controller import ElasticsearchAPIController

logger = logging.getLogger("elastic_stacker")


class PipelineController(ElasticsearchAPIController):
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

    def dump(
        self,
        include_managed: bool = False,
        data_directory: os.PathLike = None,
        **kwargs,
    ):
        working_directory = self._get_working_dir(data_directory, create=True)
        pipelines = self.get()
        for name, pipeline in pipelines.items():
            if include_managed or not pipeline.get("_meta", {}).get("managed"):
                file_path = working_directory / (name + ".json")
                self._write_file(file_path, pipeline)

    def load(
        self,
        data_directory: os.PathLike = None,
        delete_after_import: bool = False,
        allow_failure: bool = False,
        **kwargs,
    ):
        working_directory = self._get_working_dir(data_directory, create=False)
        if working_directory.is_dir():
            for pipeline_file in working_directory.glob("*.json"):
                pipeline = self._read_file(pipeline_file)
                pipeline_id = pipeline_file.stem
                try:
                    self.create(pipeline_id, pipeline)
                except HTTPStatusError as e:
                    if allow_failure:
                        logger.info(
                            "Experienced an error; continuing because allow_failure is True"
                        )
                    else:
                        raise e
                else:
                    if delete_after_import:
                        pipeline_file.unlink()
