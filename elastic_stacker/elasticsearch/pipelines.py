import logging
import functools
import json

import graphviz
from httpx import HTTPStatusError

from elastic_stacker.utils.controller import ElasticsearchAPIController

logger = logging.getLogger("elastic_stacker")


class PipelineController(ElasticsearchAPIController):
    """
    PipelineController manages the import/export of ingest pipelines.
    https://www.elastic.co/guide/en/elasticsearch/reference/current/ingest-apis.html
    """

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
        """
        Get one or all of the ingest pipelines on the system.
        https://www.elastic.co/guide/en/elasticsearch/reference/current/get-pipeline-api.html
        """
        endpoint = self._build_endpoint(id)
        query_params = {"master_timeout": master_timeout}
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
        """
        Create a new ingest pipeline.
        https://www.elastic.co/guide/en/elasticsearch/reference/current/put-pipeline-api.html
        """
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
        **kwargs,
    ):
        """
        Dump all ingest pipelines on the system to files in the data directory
        """
        self._data_directory.mkdir()
        pipelines = self.get()
        for name, pipeline in pipelines.items():
            if include_managed or not pipeline.get("_meta", {}).get("managed"):
                file_path = self._data_directory / (name + ".json")
                self._write_file(file_path, pipeline)

    def load(
        self,
        delete_after_import: bool = False,
        allow_failure: bool = False,
        **kwargs,
    ):
        """
        Load ingest pipeline configurations from files and load them into
        Elasticsearch.
        """

        for pipeline_file in self._data_directory.glob("*.json"):
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

    @functools.cache
    def _get_stored(self, pipeline_name: str):
        pipeline_file = self._data_directory / (pipeline_name + ".json")
        with pipeline_file.open("r") as file_handle:
            return json.load(file_handle)

    def _glob_stored(self, pattern: str):
        matching_files = self._data_directory.glob(f"{pattern}.json")
        return [f.stem for f in matching_files]

    @functools.cache
    def _render_pipeline(self, pipeline_name: str):

        DISPLAY_KEYS = {"field", "if", "name"}  # only show these fields in the nodes

        def node_id(pipeline_name, index):
            return f"{pipeline_name}_processor_{index}"

        pipeline = self._get_stored(pipeline_name)
        pipeline_subgraph = graphviz.Digraph(
            name=f"cluster_{pipeline_name}",
            graph_attr={
                "style": "filled",
                "color": "lightgrey",
                "label": pipeline_name,
            },
        )

        previous_node_id = None
        for index, processor in enumerate(pipeline["processors"]):
            this_node_id = node_id(pipeline_name, index)
            processor_type = list(processor.keys())[0]
            processor_opts = processor[processor_type]
            node_title = f"{processor_type.upper()}"
            node_body = (
                "\\l".join(
                    [
                        f"{k}: {v}"
                        for k, v in processor_opts.items()
                        if k in DISPLAY_KEYS
                    ]
                )
                + "\\l"
            )
            node_label = f"{node_title}\n{node_body}"

            pipeline_subgraph.node(this_node_id, label=node_label)
            if previous_node_id:
                pipeline_subgraph.edge(
                    previous_node_id,
                    this_node_id,
                    shape="rarrow",
                )

            if processor_type == "pipeline":
                next_pipeline_name = processor_opts["name"]

                try:
                    next_pipeline = self._get_stored(next_pipeline_name)
                except FileNotFoundError:
                    logger.info(
                        "found link to nonexistent pipeline %s", next_pipeline_name
                    )
                    continue
                yield from self._render_pipeline(next_pipeline_name)
                next_pipeline_start = node_id(next_pipeline_name, 0)
                next_pipeline_length = len(next_pipeline["processors"])
                next_pipeline_end = node_id(
                    next_pipeline_name, next_pipeline_length - 1
                )

                # draw an edge to the start of the next pipeline
                pipeline_subgraph.edge(
                    this_node_id,
                    next_pipeline_start,
                    shape="rarrow",
                    label=processor_opts.get("if", ""),
                )
                if index < len(pipeline["processors"]):
                    # draw a line from the end of the next pipeline
                    # back to our next node
                    next_node = node_id(pipeline_name, index + 1)
                    pipeline_subgraph.edge(
                        next_pipeline_end,
                        next_node,
                        shape="rarrow",
                        label=processor_opts.get("if", ""),
                    )

            previous_node_id = this_node_id
        yield pipeline_subgraph

    def visualize(self, pattern: str = "*"):
        graph = graphviz.Digraph(
            pattern,
            strict=True,
            format="png",
            graph_attr={"fontname": "Courier"},
            edge_attr={"fontname": "Courier", "fontsize": "9"},
            node_attr={
                "fontname": "Courier",
                "shape": "box",
            },
        )
        for pipeline_name in self._glob_stored(pattern):
            for pipeline_subgraph in self._render_pipeline(pipeline_name):
                graph.subgraph(pipeline_subgraph)
        graph.view()
