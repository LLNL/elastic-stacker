import json
import logging
import os

from httpx import HTTPStatusError

from elastic_stacker.utils.controller import ElasticsearchAPIController

logger = logging.getLogger("elastic_stacker")


class TransformController(ElasticsearchAPIController):
    """
    TransformController manages the import and export of transforms
    within Elasticsearch.
    https://www.elastic.co/guide/en/elasticsearch/reference/current/transform-apis.html
    """

    _base_endpoint = "_transform"
    _resource_directory = "transforms"

    def _build_endpoint(self, *ids: str) -> str:
        id_string = ",".join(ids)
        return f"{self._base_endpoint}/{id_string}"

    def get(
        self,
        *ids: str,
        allow_no_match: bool = None,
        exclude_generated: bool = None,
        offset: int = None,
        size: int = None,
    ):
        """
        Get some number of transforms by ID.
        https://www.elastic.co/guide/en/elasticsearch/reference/current/get-transform.html
        """
        query_params = {
            "allow_no_match": allow_no_match,
            "exclude_generated": exclude_generated,
            "from": offset,
            "size": size,
        }
        endpoint = self._build_endpoint(*ids)
        query_params = self._clean_params(query_params)
        response = self._client.get(endpoint, params=query_params)
        return response.json()

    def create(
        self,
        id: str,
        transform: dict,
        defer_validation: bool = None,
        timeout: str = None,
    ):
        """
        Create a transform.
        https://www.elastic.co/guide/en/elasticsearch/reference/current/put-transform.html
        """
        endpoint = self._build_endpoint(id)
        query_params = {"defer_validation": defer_validation, "timeout": timeout}
        query_params = self._clean_params(query_params)

        response = self._client.put(endpoint, json=transform, params=query_params)
        return response.json()

    def stats(
        self,
        *ids: str,
        allow_no_match: bool = None,
        offset: int = None,
        size: int = None,
    ):
        """
        Get transform statistics, including the running state of the transform.
        https://www.elastic.co/guide/en/elasticsearch/reference/current/get-transform-stats.html
        """
        if not ids:
            ids = ["_all"]
        endpoint = self._build_endpoint(*ids) + "/_stats"
        query_params = {
            "allow_no_match": allow_no_match,
            "from": offset,
            "size": size,
        }
        query_params = self._clean_params(query_params)
        response = self._client.get(endpoint, params=query_params)
        return response.json()

    def set_state(
        self,
        id: str,
        target_state: str,
        timeout: str = None,
    ):
        """
        Set transform state (started or stopped.)
        """
        started_states = {"started", "indexing"}
        stopped_states = {"failed", "stopped", "stopping", "aborted"}
        allowed_states = started_states | stopped_states

        if target_state not in allowed_states:
            err_msg = f"{target_state} is not a valid state for transforms; acceptable values are {allowed_states}."
            logger.error(err_msg)
            raise ValueError(err_msg)

        current_stats = self.stats(id)
        current_state = current_stats["transforms"][0]["state"]

        if target_state in started_states:
            if current_state in started_states:
                logger.debug(f"transform {id} is already started")
            else:
                self.start(id, timeout=timeout)
        elif current_state in stopped_states:
            logger.debug(f"transform {id} is already stopped")
        else:
            self.stop(id, timeout=timeout)

    def start(
        self,
        id: str,
        from_time: str = None,
        timeout: str = None,
    ):
        """
        Start a transform.
        https://www.elastic.co/guide/en/elasticsearch/reference/current/start-transform.html
        """
        endpoint = self._build_endpoint(id) + "/_start"
        query_params = {"from": from_time, "timeout": timeout}
        query_params = self._clean_params(query_params)

        logger.debug(f"starting transform {id}")
        response = self._client.post(endpoint, params=query_params)

        return response.json()

    def stop(
        self,
        id: str,
        force: bool = None,
        allow_no_match: bool = None,
        wait_for_checkpoint: bool = None,
        wait_for_completion: bool = None,
        timeout: str = None,
    ):
        """
        Stop a transform.
        https://www.elastic.co/guide/en/elasticsearch/reference/current/stop-transform.html
        """
        endpoint = self._build_endpoint(id) + "/_stop"

        query_params = {
            "force": force,
            "allow_no_match": allow_no_match,
            "wait_for_checkpoint": wait_for_checkpoint,
            "wait_for_completion": wait_for_completion,
            "timeout": timeout,
        }
        query_params = self._clean_params(query_params)

        logger.debug(f"starting transform {id}")
        response = self._client.post(endpoint, params=query_params)
        return response.json()

    def delete(
        self,
        id: str,
        force: bool = None,
        delete_dest_index: bool = None,
        timeout: str = None,
    ):
        """
        Delete a transform.
        https://www.elastic.co/guide/en/elasticsearch/reference/current/delete-transform.html
        """
        endpoint = self._build_endpoint(id)

        query_params = {
            "force": force,
            "delete_dest_index": delete_dest_index,
            "timeout": timeout,
        }

        query_params = self._clean_params(query_params)

        response = self._client.delete(endpoint, params=query_params)
        return response.json()

    def update(
        self,
        id: str,
        transform: dict,
        defer_validation: bool = None,
        timeout: str = None,
    ):
        """
        Update an existing transform.
        https://www.elastic.co/guide/en/elasticsearch/reference/current/update-transform.html
        """

        endpoint = self._build_endpoint(id) + "/_update"

        query_params = {"defer_validation": defer_validation, "timeout": timeout}
        query_params = self._clean_params(query_params)
        response = self._client.post(endpoint, json=transform, params=query_params)
        return response.json()

    def load(
        self,
        delete_after_import: bool = False,
        allow_failure: bool = False,
        data_directory: os.PathLike = None,
    ):
        """
        Load transforms from files in the data directory and create them in Elasticsearch.
        """
        working_directory = self._get_working_dir(data_directory, create=False)

        # create a map of all transforms by id
        transforms_generator = self._depaginate(
            self.get, key="transforms", page_size=100
        )

        transforms_map = {
            transform["id"]: transform for transform in transforms_generator
        }

        state_file = working_directory / "_state.json"

        transform_files = set(working_directory.glob("*.json"))
        transform_files.discard(state_file)
        for transform_file in transform_files:
            logger.debug(f"Loading {transform_file}")
            transform_id = transform_file.stem
            with transform_file.open("r") as fh:
                loaded_transform = json.load(fh)
            try:
                if transform_id in transforms_map:
                    logger.info(f"Transform {transform_id} already exists.")
                    # the transform already exists; if it's changed we need to delete and recreate it
                    existing_transform = transforms_map[transform_id]
                    for key, loaded_value in loaded_transform.items():
                        if loaded_value != existing_transform[key]:
                            logger.info(
                                f"Transform {transform_id} differs by key {key}, deleting and recreating."
                            )
                            self.stop(transform_id, wait_for_completion=True)
                            self.delete(transform_id)
                            self.create(
                                transform_id,
                                loaded_transform,
                                defer_validation=True,
                            )
                            break
                else:
                    logger.info(
                        f"Creating new transform with id {transform_id}"
                    )
                    self.create(transform_id, loaded_transform, defer_validation=True)
            except HTTPStatusError as e:
                if allow_failure:
                    logger.info(
                        "Experienced an error; continuing because allow_failure is True"
                    )
                else:
                    raise e
            else:
                if delete_after_import:
                    transform_file.unlink()

    def dump(
        self,
        include_managed: bool = False,
        data_directory: os.PathLike = None,
        purge: bool = False,
        force_purge: bool = False,
    ):
        """
        Dump all transforms from Elasticsearch out to files in the data directory.
        """
        working_directory = self._get_working_dir(data_directory, create=True)
        for transform in self._depaginate(self.get, key="transforms", page_size=100):
            if include_managed or not transform.get("_meta", {}).get("managed"):
                # trim off keys that can't be reimported
                for key in ["authorization", "version", "create_time"]:
                    transform.pop(key, None)
                file_path = working_directory / (transform.pop("id") + ".json")
                self._write_file(file_path, transform)

        if purge or force_purge:
            self._purge_untouched_files(force=force_purge)

        # we also need to know whether each transform was started at the time it was dumped
        states = {}
        for transform in self._depaginate(self.stats, key="transforms", page_size=100):
            states[transform["id"]] = transform["state"]
        state_file = working_directory / "_state.json"
        self._write_file(state_file, states)
