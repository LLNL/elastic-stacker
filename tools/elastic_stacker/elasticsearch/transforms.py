import logging
import json
import pathlib

from .generic import GenericElasticsearchController

logger = logging.getLogger("elastic_stacker")


class TransformController(GenericElasticsearchController):
    base_endpoint = "_transform"
    resource_directory = "transforms"

    def _build_endpoint(self, *ids: str) -> str:
        id_string = ",".join(ids)
        return "{}/{}".format(self.base_endpoint, id_string)

    def get(
        self,
        *ids: str,
        allow_no_match: bool = None,
        exclude_generated: bool = None,
        offset: int = None,
        size: int = None
    ):
        query_params = {
            "allow_no_match": allow_no_match,
            "exclude_generated": exclude_generated,
            "from": offset,
            "size": size,
        }
        endpoint = self._build_endpoint(*ids)
        query_params = self._clean_params(query_params)
        response = self.client.get(endpoint, params=query_params)
        return response.json()

    def create(
        self,
        id: str,
        transform: dict,
        defer_validation: bool = None,
        timeout: str = None,
    ):
        endpoint = self._build_endpoint(id)
        query_params = {"defer_validation": defer_validation, "timeout": timeout}
        query_params = self._clean_params(query_params)

        response = self.client.put(endpoint, json=transform, params=query_params)
        return response.json()

    def stats(
        self,
        *ids: str,
        allow_no_match: bool = None,
        offset: int = None,
        size: int = None
    ):
        if not ids:
            ids = ["_all"]
        endpoint = self._build_endpoint(*ids) + "/_stats"
        query_params = {
            "allow_no_match": allow_no_match,
            "from": offset,
            "size": size,
        }
        query_params = self._clean_params(query_params)
        response = self.client.get(endpoint, params=query_params)
        return response.json()

    def set_state(
        self,
        id: str,
        target_state: str,
        timeout: str = None,
    ):
        started_states = {"started", "indexing"}
        stopped_states = {"failed", "stopped", "stopping", "aborted"}
        allowed_states = started_states.union(stopped_states)

        if target_state not in allowed_states:
            err_msg = "{} is not a valid state for transforms; acceptable values are {}.".format(
                target_state, allowed_states
            )
            logger.error(err_msg)
            # TODO: this should be some more specific subclass of Exception
            raise Exception(err_msg)

        current_stats = self.stats(id)
        current_state = current_stats["transforms"][0]["state"]

        if target_state in started_states:
            if current_state in started_states:
                logger.debug("transform {} is already started".format(id))
            else:
                self.start(id, timeout=timeout)
        else:  # target_state in stopped_states
            if current_state in stopped_states:
                logger.debug("transform {} is already stopped".format(id))
            else:
                self.stop(id, timeout=timeout)

    def start(
        self,
        id: str,
        from_time: str = None,
        timeout: str = None,
    ):
        endpoint = self._build_endpoint(id) + "/_start"
        query_params = {"from": from_time, "timeout": timeout}
        query_params = self._clean_params(query_params)
        response = self.client.post(endpoint, params=query_params)
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
        endpoint = self._build_endpoint(id) + "/_stop"

        query_params = {
            "force": force,
            "allow_no_match": allow_no_match,
            "wait_for_checkpoint": wait_for_checkpoint,
            "wait_for_completion": wait_for_completion,
            "timeout": timeout,
        }
        query_params = self._clean_params(query_params)

        response = self._client.post(endpoint, params=query_params)
        return response.json()

    def delete(
        self,
        id: str,
        force: bool = None,
        delete_dest_index: bool = None,
        timeout: str = None,
    ):
        endpoint = self._build_endpoint(id)

        query_params = {
            "force": force,
            "delete_dest_index": delete_dest_index,
            "timeout": timeout,
        }

        query_params = self._clean_params(query_params)

        response = self.client.delete(endpoint, params=query_params)
        return response.json()

    def update(
        self,
        id: str,
        transform: dict,
        defer_validation: bool = None,
        timeout: str = None,
    ):
        endpoint = self._build_endpoint(id) + "/_update"

        query_params = {"defer_validation": defer_validation, "timeout": timeout}
        query_params = self._clean_params(query_params)
        response = self.client.post(endpoint, json=transform, params=query_params)
        return response.json()

    def load(self, data_directory: pathlib.Path, delete_after_import: bool = False):
        transforms_directory = data_directory / self.resource_directory
        if transforms_directory.is_dir():
            # create a map of all transforms by id
            transforms_generator = self._depaginate(
                self.get, key="transforms", page_size=100
            )

            transforms_map = {
                transform["id"]: transform for transform in transforms_generator
            }

            stats_file = transforms_directory / "_stats.json"
            with stats_file.open("r") as fh:
                reference_stats = json.load(fh)

            current_stats = {
                t["id"]: t
                for t in self._depaginate(self.stats, "transforms", page_size=100)
            }

            for transform_file in transforms_directory.glob("*.json"):
                if transform_file == stats_file:
                    continue
                logger.debug("Loading {}".format(transform_file))
                transform_id = transform_file.stem
                with transform_file.open("r") as fh:
                    loaded_transform = json.load(fh)
                if transform_id in transforms_map:
                    logger.info("Transform {} already exists.".format(transform_id))
                    # the transform already exists; if it's changed we need to delete and recreate it
                    existing_transform = transforms_map[transform_id]
                    for key, loaded_value in loaded_transform.items():
                        if loaded_value != existing_transform[key]:
                            logger.info(
                                "Transform {} differs by key {}, deleting and recreating.".format(
                                    transform_id, key
                                )
                            )
                            self.stop(transform_id, wait_for_completion=True)
                            self.delete(transform_id)
                            self.create(
                                transform_id, loaded_transform, defer_validation=True
                            )
                            break
                else:
                    logger.info(
                        "Creating new transform with id {}".format(transform_id, key)
                    )
                    self.create(transform_id, loaded_transform, defer_validation=True)

    def dump(
        self,
        data_directory: pathlib.Path,
        include_managed: bool = False,
    ):
        transforms_directory = data_directory / self.resource_directory
        transforms_directory.mkdir(exist_ok=True, parents=True)

        for transform in self._depaginate(self.get, key="transforms", page_size=100):
            if include_managed or not transform.get("_meta", {}).get("managed"):
                # trim off keys that can't be reimported
                for key in ["authorization", "version", "create_time"]:
                    if key in transform:
                        transform.pop(key)
                file_path = transforms_directory / (transform.pop("id") + ".json")
                with file_path.open("w") as file:
                    file.write(json.dumps(transform, indent=4, sort_keys=True))

        # we also need to know whether each transform was started at the time it was dumped
        stats = {}
        for transform in self._depaginate(self.stats, key="transforms", page_size=100):
            stats[transform["id"]] = transform
        transform_stats_file = transforms_directory / "_stats.json"
        with transform_stats_file.open("w") as file:
            file.write(json.dumps(stats, indent=4, sort_keys=True))
