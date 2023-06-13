import logging
import json
import os
from pathlib import Path

from httpx import HTTPStatusError

from utils.controller import ElasticsearchAPIController

logger = logging.getLogger("elastic_stacker")


# https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules.html#_static_index_settings
# These settings cannot be changed on an index without closing the index
# they are expressed as tuples, so they can index into a nested dict.


class IndexController(ElasticsearchAPIController):
    _resource_directory = "indices"

    def get(
        self,
        *index_names: str,
        expand_wildcards: bool = None,
        features: list[str] = None,
        flat_settings: bool = None,
        include_defaults: bool = None,
        ignore_unavailable: bool = None,
        local: bool = None,
        master_timeout: bool = None,
    ):
        endpoint = ",".join(index_names)
        query_params = {
            "expand_wildcards": expand_wildcards,
            "features": features,
            "flat_settings": flat_settings,
            "include_defaults": include_defaults,
            "ignore_unavailable": ignore_unavailable,
            "local": local,
            "master_timeout": master_timeout,
        }
        query_params = self._clean_params(query_params)
        response = self._client.get(endpoint, params=query_params)
        return response.json()

    def get_settings(
        self,
        index_name: str,
        setting: str = "",
        allow_no_indices: bool = None,
        expand_wildcards: str = None,
        flat_settings: bool = None,
        include_defaults: bool = None,
        ignore_unavailable: bool = None,
        master_timeout: str = None,
    ) -> dict:
        query_params = {
            "allow_no_indices": allow_no_indices,
            "expand_wildcards": expand_wildcards,
            "flat_settings": flat_settings,
            "include_defaults": include_defaults,
            "ignore_unavailable": ignore_unavailable,
            "master_timeout": master_timeout,
        }
        query_params = self._clean_params(query_params)
        endpoint = "/{}/_settings/{}".format(index_name, setting)
        response = self._client.get(endpoint, params=query_params)
        return response.json()

    def get_mappings(
        self,
        *index_names: str,
        allow_no_indices: bool = None,
        expand_wildcards: str = None,
        ignore_unavailable: bool = None,
        local: bool = None,
        master_timeout: str = None,
    ) -> dict:
        endpoint = "{}/_mapping".format(",".join(index_names))
        query_params = {
            "allow_no_indices": allow_no_indices,
            "expand_wildcards": expand_wildcards,
            "ignore_unavailable": ignore_unavailable,
            "local": local,
            "master_timeout": master_timeout,
        }
        query_params = self._clean_params(query_params)
        response = self._client.get(endpoint, params=query_params)
        return response.json()

    def get_aliases(
        self,
        *index_names: str,
        alias_name: str = "",
        allow_no_indices: bool = None,
        expand_wildcards: str = None,
        ignore_unavailable: bool = None,
        local: bool = None,
    ) -> dict:
        endpoint = "{}/_alias/{}".format(",".join(index_names), alias_name)
        query_params = {
            "allow_no_indices": allow_no_indices,
            "expand_wildcards": expand_wildcards,
            "ignore_unavailable": ignore_unavailable,
            "local": local,
        }
        query_params = self._clean_params(query_params)
        response = self._client.get(endpoint, params=query_params)
        return response.json()

    def create(
        self,
        name: str,
        index: dict,
        wait_for_active_shards: str = None,
        master_timeout: str = None,
        timeout: str = None,
    ):
        query_params = {
            "wait_for_active_shards": wait_for_active_shards,
            "master_timeout": master_timeout,
            "timeout": timeout,
        }
        query_params = self._clean_params(query_params)
        response = self._client.put(name, json=index, params=query_params)
        return response.json()

    def upsert_alias(
        self,
        alias: dict,
        name: str,
        *targets: str,
        master_timeout: str = None,
        timeout: str = None,
    ):
        endpoint = "/{}/_alias/{}".format(",".join(targets), name)
        query_params = {
            "master_timeout": master_timeout,
            "timeout": timeout,
        }
        query_params = self._clean_params(query_params)
        response = self._client.put(endpoint, json=alias, params=query_params)
        return response.json()

    def update_mapping(
        self,
        *targets: str,
        mapping: dict = {},
        allow_no_indices: bool = None,
        expand_wildcards: bool = None,
        ignore_unavailable: bool = None,
        master_timeout: str = None,
        timeout: str = None,
        write_index_only: bool = None,
    ):
        endpoint = "/{}/_mapping/".format(",".join(targets))
        query_params = {
            "allow_no_indices": allow_no_indices,
            "expand_wildcards": expand_wildcards,
            "ignore_unavailable": ignore_unavailable,
            "master_timeout": master_timeout,
            "timeout": timeout,
            "write_index_only": write_index_only,
        }
        query_params = self._clean_params(query_params)
        response = self._client.put(endpoint, json=mapping, params=query_params)
        return response.json()

    def update_settings(
        self,
        *targets: str,
        settings: dict = {},
        allow_no_indices: bool = None,
        expand_wildcards: bool = None,
        flat_settings: bool = None,
        ignore_unavailable: bool = None,
        preserve_existing: bool = None,
        master_timeout: str = None,
        timeout: str = None,
    ):
        endpoint = "/{}/_settings/".format(",".join(targets))
        query_params = {
            "allow_no_indices": allow_no_indices,
            "expand_wildcards": expand_wildcards,
            "flat_settings": flat_settings,
            "ignore_unavailable": ignore_unavailable,
            "preserve_existing": preserve_existing,
            "master_timeout": master_timeout,
            "timeout": timeout,
        }
        query_params = self._clean_params(query_params)
        response = self._client.put(endpoint, json=settings, params=query_params)
        return response.json()

    def close(
        self,
        *targets: str,
        allow_no_indices: bool = None,
        expand_wildcards: bool = None,
        ignore_unavailable: bool = None,
        wait_for_active_shards: bool = None,
        master_timeout: str = None,
        timeout: str = None,
    ):
        endpoint = "/{}/_close".format(",".join(targets))
        query_params = {
            "allow_no_indices": allow_no_indices,
            "expand_wildcards": expand_wildcards,
            "ignore_unavailable": ignore_unavailable,
            "wait_for_active_shards": wait_for_active_shards,
            "master_timeout": master_timeout,
            "timeout": timeout,
        }
        query_params = self._clean_params(query_params)
        response = self._client.post(endpoint, params=query_params)
        return response.json()

    def open(
        self,
        *targets: str,
        allow_no_indices: bool = None,
        expand_wildcards: bool = None,
        ignore_unavailable: bool = None,
        wait_for_active_shards: bool = None,
        master_timeout: str = None,
        timeout: str = None,
    ):
        endpoint = "/{}/_close".format(",".join(targets))
        query_params = {
            "allow_no_indices": allow_no_indices,
            "expand_wildcards": expand_wildcards,
            "ignore_unavailable": ignore_unavailable,
            "wait_for_active_shards": wait_for_active_shards,
            "master_timeout": master_timeout,
            "timeout": timeout,
        }
        query_params = self._clean_params(query_params)
        response = self._client.post(endpoint, params=query_params)
        return response.json()

    def update_index(
        self,
        index_name: str,
        index: dict,
        wait_for_active_shards: str = None,
        master_timeout: str = None,
        timeout: str = None,
        close_index_to_modify_settings: bool = False,
    ):
        for alias_name, alias in index["aliases"].items():
            self.upsert_alias(
                alias,
                alias_name,
                index_name,
                master_timeout=master_timeout,
                timeout=timeout,
            )
        self.update_mapping(
            index_name,
            mapping=index["mappings"],
            timeout=timeout,
            master_timeout=master_timeout,
        )
        if close_index_to_modify_settings:
            logger.warning(
                "Briefly closing the index {} to modify its settings".format(index_name)
            )
            self.close(index_name, master_timeout=master_timeout, timeout=timeout)
            self.update_settings(
                index_name,
                settings=index["settings"],
                master_timeout=master_timeout,
                timeout=timeout,
            )
            self.open(index_name)
        else:
            logger.info(
                "Not modifying settings for index {} because close_index_to_modify was false"
            )

    def dump(
        self,
        include_managed: bool = False,
        data_directory: os.PathLike = None,
        **kwargs,
    ):
        working_directory = self._get_working_dir(data_directory, create=True)

        indices = self.get("_all")

        for name, index in indices.items():
            if name.startswith(".") and not include_managed:
                continue
            for key in ["creation_date", "uuid", "provided_name", "version"]:
                if key in index["settings"]["index"]:
                    index["settings"]["index"].pop(key)
            index_file = working_directory / (name + ".json")
            with index_file.open("w") as fh:
                fh.write(json.dumps(index, indent=4, sort_keys=True))

    def load(
        self,
        data_directory: os.PathLike = None,
        delete_after_import: bool = False,
        allow_failure: bool = False,
        close_indices_to_modify_settings: bool = False,
        **kwargs,
    ):
        working_directory = self._get_working_dir(data_directory, create=False)
        existing_indices = self.get("_all")

        for index_file in working_directory.iterdir():
            index_name = index_file.stem
            with index_file.open("r") as fh:
                index = json.load(fh)
            try:
                if index_name in existing_indices:
                    logger.warn("Updating index {}".format(index_name))
                    self.update_index(
                        index_name,
                        index,
                        close_index_to_modify_settings=close_indices_to_modify_settings,
                    )
                else:
                    logger.warn("Creating new index {}".format(index_name))
                    self.create(index_name, index)
            except HTTPStatusError as e:
                if allow_failure:
                    logger.info(
                        "Experienced an error; continuing because allow_failure is True"
                    )
                else:
                    raise e
            else:
                if delete_after_import:
                    index_file.unlink()
