import logging
import os

from httpx import HTTPStatusError

from elastic_stacker.utils.controller import ElasticsearchAPIController

logger = logging.getLogger("elastic_stacker")


def substitute_passwords(
    d: dict, password_map: dict = {}, watch_id: str = ""
):
    """
    Search through a nested dict looking for redacted passwords.
    If it finds one, substitutes it with a value, which may be provided
    from a mapping or prompted from the user.
    """
    for key, value in d.items():
        if isinstance(value, dict):
            d[key] = substitute_passwords(
                value, password_map=password_map, prompt=prompt, watch_id=watch_id
            )
        if value == "::es_redacted::":
            username = d.get("username", "")
            if username in password_map:
                d[key] = password_map[username]
            else:
                logger.error(
                    f"No password found for user {username} in watch {watch_id}"
                    f"Set `options.watcher_users.{username}` in the config file, "
                    "or use the `--prompt-credentials flag to enter them at "
                    "the command line.`"
                )
                raise KeyError(
                    f"No password for {username} in watch {watch_id}"
                )
    return d


class WatchController(ElasticsearchAPIController):
    """
    WatchController manages the import and export of Watches from Elasticsearch.
    https://www.elastic.co/guide/en/elasticsearch/reference/current/watcher-api.html
    """

    _resource_directory = "watches"

    def query(
        self,
        offset: int = None,
        size: int = None,
        query: dict = None,
        sort: dict = None,
        search_after: dict = None,
    ):
        """
        Get a list of all watches that match a certain query.
        https://www.elastic.co/guide/en/elasticsearch/reference/current/watcher-api-query-watches.html
        """
        post_body = {
            "from": offset,
            "size": size,
            "query": query,
            "sort": sort,
            "search_after": search_after,
        }
        post_body = self._clean_params(post_body)
        response = self._client.post("/_watcher/_query/watches", json=post_body)
        return response.json()

    def create(self, watch_id: str, watch: dict, active: bool = None):
        """
        Create a new watch, or update an existing one.
        https://www.elastic.co/guide/en/elasticsearch/reference/current/watcher-api-put-watch.html
        """
        endpoint = f"_watcher/watch/{watch_id}"
        query_params = {"active": active}
        query_params = self._clean_params(query_params)
        response = self._client.put(endpoint, json=watch, params=query_params)
        return response.json()

    def load(
        self,
        data_directory: os.PathLike = None,
        allow_failure: bool = True,
        delete_after_import: bool = False,
    ):
        """
        Load in watches from files in the data directory and create them in Elasticsearch.
        """
        working_directory = self._get_working_dir(data_directory, create=False)

        for watch_file in working_directory.glob("*.json"):
            watch_id = watch_file.stem

            watch = self._read_file(watch_file)

            watch = substitute_passwords(
                watch,
                password_map=self._options.get("watcher_users", {}),
                watch_id=watch_id,
            )

            logger.info(f"Loading watch {watch_id}")
            # TODO store watch active state on dump
            try:
                self.create(watch_id, watch)
            except HTTPStatusError as e:
                if allow_failure:
                    logger.info(
                        f"Experienced an error importing watch {watch_id}; "
                        "continuing because allow_failure is True"
                    )
                else:
                    raise e
            else:
                if delete_after_import:
                    watch_file.unlink()

    def dump(
        self,
        data_directory: os.PathLike = None,
        purge: bool = False,
        force_purge: bool = False,
    ):
        """
        Dump out Watches from Elasticsearch to files in the data directory.
        """
        working_directory = self._get_working_dir(data_directory, create=True)
        for watch in self._depaginate(self.query, "watches", page_size=10):
            file_path = working_directory / (watch["_id"] + ".json")
            self._write_file(file_path, watch["watch"])
        if purge or force_purge:
            self._purge_untouched_files(force=force_purge)
