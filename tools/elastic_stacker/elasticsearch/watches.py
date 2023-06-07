import logging
import json
from pathlib import Path

from .generic import GenericElasticsearchController

logger = logging.getLogger("elastic_stacker")


class WatchController(GenericElasticsearchController):
    _resource_directory = "watches"

    def query(
        self,
        offset: int = None,
        size: int = None,
        query: dict = None,
        sort: dict = None,
        search_after: dict = None,
    ):
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

    # TODO
    def create():
        pass

    # TODO
    def load():
        pass

    def dump(self, output_directory: Path):
        self._create_working_dir()
        for watch in self._depaginate(self.query, "watches", page_size=10):
            file_path = self._working_directory / (watch["_id"] + ".json")
            with file_path.open("w") as file:
                file.write(json.dumps(watch["watch"], indent=4, sort_keys=True))
