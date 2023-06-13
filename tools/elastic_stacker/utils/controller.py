import os

import httpx
from pathlib import Path


class GenericController:
    _client: httpx.Client
    _options: dict
    _resource_directory: str = ""

    def __init__(self, client: httpx.Client, **options):
        self._client = client
        self._options = options

    def _clean_params(self, params: dict):
        # httpx includes query parameters even if their value is None
        # (see https://www.python-httpx.org/compatibility/#query-parameters).
        # ordinarily I'd add a pre-request hook that would remove these parameters,
        # but httpx also does not let the user modify the request before it's sent.
        # (see https://www.python-httpx.org/compatibility/#event-hooks)
        return {k: v for k, v in params.items() if v is not None}

    def _get_working_dir(self, data_directory: os.PathLike = None, create=False):
        if data_directory is None:
            data_directory = self._options.get("data_directory")
        else:
            data_directory = Path(data_directory)

        working_directory = data_directory / self._resource_directory

        if create:
            working_directory.mkdir(parents=True, exist_ok=True)

        return working_directory


class ElasticsearchAPIController(GenericController):
    def _depaginate(self, method, key, page_size=10, **kwargs):
        """
        Elasticsearch presents some of its APIs paginated, so rather than dump all of them
        in one request we can turn that pagination into a nice, Pythonic generator.
        """
        offset = 0
        results = {"count": float("inf")}
        while offset < results["count"]:
            results = method(offset=offset, size=page_size, **kwargs)
            for result in results[key]:
                offset += 1
                yield result


class FleetAPIController(GenericController):
    def _depaginate(self, method, perPage: int = None, **kwargs):
        """
        Fleet Server has paginated APIs too, but where Elasticsearch accepts
        an offset parameter ("from"), Fleet accepts only a page number,
        so the pagination logic has to be a little different.
        """
        page = 1
        results = {"items": True}
        while results["items"]:  # returns the empty list when complete
            results = method(page=page, perPage=perPage, **kwargs)
            for result in results["items"]:
                yield result
            page += 1
