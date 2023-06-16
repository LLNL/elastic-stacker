import json
import os
import re

import httpx
from pathlib import Path


class GenericController:
    """
    Stacker groups API resources as "controllers" which have methods
    for handling the specific behavior for interacting with those API
    resources.

    These controllers have some methods in common related to file reading and
    writing and argument processing, so they all inherit from this base class.
    """

    _client: httpx.Client
    _options: dict
    _resource_directory: str = ""
    _subs: dict

    def __init__(self, client: httpx.Client, subs: dict = {}, **options):
        self._client = client
        self._options = options
        self._subs = subs
        for name, sub in self._subs.items():
            self._subs[name]["search"] = re.compile(sub["search"])

    def _run_substitutions(self, value):
        for name, sub in sorted(self._subs.items()):
            search = sub["search"]
            replace = sub["replace"]
            value = re.sub(search, replace, value)
        return value

    def _write_file(self, path: os.PathLike, obj: dict):
        output = json.dumps(obj, indent=4, sort_keys=True)
        output = self._run_substitutions(output)
        with open(path, "w") as fh:
            fh.write(output)

    def _read_file(self, path: os.PathLike):
        with open(path, "r") as fh:
            value = fh.read()
        value = self._run_substitutions(value)
        return json.loads(value)

    def _clean_params(self, params: dict):
        # httpx includes query parameters even if their value is None
        # (see https://www.python-httpx.org/compatibility/#query-parameters).
        # usually I'd add a pre-request hook to remove null parameters, but
        # httpx also does not let the user modify the request before it's sent
        # (see https://www.python-httpx.org/compatibility/#event-hooks)
        return {k: v for k, v in params.items() if v is not None}

    def _get_working_dir(
        self, data_directory: os.PathLike = None, create=False
    ) -> Path:
        if data_directory is None:
            data_directory = self._options.get("data_directory")
        else:
            data_directory = Path(data_directory)

        working_directory = data_directory / self._resource_directory

        if create:
            working_directory.mkdir(parents=True, exist_ok=True)

        if not working_directory.is_dir():
            raise NotADirectoryError(
                "The data_directory {} is not valid directory".format(working_directory)
            )

        return working_directory


class ElasticsearchAPIController(GenericController):
    def _depaginate(self, method, key, page_size=10, **kwargs):
        """
        Elasticsearch presents some of its APIs paginated, so rather than dump
        all of them in one request we can turn that pagination into a nice,
        Pythonic generator.
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
