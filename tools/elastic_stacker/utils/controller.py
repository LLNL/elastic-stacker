import httpx
from pathlib import Path


class GenericController:
    _client: httpx.Client
    _resource_directory: str = ""

    def __init__(
        self,
        client: httpx.Client,
        data_directory: Path,
    ):
        self._client = client
        self._working_directory = data_directory / self._resource_directory

    def _depaginate(self, method, key, page_size=10):
        """
        Elasticsearch presents some of its APIs paginated, so rather than dump all of them
        in one request we can turn that pagination into a nice, Pythonic generator.
        """
        offset = 0
        results = {"count": float("inf")}
        while offset < results["count"]:
            # Kibana's paginated APIs don't use "offset"; I should change
            # the function signature for those methods to retain consistency.
            results = method(offset=offset, size=page_size)
            for result in results[key]:
                offset += 1
                yield result

    def _clean_params(self, params: dict):
        # httpx includes query parameters even if their value is None
        # (see https://www.python-httpx.org/compatibility/#query-parameters).
        # ordinarily I'd add a pre-request hook that would remove these parameters,
        # but httpx also does not let the user modify the request before it's sent.
        # (see https://www.python-httpx.org/compatibility/#event-hooks)
        return {k: v for k, v in params.items() if v is not None}

    def _create_working_dir(self):
        self._working_directory.mkdir(parents=True, exist_ok=True)
