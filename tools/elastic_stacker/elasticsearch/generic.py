import httpx


class GenericElasticsearchController:
    client: httpx.Client
    def __init__(self, client: httpx.Client):
        self.client = client

    def _clean_headers(self, headers: dict):
        # httpx includes query parameters even if their value is None
        # (see https://www.python-httpx.org/compatibility/#query-parameters).
        # ordinarily I'd add a pre-request hook that would remove these parameters,
        # but httpx also does not let the user modify the request before it's sent.
        # (see https://www.python-httpx.org/compatibility/#event-hooks)
        return {k:v for k,v in headers.items() if v is not None}
