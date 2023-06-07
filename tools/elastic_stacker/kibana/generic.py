import httpx


class GenericKibanaController:
    client: httpx.Client

    def __init__(self, client: httpx.Client):
        self.client = client

    def _depaginate(
        self, method: callable, perPage: int = 20, start_page: int = 1, **kwargs
    ):
        page = start_page
        index = 0
        total = float("inf")
        while index < total:
            data = method(perPage=perPage, page=page, **kwargs)
            total = data["total"]
            for item in data["items"]:
                yield item
                index += 1

    def _clean_params(self, params: dict):
        return {k: v for k, v in params.items() if v is not None}
