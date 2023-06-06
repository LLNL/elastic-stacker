import httpx

class KibanaClient(httpx.Client):
    def __init__(self, *args, **kwargs):
        if "headers" in kwargs:
            kwargs["headers"].update({"kbn-xsrf": "true"})
        else:
            kwargs["headers"] = {"kbn-xsrf": "true"}

        super().__init__(*args, **kwargs)
