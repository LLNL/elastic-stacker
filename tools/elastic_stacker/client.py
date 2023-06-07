import logging

import httpx

logger = logging.getLogger("elastic_stacker")

# subclassing httpx.client to reduce the boilerplate that goes with every request


class APIClient(httpx.Client):
    def raise_for_status(self, response: httpx.Response):
        self.log_for_status(response)
        response.raise_for_status()

    def log_for_status(self, response: httpx.Response):
        if not response.is_success:
            try:
                reason = response.json()  # ["reason"] ?
            except Exception:
                reason = "{} {}".format(response.status_code, response.reason_phrase)
            logger.error(
                "Request to {method} {url} failed with error {reason}".format(
                    method=response.request.method,
                    url=response.request.url,
                    reason=reason,
                )
            )

    def __init__(self, *args, allow_failure: bool = False, **kwargs):
        if allow_failure:
            failure_hook = self.log_for_status
        else:
            failure_hook = self.raise_for_status

        kwargs["event_hooks"] = {"response": [failure_hook]}

        super().__init__(*args, **kwargs)
