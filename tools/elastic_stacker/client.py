import logging

import httpx

logger = logging.getLogger("elastic_stacker")

# subclassing httpx.client to reduce the boilerplate that goes with every request


class APIClient(httpx.Client):
    def raise_for_status(self, response: httpx.Response):
        response.raise_for_status()

    def log_for_status(self, response: httpx.Response):
        if not response.is_success:
            response.read()
            response_doc = response.json()
            error = response_doc.get("error")
            if error:
                reason = "'{type}: {reason}'".format(**error)
            else:
                reason = "{} {}".format(response.status_code, response.reason_phrase)

            logger.error(
                "Request to {method} {url} failed: {reason}".format(
                    method=response.request.method,
                    url=response.request.url,
                    reason=reason,
                )
            )

    def __init__(self, *args, **kwargs):
        kwargs["event_hooks"] = {
            "response": [self.log_for_status, self.raise_for_status]
        }
        super().__init__(*args, **kwargs)
