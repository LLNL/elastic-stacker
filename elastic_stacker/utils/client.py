import logging

import httpx

logger = logging.getLogger("elastic_stacker")


class APIClient(httpx.Client):
    """
    A subclass of httpx.Client which encapsulates some of the client-level
    boilerplate like error-handling hooks.
    """

    def raise_for_status(self, response: httpx.Response):
        response.raise_for_status()

    def log_for_status(self, response: httpx.Response):
        if not response.is_success:
            response.read()
            response_doc = response.json()
            if "message" in response_doc:
                reason = response_doc["message"]
            elif "error" in response_doc:
                error = response_doc.get("error")
                if isinstance(response_doc.get("error"), dict):
                    if "type" in error and "reason" in error:
                        reason = "'{type}: {reason}'".format(**error)
                elif isinstance(error, str):
                    reason = error
            else:
                reason = "{} {}".format(response.status_code, response.reason_phrase)
            reason = " ".join(reason.splitlines())

            logger.error(
                "Request to {method} {url} failed: {reason}".format(
                    method=response.request.method,
                    url=response.request.url,
                    reason=reason,
                )
            )

    def __init__(self, *args, **kwargs):
        tls_params = kwargs.pop("tls", {})

        if "cert" in tls_params and "key" in tls_params:
            kwargs["cert"] = (tls_params["cert"], tls_params["key"])
        elif "cert" in tls_params:
            kwargs["cert"] = tls_params["cert"]

        kwargs["event_hooks"] = {
            "response": [self.log_for_status, self.raise_for_status]
        }
        super().__init__(*args, **kwargs)
