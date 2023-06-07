#! python3.9

# from stdlib
import json
import logging
from collections.abc import Iterable
import typing
import urllib.parse

# from pypi
import httpx

logger = logging.getLogger("elastic_stacker")


class ElasticsearchClient(httpx.Client):
    def enrich_policies(self, *args):
        endpoint = "_enrich/policy/{}".format(",".join(args))
        response = self.get(endpoint)
        response.raise_for_status()
        return response.json()

    def create_enrich_policy(self, name: str, policy: dict):
        endpoint = "_enrich/policy/{}".format(name)
        response = self.put(endpoint, json=policy)
        response_data = response.json()
        if "error" in response_data:
            if response_data["error"]["type"] == "resource_already_exists_exception":
                # Elasticsearch won't let you modify enrich policies after creation,
                # and the process for replacing an old one with a new one is a massive pain in the neck
                # so changing existing policies is not supported in version 1, but the user
                # should be warned that the policy hasn't been changed.
                logger.warn(response_data["reason"])
        logger.debug(response_data)
        response.raise_for_status()
        return response_data

    class KibanaClient(httpx.Client):
        def __init__(self, *args, **kwargs):
            if "headers" in kwargs:
                kwargs["headers"].update({"kbn-xsrf": "true"})
            else:
                kwargs["headers"] = {"kbn-xsrf": "true"}

            super().__init__(*args, **kwargs)
