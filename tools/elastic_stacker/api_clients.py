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
    def _log_if_error(response: httpx.Response):
        if not response.is_success:
            try:
                reason = response.json()
            except Exception as e2:
                reason = "{} {}".format(response.status_code, response.reason_phrase)
            logger.error(
                "Request to {method} {url} failed: {reason}".format(
                    method=response.request.method,
                    url=response.request.url,
                    reason=reason,
                )
            )

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

    def status(self):
        status_response = self.get("/api/status")
        status_response.raise_for_status()
        status = status_response.json()
        logger.info(
            "{name} status: {summary}".format(
                name=status["name"], summary=status["status"]["overall"]["summary"]
            )
        )
        return status

    def features(self):
        features_response = self.get("/api/features")
        features_response.raise_for_status()
        return features_response.json()

    def spaces(self):
        spaces_response = self.get("/api/spaces/space")
        spaces_response.raise_for_status()
        return spaces_response.json()

    def agent_policies(
        self,
        perPage: int = 20,
        page: int = 1,
        kuery: str = None,
        full: bool = False,
        noAgentCount: bool = False,
    ):
        query_params = {
            "perPage": perPage,
            "page": page,
            "full": full,
            # "noAgentCount": noAgentCount
        }
        if kuery is not None:
            query_params["kuery"] = kuery
        agent_policies_response = self.get(
            "/api/fleet/agent_policies", params=query_params
        )
        logger.debug(agent_policies_response.content)
        agent_policies_response.raise_for_status()
        return agent_policies_response.json()

    def package_policies(self, id: str = None):
        endpoint = "/api/fleet/package_policies"
        if id is not None:
            endpoint = urllib.parse.urljoin(endpoint, id)

        package_policies_response = self.get(endpoint)
        package_policies_response.raise_for_status()
        return package_policies_response.json()

    def create_package_policy(self, id: str, policy: dict):
        endpoint = "/api/fleet/package_policies/{}".format(id)
        response = self.put(endpoint, json=policy)
        logger.debug(response.json())
        response.raise_for_status()
        return response.json()
