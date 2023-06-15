import logging
import os

from httpx import HTTPStatusError

from utils.controller import ElasticsearchAPIController

logger = logging.getLogger("elastic_stacker")


class EnrichPolicyController(ElasticsearchAPIController):
    """
    EnrichPolicyController manages the import and export of Enrich Policies.
    https://www.elastic.co/guide/en/elasticsearch/reference/current/enrich-setup.html
    https://www.elastic.co/guide/en/elasticsearch/reference/current/enrich-apis.html
    """

    _resource_directory = "enrich_policies"

    def _build_endpoint(self, *names: str) -> str:
        return "_enrich/policy/{}".format(",".join(names))

    def get(self, *names):
        """
        Get enrich policies by name.
        https://www.elastic.co/guide/en/elasticsearch/reference/current/get-enrich-policy-api.html
        """
        endpoint = self._build_endpoint(*names)
        response = self._client.get(endpoint)
        return response.json()

    def create(self, name: str, policy: dict):
        """
        Create a new enrich policy.
        https://www.elastic.co/guide/en/elasticsearch/reference/current/put-enrich-policy-api.html
        """
        endpoint = self._build_endpoint(name)
        try:
            response = self._client.put(endpoint, json=policy)
            response_data = response.json()
        except HTTPStatusError as e:
            response_data = e.response.json()
            if "error" in response_data and (
                response_data["error"]["type"] == "resource_already_exists_exception"
            ):
                # Elasticsearch won't let you modify enrich policies after creation,
                # and the process for replacing an old one with a new one is a massive pain in the neck
                # so changing existing policies is not supported in version 1, but the user
                # should be warned that the policy hasn't been changed.
                logger.warn(response_data["error"]["reason"])
            else:
                raise e
        return response_data

    def execute(self, policy_name: str, wait_for_completion: bool = None):
        """
        Execute an enrich policy.
        https://www.elastic.co/guide/en/elasticsearch/reference/current/execute-enrich-policy-api.html
        """
        endpoint = "/_enrich/policy/{}/_execute".format(policy_name)
        query_params = {"wait_for_completion": wait_for_completion}
        query_params = self._clean_params(query_params)
        response = self._client.put(endpoint, params=query_params)
        return response.json()

    def dump(self, data_directory: os.PathLike = None, **kwargs):
        """
        Dump enrich policies out to files in the data directory.
        """
        working_directory = self._get_working_dir(data_directory, create=True)
        for policy in self.get()["policies"]:
            filename = policy["config"]["match"]["name"] + ".json"
            policy_file = working_directory / filename
            policy = policy["config"]
            policy["match"].pop("name")
            self._write_file(policy_file, policy)

    def load(
        self,
        data_directory: os.PathLike = None,
        allow_failure: bool = False,
        delete_after_import: bool = False,
        **kwargs
    ):
        """
        Load enrich policies from files in the data directory and create them
        on the Elasticsearch system.
        """
        working_directory = self._get_working_dir(data_directory, create=True)

        if working_directory.is_dir():
            for policy_file in working_directory.glob("*.json"):
                policy = self._read_file(policy_file)
                policy_name = policy_file.stem
                try:
                    response = self.create(policy_name, policy)
                    if (
                        "error" not in response
                        or response["error"].get("type")
                        == "resporce_already_exists_exception"
                    ):
                        logger.warning(
                            "Executing new enrich policy {}".format(policy_name)
                        )
                        # TODO: add a flag to not wait for completion on the execution
                        self.execute(policy_name)
                except HTTPStatusError as e:
                    if allow_failure:
                        logger.info(
                            "Experienced an error; continuing because allow_failure is True"
                        )
                    else:
                        raise e
                else:
                    if delete_after_import:
                        policy_file.unlink()
