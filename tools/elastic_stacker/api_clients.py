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

    def query_watches(
        self,
        offset: int = 0,
        size: int = 10,
        # query:dict=None,
        # sort=None,
        # search_after=None
    ):
        params = {
            "from": offset,
            "size": size,
            # "query": query,
            # "sort": sort,
            # "search_after": search_after,
        }
        watches_response = self.post("/_watcher/_query/watches", json=params)
        logger.debug(str(watches_response.json()))
        watches_response.raise_for_status()
        return watches_response.json()

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

    def allowed_types(self):
        types_response = self.get(
            # this isn't documented anywhere, and starts with an underscore, so it might break without warning
            "/api/kibana/management/saved_objects/_allowed_types"
        )
        types_response.raise_for_status()
        return types_response.json()

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

    def import_saved_objects(
        self,
        file: typing.BinaryIO,
        space_id: str = None,
        create_new_copies: bool = None,
        overwrite: bool = None,
        compatibility_mode: bool = None,
    ):

        base_endpoint = "saved_objects/_import"
        prefix = (
            "api/" if space_id is None else urllib.parse.urljoin("s/", space_id) + "/"
        )
        endpoint = urllib.parse.urljoin(prefix, base_endpoint)

        query_params = {}
        if create_new_copies:
            query_params["createNewCopies"] = create_new_copies
        if overwrite:
            query_params["overwrite"] = overwrite
        if compatibility_mode:
            query_params["compatibilityMode"] = compatibility_mode

        # temporary files get unhelpful or blank names, and Kibana expects specific file extensions on the name
        # so we'll pretend whatever stream we're fed comes from an ndjson file.
        if not file.name:
            upload_filename = "export.ndjson"
        elif not file.name.endswith(".ndjson"):
            upload_filename += ".ndjson"
        else:
            upload_filename = file.name

        files = {"file": (upload_filename, file, "application/ndjson")}

        response = self.post(endpoint, params=query_params, files=files)
        logger.debug(response.content)
        response.raise_for_status()
        return response.json()

    def export_saved_objects(
        self,
        types: Iterable = [],
        objects: Iterable = [],
        space_id: str = None,
        include_references_deep: bool = False,
        exclude_export_details: bool = False,
        parse=False,
    ):
        # TODO: maybe throw a nice friendly exception instead of an AssertionError?
        assert (
            types or objects
        ), """
        You must specify either a list of types or objects to export in the request body.
        see https://www.elastic.co/guide/en/kibana/master/saved-objects-api-export.html for details.
        """

        if space_id is not None:
            endpoint = "/s/%s/api/saved_objects/_export" % space_id
        else:
            endpoint = "/api/saved_objects/_export"

        post_body = {
            "includeReferencesDeep": include_references_deep,
            "excludeExportDetails": exclude_export_details,
        }

        if types:
            post_body.update({"type": list(types)})
        if objects:
            post_body.update({"objects": list(objects)})

        logger.debug(
            "Sending request to {baseurl}/{endpoint} with post body {body}".format(
                baseurl=self._base_url, endpoint=endpoint, body=json.dumps(post_body)
            )
        )
        export_response = self.post(endpoint, json=post_body)
        response_content = export_response.content.decode("utf-8")
        export_response.raise_for_status()
        return response_content
