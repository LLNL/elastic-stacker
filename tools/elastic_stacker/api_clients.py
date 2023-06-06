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

    def pipelines(self, id=None, master_timeout="30s"):

        query_params = {"master_timeout": master_timeout} if master_timeout else {}
        endpoint = (
            urllib.parse.urljoin("_ingest/pipeline/", id) if id else "_ingest/pipeline"
        )

        pipelines_response = self.get(endpoint, params=query_params)
        pipelines_response.raise_for_status()
        return pipelines_response.json()

    def create_pipeline(
        self,
        id: str,
        pipeline: dict,
        if_version: int = None,
        master_timeout: str = None,
        timeout: str = None,
    ):
        endpoint = urllib.parse.urljoin("_ingest/pipeline/", id)
        query_params = {}
        if if_version is not None:
            query_params["if_version"] = if_version
        if master_timeout is not None:
            query_params["master_timeout"] = master_timeout
        if timeout is not None:
            query_params["timeout"] = timeout

        response = self.put(endpoint, json=pipeline, params=query_params)
        logger.debug(response.json())
        response.raise_for_status()
        return response.json()

    def transforms(
        self, *args, allow_no_match=True, exclude_generated=False, offset=0, size=100
    ):
        query_params = {
            "allow_no_match": allow_no_match,
            "exclude_generated": exclude_generated,
            "from": offset,
            "size": size,
        }
        endpoint = urllib.parse.urljoin("_transform/", ",".join(args))

        transforms_response = self.get(endpoint, params=query_params)
        transforms_response.raise_for_status()
        return transforms_response.json()

    def transform_stats(self, *args, allow_no_match=True, offset=0, size=100):
        query_params = {
            "allow_no_match": allow_no_match,
            "from": offset,
            "size": size,
        }

        if not args:
            args = ["_all"]
        endpoint = "_transform/{}/_stats".format(",".join(args))

        transforms_response = self.get(endpoint, params=query_params)
        logger.debug(transforms_response.json())
        transforms_response.raise_for_status()
        return transforms_response.json()

    def create_transform(
        self,
        id: str,
        transform: dict,
        defer_validation: bool = None,
        timeout: str = None,
    ):
        endpoint = urllib.parse.urljoin("_transform/", id)

        query_params = {"defer_validation": defer_validation, "timeout": timeout}
        query_params = {k: v for k, v in query_params.items() if v is not None}
        response = self.put(endpoint, json=transform, params=query_params)
        logger.debug(response.json())
        response.raise_for_status()
        return response.json()

    def set_transform_state(
        self,
        id: str,
        target_state: str,
        timeout: str = None,
    ):
        started_states = {"started", "indexing"}
        stopped_states = {"failed", "stopped", "stopping", "aborted"}
        allowed_states = started_states.union(stopped_states)

        if target_state not in allowed_states:
            err_msg = "{} is not a valid state for transforms; acceptable values are {}.".format(
                target_state, allowed_states
            )
            logger.error(err_msg)
            # TODO: this should be some more specific subclass of Exception
            raise Exception(err_msg)

        current_stats = self.transform_stats(id)
        current_state = current_stats["transforms"][0]["state"]

        if target_state in started_states:
            if current_state in started_states:
                logger.debug("transform {} is already started".format(id))
            else:
                self.start_transform(id, timeout=timeout)
        else:  # target_state in stopped_states
            if current_state in stopped_states:
                logger.debug("transform {} is already stopped".format(id))
            else:
                self.stop_transform(id, timeout=timeout)

    def start_transform(
        self,
        id: str,
        from_time: str = None,
        timeout: str = None,
    ):
        endpoint = "_transform/{}/_start".format(id)
        query_params = {"from": from_time, "timeout": timeout}
        query_params = {k: v for k, v in query_params.items() if v is not None}

        response = self.post(endpoint, params=query_params)
        logger.debug(response.json())
        response.raise_for_status()
        return response.json()

    def stop_transform(
        self,
        id: str,
        force: bool = None,
        allow_no_match: bool = None,
        wait_for_checkpoint: bool = None,
        wait_for_completion: bool = None,
        timeout: str = None,
    ):
        endpoint = "_transform/{}/_stop".format(id)
        query_params = {
            "force": force,
            "allow_no_match": allow_no_match,
            "wait_for_checkpoint": wait_for_checkpoint,
            "wait_for_completion": wait_for_completion,
            "timeout": timeout,
        }
        query_params = {k: v for k, v in query_params.items() if v is not None}

        response = self.post(endpoint, params=query_params)
        logger.debug(response.json())
        response.raise_for_status()
        return response.json()

    def delete_transform(
        self,
        id: str,
        force: bool = None,
        delete_dest_index: bool = None,
        timeout: str = None,
    ):
        endpoint = urllib.parse.urljoin("_transform/", id)
        query_params = {
            "force": force,
            "delete_dest_index": delete_dest_index,
            "timeout": timeout,
        }
        query_params = {k: v for k, v in query_params.items() if v is not None}
        response = self.delete(endpoint, params=query_params)
        logger.debug(response.json())
        response.raise_for_status()
        return response.json()

    def update_transform(
        self,
        id: str,
        transform: dict,
        defer_validation: bool = None,
        timeout: str = None,
    ):
        endpoint = "_transform/{}/_update".format(id)

        query_params = {"defer_validation": defer_validation, "timeout": timeout}
        query_params = {k: v for k, v in query_params.items() if v is not None}
        response = self.post(endpoint, json=transform, params=query_params)
        logger.debug(response.json())
        response.raise_for_status()
        return response.json()

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

    def depaginate(self, method, key, page_size=10):
        """
        Watches are a paginated API, so rather than dump all of them
        in one request we can turn that pagination into a nice, Pythonic generator
        """
        offset = 0
        results = {"count": float("inf")}
        while offset < results["count"]:
            results = method(offset=offset, size=page_size)
            for result in results[key]:
                offset += 1
                yield result


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

    def depaginate(
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
