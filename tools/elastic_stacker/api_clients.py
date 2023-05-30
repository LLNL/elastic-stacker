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

    def pipelines(self, id=None, master_timeout="30s"):

        query_params={"master_timeout": master_timeout} if master_timeout else {}
        endpoint= urllib.parse.urljoin("_ingest/pipeline/", id) if id else "_ingest/pipeline"

        pipelines_response = self.get(endpoint, params=query_params)
        pipelines_response.raise_for_status()
        return pipelines_response.json()
    
    def transforms(self, *args, allow_no_match=True, exclude_generated=False, offset=0, size=100):
        query_params={
            "allow_no_match": allow_no_match,
            "exclude_generated": exclude_generated,
            "from ": offset,
            "size": size
        }
        endpoint= urllib.parse.urljoin("_transform/", ",".join(args))

        transforms_response = self.get(endpoint, params=query_params)
        transforms_response.raise_for_status()
        return transforms_response.json()

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
