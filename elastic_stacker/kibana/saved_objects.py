import os
import logging
import json
import shutil
import typing
import tempfile

import httpx
from slugify import slugify

from elastic_stacker.utils.controller import GenericController

logger = logging.getLogger("elastic_stacker")


class SavedObjectController(GenericController):
    """
    SavedObjectController manages the import and export of Kibana's Saved Objects.
    https://www.elastic.co/guide/en/kibana/current/saved-objects-api.html
    """

    _resource_directory = "saved_objects"

    def types(self):
        """
        Returns the types of saved objects the current user has the ability to export.
        This isn't in the docs (it's only used internally by Kibana) so it might break without warning.
        """
        response = self._client.get(
            "/api/kibana/management/saved_objects/_allowed_types"
        )
        return response.json()

    # TODO: this should return a prepared request.
    def _raw_export_objects(
        self,
        types: typing.Iterable = [],
        objects: typing.Iterable = [],
        space_id: str = None,
        include_references_deep: bool = None,
        exclude_export_details: bool = None,
        stream: bool = False,
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

        # not technically the query parameters this time, but close enough
        post_body = self._clean_params(post_body)

        if types:
            post_body.update({"type": list(types)})
        if objects:
            post_body.update({"objects": list(objects)})

        return endpoint, post_body

    def export(self, *args, **kwargs) -> bytes:
        """
        Export all saved objects as a single bytestring.
        https://www.elastic.co/guide/en/kibana/current/saved-objects-api-export.html
        """
        endpoint, post_body = self._raw_export_objects(*args, **kwargs)
        return self._client.post(endpoint, json=post_body).content

    # TODO: type hinting is not working properly on the output of this function
    def _export_stream(self, *args, **kwargs) -> typing.Iterator[httpx.Response]:
        """
        Saved object exports can get extremely large; we can handle large exports by making a single export request
        and streaming the response back line by line to be written to disk.
        https://www.elastic.co/guide/en/kibana/current/saved-objects-api-export.html
        """
        endpoint, post_body = self._raw_export_objects(*args, **kwargs)
        return self._client.stream("POST", endpoint, json=post_body)

    def import_objects(
        self,
        file: typing.BinaryIO,
        space_id: str = None,
        create_new_copies: bool = None,
        overwrite: bool = None,
        compatibility_mode: bool = None,
        timeout: int = 10,
    ):
        """
        Import saved objects from a previously exported saved objects file.
        https://www.elastic.co/guide/en/kibana/current/saved-objects-api-import.html
        """
        if space_id is not None:
            endpoint = "/s/{}/api/saved_objects/_import".format(space_id)
        else:
            endpoint = "/api/saved_objects/_import"

        query_params = {
            "createNewCopies": create_new_copies,
            "overwrite": overwrite,
            "compatibilityMode": compatibility_mode,
        }
        query_params = self._clean_params(query_params)

        # temporary files get unhelpful or blank names, and Kibana expects specific file extensions on the name
        # so we'll pretend whatever stream we're fed comes from an ndjson file.
        if not file.name:
            upload_filename = "export.ndjson"
        elif not file.name.endswith(".ndjson"):
            upload_filename += ".ndjson"
        else:
            upload_filename = file.name

        files = {"file": (upload_filename, file, "application/ndjson")}

        response = self._client.post(
            endpoint, params=query_params, files=files, timeout=timeout
        )
        return response.json()

    def load(
        self,
        intermediate_file_max_size: float = 5e8,  # 500 MB
        overwrite: bool = True,
        delete_after_import: bool = False,
        allow_failure: bool = False,
        data_directory: os.PathLike = None,
        **kwargs
    ):
        """
        Loads Saved Objects from files on disk and imports them into Kibana.
        Designed for the case where Saved Objects are split into many separate files.
        """
        working_directory = self._get_working_dir(data_directory, create=False)

        if not working_directory.exists():
            return

        with tempfile.SpooledTemporaryFile(
            mode="ab+", max_size=intermediate_file_max_size
        ) as intermediate_file:
            for object_file in working_directory.glob("*/*.json"):
                object = self._read_file(object_file)
                object_string = json.dumps(object)
                intermediate_file.write(str.encode(object_string))
                intermediate_file.write(b"\n")
            # jump back to the start of the file buffer
            intermediate_file.seek(0)
            try:
                self.import_objects(
                    intermediate_file,
                    overwrite=overwrite,
                    create_new_copies=(not overwrite),
                )
            except httpx.HTTPStatusError as e:
                if allow_failure:
                    logger.info(
                        "Experienced an error; continuing because allow_failure is True"
                    )
                else:
                    raise e
            else:
                if delete_after_import:
                    shutil.rmtree(working_directory)

    def dump(self, *types: str, data_directory: os.PathLike = None, **kwargs):
        """
        Dumps saved objects from Kibana.
        In contrast to Kibana's native export functionality, this splits the
        export across many separate files so they can be managed individually
        by e.g. version-control.
        """
        known_types = {t["name"] for t in self.types()["types"]}

        types = set(types) if types else known_types

        invalid_types = types.difference(known_types)
        assert not invalid_types, "Invalid types: {}. Valid types include: {}".format(
            invalid_types, known_types
        )

        working_directory = self._get_working_dir(data_directory, create=True)

        for obj_type in types:
            obj_type_output_dir = working_directory / obj_type
            obj_type_output_dir.mkdir(parents=True, exist_ok=True)

        with self._export_stream(
            types=types, exclude_export_details=True, stream=True
        ) as export_stream:
            for line in export_stream.iter_lines():
                # some things have a "title" and others have a "name", and others have only have an id
                # in order to get a meaningful filename for version control, we have to pick a different field for each.
                # three nested dict.get() calls for three different fields to try
                obj = json.loads(line)
                attrs = obj["attributes"]
                obj_name = attrs.get(
                    "title", attrs.get("name", obj.get("id", "NO_NAME"))
                )
                file_name = slugify(obj_name) + ".json"
                output_file = working_directory / obj["type"] / file_name
                self._write_file(output_file, obj)
