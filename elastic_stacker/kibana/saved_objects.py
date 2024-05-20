import os
import logging
import json
import shutil
import typing
import tempfile
import json

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
        resolve: bool = False,
        retries: dict = None,
    ):
        """
        Import saved objects from a previously exported saved objects file.
        https://www.elastic.co/guide/en/kibana/current/saved-objects-api-import.html

        Also resolves import conflicts, because the API is almost identical:
        https://www.elastic.co/guide/en/kibana/current/saved-objects-api-resolve-import-errors.html

        """

        query_params = {
            "createNewCopies": create_new_copies,
            "overwrite": overwrite,
            "compatibilityMode": compatibility_mode,
        }
        query_params = self._clean_params(query_params)

        if resolve:
            action = "_resolve_import_errors"
            form_data = {"retries": json.dumps(retries)}
            query_params.pop("overwrite", None)
        else:
            action = "_import"
            form_data = {}

        if space_id is not None:
            endpoint = "/s/{space}/api/saved_objects/{action}".format(
                space=space_id, action=action
            )
        else:
            endpoint = "/api/saved_objects/{}".format(action)

        # temporary files get unhelpful or blank names, and Kibana expects specific file extensions on the name
        # so we'll pretend whatever stream we're fed comes from an ndjson file.
        if not (file.name and isinstance(file.name, str)):
            upload_filename = "export.ndjson"
        elif not file.name.endswith(".ndjson"):
            upload_filename += ".ndjson"
        else:
            upload_filename = file.name

        files = {"file": (upload_filename, file, "application/ndjson")}

        response = self._client.post(
            endpoint, params=query_params, files=files, data=form_data, timeout=timeout
        )
        return response.json()

    def load(
        self,
        intermediate_file_max_size: float = 5e8,  # 500 MB
        overwrite: bool = True,
        delete_after_import: bool = False,
        allow_failure: bool = False,
        no_resolve_broken: bool = False,
        data_directory: os.PathLike = None,
        **kwargs,
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
            object_count = 0
            for object_file in working_directory.glob("*/*.json"):
                object = self._read_file(object_file)
                object_string = json.dumps(object)
                intermediate_file.write(str.encode(object_string))
                intermediate_file.write(b"\n")
                object_count += 1
            # jump back to the start of the file buffer
            logger.debug("Preparing to load {count} objects".format(count=object_count))
            intermediate_file.seek(0)
            try:
                results = self.import_objects(
                    intermediate_file,
                    overwrite=overwrite,
                    create_new_copies=(not overwrite),
                )
                logger.info(
                    "Successfully imported {count} out of {total} saved objects.".format(
                        count=results["successCount"], total=object_count
                    )
                )
                failed_ids = []
                retries = []
                for failure in results.pop("errors", []):
                    if "title" in failure["meta"]:
                        obj_name = failure["meta"]["title"]
                    elif "name" in failure["meta"]:
                        obj_name = failure["meta"]["name"]
                    else:
                        obj_name = failure["id"]
                    msg = "Failed to import {obj_type} {obj_name} due to an error of type {err_type}".format(
                        obj_type=failure["type"],
                        obj_name=obj_name,
                        err_type=failure["error"]["type"],
                    )
                    logger.warning(msg, extra={"error": failure["error"]})
                    failed_ids.append(failure["id"])
                    if not no_resolve_broken:
                        retries.append(
                            {
                                "id": failure["id"],
                                "type": failure["type"],
                                "overwrite": overwrite,
                                "ignoreMissingReferences": True,
                            }
                        )
                for success in results.get("successResults", []):
                    retries.append(
                        {
                            "id": success["id"],
                            "type": success["type"],
                            "overwrite": overwrite,
                        }
                    )
                intermediate_file.seek(0)
                resolutions = self.import_objects(
                    intermediate_file,
                    overwrite=overwrite,
                    create_new_copies=(not overwrite),
                    resolve=True,
                    retries=retries,
                )
                logger.info(
                    "Successfully retried {} objects; {} retries failed.".format(
                        resolutions["successCount"], len(resolutions.pop("errors", []))
                    )
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
                    for object_file in working_directory.glob("*/*.json"):
                        obj = self._read_file(object_file)
                        if obj["id"] not in failed_ids:
                            object_file.unlink()

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

        dedup_map = {}
        if not no_dedup:
            # load the filenames and uuids of all existing saved objects so we can
            # deduplicate saved objects that have been renamed.
            for obj_file in working_directory.glob("*/*.json"):
                obj = self._read_file(object_file)
                obj_id = obj['id']
                if obj_id in dedup_map:
                    # resolve the conflict
                    dup_file = dedup_map[objid]
                    dup_obj = self._read_file(dup_file)
                    if obj["updated_at"] > dup_obj["updated_at"]:
                        dup_file.unlink()
                    else:
                        obj_file.unlink()
                        obj_file = dup_file
                dedup_map[obj_id] = obj_file

        with self._export_stream(
            types=types, exclude_export_details=False, stream=True
        ) as export_stream:
            for line in export_stream.iter_lines():
                # some things have a "title" and others have a "name", and others have only have an id
                # in order to get a meaningful filename for version control, we have to pick a different field for each.
                # three nested dict.get() calls for three different fields to try
                obj = json.loads(line)
                if "id" not in obj:
                    # it's the export details
                    # TODO: log this and go on your merry way
                    continue
                attrs = obj["attributes"]
                obj_name = attrs.get(
                    "title", attrs.get("name", "NO_NAME")
                )
                name_slug = slugify(obj_name)
                id_slug = slugify(obj.get("id", "NO_ID"))
                file_name = name_slug + "-" + id_slug + ".json"
                object_type_dir = working_directory / obj["type"]
                if not object_type_dir.is_dir():
                    object_type_dir.mkdir(parents=True)
                output_file = object_type_dir / file_name
                self._write_file(output_file, obj)
                prior_file = dedup_map[obj['id']]
                if output_file != prior_file:
                    prior_file.unlink()


