import os
import json
import logging
from pathlib import Path
from collections.abc import Iterable

from slugify import slugify

from api_clients import KibanaClient, ElasticsearchClient

logger = logging.getLogger("elastic_stacker")


def dump_saved_objects(
    client: KibanaClient,
    output_directory: Path = Path("./export"),
    types: Iterable = None,
    split_objects: bool = True,
):
    known_types = {t["name"] for t in client.allowed_types()["types"]}
    logger.debug("Known Saved Object types: {}".format(known_types))

    types = set(types) if types is not None else known_types

    invalid_types = types.difference(known_types)
    assert not invalid_types, "Invalid types: {}. Valid types include: {}".format(
        invalid_types, known_types
    )

    logger.info(
        "Starting export of {} from {} into directory {}".format(
            types, client._base_url, output_directory
        )
    )

    export_data = client.export_saved_objects(types=types, exclude_export_details=True)

    if split_objects:
        lines = export_data.splitlines()
        # export_details = lines.pop()
        for obj_type in types:
            obj_type_output_dir = output_directory / "saved_objects" / obj_type
            obj_type_output_dir.mkdir(parents=True, exist_ok=True)
        for line in lines:
            # some things have a "title" and others have a "name", and others have only have an id
            # in order to get a meaningful filename for version control, we have to pick a different field for each.
            # three nested dict.get() calls for three different fields to try
            obj = json.loads(line)
            attrs = obj["attributes"]
            obj_name = attrs.get("title", attrs.get("name", obj.get("id", "NO_NAME")))
            file_name = slugify(obj_name) + ".json"
            output_file = output_directory / "saved_objects" / obj["type"] / file_name
            with output_file.open("w") as fh:
                json.dump(obj, fh, indent=4)
    else:
        output_file = output_directory / "saved_objects.ndjson"
        with open(output_file, "w") as fh:
            fh.write(export_data)


def dump_watches(
    client: ElasticsearchClient,
    output_directory: Path = Path("./export"),
):
    watches_directory = output_directory / "watches"
    watches_directory.mkdir(exist_ok=True)
    for watch in client.depaginate(client.query_watches, "watches", page_size=10):
        file_path = watches_directory / (watch["_id"] + ".json")
        with file_path.open("w") as file:
            file.write(json.dumps(watch["watch"], indent=4))


def dump_transforms(
    client: ElasticsearchClient,
    output_directory: Path = Path("./export"),
    include_managed: bool = False
):
    transforms_directory = output_directory / "transforms"
    transforms_directory.mkdir(exist_ok=True)
    for transform in client.depaginate(client.transforms, "transforms", page_size=100):
        if include_managed or not transform.get("_meta", {}).get("managed"):
            for key in ["authorization", "version", "created_time"]:
                if key in transform:
                    transform.pop(key)
            file_path = transforms_directory / (transform.pop("id") + ".json")
            with file_path.open("w") as file:
                file.write(json.dumps(transform, indent=4))


def dump_pipelines(
    client: ElasticsearchClient,
    output_directory: Path = Path("./export"),
    include_managed: bool = False,
):
    pipelines_directory = output_directory / "pipelines"
    pipelines_directory.mkdir(exist_ok=True)
    for name, pipeline in client.pipelines().items():
        logger.debug(pipeline)
        if include_managed or not pipeline.get("_meta", {}).get("managed"):
            file_path = pipelines_directory / (name + ".json")
            with file_path.open("w") as file:
                file.write(json.dumps(pipeline, indent=4))


def dump_package_policies(
    client: KibanaClient, output_directory: Path = Path("./export")
):
    package_policies_directory = output_directory / "package_policies"
    package_policies_directory.mkdir(exist_ok=True)
    for policy in client.package_policies()["items"]:
        filename = slugify(policy["name"]) + ".json"
        file_path = package_policies_directory / filename
        with file_path.open("w") as file:
            file.write(json.dumps(policy, indent=4))


def dump_agent_policies(
    client: KibanaClient, output_directory: Path = Path("./export"), include_managed:bool = False
):
    agent_policies_directory = output_directory / "agent_policies"
    agent_policies_directory.mkdir(exist_ok=True)
    for policy in client.depaginate(client.agent_policies):
        if include_managed or not policy["is_managed"]:
            filename = slugify(policy["name"]) + ".json"
            file_path = agent_policies_directory / filename
            with file_path.open("w") as file:
                file.write(json.dumps(policy, indent=4))
