import os
import json
import logging
import tempfile
from pathlib import Path
from collections.abc import Iterable
import httpx

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
            file.write(json.dumps(watch["watch"], indent=4, sort_keys=True))


def dump_enrich_policies(
    client: ElasticsearchClient, output_directory: Path = Path("./export")
):
    enrich_policies_directory = output_directory / "enrich_policies"
    enrich_policies_directory.mkdir(exist_ok=True)
    for policy in client.enrich_policies()["policies"]:
        filename = policy["config"]["match"]["name"] + ".json"
        policy_file = enrich_policies_directory / filename
        policy = policy["config"]
        policy["match"].pop("name")
        with policy_file.open("w") as fh:
            fh.write(json.dumps(policy, sort_keys=True, indent=4))


def dump_transforms(
    client: ElasticsearchClient,
    output_directory: Path = Path("./export"),
    include_managed: bool = False,
):
    transforms_directory = output_directory / "transforms"
    transforms_directory.mkdir(exist_ok=True)
    for transform in client.depaginate(client.transforms, "transforms", page_size=100):
        if include_managed or not transform.get("_meta", {}).get("managed"):
            for key in ["authorization", "version", "create_time"]:
                if key in transform:
                    transform.pop(key)
            file_path = transforms_directory / (transform.pop("id") + ".json")
            with file_path.open("w") as file:
                file.write(json.dumps(transform, indent=4, sort_keys=True))

    # we also need to know whether a transform was started at the time it was dumped
    stats = {}
    for transform in client.depaginate(
        client.transform_stats, "transforms", page_size=100
    ):
        stats[transform["id"]] = transform
    transform_stats_file = transforms_directory / "_stats.json"
    with transform_stats_file.open("w") as file:
        file.write(json.dumps(stats, indent=4, sort_keys=True))


def dump_agent_policies(
    client: KibanaClient,
    output_directory: Path = Path("./export"),
    include_managed: bool = False,
):
    agent_policies_directory = output_directory / "agent_policies"
    agent_policies_directory.mkdir(exist_ok=True)
    for policy in client.depaginate(client.agent_policies):
        if include_managed or not policy["is_managed"]:
            filename = slugify(policy["name"]) + ".json"
            file_path = agent_policies_directory / filename
            with file_path.open("w") as file:
                file.write(json.dumps(policy, indent=4))


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


def load_enrich_policies(
    client: ElasticsearchClient,
    data_directory: Path = Path("./export"),
    allow_failure: bool = False,
    delete_after_import: bool = False,
):
    enrich_policies_directory = data_directory / "enrich_policies"
    if enrich_policies_directory.is_dir():
        for policy_file in enrich_policies_directory.glob("*.json"):
            with policy_file.open("r") as fh:
                policy = json.load(fh)
            policy_name = policy_file.stem
            client.create_enrich_policy(policy_name, policy)


def load_transforms(
    client: ElasticsearchClient,
    data_directory: Path = Path("./export"),
    delete_after_import: bool = False,
    allow_failure: bool = False,
):
    transforms_directory = data_directory / "transforms"
    if transforms_directory.is_dir():
        # create a map of all transforms by id
        transforms_generator = client.depaginate(
            client.transforms, "transforms", page_size=100
        )
        transforms_map = {
            transform["id"]: transform for transform in transforms_generator
        }

        stats_file = transforms_directory / "_stats.json"
        with stats_file.open("r") as fh:
            reference_stats = json.load(fh)

        current_stats = {
            t["id"]: t
            for t in client.depaginate(
                client.transform_stats, "transforms", page_size=100
            )
        }

        for transform_file in transforms_directory.glob("*.json"):
            if transform_file == stats_file:
                continue
            logger.debug("Loading {}".format(transform_file))
            transform_id = transform_file.stem
            with transform_file.open("r") as fh:
                loaded_transform = json.load(fh)
            if transform_id in transforms_map:
                logger.info("Transform {} already exists.".format(transform_id))
                # the transform already exists; if it's changed we need to delete and recreate it
                existing_transform = transforms_map[transform_id]
                for key, loaded_value in loaded_transform.items():
                    if loaded_value != existing_transform[key]:
                        logger.info(
                            "Transform {} differs by key {}, deleting and recreating.".format(
                                transform_id, key
                            )
                        )
                        client.stop_transform(transform_id, wait_for_completion=True)
                        client.delete_transform(transform_id)
                        client.create_transform(
                            transform_id, loaded_transform, defer_validation=True
                        )
                        break
            else:
                logger.info(
                    "Creating new transform with id {}".format(transform_id, key)
                )
                client.create_transform(
                    transform_id, loaded_transform, defer_validation=True
                )
            # client.set_transform_state(transform_id, target_state=reference_stats[transform_id]["state"])


def load_package_policies(
    client: KibanaClient,
    data_directory: Path = Path("./export"),
    delete_after_import: bool = False,
    allow_failure: bool = False,
):
    package_policies_directory = data_directory / "package_policies"
    for policy_file in package_policies_directory.glob("*.json"):
        with policy_file.open("r") as fh:
            policy = json.load(fh)
        policy_id = policy["id"]
        client.create_package_policy(id=policy_id, policy=policy)


def load_saved_objects(
    client: KibanaClient,
    data_directory: Path = Path("./export"),
    intermediate_file_max_size: float = 5e8,  # 500 MB
    overwrite: bool = True,
    delete_after_import: bool = False,
    allow_failure: bool = False,
):
    so_file = data_directory / "saved_objects.ndjson"
    so_dir = data_directory / "saved_objects"

    # We could just iterate over all the files and POST them all individually,
    # but that'd be awful slow so we can instead send them all as one batch
    # by first concatenating them into this temporary file-like object in memory.
    with tempfile.SpooledTemporaryFile(
        mode="ab+", max_size=intermediate_file_max_size
    ) as intermediate_file:
        if so_file.exists():
            assert so_file.is_file()
            with so_file.open("rb") as fh:
                intermediate_file.write(
                    fh.read()
                )  # This assumes that the size of the saved objects file is not too large to hold in memory.
                intermediate_file.write(b"\n")
        if so_dir.exists():
            assert so_dir.is_dir()
            for object_file in so_dir.glob("*/*.json"):
                with object_file.open("rb") as fh:
                    # kibana doesn't like the pretty-printing,
                    # so we have to flatten it down one line each.
                    object = json.load(fh)
                    object_string = json.dumps(object)
                    intermediate_file.write(str.encode(object_string))
                    intermediate_file.write(b"\n")
        # jump back to the start of the file
        intermediate_file.seek(0)
        client.import_saved_objects(
            intermediate_file, overwrite=overwrite, create_new_copies=(not overwrite)
        )
