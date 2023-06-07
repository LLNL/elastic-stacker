import json
import logging
from pathlib import Path

from api_clients import KibanaClient, ElasticsearchClient

logger = logging.getLogger("elastic_stacker")


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
