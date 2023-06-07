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
