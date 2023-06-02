#! python3.9

import os
import sys
import logging
import pathlib

import click
import tomli

from schemas import ConfigSchema, ProfileSchema
from api_clients import KibanaClient, ElasticsearchClient
from exporters import (
    dump_saved_objects,
    dump_watches,
    dump_transforms,
    dump_pipelines,
    dump_package_policies,
    dump_agent_policies,
    dump_enrich_policies,
    load_enrich_policies,
    load_saved_objects,
    load_pipelines,
    load_transforms,
)

CONFIG_FILE_PRECEDENCE = [
    "./stacker.toml",
    "~/.stacker.toml",
    "~/.config/stacker.toml",
]

# setup logging globally before we start
log_handler = logging.StreamHandler(sys.stderr)
log_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] : %(message)s"))

logger = logging.getLogger("elastic_stacker")
logger.setLevel(logging.WARN)
logger.addHandler(log_handler)


def set_loglevel(ctx, param, value: int):
    if value:
        if param.name == "verbose":
            value = 0 - value
        # else, param is "quiet"
        adjustment = 10 * value
        adjusted_value = logger.getEffectiveLevel() + adjustment
        adjusted_value = max(adjusted_value, logging.DEBUG)
        adjusted_value = min(adjusted_value, logging.CRITICAL)
        logger.setLevel(adjusted_value)
        logger.info(
            "--{param_name}: setting loglevel to {level_name}".format(
                param_name=param.name, level_name=logging.getLevelName(adjusted_value)
            )
        )
    return value


def load_config(ctx, param, config_file: pathlib.Path):
    for config_path in [config_file] + CONFIG_FILE_PRECEDENCE:
        config_file = pathlib.Path(config_path).expanduser().absolute()
        logger.debug("Looking for configuration in {}".format(config_file))
        if config_file.is_file():
            logger.debug("Found configuration in {}".format(config_file))
            break

    if not config_file.is_file:
        err = """
            Failed to find a config file from:
            --config argument
            STACKER_CONFIG environment variable
            {}
        """.format(
            "\n".join(CONFIG_FILE_PRECEDENCE)
        )
        logger.critical(err)
        sys.exit(1)

    logger.info(
        "Loading configuration from {config}".format(config=config_file.absolute())
    )

    with config_file.open("rb") as fh:
        config_raw = tomli.load(fh)
    config = ConfigSchema().load(config_raw)
    return config


@click.group()
@click.option(
    "-c",
    "--config",
    type=click.Path(dir_okay=False, readable=True, path_type=pathlib.Path),
    help="The path to the configuration file to use.",
    callback=load_config,
    default=os.getenv("STACKER_CONFIG", CONFIG_FILE_PRECEDENCE[0]),
)
@click.option(
    "-p",
    "--profile-name",
    type=str,
    help="The name of the configuration profile to use, as configured in the main config file. Defaults to the [default.client] table.",
)
@click.option(
    "-q",
    "--quiet",
    count=True,
    help="quietness (add another q to get less information)",
    callback=set_loglevel,
)
@click.option(
    "-v",
    "--verbose",
    count=True,
    help="verbosity (add another v to get more information)",
    callback=set_loglevel,
)
@click.pass_context
def cli(ctx, config, profile_name, quiet, verbose):
    ctx.obj = config
    logger.debug(dict(ctx.obj))
    if not profile_name:
        profile = ProfileSchema().load(dict(ctx.obj.default.profile))
        logging.warning(
            "No profile name specified; using defaults from [default.profile] table"
        )
    elif ctx.obj.profile[profile_name]:
        profile = ctx.obj.profile[profile_name]
    else:
        raise click.BadParameter(
            "profile '{}' not defined in configuration.".format(profile_name)
        )

    ctx.obj = profile


@cli.group("export")
@click.pass_context
@click.option(
    "-d",
    "--data_directory",
    type=click.Path(file_okay=False, writable=True, path_type=pathlib.Path),
    help="The directory where the exported files should be written.",
    default=pathlib.Path("./export"),
)
def export_group(ctx: click.Context, data_directory: pathlib.Path):
    data_directory = data_directory.expanduser()
    if data_directory.exists():
        logger.warning("Data directory {} already exists.".format(data_directory))
        if len(list(data_directory.iterdir())):
            logger.warning(
                "Output directory {} already contains data, this will overwrite it.".format(
                    data_directory.absolute()
                )
            )
    else:  # no need to catch is_file -- Click will catch that before it gets here
        data_directory.mkdir()

    ctx.obj.data_directory = data_directory


@export_group.command("all")
@click.pass_obj
def export_all_command(obj):
    dump_saved_objects(obj.kibana, output_directory=obj.data_directory)
    dump_watches(obj.elasticsearch, output_directory=obj.data_directory)
    dump_transforms(obj.elasticsearch, output_directory=obj.data_directory)
    dump_pipelines(obj.elasticsearch, output_directory=obj.data_directory)
    dump_package_policies(obj.kibana, output_directory=obj.data_directory)
    dump_agent_policies(obj.kibana, output_directory=obj.data_directory)


@export_group.command("saved-objects")
@click.pass_obj
@click.argument("types", type=str, nargs=-1)
def export_saved_objects_command(obj, types):
    # click.argument(nargs=-1) returns a tuple when no arguments are specified,
    # but I want export() to use None to indicate that it should export everything.
    if types == ():
        logger.warning("No types specified, passing None and exporting all types")
        types = None

    dump_saved_objects(obj.kibana, types=types, output_directory=obj.data_directory)


@export_group.command("watches")
@click.pass_obj
def export_watches_command(obj):
    dump_watches(obj.elasticsearch, output_directory=obj.data_directory)


@export_group.command("transforms")
@click.option("--include-managed", is_flag=True)
@click.pass_obj
def export_transforms_command(obj, include_managed):
    dump_transforms(obj.elasticsearch, output_directory=obj.data_directory, include_managed=include_managed)


@export_group.command("pipelines")
@click.pass_obj
@click.option("--include-managed", is_flag=True)
def export_pipelines_command(obj, include_managed):
    dump_pipelines(obj.elasticsearch, output_directory=obj.data_directory, include_managed=include_managed)


@export_group.command("package-policies")
@click.pass_obj
def export_package_policies_command(obj):
    dump_package_policies(obj.kibana, output_directory=obj.data_directory)


@export_group.command("agent-policies")
@click.pass_obj
@click.option("--include-managed", is_flag=True)
def export_package_policies_command(obj, include_managed):
    dump_agent_policies(obj.kibana, output_directory=obj.data_directory, include_managed=include_managed)

@export_group.command("enrich-policies")
@click.pass_obj
def export_enrich_policies_command(obj):
    dump_enrich_policies(obj.elasticsearch, output_directory=obj.data_directory)


@cli.group("import")
@click.pass_context
@click.option(
    "-d",
    "--data_directory",
    type=click.Path(file_okay=False, exists=True, readable=True, path_type=pathlib.Path),
    help="The directory where the imported files should be read from.",
    default=pathlib.Path("./export"),
)
def import_group(ctx: click.Context, data_directory: pathlib.Path):
    ctx.obj.data_directory = data_directory.expanduser()

@import_group.command("all")
@click.pass_obj
def import_all_command(obj):
    load_saved_objects(obj.kibana, data_directory=obj.data_directory)
    load_pipelines(obj.transforms, data_directory=obj.data_directory)
    load_transforms(obj.transforms, data_directory=obj.data_directory)

@import_group.command("saved-objects")
# TODO add --overwrite flag
@click.pass_obj
def import_saved_objects_command(obj):
    load_saved_objects(obj.kibana, data_directory=obj.data_directory)

@import_group.command("pipelines")
@click.pass_obj
def import_pipelines_command(obj):
    load_pipelines(obj.elasticsearch, data_directory=obj.data_directory)

@import_group.command("enrich-policies")
@click.pass_obj
def import_enrich_policies_command(obj):
    load_enrich_policies(obj.elasticsearch, data_directory=obj.data_directory)

@import_group.command("transforms")
@click.pass_obj
def import_transforms_command(obj):
    load_transforms(obj.elasticsearch, data_directory=obj.data_directory)


if __name__ == "__main__":
    cli()
