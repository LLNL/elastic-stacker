#! python3.9

import os
import sys
import logging
import pathlib

import click
import tomli

from schemas import ConfigSchema, StackSchema
from api_clients import KibanaClient, ElasticsearchClient
from exporters import (
    dump_saved_objects, 
    dump_watches, 
    dump_transforms,
    dump_pipelines,
    dump_package_policies,
    dump_agent_policies,
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
    "-s",
    "--stack-name",
    type=str,
    help="The name of the Kibana server to use, as configured in the main config file. Defaults to the [defaults.client] table.",
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
def cli(ctx, config, stack_name, quiet, verbose):
    ctx.obj = config
    logger.debug(dict(ctx.obj))
    if not stack_name:
        stack = StackSchema().load(dict(ctx.obj.default.stack))
        logging.warning(
            "No stack name specified; using stack defaults from [default.client] table"
        )
    elif ctx.obj.stack[stack_name]:
        stack = ctx.obj.stack[stack_name]
    else:
        raise click.BadParameter(
            "stack '{}' not defined in configuration.".format(
                stack_name
            )
        )
    # test client connections
    # stack.kibana.health()
    stack.kibana.status()
    ctx.obj.stack = stack


@cli.group("export")
@click.pass_context
@click.option(
    "-o",
    "--output",
    type=click.Path(file_okay=False, writable=True, path_type=pathlib.Path),
    help="The directory where the exported files should be written.",
    default=pathlib.Path("./export"),
)
def export_group(ctx: click.Context, output: pathlib.Path):
    output = output.expanduser()
    if output.exists():
        logger.warning("Output directory {} already exists.".format(output))
        if len(list(output.iterdir())):
            logger.warning(
                "Output directory {} already contains data, this will overwrite it.".format(
                    output.absolute()
                )
            )
    else:  # no need to catch is_file -- Click will catch that before it gets here
        output.mkdir()

    ctx.obj.output = output


@export_group.command("all")
@click.pass_obj
def export_all_command(obj):
    dump_saved_objects(obj.stack.kibana, output_directory=obj.output)
    dump_watches(obj.stack.elasticsearch, output_directory=obj.output)
    dump_transforms(obj.stack.elasticsearch, output_directory=obj.output)
    dump_pipelines(obj.stack.elasticsearch, output_directory=obj.output)
    dump_package_policies(obj.stack.kibana, output_directory=obj.output)
    dump_agent_policies(obj.stack.kibana, output_directory=obj.output)


@export_group.command("saved-objects")
@click.pass_obj
@click.argument("types", type=str, nargs=-1)
def export_saved_objects_command(obj, types):
    # click.argument(nargs=-1) returns a tuple when no arguments are specified,
    # but I want export() to use None to indicate that it should export everything.
    if types == ():
        logger.warning("No types specified, passing None and exporting all types")
        types = None

    dump_saved_objects(obj.stack.kibana, types=types, output_directory=obj.output)


@export_group.command("watches")
@click.pass_obj
def export_watches_command(obj):
    dump_watches(obj.stack.elasticsearch, output_directory=obj.output)

@export_group.command("transforms")
@click.pass_obj
def export_transforms_command(obj):
    dump_transforms(obj.stack.elasticsearch, output_directory=obj.output)

@export_group.command("pipelines")
@click.pass_obj
def export_pipelines_command(obj):
    dump_pipelines(obj.stack.elasticsearch, output_directory=obj.output)

@export_group.command("package-policies")
@click.pass_obj
def export_package_policies_command(obj):
    dump_package_policies(obj.stack.kibana, output_directory=obj.output)

@export_group.command("agent-policies")
@click.pass_obj
def export_package_policies_command(obj):
    dump_agent_policies(obj.stack.kibana, output_directory=obj.output)


if __name__ == "__main__":
    cli()
