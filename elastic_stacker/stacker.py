# stdlib
import logging
import os
import shutil
import tempfile
from importlib.metadata import version

# local project
from .elasticsearch.indices import IndexController
from .elasticsearch.index_templates import IndexTemplateController
from .elasticsearch.component_templates import ComponentTemplateController
from .elasticsearch.pipelines import PipelineController
from .elasticsearch.transforms import TransformController
from .elasticsearch.watches import WatchController
from .elasticsearch.enrich_policies import EnrichPolicyController
from .elasticsearch.roles import RoleController
from .elasticsearch.role_mappings import RoleMappingController
from .kibana.saved_objects import SavedObjectController
from .kibana.agent_policies import AgentPolicyController
from .kibana.package_policies import PackagePolicyController

from .utils.config import load_config, find_config, make_profile
from .utils.client import APIClient
from .utils.logging import configure_logger
from .utils.controller import PURGE_PROMPT

logger = logging.getLogger("elastic_stacker")


class Stacker(object):
    """
    Stacker is a tool for moving Elasticsearch and Kibana configuration
    objects across multiple instances of these services.
    You can run this command with no arguments to see a list of subcommands.
    """

    version: str = version("elastic-stacker")
    profile: dict
    package_policies: PackagePolicyController
    agent_policies: AgentPolicyController
    indices: IndexController
    index_templates: IndexTemplateController
    component_templates: ComponentTemplateController
    saved_objects: SavedObjectController
    pipelines: PipelineController
    transforms: TransformController
    watches: WatchController
    enrich_policies: EnrichPolicyController
    roles: RoleController
    role_mappings: RoleMappingController

    def __init__(
        self,
        config: os.PathLike = None,
        profile: str = None,
        elasticsearch: str = None,
        kibana: str = None,
        ca: os.PathLike = None,
        timeout: float = None,
        log_level: str = None,
        ecs_log: bool = None,
    ):
        global_config = load_config(config)  # if None, checks list of default locations

        overrides = {
            # TODO: add CLI arguments here to override configuration values, for example
            "elasticsearch": {"base_url": elasticsearch},
            "kibana": {"base_url": kibana},
            "client": {"verify": ca, "timeout": timeout},
            "log": {"level": log_level, "ecs": ecs_log},
        }

        self.profile = make_profile(
            global_config, profile_name=profile, overrides=overrides
        )

        configure_logger(**self.profile["log"])
        logger.info("Configuration was read from %s", config or find_config())

        # https://www.elastic.co/guide/en/kibana/master/api.html#api-request-headers
        self.profile["kibana"]["headers"] = self.profile["kibana"].get(
            "headers", {}
        ) | {"kbn-xsrf": "true"}
        self._options = self.profile["options"]

        subs = self.profile.get("substitutions", {})

        kibana_client = APIClient(**self.profile["kibana"])
        elasticsearch_client = APIClient(**self.profile["elasticsearch"])

        self.package_policies = PackagePolicyController(
            kibana_client, subs=subs, **self._options
        )
        self.agent_policies = AgentPolicyController(
            kibana_client, subs=subs, **self._options
        )
        self.indices = IndexController(elasticsearch_client, subs=subs, **self._options)
        self.index_templates = IndexTemplateController(
            elasticsearch_client, subs=subs, **self._options
        )
        self.component_templates = ComponentTemplateController(
            elasticsearch_client, subs=subs, **self._options
        )
        self.roles = RoleController(elasticsearch_client, subs=subs, **self._options)
        self.role_mappings = RoleMappingController(
            elasticsearch_client, subs=subs, **self._options
        )
        self.saved_objects = SavedObjectController(
            kibana_client, subs=subs, **self._options
        )
        self.pipelines = PipelineController(
            elasticsearch_client, subs=subs, **self._options
        )
        self.transforms = TransformController(
            elasticsearch_client, subs=subs, **self._options
        )
        self.watches = WatchController(elasticsearch_client, subs=subs, **self._options)
        self.enrich_policies = EnrichPolicyController(
            elasticsearch_client, subs=subs, **self._options
        )
        self._controllers = {
            "indices": self.indices,
            "component_templates": self.component_templates,
            "index_templates": self.index_templates,
            "saved_objects": self.saved_objects,
            "watches": self.watches,
            "roles": self.roles,
            "role_mappings": self.role_mappings,
            "pipelines": self.pipelines,
            "transforms": self.transforms,
            "enrich_policies": self.enrich_policies,
        }
        self._experimental_controllers = {
            "package_policies": self.package_policies,
            "agent_policies": self.agent_policies,
        }

    def system_dump(
        self,
        *types: str,
        include_managed: bool = False,
        include_experimental: bool = False,
        purge: bool = False,
        force_purge: bool = False,
        data_directory: os.PathLike = None,
    ):
        """
        Dumps all supported resource types, or the ones specified with the
        `types` argument, out to their own files.
        In general, this accepts all the same arguments as any of the
        individual resources' dump() methods.
        The `include_experimental` argument is also included to allow you to
        dump resources for which the loader may not yet be written.
        """
        valid_controllers = self._controllers
        if include_experimental:
            logger.warning(
                "Including experimental objects which do not have a load function yet."
            )
            valid_controllers |= self._experimental_controllers

        invalid_types = set(types) - set(valid_controllers.keys())
        if invalid_types:
            raise Exception("types {} are invalid".format(invalid_types))
        if len(types) == 0:
            types = valid_controllers.keys()

        dump_arguments = {
            "include_managed": include_managed,
            "data_directory": data_directory,
            "purge": False,
        }

        untouched_files = set()
        for type_name in types:
            logger.info("exporting {}".format(type_name))
            controller = valid_controllers[type_name]
            controller.dump(**dump_arguments)
            untouched_files |= controller._untouched_files(relative=True)

        if untouched_files and purge or force_purge:
            purge_list = "\n".join(sorted(map(str, untouched_files)))
            confirmation_message = PURGE_PROMPT.format(
                count=len(untouched_files), purge_list=purge_list
            )
            confirmed = force_purge or input(confirmation_message).lower() in {
                "y",
                "yes",
            }
            if confirmed:
                for type_name in types:
                    controller = valid_controllers[type_name]
                    controller._purge_untouched_files(force=True)

    def system_load(
        self,
        *types,
        temp: bool = False,
        data_directory: os.PathLike = None,
        delete_after_import: bool = False,
        retries: int = 0,
        allow_failure: bool = False,
        stubborn: bool = False,
    ):
        """
        Load all resources from a previous dump into the specified Elastic
        Stack.
        Includes a couple arguments not supported by the subordinate resources.
        `retries` sets the number of times to attempt to import all the
        specified resources; this can be useful to resolve weird dependency
        loops.
        `delete` deletes each resource file from the filesystem after it was
        successfully imported -- this can make loads with a high number of
        retries much faster.
        `temp` makes a temporary copy of the dump and operates on that
        instead of the specified data directory; this is useful when `delete`
        is set.
        `stubborn` is equivalent to `--delete --temp --retries=5`, but the
        value for retries will be used instead if specified.
        """
        if stubborn:
            delete_after_import = True
            temp = True
            allow_failure = True
            if not retries:
                retries = 5

        invalid_types = set(types) - set(self._controllers.keys())
        if invalid_types:
            raise Exception("types {} are invalid".format(invalid_types))
        if len(types) == 0:
            types = self._controllers.keys()

        if data_directory is None:
            data_directory = self._options["data_directory"]

        if temp:
            working_data_directory = tempfile.mkdtemp(prefix="stacker_data_")
            shutil.copytree(data_directory, working_data_directory, dirs_exist_ok=True)
        else:
            working_data_directory = data_directory

        load_arguments = {
            "allow_failure": allow_failure,
            "data_directory": working_data_directory,
            "delete_after_import": delete_after_import,
        }
        for _ in range(retries + 1):
            logger.info("beginning attempt no.")
            for type_name in types:
                logger.warning("importing {}".format(type_name))
                controller = self._controllers[type_name]
                controller.load(**load_arguments)
