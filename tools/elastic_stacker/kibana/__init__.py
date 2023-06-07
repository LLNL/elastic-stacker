import logging

import httpx

from .generic import GenericKibanaController
from .saved_objects import SavedObjectController
from .agent_policies import AgentPolicyController
from .package_policies import PackagePolicyController

logger = logging.getLogger("elastic_stacker")


class KibanaController(GenericKibanaController):
    def __init__(self, client: httpx.Client):
        super().__init__(client)
        self.saved_objects = SavedObjectController(client=client)
        self.agent_policies = AgentPolicyController(client=client)
        self.package_policies = PackagePolicyController(client=client)
