import logging

import httpx

from .generic import GenericKibanaController
from .saved_objects import SavedObjectController

logger = logging.getLogger("elastic_stacker")


class KibanaController(GenericKibanaController):
    def __init__(self, client: httpx.Client):
        super().__init__(client)
        self.saved_objects = SavedObjectController(client=client)
