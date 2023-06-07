import logging

import httpx

from .generic import GenericKibanaController

logger = logging.getLogger("elastic_stacker")


class KibanaController(GenericKibanaController):
    def __init__(self, client: httpx.Client):
        super().__init__(client)
