import logging

import httpx

from .generic import GenericElasticsearchController
from .pipelines import PipelineController

logger = logging.getLogger("elastic_stacker")


class ElasticsearchController(GenericElasticsearchController):
    def __init__(self, client: httpx.Client):
        super().__init__(client)
        self.pipelines = PipelineController(client)
