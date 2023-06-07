import logging

import httpx

from .generic import GenericElasticsearchController
from .pipelines import PipelineController
from .transforms import TransformController
from .watches import WatchController

logger = logging.getLogger("elastic_stacker")


class ElasticsearchController(GenericElasticsearchController):
    def __init__(self, client: httpx.Client):
        super().__init__(client)
        self.pipelines = PipelineController(client)
        self.transforms = TransformController(client)
        self.watches = WatchController(client)
