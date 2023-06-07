import logging

import httpx
from pathlib import Path

from .generic import GenericElasticsearchController
from .pipelines import PipelineController
from .transforms import TransformController
from .watches import WatchController
from .enrich_policies import EnrichPolicyController

logger = logging.getLogger("elastic_stacker")


class ElasticsearchController(GenericElasticsearchController):
    def __init__(
        self,
        client: httpx.Client,
        data_directory: Path,
    ):
        super().__init__(client, data_directory)
        self.pipelines = PipelineController(client)
        self.transforms = TransformController(client)
        self.watches = WatchController(client)
        self.enrich_policies = EnrichPolicyController(client)
