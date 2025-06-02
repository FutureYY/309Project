from kedro.framework.hooks import hook_impl
from kedro.pipeline.node import Node
from typing import Any
from kedro.pipeline import Pipeline, pipeline
from conf.base import Catalog

class pipeline_hooks:
    def after_pipeline_run(
        self, run_params: dict[str, Any], run_result: dict[str, Any],
            pipeline: Pipeline, catalog: Catalog,) -> None:
        
        pass
    
    
    
    