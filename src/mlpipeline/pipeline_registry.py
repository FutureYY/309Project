from __future__ import annotations

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline

def register_pipelines() -> dict[str, Pipeline]:
    #Register the project's data processing pipelines.
    pipelines = find_pipelines()
    pipelines["__default__"] = sum(pipelines.values())
    return pipelines
