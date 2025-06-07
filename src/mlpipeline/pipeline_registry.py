from __future__ import annotations
from kedro.pipeline import Pipeline
from mlpipeline import dataprep, model, model_report

#defining the pathway to run the pipeline in a order, dataprep -> model -> evaluation. 
def register_pipelines() -> dict[str, Pipeline]:
    # Create each modular pipeline
    dataprep_pipeline = dataprep.create_pipeline()
    model_pipeline = model.create_pipeline()
    evaluation_pipeline = model_report.create_pipeline()

    # Combine them in the correct logical order
    default_pipeline = dataprep_pipeline + model_pipeline + evaluation_pipeline

    return {
        "dataprep": dataprep_pipeline,
        "model": model_pipeline,
        "evaluation": evaluation_pipeline,
        "__default__": default_pipeline,
    }