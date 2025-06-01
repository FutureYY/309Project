from kedro.pipeline import Pipeline, node, pipeline

from .nodes import generate_targets, merge_dataset, feature_engineering

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=generate_targets,
                #inputs=
                #outputs=
                name="generate_targets_node",
            ),
            node(
                func=merge_dataset,
                #inputs=
                #outputs= 
                name="merge_dataset_node",
                
            ),
            node(
                func=feature_engineering,
                #inputs=
                #outputs= 
                name="feature_engineering_node",
            ),
        ]
    )
    
    
    
    