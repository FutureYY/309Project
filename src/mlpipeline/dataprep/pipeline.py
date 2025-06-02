from kedro.pipeline import Pipeline, node, pipeline

from .nodes import target_dataset_A, target_dataset_B

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=target_dataset_A,
                inputs=["processed_data"],
                outputs=["processed_data_A"],
                name="target_dataset_A_node",
            ),
            
            node(
                func=target_dataset_B,
                inputs=["processed_data"],
                outputs= ["processed_data_B"],
                name="target_dataset_B_node",
            ),
        ]
    )
    
    
    
    