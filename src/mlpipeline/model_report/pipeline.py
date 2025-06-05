from kedro.pipeline import Pipeline, node, pipeline

from .nodes import confusion_matrix_rfc, confusion_matrix_blr

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=confusion_matrix_rfc,
                inputs=["y_test", "y_pred_rfc"],
                outputs=["confusion_matrix_rfc_viz"],
                name="confusion_matrix_rfc_node",
            ),
            
            node(
                func=confusion_matrix_blr,
                inputs=["y_test", "y_pred_blr"],
                outputs=["confusion_matrix_blr_viz"], 
                name="confusion_matrix_blr_node", 
            ), 
        ]
    )

